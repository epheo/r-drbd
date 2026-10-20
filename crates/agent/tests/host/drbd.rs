//! DRBD on the single test host. Resources, LVs and files are named rw-*,
//! minors 99990 to 99999, ports 11990 to 11999; resource files go to
//! /var/lib/r-drbd. The replicas of one volume run as separate resources,
//! each its own node on a loopback address, so they connect for real.
//! One kernel refuses two paths to the same remote address and port, so
//! a member that two others would connect to is hidden from one of them.

// Members are a, b and c, as the resources are named.
#![allow(clippy::many_single_char_names)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent::drbd::{self, Notify};
use agent::events2::Tracker;
use agent::reconcile::{DrbdEnv, Reconciler, StatusWrite};
use agent::watch::Trigger;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use model::{
    AssignedReplica, Assignment, Pool, ReplicaStatus, Role, Snapshot, SnapshotPhase, SnapshotSpec,
    SnapshotStatus, Volume, VolumeSource, VolumeSpec,
};

use super::{GIB, VG, clean, enabled, hostname, lv_names, lvm, sh};

const RES_DIR: &str = "/var/lib/r-drbd";
const SECRET: &str = "dGVzdHNlY3JldHRlc3RzZWNyZXR0ZXN0c2VjcmV0MTI=";
const MIB: u64 = 1 << 20;
/// The VG's extent, the most the backing size may round up by.
const EXTENT: u64 = 4 * MIB;

fn now() -> Time {
    Time(k8s_openapi::jiff::Timestamp::now())
}

struct Host {
    node: String,
    exec: Arc<dyn agent::Exec>,
    features: drbd_config::Features,
    tracker: Arc<Mutex<Tracker>>,
    events: tokio::task::JoinHandle<()>,
}

impl Drop for Host {
    fn drop(&mut self) {
        self.events.abort();
    }
}

async fn start() -> Host {
    let exec: Arc<dyn agent::Exec> = Arc::new(super::exec());
    let tools = agent::node::probe(exec.as_ref()).await;
    let features = tools.drbd().expect("DRBD tools and module on the host");
    println!("features: {features:?}");
    let tracker = Arc::new(Mutex::new(Tracker::default()));
    let events = tokio::spawn(drbd::events2(
        None,
        tracker.clone(),
        Notify {
            changes: Arc::new(Trigger::default()),
            pass: Arc::new(Trigger::default()),
        },
    ));
    let host = Host {
        node: hostname(),
        exec,
        features,
        tracker,
        events,
    };
    wait(&host.tracker, "initial dump", Tracker::is_available).await;
    host
}

async fn wait<F: Fn(&Tracker) -> bool>(tracker: &Mutex<Tracker>, what: &str, f: F) {
    for _ in 0..200 {
        if f(&tracker.lock().unwrap()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for events2: {what}");
}

/// Every name these tests use: a leftover of any of them can hold an
/// address another test needs.
const EVERY: &[&str] = &["rw-a", "rw-b", "rw-c", "rw-d", "rw-e", "rw-src"];

/// Leftovers of an earlier failed run: every rw-* resource, LV and
/// resource file, `names` among them.
async fn drbd_clean(names: &[&str]) {
    super::clean_rw(&lvm()).await;
    for e in std::fs::read_dir(RES_DIR).into_iter().flatten().flatten() {
        let file = e.file_name();
        let file = file.to_string_lossy();
        if file.starts_with("rw-") || file.starts_with(".rw-") {
            let _ = std::fs::remove_file(e.path());
        }
    }
    for n in names {
        let _ = sh(&["drbdadm", "resume-io", n]).await;
        let _ = sh(&["drbdsetup", "down", n]).await;
        let _ = std::fs::remove_file(format!("{RES_DIR}/{n}.res"));
        let _ = std::fs::remove_file(format!("{RES_DIR}/.{n}.candidate"));
        clean(&lvm(), n).await;
    }
}

/// The test Pool on `node`, with the chunk size the agent's pool pass reads
/// from the thin pool: without it the resource files lack
/// rs-discard-granularity and a resync sends zero blocks as data.
async fn pool(node: &str) -> Arc<Pool> {
    let mut p = (*super::pool()).clone();
    node.clone_into(&mut p.spec.node);
    let rows = lvm().lvs(VG).await.unwrap();
    let cap = agent::storage::pool_capacity(&rows, super::THIN).unwrap();
    p.status = Some(model::PoolStatus {
        chunk_bytes: Some(cap.chunk_bytes),
        ..model::PoolStatus::default()
    });
    Arc::new(p)
}

fn disk(t: &Tracker, res: &str) -> Option<String> {
    t.resource(res)?.devices.get(&0)?.disk.clone()
}

fn connected(t: &Tracker, res: &str) -> bool {
    t.resource(res).is_some_and(|r| {
        !r.connections.is_empty() && r.connections.values().all(|c| c.state == "Connected")
    })
}

fn sectors(minor: u32) -> u64 {
    std::fs::read_to_string(format!("/sys/block/drbd{minor}/size"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// What a sync must carry: zero blocks travel as discards, so only written
/// data proves one.
const PAYLOAD: u64 = 64 * MIB;

/// Writes PAYLOAD of random data at the start of `res` through its device,
/// Primary for the write only; returns its sha256 line.
async fn write_payload(res: &str, minor: u32) -> String {
    assert_eq!(sh(&["drbdsetup", "primary", res]).await.exit_code, Some(0));
    let out = sh(&[
        "sh",
        "-c",
        &format!(
            "dd if=/dev/urandom of=/dev/drbd{minor} bs=1M count={} oflag=direct status=none && dd if=/dev/drbd{minor} bs=1M count={} iflag=direct status=none | sha256sum",
            PAYLOAD / MIB,
            PAYLOAD / MIB
        ),
    ])
    .await;
    assert_eq!(
        sh(&["drbdsetup", "secondary", res]).await.exit_code,
        Some(0)
    );
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    out.stdout
}

/// The sha256 line of PAYLOAD at the start of a backing LV, read directly:
/// the data area starts at offset 0 under internal metadata.
async fn payload_on(lv: &str) -> String {
    sh(&[
        "sh",
        "-c",
        &format!(
            "dd if=/dev/{VG}/{lv} bs=1M count={} iflag=direct status=none | sha256sum",
            PAYLOAD / MIB
        ),
    ])
    .await
    .stdout
}

/// KiB received and sent by resync and replication on every peer device.
async fn traffic(res: &str) -> (u64, u64) {
    let out = sh(&["drbdsetup", "status", "--json", "--statistics", res]).await;
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    let v: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    let mut sum = (0, 0);
    for c in v[0]["connections"].as_array().into_iter().flatten() {
        for p in c["peer_devices"].as_array().into_iter().flatten() {
            let get = |k: &str| {
                p[k].as_u64()
                    .unwrap_or_else(|| panic!("no {k} in {}", out.stdout))
            };
            sum.0 += get("received");
            sum.1 += get("sent");
        }
    }
    sum
}

async fn dry_run(res: &str) -> String {
    let argv = drbd::argv::adjust_dry_run(res);
    let parts: Vec<&str> = argv.iter().map(String::as_str).collect();
    let out = sh(&parts).await;
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    out.stdout
}

/// One member of a volume: its own resource on 127.0.0.<index + 1>.
#[derive(Clone, Copy)]
struct Spec {
    res: &'static str,
    minor: u32,
    port: u16,
    role: Role,
}

/// The API server of a member: the Volume its pass was last given, so a
/// live read agrees with what the member saw.
#[derive(Debug, Default)]
struct MemberLive(Mutex<Option<Volume>>);

#[async_trait::async_trait]
impl agent::reconcile::LiveVolumes for MemberLive {
    async fn get(&self, _: &str) -> Result<Volume, String> {
        self.0
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "no pass yet".to_owned())
    }
}

struct Member {
    res: &'static str,
    minor: u32,
    rec: Reconciler,
    live: Arc<MemberLive>,
    vol: Volume,
    entry: Option<ReplicaStatus>,
}

impl Member {
    fn current(&self) -> Volume {
        let mut v = self.vol.clone();
        let st = v.status.get_or_insert_with(Default::default);
        st.replicas = self.entry.clone().into_iter().collect();
        v
    }

    async fn pass(&mut self, pools: &[Arc<Pool>], snaps: &[Arc<Snapshot>]) -> Vec<StatusWrite> {
        let v = self.current();
        *self.live.0.lock().unwrap() = Some(v.clone());
        let w = self.rec.pass(pools, &[Arc::new(v)], snaps).await;
        for x in &w {
            match x {
                StatusWrite::Volume { name, entry } if name == self.res => {
                    self.entry = Some(entry.clone());
                }
                StatusWrite::VolumeGone { name } if name == self.res => self.entry = None,
                _ => {}
            }
        }
        w
    }

    fn error(&self) -> Option<String> {
        self.entry.as_ref().and_then(|e| e.last_error.clone())
    }
}

fn ip(i: usize) -> std::net::IpAddr {
    format!("127.0.0.{}", i + 1).parse().unwrap()
}

/// The members of one volume of `size`, member 0 the initial node. Each
/// (viewer, member) pair in `hidden` has the viewer see that member at an
/// unused address of its own, so they never connect.
fn group(
    h: &Host,
    size: u64,
    specs: &[Spec],
    hidden: &[(usize, usize)],
    source: Option<&str>,
) -> Vec<Member> {
    let addr = |me: usize, j: usize| -> std::net::IpAddr {
        if hidden.contains(&(me, j)) {
            format!("127.0.0.{}", 10 + me).parse().unwrap()
        } else {
            ip(j)
        }
    };
    let name_of = |me: usize, j: usize| {
        if j == me {
            h.node.clone()
        } else {
            format!("m{j}")
        }
    };
    (0..specs.len())
        .map(|me| {
            let replicas = specs
                .iter()
                .enumerate()
                .map(|(j, s)| AssignedReplica {
                    node: name_of(me, j),
                    pool: (s.role == Role::Diskful).then(|| "test-host-pool".to_owned()),
                    node_id: u8::try_from(j).unwrap(),
                    port: Some(s.port),
                    role: s.role,
                })
                .collect();
            let s = specs[me];
            let mut vol = Volume::new(
                s.res,
                VolumeSpec {
                    size_bytes: size,
                    replicas: 2,
                    tier: "test".into(),
                    pool: None,
                    node: None,
                    attachments: vec![],
                    access_mode: model::AccessMode::default(),
                    evict: vec![],
                    source: source.map(|s| VolumeSource { snapshot: s.into() }),
                    resolve: None,
                    assignment: Some(Assignment {
                        replicas,
                        minor: Some(s.minor),
                        secret: Some(SECRET.into()),
                        initial_up_to_date_node: Some(name_of(me, 0)),
                    }),
                },
            );
            vol.metadata.generation = Some(1);
            vol.metadata.uid = Some(format!("uid-{}", s.res));
            let mut env = DrbdEnv::new(Some(h.features), RES_DIR, h.tracker.clone());
            env.init_wait = Duration::from_secs(3);
            let live = Arc::new(MemberLive::default());
            let mut rec = Reconciler::new(h.node.clone(), h.exec.clone())
                .with_drbd(env)
                .with_live(live.clone());
            rec.set_addresses(
                (0..specs.len())
                    .map(|j| (name_of(me, j), addr(me, j)))
                    .collect(),
            );
            Member {
                res: s.res,
                minor: s.minor,
                rec,
                live,
                vol,
                entry: None,
            }
        })
        .collect()
}

/// Passes on `members` until `done` holds.
async fn settle<F: Fn(&Tracker) -> bool>(
    h: &Host,
    members: &mut [&mut Member],
    what: &str,
    done: F,
) {
    let pools = [pool(&h.node).await];
    for _ in 0..120 {
        for m in members.iter_mut() {
            m.pass(&pools, &[]).await;
        }
        if done(&h.tracker.lock().unwrap()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let errors: Vec<_> = members.iter().map(|m| (m.res, m.error())).collect();
    let t = h.tracker.lock().unwrap();
    let state: Vec<_> = members
        .iter()
        .map(|m| (m.res, t.resource(m.res).cloned()))
        .collect();
    panic!("not settled: {what}\nerrors {errors:?}\nstate {state:?}");
}

async fn delete(h: &Host, m: &mut Member) {
    m.vol.metadata.deletion_timestamp = Some(now());
    m.pass(&[pool(&h.node).await], &[]).await;
    assert_eq!(m.error(), None, "{}", m.res);
}

const A: Spec = Spec {
    res: "rw-a",
    minor: 99990,
    port: 11990,
    role: Role::Diskful,
};
const B: Spec = Spec {
    res: "rw-b",
    minor: 99991,
    port: 11991,
    role: Role::Diskful,
};
const C: Spec = Spec {
    res: "rw-c",
    minor: 99992,
    port: 11992,
    role: Role::Tiebreaker,
};

/// Two diskful replicas: peer entries, sha256 authentication, a start
/// without sync, and the dry run on converged states.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_fresh_volume_starts_without_sync() {
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let h = start().await;
    let mut g = group(&h, GIB, &[A, B], &[], None);
    let [a, b] = &mut g[..] else { unreachable!() };
    settle(&h, &mut [a, b], "both UpToDate and connected", |t| {
        disk(t, "rw-a").as_deref() == Some("UpToDate")
            && disk(t, "rw-b").as_deref() == Some("UpToDate")
            && connected(t, "rw-a")
            && connected(t, "rw-b")
    })
    .await;
    for m in [&*a, &*b] {
        assert_eq!(m.error(), None, "{}", m.res);
    }
    println!(
        "--- rw-a.res\n{}",
        std::fs::read_to_string(format!("{RES_DIR}/rw-a.res")).unwrap()
    );
    let ta = traffic("rw-a").await;
    let tb = traffic("rw-b").await;
    println!("sync volume (received, sent) KiB: a {ta:?} b {tb:?}");
    assert_eq!((ta, tb), ((0, 0), (0, 0)), "no initial sync");
    for (m, slot) in [(&*a, 0), (&*b, 1)] {
        let tags = lvm().tags(VG).await.unwrap().remove(m.res).unwrap();
        assert_eq!(tags, [format!("r-drbd.md.uid-{}.{slot}", m.res)]);
    }

    for r in ["rw-a", "rw-b"] {
        let d = dry_run(r).await;
        println!("dry run, converged, {r}: {d:?}");
        assert_eq!(d, "");
    }
    for p in [&mut *a, &mut *b] {
        p.pass(&[pool(&h.node).await], &[]).await;
        assert_eq!(p.error(), None);
    }
    assert_eq!(
        sh(&["drbdadm", "suspend-io", "rw-a"]).await.exit_code,
        Some(0)
    );
    wait(&h.tracker, "suspended", |t| {
        t.resource("rw-a")
            .is_some_and(agent::events2::Resource::suspended_by_user)
    })
    .await;
    let d = dry_run("rw-a").await;
    println!("dry run, IO suspended by us: {d:?}");
    assert_eq!(d, "");
    assert_eq!(
        sh(&["drbdadm", "resume-io", "rw-a"]).await.exit_code,
        Some(0)
    );
    assert_eq!(sh(&["drbdsetup", "down", "rw-b"]).await.exit_code, Some(0));
    wait(&h.tracker, "b gone, a Connecting to it", |t| {
        t.resource("rw-b").is_none()
            && t.resource("rw-a")
                .and_then(|r| r.connections.get("m1"))
                .is_some_and(|c| c.state == "Connecting")
    })
    .await;
    let d = dry_run("rw-a").await;
    println!("dry run, peer down: {d:?}");
    assert_eq!(d, "");

    for m in [&mut *a, &mut *b] {
        delete(&h, m).await;
    }
    assert!(!lv_names(&lvm()).await.iter().any(|n| n.starts_with("rw-")));
}

/// A diskful replica with its diskless tiebreaker connected and the other
/// diskful replica away: the dry run plans nothing on either.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_tiebreaker_converged() {
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let h = start().await;
    let mut g = group(&h, GIB, &[A, B, C], &[(0, 1), (2, 1)], None);
    let mut it = g.iter_mut();
    let (a, _, c) = (it.next().unwrap(), it.next(), it.next().unwrap());
    settle(
        &h,
        &mut [&mut *a, &mut *c],
        "a attached, c connected to it",
        |t| {
            disk(t, "rw-a").is_some_and(|d| d == "Inconsistent" || d == "UpToDate")
                && disk(t, "rw-c").as_deref() == Some("Diskless")
                && t.resource("rw-c")
                    .and_then(|r| r.connections.get("m0"))
                    .is_some_and(|c| c.state == "Connected")
        },
    )
    .await;
    println!(
        "--- rw-c.res\n{}",
        std::fs::read_to_string(format!("{RES_DIR}/rw-c.res")).unwrap()
    );
    for r in ["rw-a", "rw-c"] {
        let d = dry_run(r).await;
        println!("dry run, tiebreaker present, peer away, {r}: {d:?}");
        assert_eq!(d, "");
    }
    let q = h
        .tracker
        .lock()
        .unwrap()
        .resource("rw-a")
        .and_then(|r| r.devices.get(&0)?.quorum);
    println!("a quorum with the tiebreaker, the other diskful never seen: {q:?}");
    for m in [a, c] {
        delete(&h, m).await;
    }
}

const E: Spec = Spec {
    res: "rw-e",
    minor: 99994,
    port: 11994,
    role: Role::Diskful,
};

/// Three diskful replicas, one late: after its wait the initial node starts
/// with the two connected (quorum), and the late one full-syncs. One kernel
/// takes one path per remote endpoint, so e is hidden from b, and b goes
/// down before e connects to a.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_late_peer_full_syncs() {
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let h = start().await;
    let size = 256 * MIB;
    let mut g = group(&h, size, &[A, B, E], &[(1, 2), (2, 1)], None);
    let [a, b, e] = &mut g[..] else {
        unreachable!()
    };
    settle(&h, &mut [a, b], "a and b UpToDate without e", |t| {
        disk(t, "rw-a").as_deref() == Some("UpToDate")
            && disk(t, "rw-b").as_deref() == Some("UpToDate")
    })
    .await;
    let tb = traffic("rw-b").await;
    println!("b, started with a: (received, sent) {tb:?} KiB");
    assert_eq!(tb, (0, 0), "no sync between the two that started");
    let written = write_payload("rw-a", A.minor).await;
    // b's path to a would clash with e's in this one kernel: b goes down.
    assert_eq!(sh(&["drbdsetup", "down", "rw-b"]).await.exit_code, Some(0));
    wait(&h.tracker, "b down", |t| t.resource("rw-b").is_none()).await;
    settle(&h, &mut [a, e], "e synced", |t| {
        disk(t, "rw-e").as_deref() == Some("UpToDate")
    })
    .await;
    let (received, _) = traffic("rw-e").await;
    println!(
        "late peer received {received} KiB of a {} KiB volume, {} KiB written",
        size / 1024,
        PAYLOAD / 1024
    );
    assert!(received * 1024 >= PAYLOAD, "full sync carries the data");
    assert_eq!(payload_on("rw-e").await, written, "the late peer holds it");
    for m in [&mut *a, &mut *b, &mut *e] {
        delete(&h, m).await;
    }
}

/// Two diskful replicas, the peer away: no quorum, so the initial node
/// keeps waiting past its wait instead of starting alone.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_no_start_without_quorum() {
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let h = start().await;
    let mut g = group(&h, 64 * MIB, &[A, B], &[(0, 1)], None);
    let a = &mut g[0];
    let pools = [pool(&h.node).await];
    for _ in 0..20 {
        a.pass(&pools, &[]).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let d = disk(&h.tracker.lock().unwrap(), "rw-a");
    println!("a after 6 s, wait 3 s, peer away: {d:?}");
    assert_eq!(d.as_deref(), Some("Inconsistent"));
    delete(&h, a).await;
}

/// A replicated restore: each member restores from the snapshot LV and the
/// initial node clears the bitmap, so nothing but metadata crosses the
/// network and the copies verify equal. Restored to twice the size, the
/// grown area syncs, mostly as discards.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_restore_uses_every_copy() {
    if !enabled() {
        return;
    }
    let h = start().await;
    restore_case(&h, 1, 8 * MIB).await;
    restore_case(&h, 2, 64 * MIB).await;
}

/// Restores a 256 MiB snapshot to `factor` times its size; the peer may
/// receive at most `max_received` bytes.
async fn restore_case(h: &Host, factor: u64, max_received: u64) {
    drbd_clean(EVERY).await;
    let source_size = 256 * MIB;
    let size = factor * source_size;
    println!("restore of {source_size} bytes to {size}");
    let l = lvm();
    let gross = drbd_config::gross_bytes(source_size, model::keys::PEER_SLOTS);
    let bytes = agent::storage::desired_bytes(gross, EXTENT);
    l.create(VG, super::THIN, "rw-src", bytes, &[])
        .await
        .unwrap();
    let marker = sh(&[
        "sh",
        "-c",
        &format!("dd if=/dev/urandom of=/dev/{VG}/rw-src bs=1M count=4 oflag=direct status=none && dd if=/dev/{VG}/rw-src bs=1M count=4 iflag=direct status=none | sha256sum"),
    ])
    .await;
    l.snapshot(VG, "rw-src", "rw-src+rw-snap").await.unwrap();
    let mut snap = Snapshot::new(
        "rw-snap",
        SnapshotSpec {
            volume: "rw-src".into(),
        },
    );
    snap.metadata.uid = Some("uid-rw-snap".into());
    snap.status = Some(SnapshotStatus {
        phase: SnapshotPhase::Ready,
        size_bytes: Some(source_size),
        ..SnapshotStatus::default()
    });
    let snaps = [Arc::new(snap)];
    let pools = [pool(&h.node).await];
    let mut g = group(h, size, &[A, B], &[], Some("rw-snap"));
    let mut proven = [false, false];
    let mut done = false;
    for _ in 0..120 {
        // Each member sees the other's entry, as Volume status shows it.
        for i in 0..g.len() {
            let others: Vec<(String, ReplicaStatus)> = (0..g.len())
                .filter(|j| *j != i)
                .filter_map(|j| Some((format!("m{j}"), g[j].entry.clone()?)))
                .collect();
            let others: Vec<(&str, ReplicaStatus)> = others
                .iter()
                .map(|(n, e)| (n.as_str(), e.clone()))
                .collect();
            g[i].pass_seeing_snapshots(&pools, &others, &snaps).await;
            proven[i] |= g[i]
                .entry
                .as_ref()
                .is_some_and(|e| e.restored_from.as_deref() == Some("rw-snap"));
        }
        done = {
            let t = h.tracker.lock().unwrap();
            disk(&t, "rw-b").as_deref() == Some("UpToDate")
                && disk(&t, "rw-a").as_deref() == Some("UpToDate")
        } && g.iter().all(|m| sectors(m.minor) * 512 >= size);
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        done,
        "{:?}",
        g.iter().map(Member::error).collect::<Vec<_>>()
    );
    // Reported while at the snapshot's size, so before any growth.
    assert_eq!(proven, [true, true], "both members proved their copy");
    let tags = l.tags(VG).await.unwrap();
    for lv in ["rw-a", "rw-b"] {
        assert!(
            tags[lv]
                .iter()
                .any(|t| *t == format!("r-drbd.from.uid-{lv}.uid-rw-snap")),
            "{lv}: {:?}",
            tags[lv]
        );
        let origin = sh(&["lvs", "--noheadings", "-o", "origin", &format!("{VG}/{lv}")]).await;
        assert_eq!(origin.stdout.trim(), "rw-src+rw-snap", "{lv}");
    }
    let (received, _) = traffic("rw-b").await;
    println!(
        "restore peer received {received} KiB of a {} KiB volume",
        size / 1024
    );
    assert!(
        received * 1024 < max_received,
        "at most {max_received} bytes: {received} KiB"
    );

    // Online verify compares every block; the resource files carry no
    // verify-alg, so it is set on the live connections for this check.
    for (res, peer) in [("rw-a", "1"), ("rw-b", "0")] {
        let out = sh(&["drbdsetup", "net-options", res, peer, "--verify-alg=sha256"]).await;
        assert_eq!(out.exit_code, Some(0), "{out:?}");
    }
    let out = sh(&["drbdadm", "verify", "rw-a"]).await;
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    let mut verified = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let out = sh(&["drbdsetup", "status", "--json", "--statistics", "rw-a"]).await;
        let v: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
        let p = &v[0]["connections"][0]["peer_devices"][0];
        let state = p["replication-state"].as_str().unwrap_or_default();
        if !state.starts_with("Verify") {
            verified = Some((p["out-of-sync"].as_u64(), out.stdout));
            break;
        }
    }
    let (oos, status) = verified.expect("verify did not end within 60 s");
    println!("after verify: {status}");
    assert_eq!(oos, Some(0), "out of sync after verify");

    // Direct reads: udev probed the new LVs through the page cache before
    // DRBD wrote under it.
    let head = |lv: &'static str| async move {
        sh(&[
            "sh",
            "-c",
            &format!("dd if=/dev/{VG}/{lv} bs=1M count=4 iflag=direct status=none | sha256sum"),
        ])
        .await
        .stdout
    };
    let (source, copy) = (head("rw-a").await, head("rw-b").await);
    println!(
        "first 4 MiB: snapshot {} initial {} peer {}",
        marker.stdout.trim(),
        source.trim(),
        copy.trim()
    );
    assert_eq!(
        source, marker.stdout,
        "the initial node holds the restored data"
    );
    assert_eq!(copy, marker.stdout, "the peer holds the restored data");
    for m in &mut g {
        delete(h, m).await;
    }
    drbd_clean(&["rw-src"]).await;
}

/// Backing LVs sized in closed form: usable at least the net size, at most
/// one extent more.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_metadata_sizing() {
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let h = start().await;
    for net in [
        MIB,
        100 * MIB + 12345,
        128 * MIB - 36 * 1024,
        GIB,
        GIB + 4096,
        3 * GIB + 7 * MIB,
        20 * GIB,
    ] {
        let mut g = group(&h, net, &[A], &[], None);
        let a = &mut g[0];
        settle(&h, &mut [&mut *a], "attached", |t| {
            disk(t, "rw-a").as_deref() == Some("Inconsistent")
                || disk(t, "rw-a").as_deref() == Some("UpToDate")
        })
        .await;
        let usable = sectors(a.minor) * 512;
        let lv = lvm()
            .lvs(VG)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.name == "rw-a")
            .unwrap()
            .size_bytes;
        println!(
            "net {net} gross {lv} usable {usable} waste {}",
            usable - net.min(usable)
        );
        assert!(usable >= net, "net {net}: usable {usable}");
        assert!(usable - net < EXTENT, "net {net}: usable {usable}");
        delete(&h, a).await;
    }
}

/// One replica: snapshot rounds, resize, departure and return, delete.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_replicated_lifecycle() {
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let h = start().await;
    let pools = [pool(&h.node).await];
    let mut g = group(&h, GIB, &[A], &[], None);
    let a = &mut g[0];
    settle(&h, &mut [&mut *a], "UpToDate alone", |t| {
        disk(t, "rw-a").as_deref() == Some("UpToDate")
    })
    .await;
    a.vol
        .status
        .get_or_insert_with(Default::default)
        .initialized_at = Some(now());
    let file = std::fs::read_to_string(format!("{RES_DIR}/rw-a.res")).unwrap();
    a.pass(&pools, &[]).await;
    assert_eq!(
        std::fs::read_to_string(format!("{RES_DIR}/rw-a.res")).unwrap(),
        file
    );

    let snap = |phase| {
        let mut s = Snapshot::new(
            "rw-s",
            SnapshotSpec {
                volume: "rw-a".into(),
            },
        );
        s.status = Some(SnapshotStatus {
            phase,
            ..SnapshotStatus::default()
        });
        Arc::new(s)
    };
    for phase in [
        SnapshotPhase::Suspending,
        SnapshotPhase::Taking,
        SnapshotPhase::Ready,
        SnapshotPhase::Deleting,
    ] {
        let w = a.pass(&pools, &[snap(phase)]).await;
        let ack = w.iter().find_map(|w| match w {
            StatusWrite::Snapshot { ack, .. } => Some(ack.clone()),
            _ => None,
        });
        println!("{phase:?}: {ack:?}");
        // Ready: the agent resumes on its own and has nothing to report.
        assert_eq!(ack.is_none(), phase == SnapshotPhase::Ready, "{ack:?}");
        assert!(ack.is_none_or(|a| a.ok));
        let suspended = matches!(phase, SnapshotPhase::Suspending | SnapshotPhase::Taking);
        wait(&h.tracker, "suspend state", |t| {
            t.resource("rw-a")
                .is_some_and(|r| r.suspended_by_user() == suspended)
        })
        .await;
    }
    assert!(!lv_names(&lvm()).await.contains(&"rw-a+rw-s".to_owned()));

    // Resize: alone, this node grows the LV and then the device.
    let before = sectors(a.minor);
    a.vol.spec.size_bytes = 2 * GIB;
    a.vol.metadata.generation = Some(2);
    a.pass(&pools, &[]).await;
    assert_eq!(a.error(), None);
    let after = sectors(a.minor);
    println!("device sectors {before} -> {after}");
    assert!(after * 512 >= 2 * GIB);

    // Departure: down, file gone, LV kept without its tag.
    let mut away = a.vol.clone();
    away.spec.assignment.as_mut().unwrap().replicas[0].node = "elsewhere".into();
    let saved = std::mem::replace(&mut a.vol, away);
    a.pass(&pools, &[]).await;
    assert!(a.entry.is_none());
    assert!(lv_names(&lvm()).await.contains(&"rw-a".to_owned()));
    assert_eq!(
        lvm()
            .tags(VG)
            .await
            .unwrap()
            .remove("rw-a")
            .unwrap_or_default(),
        Vec::<String>::new()
    );
    wait(&h.tracker, "down", |t| t.resource("rw-a").is_none()).await;

    // Back with no entry on an initialized volume: stale, made new and
    // created again.
    a.vol = saved;
    a.pass(&pools, &[]).await;
    assert_eq!(a.error(), None);
    assert_eq!(
        lvm().tags(VG).await.unwrap().remove("rw-a").unwrap(),
        ["r-drbd.md.uid-rw-a.0"]
    );
    wait(&h.tracker, "attached again", |t| disk(t, "rw-a").is_some()).await;
    delete(&h, a).await;
    assert!(!std::path::Path::new(&format!("{RES_DIR}/rw-a.res")).exists());
    assert_ne!(
        sh(&["drbdsetup", "status", "rw-a"]).await.exit_code,
        Some(0)
    );
    assert!(!lv_names(&lvm()).await.iter().any(|n| n.starts_with("rw-")));
}

/// A reported replica whose LV vanished is rebuilt empty and full-syncs
/// from its peer.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_lost_lv_is_rebuilt_and_synced() {
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let h = start().await;
    let size = 256 * MIB;
    let mut g = group(&h, size, &[A, B], &[], None);
    let [a, b] = &mut g[..] else { unreachable!() };
    settle(&h, &mut [a, b], "both UpToDate", |t| {
        disk(t, "rw-a").as_deref() == Some("UpToDate")
            && disk(t, "rw-b").as_deref() == Some("UpToDate")
    })
    .await;
    for m in [&mut *a, &mut *b] {
        m.vol
            .status
            .get_or_insert_with(Default::default)
            .initialized_at = Some(now());
        m.pass(&[pool(&h.node).await], &[]).await;
    }
    assert!(b.entry.as_ref().is_some_and(|e| e.present));
    let written = write_payload("rw-a", A.minor).await;

    // b's LV vanishes behind the agent's back.
    assert_eq!(sh(&["drbdsetup", "down", "rw-b"]).await.exit_code, Some(0));
    wait(&h.tracker, "b down", |t| t.resource("rw-b").is_none()).await;
    lvm().remove(VG, "rw-b").await.unwrap();
    settle(&h, &mut [a, b], "b rebuilt and synced", |t| {
        disk(t, "rw-b").as_deref() == Some("UpToDate")
    })
    .await;
    let (received, _) = traffic("rw-b").await;
    println!(
        "rebuilt replica received {received} KiB, {} KiB written",
        PAYLOAD / 1024
    );
    assert!(received * 1024 >= PAYLOAD, "full sync carries the data");
    assert_eq!(payload_on("rw-b").await, written, "the rebuilt LV holds it");
    assert_eq!(b.error(), None);
    for m in [&mut *a, &mut *b] {
        delete(&h, m).await;
    }
}

const D: Spec = Spec {
    res: "rw-d",
    minor: 99993,
    port: 11993,
    role: Role::Diskless,
};

/// A diskless attachment renders `tiebreaker no` and connects; promoted to
/// diskful with its node id kept (what an evict onto it does), the
/// survivor's peer device gets a bitmap slot and the new disk full-syncs.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_attachment_then_promoted() {
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let h = start().await;
    let size = 256 * MIB;
    let mut g = group(&h, size, &[A, D], &[], None);
    let [a, d] = &mut g[..] else { unreachable!() };
    settle(&h, &mut [a, d], "a UpToDate, d attached diskless", |t| {
        disk(t, "rw-a").as_deref() == Some("UpToDate")
            && disk(t, "rw-d").as_deref() == Some("Diskless")
            && connected(t, "rw-a")
    })
    .await;
    let file = std::fs::read_to_string(format!("{RES_DIR}/rw-a.res")).unwrap();
    println!("--- rw-a.res\n{file}");
    assert!(file.contains("            disk none;\n            tiebreaker no;\n"));
    let show = sh(&["drbdsetup", "show", "rw-a"]).await;
    println!("--- drbdsetup show rw-a\n{}", show.stdout);
    assert!(
        show.stdout.lines().any(|l| {
            let t = l.trim();
            t.starts_with("peer-tiebreaker") && t.ends_with("no;")
        }),
        "{}",
        show.stdout
    );
    let dev = format!("/dev/{VG}/rw-a");
    let slots = |out: String| drbd::slot_holders(&out).unwrap();
    let before = slots(
        sh(&drbd::argv::dump_superblock(A.minor, &dev)
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>())
        .await
        .stdout,
    );
    println!("a slots with d diskless: {before:?}");
    assert!(!before.contains(&1));
    let written = write_payload("rw-a", A.minor).await;

    for m in [&mut *a, &mut *d] {
        let r = m
            .vol
            .spec
            .assignment
            .as_mut()
            .unwrap()
            .replicas
            .iter_mut()
            .find(|r| r.node_id == 1)
            .unwrap();
        r.role = Role::Diskful;
        r.pool = Some("test-host-pool".into());
        m.vol.metadata.generation = Some(2);
    }
    settle(&h, &mut [a, d], "d diskful and synced", |t| {
        disk(t, "rw-d").as_deref() == Some("UpToDate")
    })
    .await;
    let after = slots(
        sh(&drbd::argv::dump_superblock(A.minor, &dev)
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>())
        .await
        .stdout,
    );
    let (received, _) = traffic("rw-d").await;
    println!(
        "a slots after the promotion: {after:?}; d received {received} KiB, {} KiB written",
        PAYLOAD / 1024
    );
    assert!(after.contains(&1), "bitmap slot for the promoted peer");
    assert!(received * 1024 >= PAYLOAD, "full sync carries the data");
    assert_eq!(payload_on("rw-d").await, written, "the new disk holds it");
    for m in [&mut *a, &mut *d] {
        delete(&h, m).await;
    }
}

/// The test-only nft table; dropped even when the test fails.
const FW_TABLE: &str = "rw-peer-probe";

struct Firewall;

impl Firewall {
    /// Rejects TCP to a and b's DRBD ports as a firewall does, and drops
    /// TCP to 127.0.0.3:11992. Only loopback addresses, in its own table.
    fn block() -> Self {
        Self::remove();
        let rules = format!(
            "table inet {FW_TABLE} {{\n chain input {{\n  type filter hook input priority -300; policy accept;\n  ip daddr {{ 127.0.0.1, 127.0.0.2 }} tcp dport {{ {}, {} }} reject with icmpx type admin-prohibited\n  ip daddr 127.0.0.3 tcp dport 11992 drop\n }}\n}}\n",
            A.port, B.port
        );
        let mut child = super::host_command("nft")
            .args(["-f", "-"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .expect("nft");
        std::io::Write::write_all(child.stdin.as_mut().unwrap(), rules.as_bytes()).unwrap();
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success(), "nft -f");
        Self
    }

    fn remove() {
        let _ = super::host_command("nft")
            .args(["delete", "table", "inet", FW_TABLE])
            .stderr(std::process::Stdio::null())
            .status();
    }
}

impl Drop for Firewall {
    fn drop(&mut self) {
        Self::remove();
    }
}

impl Member {
    /// A pass that sees `others` reported too, under the names this
    /// member gives them.
    async fn pass_seeing(
        &mut self,
        pools: &[Arc<Pool>],
        others: &[(&str, ReplicaStatus)],
    ) -> Vec<StatusWrite> {
        self.pass_seeing_snapshots(pools, others, &[]).await
    }

    async fn pass_seeing_snapshots(
        &mut self,
        pools: &[Arc<Pool>],
        others: &[(&str, ReplicaStatus)],
        snaps: &[Arc<Snapshot>],
    ) -> Vec<StatusWrite> {
        let mut v = self.current();
        let st = v.status.get_or_insert_with(Default::default);
        for (name, e) in others {
            let mut e = e.clone();
            (*name).clone_into(&mut e.node);
            st.replicas.push(e);
        }
        *self.live.0.lock().unwrap() = Some(v.clone());
        let w = self.rec.pass(pools, &[Arc::new(v)], snaps).await;
        for x in &w {
            if let StatusWrite::Volume { name, entry } = x
                && name == self.res
            {
                self.entry = Some(entry.clone());
            }
        }
        w
    }

    fn stuck(&self) -> Vec<model::PeerProbe> {
        self.entry
            .as_ref()
            .map(|e| e.unreachable_peers.clone())
            .unwrap_or_default()
    }
}

/// A peer that stands for one whose resource is up.
fn up_entry(node: &str) -> ReplicaStatus {
    ReplicaStatus {
        node: node.into(),
        present: true,
        ..ReplicaStatus::default()
    }
}

/// One probe from `from` to `to`, classified.
async fn probe(from: &str, to: &str) -> (model::ProbeResult, Option<String>) {
    use agent::drbd::Host as _;
    let r = agent::drbd::RealHost
        .connect(
            Some(from.parse().unwrap()),
            to.parse().unwrap(),
            agent::peer::PROBE_TIMEOUT,
        )
        .await;
    agent::peer::classify(&r)
}

/// Both DRBD ports rejected by a test firewall: each side reports the
/// other Filtered once the window passed; the record goes once they
/// connect and the result is kept for the next drop. Also the raw probe
/// against a dropping rule, a closed port and an unbindable source.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn drbd_blocked_port_is_reported() {
    use model::ProbeResult;
    if !enabled() {
        return;
    }
    drbd_clean(EVERY).await;
    let fw = Firewall::block();
    assert_eq!(
        probe("127.0.0.1", "127.0.0.3:11992").await.0,
        ProbeResult::TimedOut
    );
    assert_eq!(
        probe("127.0.0.1", "127.0.0.4:11993").await.0,
        ProbeResult::Refused
    );
    assert_eq!(
        probe("127.0.0.1", "127.0.0.2:11991").await.0,
        ProbeResult::Filtered
    );
    // The source is this node's DRBD address; one not on the host fails.
    let (r, e) = probe("192.0.2.1", "127.0.0.2:11991").await;
    assert_eq!(r, ProbeResult::Failed);
    assert!(e.unwrap().starts_with("bind 192.0.2.1"));

    let h = start().await;
    let pools = [pool(&h.node).await];
    let mut g = group(&h, GIB, &[A, B], &[], None);
    let [a, b] = &mut g[..] else { unreachable!() };
    let started = tokio::time::Instant::now();
    loop {
        let eb = b.entry.clone().unwrap_or_else(|| up_entry("x"));
        let ea = a.entry.clone().unwrap_or_else(|| up_entry("x"));
        a.pass_seeing(&pools, &[("m1", eb)]).await;
        b.pass_seeing(&pools, &[("m0", ea)]).await;
        if !a.stuck().is_empty() && !b.stuck().is_empty() {
            break;
        }
        assert!(
            started.elapsed() < agent::peer::STUCK + Duration::from_secs(30),
            "no stuck report: a {:?} b {:?}",
            a.entry,
            b.entry
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    println!("reported after {:?}", started.elapsed());
    assert!(started.elapsed() >= agent::peer::STUCK);
    for (m, peer, port) in [(&*a, "m1", B.port), (&*b, "m0", A.port)] {
        let p = m.stuck();
        println!("{}: {p:?}", m.res);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].node, peer);
        assert_eq!(p[0].port, port);
        assert_eq!(p[0].result, ProbeResult::Filtered, "{}", m.res);
    }

    drop(fw);
    let open = tokio::time::Instant::now();
    loop {
        let eb = b.entry.clone().unwrap();
        let ea = a.entry.clone().unwrap();
        a.pass_seeing(&pools, &[("m1", eb)]).await;
        b.pass_seeing(&pools, &[("m0", ea)]).await;
        if a.stuck().is_empty() && b.stuck().is_empty() {
            break;
        }
        assert!(
            open.elapsed() < agent::peer::STABLE + Duration::from_secs(40),
            "still reported: a {:?} b {:?}",
            a.stuck(),
            b.stuck()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    println!("cleared {:?} after the firewall went", open.elapsed());
    {
        let t = h.tracker.lock().unwrap();
        assert!(connected(&t, "rw-a") && connected(&t, "rw-b"));
    }

    // b down: stuck again; the last probe is younger than PROBE_EVERY, so
    // its result is reported again without a new probe.
    assert_eq!(sh(&["drbdsetup", "down", "rw-b"]).await.exit_code, Some(0));
    let down = tokio::time::Instant::now();
    loop {
        a.pass_seeing(&pools, &[("m1", up_entry("x"))]).await;
        if !a.stuck().is_empty() {
            break;
        }
        assert!(
            down.elapsed() < agent::peer::STUCK + Duration::from_secs(30),
            "no stuck report"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(a.stuck()[0].result, ProbeResult::Filtered);

    for m in [&mut *a, &mut *b] {
        delete(&h, m).await;
    }
    assert!(!lv_names(&lvm()).await.iter().any(|n| n.starts_with("rw-")));
}
