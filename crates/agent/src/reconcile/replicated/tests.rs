use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drbd_config::Features;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use model::{
    AssignedReplica, Assignment, Pool, PoolSpec, PoolStatus, ReplicaStatus, Resolve, Role,
    Snapshot, SnapshotAck, SnapshotPhase, SnapshotSpec, SnapshotStatus, Volume, VolumeSource,
    VolumeSpec, VolumeStatus,
};

use super::{DrbdEnv, NOT_OURS};
use crate::drbd::{Host, argv};
use crate::events2::Tracker;
use crate::exec::scripted::{ScriptedExec, code, ok};
use crate::reconcile::{LV_LOST, Reconciler, StatusWrite};
use crate::storage;
use crate::storage::fixtures::*;

const NODE: &str = "n1";
const POOL: &str = "pool-n1";
const GIB: u64 = 1 << 30;
const UID: &str = "uid-1";
/// gross_bytes(1 GiB, 3 slots) rounded up to the 4 MiB extent.
const LV_KIB: u64 = 1_052_672;
/// The same for 2 GiB.
const LV2_KIB: u64 = 2_101_248;
/// What DRBD exposes on LV_KIB, in 512 byte sectors.
const NET_SECTORS: u64 = 1_052_440 * 2;
const NEW: &str = storage::NEW_TAG;
/// n1's ownership tag (node id 0).
const OWN: &str = "r-drbd.md.uid-1.0";
const ALL: Features = Features {
    tiebreaker: true,
    discard_granularity: true,
    suspended_primary_outdated: true,
};

#[test]
fn sizes_used_here() {
    let gross = drbd_config::gross_bytes(GIB, model::keys::PEER_SLOTS);
    assert_eq!(storage::desired_bytes(gross, EXTENT), LV_KIB * 1024);
    let gross = drbd_config::gross_bytes(2 * GIB, model::keys::PEER_SLOTS);
    assert_eq!(storage::desired_bytes(gross, EXTENT), LV2_KIB * 1024);
    assert_eq!(
        drbd_config::usable_bytes(LV_KIB * 1024, model::keys::PEER_SLOTS),
        NET_SECTORS * 512
    );
}

#[derive(Debug, Default)]
struct FakeHost {
    sectors: Mutex<u64>,
    port_busy: bool,
    probes: AtomicU32,
}

#[async_trait::async_trait]
impl Host for FakeHost {
    fn device_sectors(&self, _: u32) -> std::io::Result<u64> {
        Ok(*self.sectors.lock().unwrap())
    }
    fn port_in_use(&self, _: IpAddr, _: u16) -> bool {
        self.port_busy
    }
    async fn connect(
        &self,
        _: Option<IpAddr>,
        _: std::net::SocketAddr,
        _: Duration,
    ) -> std::io::Result<()> {
        self.probes.fetch_add(1, Ordering::Relaxed);
        Err(std::io::ErrorKind::HostUnreachable.into())
    }
}

fn up_host() -> FakeHost {
    FakeHost {
        sectors: Mutex::new(NET_SECTORS),
        ..FakeHost::default()
    }
}

fn res_dir() -> PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    let d = std::env::temp_dir().join(format!(
        "agent-drbd-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn tracker(lines: &[&str]) -> Arc<Mutex<Tracker>> {
    let mut t = Tracker::default();
    for l in lines {
        t.feed(l).unwrap();
    }
    t.feed("exists -").unwrap();
    Arc::new(Mutex::new(t))
}

/// The resource up on n1, connected to n2 (diskful) and n3 (tiebreaker).
fn up(disk: &str, peer_disk: &str) -> Vec<String> {
    vec![
        "exists resource name:pvc-1 role:Secondary suspended:no".into(),
        "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connected role:Secondary".into(),
        "exists connection name:pvc-1 peer-node-id:2 conn-name:n3 connection:Connected role:Secondary".into(),
        format!("exists device name:pvc-1 volume:0 minor:50000 disk:{disk} client:no quorum:yes"),
        format!("exists peer-device name:pvc-1 peer-node-id:1 conn-name:n2 volume:0 replication:Established peer-disk:{peer_disk}"),
        "exists peer-device name:pvc-1 peer-node-id:2 conn-name:n3 volume:0 replication:Established peer-disk:Diskless peer-client:no".into(),
    ]
}

fn healthy() -> Vec<String> {
    up("UpToDate", "UpToDate")
}

struct Rig {
    x: Arc<ScriptedExec>,
    rec: Reconciler,
    dir: PathBuf,
    tracker: Arc<Mutex<Tracker>>,
    host: Arc<FakeHost>,
}

impl Rig {
    fn new(lines: &[String], host: FakeHost) -> Self {
        Self::on(NODE, lines, host)
    }

    fn on(node: &str, lines: &[String], host: FakeHost) -> Self {
        let x = Arc::new(ScriptedExec::new());
        let dir = res_dir();
        let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
        let tracker = tracker(&lines);
        let host = Arc::new(host);
        let mut env = DrbdEnv::new(Some(ALL), &dir, tracker.clone());
        env.host = host.clone();
        env.down_wait = Duration::ZERO;
        env.adjust_retry = vec![Duration::ZERO; 2];
        let mut rec = Reconciler::new(node, x.clone()).with_drbd(env);
        rec.set_addresses(addresses(&["n1", "n2", "n3"]));
        Self {
            x,
            rec,
            dir,
            tracker,
            host,
        }
    }

    async fn pass(&mut self, vols: &[Volume], snaps: &[Snapshot]) -> Vec<StatusWrite> {
        self.pass_pools(&[pool()], vols, snaps).await
    }

    async fn pass_pools(
        &mut self,
        pools: &[Arc<Pool>],
        vols: &[Volume],
        snaps: &[Snapshot],
    ) -> Vec<StatusWrite> {
        let vols: Vec<Arc<Volume>> = vols.iter().cloned().map(Arc::new).collect();
        let snaps: Vec<Arc<Snapshot>> = snaps.iter().cloned().map(Arc::new).collect();
        let w = self.rec.pass(pools, &vols, &snaps).await;
        self.x.assert_done();
        w
    }

    fn feed(&self, lines: &[&str]) {
        let mut t = self.tracker.lock().unwrap();
        for l in lines {
            t.feed(l).unwrap();
        }
    }

    fn res_file(&self) -> Option<String> {
        std::fs::read_to_string(self.dir.join("pvc-1.res")).ok()
    }

    fn candidate(&self) -> PathBuf {
        self.dir.join(".pvc-1.candidate")
    }

    /// drbdadm checks the candidate file.
    fn expect_check(&self) {
        self.x.expect(self.check_argv(), ok(""));
    }

    /// The bitmap slot sweep finds only assigned node ids.
    fn expect_sweep(&self) {
        self.x.expect(
            argv::dump_superblock(50000, "/dev/vg1/pvc-1"),
            ok(r#"{ "peers": [ { "bitmap_index": -1 }, { "bitmap_index": 0 } ] }"#),
        );
    }

    fn check_argv(&self) -> Vec<String> {
        argv::validate(
            &self.candidate().to_string_lossy(),
            &self.dir.join("pvc-1.res").to_string_lossy(),
            "pvc-1",
        )
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// n1 at 10.0.0.1 and so on.
fn addresses(nodes: &[&str]) -> BTreeMap<String, IpAddr> {
    nodes
        .iter()
        .map(|n| {
            let ip: IpAddr = format!("10.0.0.{}", &n[1..]).parse().unwrap();
            ((*n).to_owned(), ip)
        })
        .collect()
}

/// The dry run plans `planned`.
fn dry(x: &ScriptedExec, planned: &str) {
    x.expect(argv::adjust_dry_run("pvc-1"), ok(planned));
}

const PLAN: &str = "drbdsetup new-resource pvc-1 0\n";

fn up_plan(x: &ScriptedExec) {
    dry(x, PLAN);
    x.expect(argv::adjust("pvc-1"), ok(""));
}

fn pool_on(node: &str) -> Arc<Pool> {
    let mut p = Pool::new(
        &format!("pool-{node}"),
        PoolSpec {
            node: node.into(),
            tier: "test".into(),
            vg: VG.into(),
            thin_pool: THIN.into(),
            overcommit_percent: model::DEFAULT_OVERCOMMIT_PERCENT,
            reserve_percent: model::DEFAULT_RESERVE_PERCENT,
            devices: None,
            chunk_size: None,
            reclaim_policy: None,
        },
    );
    p.metadata.generation = Some(1);
    p.status = Some(PoolStatus {
        chunk_bytes: Some(65536),
        ..PoolStatus::default()
    });
    Arc::new(p)
}

fn pool() -> Arc<Pool> {
    pool_on(NODE)
}

fn replica(node: &str, id: u8, role: Role) -> AssignedReplica {
    AssignedReplica {
        node: node.into(),
        pool: (role == Role::Diskful).then(|| format!("pool-{node}")),
        node_id: id,
        port: Some(11000),
        role,
    }
}

/// n1 and n2 diskful, n3 tiebreaker; n1 makes the first data current.
fn volume(size: u64) -> Volume {
    let mut v = Volume::new(
        "pvc-1",
        VolumeSpec {
            size_bytes: size,
            replicas: 2,
            tier: "test".into(),
            pool: None,
            node: None,
            access_mode: model::AccessMode::default(),
            attachments: vec![],
            evict: vec![],
            source: None,
            resolve: None,
            assignment: Some(Assignment {
                replicas: vec![
                    replica("n1", 0, Role::Diskful),
                    replica("n2", 1, Role::Diskful),
                    replica("n3", 2, Role::Tiebreaker),
                ],
                minor: Some(50000),
                secret: Some("c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0MTI=".into()),
                initial_up_to_date_node: Some("n1".into()),
            }),
        },
    );
    v.metadata.generation = Some(3);
    v.metadata.uid = Some(UID.into());
    v
}

fn with_entries(mut v: Volume, entries: Vec<ReplicaStatus>) -> Volume {
    v.status.get_or_insert_with(VolumeStatus::default).replicas = entries;
    v
}

fn initialized(mut v: Volume) -> Volume {
    v.status
        .get_or_insert_with(VolumeStatus::default)
        .initialized_at = Some(Time(k8s_openapi::jiff::Timestamp::now()));
    v
}

fn restored(mut v: Volume) -> Volume {
    v.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
    v
}

fn source_snapshot() -> Snapshot {
    let mut s = snapshot(SnapshotPhase::Ready);
    s.spec.volume = "pvc-0".into();
    s
}

fn source_lv() -> String {
    lv_row("pvc-0+snap-1", LV_KIB, St::Inactive)
}

fn deleting<T: kube::Resource>(mut o: T) -> T {
    o.meta_mut().deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
    o
}

fn snapshot(phase: SnapshotPhase) -> Snapshot {
    let mut s = Snapshot::new(
        "snap-1",
        SnapshotSpec {
            volume: "pvc-1".into(),
        },
    );
    s.status = Some(SnapshotStatus {
        phase,
        ..SnapshotStatus::default()
    });
    s
}

fn lv(kib: u64) -> String {
    lv_row("pvc-1", kib, St::Active)
}

/// The first reads of a pass: lvs, vgs.
fn reads(x: &ScriptedExec, rows: &[String]) {
    expect_lvs(x, rows);
    expect_vgs(x);
}

/// The reads of a later pass.
fn reads_again(x: &ScriptedExec, rows: &[String]) {
    expect_lvs(x, rows);
    expect_vgs(x);
}

fn entry_of(w: &[StatusWrite]) -> ReplicaStatus {
    w.iter()
        .find_map(|w| match w {
            StatusWrite::Volume { name, entry } if name == "pvc-1" => Some(entry.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no pvc-1 entry in {w:?}"))
}

fn acks(w: &[StatusWrite]) -> Vec<SnapshotAck> {
    w.iter()
        .filter_map(|w| match w {
            StatusWrite::Snapshot { ack, .. } => Some(ack.clone()),
            _ => None,
        })
        .collect()
}

/// The `lvs -o lv_name,lv_tags` read: each LV with its tags.
fn expect_lv_tags(x: &ScriptedExec, lvs: &[(&str, &[&str])]) {
    x.expect(lvm::cmd::lv_tags(VG), ok(&tags_out(lvs)));
}

fn expect_change(x: &ScriptedExec, lv: &str, add: &[&str], del: &[&str]) {
    x.expect(lvm::cmd::change_tags(VG, lv, add, del), ok(""));
}

/// lvcreate of pvc-1 with the new tag, then the fresh read.
fn expect_create(x: &ScriptedExec, other_rows: &[String]) {
    x.expect(
        lvm::cmd::create_thin(VG, THIN, "pvc-1", LV_KIB * 1024, &[NEW]),
        ok(""),
    );
    let mut rows = other_rows.to_vec();
    rows.push(lv(LV_KIB));
    expect_lvs(x, &rows);
}

/// create-md, then `own` in and `del` out.
fn expect_md_as(x: &ScriptedExec, own: &str, del: &[&str]) {
    x.expect(argv::create_md("pvc-1", 6), ok(""));
    expect_change(x, "pvc-1", &[own], del);
}

fn expect_md(x: &ScriptedExec, del: &[&str]) {
    expect_md_as(x, OWN, del);
}

fn seen(x: &ScriptedExec, word: &str) -> bool {
    x.seen
        .lock()
        .unwrap()
        .iter()
        .any(|a| a.iter().any(|w| w == word))
}

/// This node's entry once it had been reported UpToDate.
fn reported() -> ReplicaStatus {
    ReplicaStatus {
        node: NODE.into(),
        present: true,
        disk_state: Some("UpToDate".into()),
        allocated_bytes: Some(LV_KIB * 1024),
        observed_generation: Some(3),
        ..ReplicaStatus::default()
    }
}

#[tokio::test]
async fn first_creation_on_the_initial_node() {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[]);
    expect_create(&x, &[]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert!(!e.present, "not up until events2 says so");
    assert_eq!(e.allocated_bytes, Some(LV_KIB * 1024));
    assert_eq!(
        (e.observed_generation, e.last_error.as_deref()),
        (Some(3), None)
    );
    let file = r.res_file().unwrap();
    assert!(file.starts_with("# r-drbd "));
    assert!(file.contains("disk \"/dev/vg1/pvc-1\";"));
    assert!(file.contains("device minor 50000;"));
    assert!(file.contains("rs-discard-granularity 65536;"));
    assert!(file.contains("host \"n1\" address ipv4 10.0.0.1:11000;"));
    assert_eq!(file.matches("connection {").count(), 2);
    assert!(!r.candidate().exists());
    assert!(!seen(&x, "new-current-uuid"));

    // Its events ask for a pass, which finds every diskful peer connected
    // and Inconsistent: the bitmap is cleared, nobody syncs.
    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    let lines = up("Inconsistent", "Inconsistent");
    r.feed(&lines.iter().map(String::as_str).collect::<Vec<_>>());
    let vols = [Arc::new(with_entries(volume(GIB), vec![e.clone()]))];
    let w = r.rec.refresh(&vols).await;
    assert!(r.rec.take_pass_wanted() && !r.rec.take_pass_wanted());
    let e = entry_of(&w);
    assert!(e.present);
    assert_eq!(e.disk_state.as_deref(), Some("Inconsistent"));
    assert_eq!(e.device_path.as_deref(), Some("/dev/drbd50000"));

    reads_again(&x, &[lv(LV_KIB)]);
    r.expect_sweep();
    dry(&x, "");
    x.expect(argv::start_without_sync("pvc-1"), ok(""));
    r.pass(&[with_entries(volume(GIB), vec![e])], &[]).await;
    assert!(r.rec.init_started.is_empty());
}

#[tokio::test]
async fn initial_node_waits_for_its_peers_then_starts_alone() {
    let mut lines = up("Inconsistent", "DUnknown");
    lines[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting role:Unknown".into();
    let mut r = Rig::new(&lines, up_host());
    r.rec.drbd.init_wait = Duration::from_millis(100);
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    r.pass(&[volume(GIB)], &[]).await;
    assert!(r.rec.init_started["pvc-1"].is_some());
    assert!(!seen(&x, "new-current-uuid"));

    tokio::time::sleep(Duration::from_millis(150)).await;
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    x.expect(argv::start_without_sync("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;
}

#[tokio::test]
async fn only_the_initial_node_of_an_uninitialized_volume_starts_it() {
    // n2 is not the initial node.
    let mut lines = up("Inconsistent", "Inconsistent");
    lines[1] = "exists connection name:pvc-1 peer-node-id:0 conn-name:n1 connection:Connected role:Secondary".into();
    lines[4] = "exists peer-device name:pvc-1 peer-node-id:0 conn-name:n1 volume:0 replication:Established peer-disk:Inconsistent".into();
    let mut r = Rig::on("n2", &lines, up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    r.pass_pools(&[pool_on("n2")], &[volume(GIB)], &[]).await;
    assert!(!seen(&x, "new-current-uuid") && r.rec.init_started.is_empty());

    // Initialized, or a peer already holding data: nothing either.
    for (v, peer) in [
        (initialized(volume(GIB)), "Inconsistent"),
        (volume(GIB), "UpToDate"),
    ] {
        let lines = up("Inconsistent", peer);
        let mut r = Rig::new(&lines, up_host());
        let x = r.x.clone();
        reads(&x, &[lv(LV_KIB)]);
        r.expect_check();
        r.expect_sweep();
        dry(&x, "");
        r.pass(&[v], &[]).await;
        assert!(!seen(&x, "new-current-uuid") && r.rec.init_started.is_empty());
    }
}

#[tokio::test]
async fn restore_initial_node_is_the_source_at_once() {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    x.expect(
        lvm::cmd::restore(VG, "pvc-0+snap-1", "pvc-1", &[NEW]),
        ok(""),
    );
    expect_lvs(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    // The source's tags came along with the copy.
    expect_lv_tags(&x, &[("pvc-1", &["r-drbd.md.uid-0.0", NEW])]);
    expect_md(&x, &["r-drbd.md.uid-0.0", NEW]);
    up_plan(&x);
    let vol = restored(volume(GIB));
    r.pass(std::slice::from_ref(&vol), &[source_snapshot()])
        .await;

    // Up and Inconsistent, n2 not even connected: the source now.
    let mut lines = up("Inconsistent", "DUnknown");
    lines[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting role:Unknown".into();
    r.feed(&lines.iter().map(String::as_str).collect::<Vec<_>>());
    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_sweep();
    dry(&x, "");
    x.expect(argv::start_as_source("pvc-1"), ok(""));
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(!seen(&x, "--clear-bitmap"));
}

#[tokio::test]
async fn restore_peers_start_empty() {
    let mut r = Rig::on("n2", &[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    expect_create(&x, &[source_lv()]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    expect_md_as(&x, "r-drbd.md.uid-1.1", &[NEW]);
    up_plan(&x);
    let w = r
        .pass_pools(
            &[pool_on("n2")],
            &[restored(volume(GIB))],
            &[source_snapshot()],
        )
        .await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn resource_file_is_checked_installed_and_stable() {
    let mut r = Rig::new(&healthy(), up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert!(e.present && e.last_error.is_none());
    let first = r.res_file().unwrap();
    let mtime = std::fs::metadata(r.dir.join("pvc-1.res"))
        .unwrap()
        .modified()
        .unwrap();

    // Same content: no write, no check.
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    let w = r.pass(&[with_entries(volume(GIB), vec![e])], &[]).await;
    assert!(
        w.iter().all(|w| !matches!(w, StatusWrite::Volume { .. })),
        "{w:?}"
    );
    let again = std::fs::metadata(r.dir.join("pvc-1.res"))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!((r.res_file().unwrap(), mtime), (first.clone(), again));

    // A file drbdadm rejects is never installed.
    let mut moved = volume(GIB);
    moved.spec.assignment.as_mut().unwrap().replicas[1].port = Some(11005);
    reads_again(&x, &[lv(LV_KIB)]);
    x.expect(
        r.check_argv(),
        code(10, "", "conflicting use of device-minor"),
    );
    let w = r.pass(&[moved], &[]).await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some(
            "drbdadm rejects the new resource file: drbdadm: exit 10: conflicting use of device-minor"
        )
    );
    assert_eq!(r.res_file().unwrap(), first);
    assert!(!r.candidate().exists());
}

#[tokio::test]
async fn adjust_runs_only_when_the_dry_run_plans_something() {
    let mut r = Rig::new(&healthy(), up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "drbdsetup connect pvc-1 1\n");
    x.expect(
        argv::adjust("pvc-1"),
        code(10, "", "pvc-1: Resize not allowed during resync."),
    )
    .expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);

    // Busy past the retries: an error, retried next pass.
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "drbdsetup resize 50000\n");
    for _ in 0..3 {
        x.expect(
            argv::adjust("pvc-1"),
            code(10, "", "pvc-1: Resize not allowed during resync."),
        );
    }
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert!(
        entry_of(&w)
            .last_error
            .unwrap()
            .starts_with("adjust pvc-1: drbdadm: exit 10")
    );
}

#[tokio::test]
async fn a_plan_that_keeps_coming_back_backs_off() {
    let mut lines = healthy();
    lines[1] =
        "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:StandAlone".into();
    let mut r = Rig::new(&lines, up_host());
    r.rec.drbd.repeat_backoff = Duration::from_millis(200);
    let x = r.x.clone();
    let plan = "drbdsetup connect pvc-1 1\n";
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, plan);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);
    // Back after a successful adjust: held and reported.
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, plan);
    let w = r.pass(&[with_entries(volume(GIB), vec![e])], &[]).await;
    let e = entry_of(&w);
    let err = e.last_error.clone().unwrap();
    assert!(err.starts_with("adjust pvc-1 does not converge"), "{err}");
    // The wait passed: tried again, the next wait doubles.
    tokio::time::sleep(Duration::from_millis(250)).await;
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, plan);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[with_entries(volume(GIB), vec![e.clone()])], &[])
        .await;
    assert_eq!(r.rec.repeats["pvc-1"].times, 1);
    // A changed plan starts over at once; an empty one clears the record.
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "drbdsetup connect pvc-1 2\n");
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[with_entries(volume(GIB), vec![e.clone()])], &[])
        .await;
    assert_eq!(r.rec.repeats["pvc-1"].times, 0);
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    r.pass(&[with_entries(volume(GIB), vec![e])], &[]).await;
    assert!(r.rec.repeats.is_empty());
}

#[tokio::test]
async fn split_brain_holds_adjust_until_resolve() {
    let mut lines = healthy();
    lines[1] =
        "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:StandAlone".into();
    let mut r = Rig::new(&lines, up_host());
    let x = r.x.clone();
    // A StandAlone without a split brain: drbdadm plans the connect.
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "drbdsetup connect pvc-1 1\n");
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;

    // After the split-brain helper: no dry run, no adjust (the script
    // would fail on any unexpected command).
    r.feed(&[
        "call helper name:pvc-1 peer-node-id:1 conn-name:n2 helper:split-brain",
        "response helper name:pvc-1 peer-node-id:1 conn-name:n2 helper:split-brain status:0",
        "change connection name:pvc-1 conn-name:n2 connection:StandAlone",
    ]);
    for _ in 0..2 {
        reads_again(&x, &[lv(LV_KIB)]);
        r.pass(&[volume(GIB)], &[]).await;
    }
}

#[tokio::test]
async fn diskless_tiebreaker_creation() {
    let mut r = Rig::on("n3", &[], FakeHost::default());
    let x = r.x.clone();
    // The pool belongs to n1, so this node reads no LVM.
    r.expect_check();
    up_plan(&x);
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);
    let file = r.res_file().unwrap();
    assert!(file.contains("disk none;"));
    assert!(file.contains("disk \"/dev/r-drbd/peer-disk-never-opened\";"));
    assert!(!file.contains("/dev/vg1"));
    assert_eq!(file.matches("connection {").count(), 2);

    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    r.feed(&[
        "create resource name:pvc-1 role:Secondary suspended:no",
        "create device name:pvc-1 volume:0 minor:50000 disk:Diskless client:yes quorum:yes",
    ]);
    let vols = [Arc::new(with_entries(volume(GIB), vec![e]))];
    let e = entry_of(&r.rec.refresh(&vols).await);
    assert!(e.present);
    assert_eq!(e.disk_state.as_deref(), Some("Diskless"));
    assert_eq!(e.allocated_bytes, None);
}

#[tokio::test]
async fn missing_settings_are_left_out_and_logged_once() {
    let mut r = Rig::on("n3", &[], FakeHost::default());
    r.rec.drbd.features = Some(Features::default());
    r.rec.set_addresses(addresses(&["n1", "n2", "n3", "n4"]));
    let x = r.x.clone();
    let mut v = volume(GIB);
    v.spec
        .assignment
        .as_mut()
        .unwrap()
        .replicas
        .push(replica("n4", 3, Role::Diskless));
    r.expect_check();
    up_plan(&x);
    r.pass(&[v], &[]).await;
    let file = r.res_file().unwrap();
    assert!(!file.contains("tiebreaker") && !file.contains("on-suspended-primary-outdated"));
    assert!(r.rec.dropped_logged.contains("pvc-1"));
}

#[tokio::test]
async fn port_in_use_blocks_the_first_adjust() {
    let mut r = Rig::on(
        "n3",
        &[],
        FakeHost {
            port_busy: true,
            ..FakeHost::default()
        },
    );
    r.expect_check();
    dry(&r.x, PLAN);
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("DRBD port 11000 on 10.0.0.3 is in use")
    );
}

/// A pass on n1's LV that exists, carries `tags` and is not attached.
async fn ownership(
    vol: Volume,
    tags: &[&str],
    script: impl FnOnce(&ScriptedExec),
) -> (Rig, ReplicaStatus) {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", tags)]);
    script(&x);
    let w = r.pass(&[vol], &[]).await;
    let e = entry_of(&w);
    (r, e)
}

#[tokio::test]
async fn own_tag_keeps_the_metadata() {
    let (r, e) = ownership(volume(GIB), &[OWN], up_plan).await;
    assert_eq!(e.last_error, None);
    assert!(!seen(&r.x, "create-md"));
}

#[tokio::test]
async fn new_tag_gets_metadata() {
    // An LV cut short between create-md and the tag swap.
    let (_, e) = ownership(volume(GIB), &[NEW], |x| {
        expect_md(x, &[NEW]);
        up_plan(x);
    })
    .await;
    assert_eq!(e.last_error, None);
}

#[tokio::test]
async fn stale_tags_are_made_new_then_recreated() {
    // Another Volume's UID (a reused name), or this node's earlier node id.
    for stale in ["r-drbd.md.uid-0.0", "r-drbd.md.uid-1.5"] {
        let prev = with_entries(volume(GIB), vec![reported()]);
        let (_, e) = ownership(prev, &[stale, "keep"], |x| {
            expect_change(x, "pvc-1", &[NEW], &[stale]);
            expect_md(x, &[NEW]);
            up_plan(x);
        })
        .await;
        assert_eq!(e.last_error, None, "{stale}");
    }
}

#[tokio::test]
async fn untagged_lv_is_stale_only_for_a_departed_node() {
    // Initialized and no entry here: this node left and came back.
    let (_, e) = ownership(initialized(volume(GIB)), &[], |x| {
        expect_change(x, "pvc-1", &[NEW], &[]);
        expect_md(x, &[NEW]);
        up_plan(x);
    })
    .await;
    assert_eq!(e.last_error, None);

    // With an entry, or before initializedAt: not provably ours.
    for v in [
        initialized(with_entries(volume(GIB), vec![reported()])),
        volume(GIB),
    ] {
        let (r, e) = ownership(v, &[], |_| {}).await;
        assert_eq!(e.last_error.as_deref(), Some(NOT_OURS));
        assert!(!seen(&r.x, "create-md") && !seen(&r.x, "lvremove"));
    }
}

#[tokio::test]
async fn lost_lv_is_rebuilt_empty() {
    // Even for a restore: the peers hold newer data than the snapshot.
    let vol = initialized(with_entries(restored(volume(GIB)), vec![reported()]));
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    expect_create(&x, &[source_lv()]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    x.expect(argv::create_md("pvc-1", 6), code(10, "", "io error"));
    // The LV made this pass goes again; the loss stays on record.
    expect_lvs(&x, &[source_lv(), lv(LV_KIB)]);
    x.expect(lvm::cmd::remove(VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[source_lv()]);
    let w = r
        .pass(std::slice::from_ref(&vol), &[source_snapshot()])
        .await;
    let e = entry_of(&w);
    assert!(
        e.last_error.as_deref().unwrap().starts_with(LV_LOST),
        "{e:?}"
    );

    // Next pass: rebuilt again, never restored.
    reads_again(&x, &[source_lv()]);
    expect_create(&x, &[source_lv()]);
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    let vol = with_entries(vol, vec![e]);
    let w = r.pass(&[vol], &[source_snapshot()]).await;
    assert_eq!(entry_of(&w).last_error, None);
    assert!(!seen(&x, "new-current-uuid"));
}

#[tokio::test]
async fn peer_leaving() {
    let mut lines = healthy();
    lines.push(
        "exists connection name:pvc-1 peer-node-id:3 conn-name:n4 connection:Connected".into(),
    );
    lines.push("exists peer-device name:pvc-1 peer-node-id:3 conn-name:n4 volume:0 replication:Established peer-disk:UpToDate".into());
    let mut r = Rig::new(&lines, up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    x.expect(argv::del_peer("pvc-1", 3), ok(""))
        .expect(argv::forget_peer("pvc-1", 3), ok(""));
    r.expect_sweep();
    dry(&x, "");
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);

    // Both fail for a still connected diskful peer: the del-peer error stands.
    let mut r = Rig::new(&lines, up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    x.expect(argv::del_peer("pvc-1", 3), code(10, "", "busy"))
        .expect(argv::forget_peer("pvc-1", 3), code(10, "", "in use"));
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("drbdsetup: exit 10: busy")
    );
}

#[tokio::test]
async fn delete_sequence() {
    let mut lines = healthy();
    lines[0] = "exists resource name:pvc-1 role:Secondary suspended:user".into();
    let mut r = Rig::new(&lines, FakeHost::default());
    let x = r.x.clone();
    std::fs::create_dir_all(&r.dir).unwrap();
    std::fs::write(r.dir.join("pvc-1.res"), "resource \"pvc-1\" {}").unwrap();
    std::fs::write(r.candidate(), "left over").unwrap();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::resume_io("pvc-1"), ok(""))
        .expect(argv::down("pvc-1"), ok(""))
        .expect(
            argv::status("pvc-1"),
            code(10, "", "pvc-1: No such resource"),
        );
    expect_lvs(&x, &[lv(LV_KIB)]);
    x.expect(lvm::cmd::remove(VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[]);
    let w = r.pass(&[deleting(volume(GIB))], &[]).await;
    let e = entry_of(&w);
    assert!(!e.present && e.last_error.is_none());
    assert_eq!(e.observed_generation, Some(3));
    assert!(r.res_file().is_none() && !r.candidate().exists());
}

#[tokio::test]
async fn down_that_does_not_stick_is_an_error() {
    let mut r = Rig::new(&healthy(), FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::down("pvc-1"), ok(""))
        .expect(argv::status("pvc-1"), ok("pvc-1 role:Secondary"));
    let w = r
        .pass(
            &[deleting(with_entries(volume(GIB), vec![reported()]))],
            &[],
        )
        .await;
    let e = entry_of(&w);
    assert!(
        e.present,
        "an unfinished teardown keeps the replica present"
    );
    assert_eq!(
        e.last_error.as_deref(),
        Some("DRBD resource pvc-1 still up after down")
    );
}

#[tokio::test]
async fn delete_confirms_down_when_events2_lost_track() {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::status("pvc-1"), ok("pvc-1 role:Secondary"));
    let w = r
        .pass(
            &[deleting(with_entries(volume(GIB), vec![reported()]))],
            &[],
        )
        .await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("DRBD resource pvc-1 still up after down")
    );
}

#[tokio::test]
async fn node_leaving_the_assignment_keeps_its_data() {
    let mut r = Rig::new(&healthy(), FakeHost::default());
    let x = r.x.clone();
    std::fs::create_dir_all(&r.dir).unwrap();
    std::fs::write(r.dir.join("pvc-1.res"), "resource \"pvc-1\" {}").unwrap();
    let mut v = volume(GIB);
    v.spec
        .assignment
        .as_mut()
        .unwrap()
        .replicas
        .retain(|r| r.node != NODE);
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::down("pvc-1"), ok(""))
        .expect(argv::status("pvc-1"), code(10, "", ""));
    // The bytes stay, the proof of ownership goes.
    expect_lv_tags(&x, &[("pvc-1", &[OWN, "other"])]);
    expect_change(&x, "pvc-1", &[], &[OWN]);
    let w = r.pass(&[with_entries(v, vec![reported()])], &[]).await;
    assert!(
        w.iter()
            .any(|w| matches!(w, StatusWrite::VolumeGone { name } if name == "pvc-1"))
    );
    assert!(r.res_file().is_none());
    assert!(!seen(&x, "lvremove"));
}

fn grown(size: u64, generation: i64, peer_kib: Option<u64>) -> Volume {
    let mut v = volume(size);
    v.metadata.generation = Some(generation);
    let entries = vec![
        ReplicaStatus {
            node: NODE.into(),
            present: true,
            observed_generation: Some(3),
            ..ReplicaStatus::default()
        },
        ReplicaStatus {
            node: "n2".into(),
            present: true,
            allocated_bytes: peer_kib.map(|k| k * 1024),
            ..ReplicaStatus::default()
        },
    ];
    with_entries(v, entries)
}

#[tokio::test]
async fn resize_waits_for_every_peer() {
    let mut r = Rig::new(&healthy(), up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(lvm::cmd::extend(VG, "pvc-1", LV2_KIB * 1024), ok(""));
    expect_lvs(&x, &[lv(LV2_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    let w = r.pass(&[grown(2 * GIB, 4, Some(LV_KIB))], &[]).await;
    let e = entry_of(&w);
    assert_eq!(e.allocated_bytes, Some(LV2_KIB * 1024));
    assert_eq!(e.observed_generation, Some(3), "waiting on n2");

    // Grown but not connected: still waiting.
    let mut lines = healthy();
    lines[1] =
        "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting".into();
    let mut r = Rig::new(&lines, up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV2_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    let w = r.pass(&[grown(2 * GIB, 4, Some(LV2_KIB))], &[]).await;
    assert_eq!(entry_of(&w).observed_generation, Some(3));
}

#[tokio::test]
async fn resize_runs_once_peers_have_grown() {
    let mut r = Rig::new(&healthy(), up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV2_KIB)]);
    r.expect_check();
    r.expect_sweep();
    x.expect(argv::resize("pvc-1"), ok(""));
    dry(&x, "");
    let w = r.pass(&[grown(2 * GIB, 4, Some(LV2_KIB))], &[]).await;
    let e = entry_of(&w);
    assert_eq!(
        (e.observed_generation, e.last_error.as_deref()),
        (Some(4), None)
    );
}

#[tokio::test]
async fn only_the_lowest_node_id_resizes() {
    let mut lines = healthy();
    lines[1] =
        "exists connection name:pvc-1 peer-node-id:0 conn-name:n1 connection:Connected".into();
    lines[4] = "exists peer-device name:pvc-1 peer-node-id:0 conn-name:n1 volume:0 replication:Established peer-disk:UpToDate".into();
    let mut r = Rig::on("n2", &lines, up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(lvm::cmd::extend(VG, "pvc-1", LV2_KIB * 1024), ok(""));
    expect_lvs(&x, &[lv(LV2_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    let w = r
        .pass_pools(&[pool_on("n2")], &[grown(2 * GIB, 4, Some(LV2_KIB))], &[])
        .await;
    let e = entry_of(&w);
    assert_eq!(
        (e.observed_generation, e.last_error.as_deref()),
        (Some(4), None)
    );
    assert!(!seen(&x, "resize"));
}

#[tokio::test]
async fn snapshot_rounds() {
    let ack = |phase, ok| SnapshotAck {
        node: NODE.into(),
        phase,
        pool: Some(POOL.into()),
        ok,
        error: None,
    };
    let mut sus = healthy();
    sus[0] = "exists resource name:pvc-1 role:Secondary suspended:user".into();

    let mut r = Rig::new(&healthy(), up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    r.x.expect(argv::suspend_io("pvc-1"), ok(""));
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Suspending, true)]);

    // Suspended for the round: no adjust at all, no second suspend.
    let mut r = Rig::new(&sus, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Suspending, true)]);
    assert!(!seen(&r.x, "--dry-run"));

    let mut r = Rig::new(&sus, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&r.x, &[("pvc-1", &[OWN])]);
    r.x.expect(lvm::cmd::snapshot(VG, "pvc-1", "pvc-1+snap-1"), ok(""));
    expect_change(&r.x, "pvc-1+snap-1", &[], &[OWN]);
    expect_lvs(
        &r.x,
        &[lv(LV_KIB), lv_row("pvc-1+snap-1", LV_KIB, St::Inactive)],
    );
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Taking)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Taking, true)]);

    let mut r = Rig::new(&sus, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    r.x.expect(argv::resume_io("pvc-1"), ok(""));
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Resuming)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Resuming, true)]);

    // A suspend no round accounts for (snapshot gone mid-round) is resumed.
    let mut r = Rig::new(&sus, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    r.x.expect(argv::resume_io("pvc-1"), ok(""));
    dry(&r.x, "");
    r.pass(&[volume(GIB)], &[]).await;
}

#[tokio::test]
async fn diskless_replica_suspends_and_acks_without_pool() {
    let mut r = Rig::on(
        "n3",
        &[
            "exists resource name:pvc-1 role:Primary suspended:no".into(),
            "exists connection name:pvc-1 peer-node-id:0 conn-name:n1 connection:Connected".into(),
            "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connected".into(),
            "exists device name:pvc-1 volume:0 minor:50000 disk:Diskless client:yes quorum:yes"
                .into(),
        ],
        FakeHost::default(),
    );
    r.expect_check();
    dry(&r.x, "");
    r.x.expect(argv::suspend_io("pvc-1"), ok(""));
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    let a = acks(&w);
    assert_eq!((a[0].ok, a[0].pool.as_deref()), (true, None));
}

fn resolving(survivor: &str, split: bool) -> Volume {
    let mut v = volume(GIB);
    v.spec.resolve = Some(Resolve {
        survivor: survivor.into(),
    });
    if split {
        v.status = Some(VolumeStatus {
            conditions: vec![model::condition::new(
                model::condition::types::SPLIT_BRAIN,
                model::condition::TRUE,
                "SplitBrain",
                "",
                Some(3),
                Time(k8s_openapi::jiff::Timestamp::now()),
            )],
            ..VolumeStatus::default()
        });
    }
    v
}

fn split_brain(role: &str) -> Vec<String> {
    vec![
        format!("exists resource name:pvc-1 role:{role} suspended:no"),
        "exists connection name:pvc-1 peer-node-id:0 conn-name:n1 connection:StandAlone".into(),
        "exists connection name:pvc-1 peer-node-id:2 conn-name:n3 connection:Connected".into(),
        "exists device name:pvc-1 volume:0 minor:50000 disk:UpToDate client:no quorum:yes".into(),
    ]
}

#[tokio::test]
async fn resolve_discards_once_per_epoch() {
    let mut r = Rig::on("n2", &split_brain("Secondary"), up_host());
    let x = r.x.clone();
    let v = [resolving("n1", true)];
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    x.expect(argv::disconnect("pvc-1"), ok(""))
        .expect(argv::connect_discard("pvc-1"), ok(""));
    r.pass_pools(&[pool_on("n2")], &v, &[]).await;
    // Still StandAlone (events lag, or it failed): no second discard, and
    // no adjust while the controller's SplitBrain holds.
    reads_again(&x, &[lv(LV_KIB)]);
    r.pass_pools(&[pool_on("n2")], &v, &[]).await;
}

#[tokio::test]
async fn resolve_refuses_a_published_replica() {
    let mut r = Rig::on("n2", &split_brain("Primary"), up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    let w = r
        .pass_pools(&[pool_on("n2")], &[resolving("n1", true)], &[])
        .await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("resolve needs the volume unpublished on n2")
    );
}

#[tokio::test]
async fn resolve_needs_split_brain_and_a_diskful_survivor() {
    // No SplitBrain condition: no discard, the StandAlone is adjusted.
    let mut r = Rig::on("n2", &split_brain("Secondary"), up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "drbdsetup connect pvc-1 0\n");
    r.x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass_pools(&[pool_on("n2")], &[resolving("n1", false)], &[])
        .await;
    assert!(!seen(&r.x, "--discard-my-data"));
    // A survivor that holds no disk: no discard; the latched SplitBrain
    // still holds the adjust, even with the helper call lost.
    let mut r = Rig::on("n2", &split_brain("Secondary"), up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    r.pass_pools(&[pool_on("n2")], &[resolving("n3", true)], &[])
        .await;
    assert!(!seen(&r.x, "--discard-my-data") && !seen(&r.x, "--dry-run"));
}

#[tokio::test]
async fn resolve_survivor_reconnects() {
    let mut lines = split_brain("Secondary");
    lines[1] =
        "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:StandAlone".into();
    let mut r = Rig::new(&lines, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    r.x.expect(argv::connect_peer("pvc-1", "n2"), ok(""));
    r.pass(&[resolving("n1", true)], &[]).await;
}

#[tokio::test]
async fn nothing_before_the_initial_dump() {
    let x = Arc::new(ScriptedExec::new());
    let dir = res_dir();
    let env = DrbdEnv::new(Some(ALL), &dir, Arc::new(Mutex::new(Tracker::default())));
    let mut rec = Reconciler::new(NODE, x.clone()).with_drbd(env);
    reads(&x, &[]);
    let w = rec
        .pass(
            &[pool()],
            &[Arc::new(volume(GIB))],
            &[Arc::new(snapshot(SnapshotPhase::Suspending))],
        )
        .await;
    x.assert_done();
    assert!(
        w.iter().all(|w| matches!(w, StatusWrite::Pool { .. })),
        "{w:?}"
    );
    assert!(!dir.exists());
}

/// The initial node of a restore that lost its LV before initializedAt
/// restores it again; nobody else holds the data.
#[tokio::test]
async fn restore_lost_before_initialized_is_restored_again() {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    x.expect(
        lvm::cmd::restore(VG, "pvc-0+snap-1", "pvc-1", &[NEW]),
        ok(""),
    );
    expect_lvs(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    let vol = with_entries(restored(volume(GIB)), vec![reported()]);
    let w = r
        .pass(std::slice::from_ref(&vol), &[source_snapshot()])
        .await;
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);
    let empty = lvm::cmd::create_thin(VG, THIN, "pvc-1", LV_KIB * 1024, &[NEW]);
    assert!(
        !x.seen.lock().unwrap().contains(&empty),
        "never an empty LV"
    );
    assert!(!r.rec.rebuilt_seen.contains("pvc-1"));

    // Up and Inconsistent: the restored copy is the source.
    let lines = up("Inconsistent", "Inconsistent");
    r.feed(&lines.iter().map(String::as_str).collect::<Vec<_>>());
    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_sweep();
    dry(&x, "");
    x.expect(argv::start_as_source("pvc-1"), ok(""));
    r.pass(&[with_entries(vol, vec![e])], &[source_snapshot()])
        .await;
}

#[tokio::test]
async fn restore_lost_with_its_snapshot_gone_never_starts() {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[]);
    let vol = with_entries(restored(volume(GIB)), vec![reported()]);
    expect_lvs(&x, &[]);
    let w = r.pass(std::slice::from_ref(&vol), &[]).await;
    let err = entry_of(&w).last_error.unwrap();
    assert!(err.starts_with(LV_LOST), "{err}");
    assert!(
        err.contains("restore before the volume was initialized"),
        "{err}"
    );
    assert!(!seen(&x, "lvcreate") && !seen(&x, "new-current-uuid"));
}

/// The timeout start, with n2 never connected.
fn waiting_lines(quorum: bool) -> Vec<String> {
    let mut lines = up("Inconsistent", "DUnknown");
    lines[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting role:Unknown".into();
    if !quorum {
        lines[3] = lines[3].replace("quorum:yes", "quorum:no");
    }
    lines
}

async fn after_the_wait(vol: Volume, lines: &[String], lost: bool) -> Arc<ScriptedExec> {
    let mut r = Rig::new(lines, up_host());
    r.rec.drbd.init_wait = Duration::ZERO;
    if lost {
        r.rec.rebuilt_seen.insert("pvc-1".into());
    }
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    r.pass(&[vol], &[]).await;
    x
}

#[tokio::test]
async fn timeout_start_needs_no_rebuild_no_seen_data_and_quorum() {
    // Rebuilt: data may exist on an unseen peer.
    let x = after_the_wait(volume(GIB), &waiting_lines(true), true).await;
    assert!(!seen(&x, "new-current-uuid"));
    // A peer once reported UpToDate.
    let peer_had_data = with_entries(
        volume(GIB),
        vec![ReplicaStatus {
            node: "n2".into(),
            disk_state: Some("UpToDate".into()),
            ..ReplicaStatus::default()
        }],
    );
    let x = after_the_wait(peer_had_data, &waiting_lines(true), false).await;
    assert!(!seen(&x, "new-current-uuid"));
    // No quorum: still waiting.
    let x = after_the_wait(volume(GIB), &waiting_lines(false), false).await;
    assert!(!seen(&x, "new-current-uuid"));
}

#[tokio::test]
async fn rebuilt_initial_node_starts_only_with_every_peer_connected() {
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.rebuilt_seen.insert("pvc-1".into());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    x.expect(argv::start_without_sync("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;
}

#[tokio::test]
async fn stale_bitmap_slots_are_freed_and_retried() {
    let stale = r#"{ "peers": [ { "bitmap_index": -1 }, { "bitmap_index": 0 }, { "bitmap_index": -1 }, { "bitmap_index": -1 }, { "bitmap_index": -1 }, { "bitmap_index": 1 } ] }"#;
    let mut r = Rig::new(&healthy(), up_host());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    x.expect(argv::dump_superblock(50000, "/dev/vg1/pvc-1"), ok(stale))
        .expect(argv::forget_peer("pvc-1", 5), code(10, "", "busy"));
    dry(&x, "");
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);
    // Retried on the next pass; once clean, not read again.
    reads_again(&x, &[lv(LV_KIB)]);
    x.expect(argv::dump_superblock(50000, "/dev/vg1/pvc-1"), ok(stale))
        .expect(argv::forget_peer("pvc-1", 5), ok(""));
    dry(&x, "");
    r.pass(&[volume(GIB)], &[]).await;
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    r.pass(&[volume(GIB)], &[]).await;
}

#[tokio::test]
async fn first_data_runs_while_an_adjust_is_backed_off() {
    let mut lines = up("Inconsistent", "DUnknown");
    lines[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting role:Unknown".into();
    let mut r = Rig::new(&lines, up_host());
    let x = r.x.clone();
    let plan = "drbdsetup peer-device-options pvc-1 2 0 --set-defaults\n";
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, plan);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;
    assert!(!seen(&x, "new-current-uuid"));

    // n2 arrives; the same plan is back and held, the first data still goes.
    r.feed(&[
        "change connection name:pvc-1 conn-name:n2 connection:Connected",
        "change peer-device name:pvc-1 conn-name:n2 volume:0 replication:Established peer-disk:Inconsistent",
    ]);
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, plan);
    x.expect(argv::start_without_sync("pvc-1"), ok(""));
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert!(
        e.last_error
            .as_deref()
            .unwrap()
            .starts_with("adjust pvc-1 does not converge"),
        "{e:?}"
    );
    assert_eq!(e.observed_generation, None, "not applied while held");
}

#[tokio::test]
async fn snapshot_lv_name_lvm_refuses_never_suspends() {
    let mut r = Rig::new(&healthy(), up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    let mut snap = snapshot(SnapshotPhase::Suspending);
    snap.metadata.name = Some("s".repeat(120));
    let w = r.pass(&[volume(GIB)], &[snap]).await;
    let a = &acks(&w)[0];
    assert!(!a.ok);
    assert!(
        a.error.as_deref().unwrap().contains("device-mapper"),
        "{a:?}"
    );
    assert!(!seen(&r.x, "suspend-io"), "no suspend");
}

#[tokio::test]
async fn snapshot_of_a_disk_not_up_to_date_is_refused() {
    let mut sus = up("Inconsistent", "UpToDate");
    sus[0] = "exists resource name:pvc-1 role:Secondary suspended:user".into();
    let mut r = Rig::new(&sus, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Taking)])
        .await;
    let a = &acks(&w)[0];
    assert_eq!(
        a.error.as_deref(),
        Some("local DRBD disk is Inconsistent, not UpToDate")
    );
    assert!(!seen(&r.x, "lvcreate"));
}

#[tokio::test]
async fn snapshot_of_a_replica_rebuilt_this_pass_is_refused() {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[]);
    expect_create(&x, &[]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    let vol = initialized(with_entries(volume(GIB), vec![reported()]));
    let w = r.pass(&[vol], &[snapshot(SnapshotPhase::Taking)]).await;
    let a = &acks(&w)[0];
    assert!(
        a.error
            .as_deref()
            .unwrap()
            .contains("was created this pass"),
        "{a:?}"
    );
    assert!(
        !seen(&x, "--snapshot")
            && !x
                .seen
                .lock()
                .unwrap()
                .iter()
                .any(|c| c.iter().any(|w| w.contains("+snap-1")))
    );
}

#[tokio::test(start_paused = true)]
async fn a_stuck_connection_is_probed_and_reported() {
    let mut lines = healthy();
    lines[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting role:Unknown".into();
    let mut rig = Rig::new(&lines, up_host());
    let x = rig.x.clone();
    let peer = ReplicaStatus {
        node: "n2".into(),
        present: true,
        ..ReplicaStatus::default()
    };
    let vol = with_entries(volume(GIB), vec![reported(), peer]);
    reads(&x, &[lv(LV_KIB)]);
    rig.expect_check();
    rig.expect_sweep();
    dry(&x, "");
    let writes = rig.pass(std::slice::from_ref(&vol), &[]).await;
    assert_eq!(entry_of(&writes).unreachable_peers, vec![]);
    assert_eq!(rig.host.probes.load(Ordering::Relaxed), 0);

    tokio::time::advance(crate::peer::STUCK).await;
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    let writes = rig.pass(std::slice::from_ref(&vol), &[]).await;
    let e = entry_of(&writes);
    assert_eq!(rig.host.probes.load(Ordering::Relaxed), 1);
    let [p] = e.unreachable_peers.as_slice() else {
        panic!("one stuck peer expected: {e:?}");
    };
    assert_eq!(p.node, "n2");
    assert_eq!((p.address.as_str(), p.port), ("10.0.0.2", 11000));
    assert_eq!(p.result, model::ProbeResult::Filtered);

    // Rate limited: the next pass reports the same and probes nothing.
    let vol = with_entries(
        volume(GIB),
        vec![e.clone(), vol.status.clone().unwrap().replicas[1].clone()],
    );
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    let writes = rig.pass(std::slice::from_ref(&vol), &[]).await;
    assert!(writes.iter().all(|x| x.name() != "pvc-1"), "{writes:?}");
    assert_eq!(rig.host.probes.load(Ordering::Relaxed), 1);

    // Connected and held: the record goes.
    rig.feed(&["change connection name:pvc-1 conn-name:n2 connection:Connected role:Secondary"]);
    // Not held yet: a flap keeps the record.
    let writes = rig.rec.refresh(&[Arc::new(vol.clone())]).await;
    assert_eq!(entry_of(&writes).unreachable_peers.len(), 1);
    tokio::time::advance(crate::peer::STABLE).await;
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    let writes = rig.pass(&[vol], &[]).await;
    assert_eq!(entry_of(&writes).unreachable_peers, vec![]);
}

#[tokio::test(start_paused = true)]
async fn a_peer_with_a_stale_lease_is_not_probed() {
    let mut lines = healthy();
    lines[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting role:Unknown".into();
    let mut rig = Rig::new(&lines, up_host());
    let x = rig.x.clone();
    let old = k8s_openapi::jiff::Timestamp::now()
        .checked_sub(std::time::Duration::from_secs(120))
        .unwrap();
    let lease = crate::peer::PeerLease {
        acquired: Some(old),
        renewed: Some(old),
    };
    rig.rec
        .set_peer_leases(BTreeMap::from([("n2".to_owned(), lease)]));
    let peer = ReplicaStatus {
        node: "n2".into(),
        present: true,
        ..ReplicaStatus::default()
    };
    let vol = with_entries(volume(GIB), vec![reported(), peer]);
    reads(&x, &[lv(LV_KIB)]);
    rig.expect_check();
    rig.expect_sweep();
    dry(&x, "");
    rig.pass(std::slice::from_ref(&vol), &[]).await;
    tokio::time::advance(crate::peer::STUCK).await;
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    rig.pass(std::slice::from_ref(&vol), &[]).await;
    assert_eq!(rig.host.probes.load(Ordering::Relaxed), 0);
}
