use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drbd_config::{ToolVersions, Version};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use model::{
    AssignedReplica, Assignment, Pool, PoolSpec, PoolStatus, ReplicaStatus, Resolve, Role,
    Snapshot, SnapshotAck, SnapshotPhase, SnapshotSpec, SnapshotStatus, Volume, VolumeSource,
    VolumeSpec, VolumeStatus,
};

use super::DrbdEnv;
use crate::drbd::{self, Host, MD_BLOCK, MD_MAGIC, argv};
use crate::events2::Tracker;
use crate::exec::scripted::{ScriptedExec, code, ok};
use crate::reconcile::{Reconciler, StatusWrite};
use crate::storage;
use crate::storage::fixtures::*;

const NODE: &str = "n1";
const POOL: &str = "pool-n1";
const GIB: u64 = 1 << 30;
/// gross_size_kib(1 GiB) = 1048840, rounded to the 4 MiB extent.
const LV_KIB: u64 = 1_052_672;
/// net_size_kib(LV_KIB) in 512 byte sectors.
const NET_SECTORS: u64 = 1_052_408 * 2;
const MINOR: u32 = 1000;
const DAY0: &str = "5F3E2D1C0B0A0908";
const RANDOM: u64 = 0xA1B2_C3D4_E5F6_0719;
const DEV: &str = "/dev/vg1/pvc-1";

#[derive(Debug, Default)]
struct FakeHost {
    /// Tail magic answers in order; the last one repeats.
    magic: Mutex<Vec<bool>>,
    sectors: Mutex<u64>,
    port_busy: bool,
}

impl Host for FakeHost {
    fn read_tail(&self, _: &str) -> std::io::Result<Vec<u8>> {
        let mut t = vec![0; MD_BLOCK];
        let mut m = self.magic.lock().unwrap();
        let magic = if m.len() > 1 {
            m.remove(0)
        } else {
            m.first().copied().unwrap_or(false)
        };
        if magic {
            t[60..64].copy_from_slice(&MD_MAGIC);
        }
        Ok(t)
    }
    fn device_sectors(&self, _: u32) -> std::io::Result<u64> {
        Ok(*self.sectors.lock().unwrap())
    }
    fn port_in_use(&self, _: IpAddr, _: u16) -> bool {
        self.port_busy
    }
    fn random(&self) -> std::io::Result<u64> {
        Ok(RANDOM)
    }
}

fn versions() -> ToolVersions {
    let v = |major, minor, patch| Version {
        major,
        minor,
        patch,
    };
    ToolVersions {
        kernel: v(9, 3, 4),
        utils: v(9, 35, 0),
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

/// The resource up, connected to both peers, disk as given.
fn healthy(disk: &str) -> Vec<String> {
    vec![
        "exists resource name:pvc-1 role:Secondary suspended:no".into(),
        "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connected role:Secondary".into(),
        "exists connection name:pvc-1 peer-node-id:2 conn-name:n3 connection:Connected role:Secondary".into(),
        format!("exists device name:pvc-1 volume:0 minor:1000 disk:{disk} client:no quorum:yes"),
        "exists peer-device name:pvc-1 peer-node-id:1 conn-name:n2 volume:0 replication:Established peer-disk:UpToDate".into(),
        "exists peer-device name:pvc-1 peer-node-id:2 conn-name:n3 volume:0 replication:Established peer-disk:Diskless peer-client:no".into(),
    ]
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
        let mut env = DrbdEnv::new(Some(versions()), &dir, tracker.clone());
        env.host = host.clone();
        env.down_wait = Duration::ZERO;
        env.adjust_retry = [Duration::ZERO; 3];
        let mut rec = Reconciler::new(node, x.clone()).with_drbd(env);
        let addresses: BTreeMap<String, IpAddr> =
            [("n1", "10.0.0.1"), ("n2", "10.0.0.2"), ("n3", "10.0.0.3")]
                .into_iter()
                .map(|(n, a)| (n.to_owned(), a.parse().unwrap()))
                .collect();
        rec.set_addresses(addresses);
        Self {
            x,
            rec,
            dir,
            tracker,
            host,
        }
    }

    async fn pass(&mut self, vols: &[Volume], snaps: &[Snapshot]) -> Vec<StatusWrite> {
        let vols: Vec<Arc<Volume>> = vols.iter().cloned().map(Arc::new).collect();
        let snaps: Vec<Arc<Snapshot>> = snaps.iter().cloned().map(Arc::new).collect();
        let w = self.rec.pass(&[pool()], &vols, &snaps).await;
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

    fn backup(&self) -> Option<String> {
        std::fs::read_to_string(self.dir.join(".backup/pvc-1.res")).ok()
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn pool() -> Arc<Pool> {
    let mut p = Pool::new(
        POOL,
        PoolSpec {
            node: NODE.into(),
            tier: "test".into(),
            vg: VG.into(),
            thin_pool: THIN.into(),
        },
    );
    p.metadata.generation = Some(1);
    p.status = Some(PoolStatus {
        chunk_bytes: Some(65536),
        ..PoolStatus::default()
    });
    Arc::new(p)
}

fn replica(node: &str, id: u8, role: Role) -> AssignedReplica {
    AssignedReplica {
        node: node.into(),
        pool: (role == Role::Diskful).then(|| format!("pool-{node}")),
        node_id: id,
        port: Some(7000),
        role,
    }
}

/// n1 and n2 diskful, n3 tiebreaker; n1 wins the initial UpToDate.
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
                minor: Some(MINOR),
                secret: Some("c2VjcmV0c2VjcmV0c2Vj".into()),
                gi: Some(DAY0.into()),
                initial_up_to_date_node: Some("n1".into()),
            }),
        },
    );
    v.metadata.generation = Some(3);
    v
}

fn with_entries(mut v: Volume, entries: Vec<ReplicaStatus>) -> Volume {
    v.status = Some(VolumeStatus {
        replicas: entries,
        ..VolumeStatus::default()
    });
    v
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
    lv_row("pvc-1", kib, "Vwi-a-tz--", "")
}

fn reads(x: &ScriptedExec, rows: &[String]) {
    expect_pvdisplay(x);
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

/// get-gi of a slot holding our day-0 id as bitmap id.
fn ours() -> String {
    format!("A1B2C3D4E5F60719:{DAY0}:0000000000000000:0000000000000000:1:1:0:0:1:0:0:0:0:0:0:0:0\n")
}

/// get-gi and dump-md of the snapshot source's metadata.
fn expect_other_md(x: &ScriptedExec) {
    x.expect(
        argv::get_gi(0, MINOR, DEV),
        ok("1111111111111110:2222222222222222:0000000000000000:0000000000000000:1:1:0\n"),
    )
    .expect(
        argv::dump_superblock(MINOR, DEV),
        ok(r#"{ "current_uuid": "0x1111111111111110", "peers": [ { "bitmap_uuid": "0x2222222222222222", "flags": "0x00000000" } ], "history_uuids": [ "0x3333333333333332" ] }"#),
    );
}

const NEW: &str = storage::NEW_TAG;
/// n1's metadata tag (node id 0).
const OWN: &str = "r-drbd.md.0";

/// The `lvs -o lv_name,lv_tags` read: each LV with its tags.
fn expect_lv_tags(x: &ScriptedExec, lvs: &[(&str, &[&str])]) {
    let out: String = lvs
        .iter()
        .flat_map(|(n, t)| {
            [
                "  ".to_owned(),
                (*n).to_owned(),
                ";".to_owned(),
                t.join(","),
                "\n".to_owned(),
            ]
        })
        .collect();
    x.expect(storage::tags_argv(&filter(), VG), ok(&out));
}

/// Those LVs carry n1's metadata tag.
fn expect_tags(x: &ScriptedExec, tagged: &[&str]) {
    let lvs: Vec<(&str, &[&str])> = tagged.iter().map(|n| (*n, &[OWN][..])).collect();
    expect_lv_tags(x, &lvs);
}

/// The read right after this pass created pvc-1.
fn expect_new_tags(x: &ScriptedExec) {
    expect_lv_tags(x, &[("pvc-1", &[NEW])]);
}

fn expect_change(x: &ScriptedExec, lv: &str, add: &[&str], del: &[&str]) {
    x.expect(storage::lvchange_tags(&filter(), add, del, VG, lv), ok(""));
}

/// Metadata proven: n1's tag in, `del` out.
fn expect_seal(x: &ScriptedExec, del: &[&str]) {
    expect_change(x, "pvc-1", &[OWN], del);
}

fn check_resize_fails(x: &ScriptedExec) {
    x.expect(
        argv::check_resize(MINOR, DEV),
        code(20, "", "no last-known size"),
    );
}

fn initialized(mut v: Volume) -> Volume {
    v.status
        .get_or_insert_with(VolumeStatus::default)
        .initialized_at = Some(Time(k8s_openapi::jiff::Timestamp::now()));
    v
}

fn expect_set_gi(x: &ScriptedExec, own: u8, current: Option<&str>) {
    for id in 0..=31 {
        x.expect(
            argv::set_gi(id, MINOR, DEV, &drbd::gi_value(id, own, DAY0, current)),
            ok(""),
        );
    }
}

fn assert_no_set_gi(x: &ScriptedExec) {
    let seen = x.seen.lock().unwrap();
    assert!(
        !seen.iter().any(|a| a.iter().any(|w| w == "set-gi")),
        "{seen:?}"
    );
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

/// The LV is gone behind a reported replica: an empty LV of the gross size,
/// just-created metadata, no set-gi, no restore; DRBD then syncs it in full.
async fn rebuild(vol: Volume, snaps: &[Snapshot], other_rows: &[String]) {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, other_rows);
    x.expect(
        lvm::argv::lvcreate_thin_tagged(&filter(), VG, THIN, "pvc-1", LV_KIB, Some(NEW)),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    let mut rows = other_rows.to_vec();
    rows.push(lv(LV_KIB));
    expect_lvs(&x, &rows);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let vol = with_entries(vol, vec![reported()]);
    let w = r.pass(std::slice::from_ref(&vol), snaps).await;
    assert_no_set_gi(&x);
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);
    assert_eq!(e.allocated_bytes, Some(LV_KIB * 1024));

    // DRBD attaches the empty disk and syncs from n2.
    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    let mut lines = healthy("Inconsistent");
    lines[4] = "exists peer-device name:pvc-1 peer-node-id:1 conn-name:n2 volume:0 replication:SyncTarget peer-disk:UpToDate done:12.50".into();
    r.feed(&lines.iter().map(String::as_str).collect::<Vec<_>>());
    let vols = [Arc::new(with_entries(vol, vec![e]))];
    let e = entry_of(&r.rec.refresh(&vols).await);
    assert!(e.present, "{e:?}");
    assert_eq!(e.disk_state.as_deref(), Some("Inconsistent"));
    assert_eq!(e.last_error, None);
}

#[tokio::test]
async fn lost_lv_of_initialized_volume_is_rebuilt_empty() {
    let (vol, snap) = restored();
    let src = lv_row("pvc-0_snap-1", LV_KIB, "Vri---tz-k", "pvc-0");
    rebuild(initialized(vol), &[snap], &[src]).await;
}

#[tokio::test]
async fn lost_lv_before_initialized_is_rebuilt_empty() {
    // Reported present before initializedAt: still never day 0, never fresh.
    rebuild(volume(GIB), &[], &[]).await;
}

#[tokio::test]
async fn first_creation_sequence() {
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[]);
    x.expect(
        lvm::argv::lvcreate_thin_tagged(&filter(), VG, THIN, "pvc-1", LV_KIB, Some(NEW)),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    let current = drbd::fresh_gi(RANDOM);
    expect_set_gi(&x, 0, Some(&current));
    expect_seal(&x, &[NEW]);
    x.expect(
        argv::adjust("pvc-1"),
        code(10, "", "pvc-1: Resize not allowed during resync."),
    )
    .expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert!(!e.present, "not up until events2 says so");
    assert_eq!(e.allocated_bytes, Some(LV_KIB * 1024));
    assert_eq!(
        (e.observed_generation, e.last_error.as_deref()),
        (Some(3), None)
    );

    let file = r.res_file().unwrap();
    assert_eq!(r.backup().as_deref(), Some(file.as_str()));
    let body = &file[file.find("resource \"").unwrap()..];
    assert!(body.contains("disk        /dev/vg1/pvc-1;"));
    assert!(body.contains("device      minor 1000;"));
    assert!(body.contains("shared-secret     \"c2VjcmV0c2VjcmV0c2Vj\";"));
    assert!(body.contains("rs-discard-granularity 65536;"));
    assert!(body.contains("host \"n1\" address ipv4 10.0.0.1:7000;"));
    assert_eq!(body.matches("    connection\n").count(), 2);

    // events2 brings it up: the refresh reports the replica without host work.
    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    let lines = healthy("UpToDate");
    r.feed(&lines.iter().map(String::as_str).collect::<Vec<_>>());
    let vols = [Arc::new(with_entries(volume(GIB), vec![e]))];
    let w = r.rec.refresh(&vols).await;
    let e = entry_of(&w);
    assert!(e.present && !e.in_use);
    assert_eq!(e.disk_state.as_deref(), Some("UpToDate"));
    assert_eq!(e.device_path.as_deref(), Some("/dev/drbd1000"));
    assert_eq!(e.size_bytes, Some(NET_SECTORS * 512));
    assert_eq!(e.quorum, Some(true));
    assert_eq!(e.peers.get("n2").map(String::as_str), Some("Connected"));
    // Nothing new: nothing to write.
    let vols = [Arc::new(with_entries(volume(GIB), vec![e]))];
    assert_eq!(r.rec.refresh(&vols).await, vec![]);
}

#[tokio::test]
async fn second_diskful_node_is_not_the_winner() {
    let mut r = Rig::on("n2", &[], FakeHost::default());
    let x = r.x.clone();
    let mut pl = (*pool()).clone();
    pl.metadata.name = Some("pool-n2".into());
    pl.spec.node = "n2".into();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &[]);
    x.expect(argv::check_resize(MINOR, DEV), code(20, "", ""));
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_set_gi(&x, 1, None);
    // n2's own node id.
    expect_change(&x, "pvc-1", &["r-drbd.md.1"], &[]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r
        .rec
        .pass(&[Arc::new(pl)], &[Arc::new(volume(GIB))], &[])
        .await;
    x.assert_done();
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn winner_only_before_the_volume_is_initialized() {
    // initializedAt set (a node placed later, LV made this pass): metadata
    // stays just created, no set-gi at all, so DRBD runs a full sync.
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[]);
    x.expect(
        lvm::argv::lvcreate_thin_tagged(&filter(), VG, THIN, "pvc-1", LV_KIB, Some(NEW)),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[initialized(volume(GIB))], &[]).await;
    assert_no_set_gi(&x);

    // Not initialized: still the winner although a peer already reports.
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &[]);
    check_resize_fails(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_set_gi(&x, 0, Some(&drbd::fresh_gi(RANDOM)));
    expect_seal(&x, &[]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let peer = ReplicaStatus {
        node: "n2".into(),
        present: true,
        disk_state: Some("UpToDate".into()),
        ..ReplicaStatus::default()
    };
    r.pass(&[with_entries(volume(GIB), vec![peer])], &[]).await;
}

#[tokio::test]
async fn shifted_metadata_is_never_recreated() {
    // LV grown while DRBD was down: no magic at the new end, the replica held data.
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV2_KIB)]);
    expect_tags(&x, &[]);
    check_resize_fails(&x);
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: true,
        observed_generation: Some(2),
        ..ReplicaStatus::default()
    };
    let w = r.pass(&[with_entries(volume(GIB), vec![prev])], &[]).await;
    let e = entry_of(&w);
    assert_eq!(
        e.last_error.as_deref(),
        Some("metadata not at device end; refusing create-md on a replica that held data")
    );
    assert_eq!(e.observed_generation, Some(2));
}

#[tokio::test]
async fn backing_lv_grows_only_under_an_attached_disk() {
    // Down: the LV stays, metadata found at its end, adjust brings it up.
    let mut r = Rig::new(
        &[],
        FakeHost {
            magic: Mutex::new(vec![true]),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &[]);
    x.expect(argv::get_gi(0, MINOR, DEV), ok(&ours()));
    expect_seal(&x, &[]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: true,
        observed_generation: Some(3),
        ..ReplicaStatus::default()
    };
    let mut vol = with_entries(volume(2 * GIB), vec![prev]);
    vol.metadata.generation = Some(4);
    let w = r.pass(&[vol.clone()], &[]).await;
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);
    assert_eq!(e.allocated_bytes, Some(LV_KIB * 1024));
    assert_eq!(e.observed_generation, Some(3), "growth still pending");

    // Attached: it grows.
    let mut r = Rig::new(
        &healthy("UpToDate"),
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(lvm::argv::lvresize(&filter(), VG, "pvc-1", LV2_KIB), ok(""));
    expect_lvs(&x, &[lv(LV2_KIB)]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[vol], &[]).await;
    assert_eq!(entry_of(&w).allocated_bytes, Some(LV2_KIB * 1024));
}
#[tokio::test]
async fn resource_file_byte_stable_across_passes() {
    let mut r = Rig::new(
        &healthy("UpToDate"),
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    // No file yet: written, adjusted once.
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert!(e.present && e.last_error.is_none());
    let first = r.res_file().unwrap();
    let mtime = std::fs::metadata(r.dir.join("pvc-1.res"))
        .unwrap()
        .modified()
        .unwrap();

    // Same input: no write, no command.
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    let w = r.pass(&[with_entries(volume(GIB), vec![e])], &[]).await;
    assert!(
        w.iter().all(|w| !matches!(w, StatusWrite::Volume { .. })),
        "{w:?}"
    );
    assert_eq!(r.res_file().unwrap(), first);
    let again = std::fs::metadata(r.dir.join("pvc-1.res"))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(mtime, again);
}

#[tokio::test]
async fn diskless_tiebreaker_creation() {
    let mut r = Rig::on("n3", &[], FakeHost::default());
    let x = r.x.clone();
    // The pool belongs to n1, so this node probes nothing.
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);
    let file = r.res_file().unwrap();
    assert!(file.contains("disk        none;"));
    assert!(!file.contains("/dev/vg1"));
    assert_eq!(file.matches("    connection\n").count(), 2);

    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    r.feed(&[
        "create resource name:pvc-1 role:Secondary suspended:no",
        "create device name:pvc-1 volume:0 minor:1000 disk:Diskless client:yes quorum:yes",
    ]);
    let vols = [Arc::new(with_entries(volume(GIB), vec![e]))];
    let e = entry_of(&r.rec.refresh(&vols).await);
    assert!(e.present);
    assert_eq!(e.disk_state.as_deref(), Some("Diskless"));
    assert_eq!(e.allocated_bytes, None);
    assert_eq!(e.device_path.as_deref(), Some("/dev/drbd1000"));
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
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("DRBD port 7000 on 10.0.0.3 is in use")
    );
}

#[tokio::test]
async fn failed_adjust_restores_the_backup() {
    let mut r = Rig::on("n3", &[], FakeHost::default());
    let x = r.x.clone();
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;
    let good = r.backup().unwrap();
    let mut v = volume(GIB);
    if let Some(a) = v.spec.assignment.as_mut() {
        a.replicas[1].port = Some(7005);
    }
    x.expect(argv::adjust("pvc-1"), code(10, "", "bad config"));
    let w = r.pass(&[v], &[]).await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("adjust pvc-1: drbdadm: exit 10: bad config")
    );
    assert_eq!(r.res_file().unwrap(), good);
}

#[tokio::test]
async fn peer_leaving() {
    let mut lines = healthy("UpToDate");
    lines.push(
        "exists connection name:pvc-1 peer-node-id:3 conn-name:n4 connection:Connected".into(),
    );
    lines.push("exists peer-device name:pvc-1 peer-node-id:3 conn-name:n4 volume:0 replication:Established peer-disk:UpToDate".into());
    let mut r = Rig::new(
        &lines,
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::del_peer("pvc-1", 3), ok(""))
        .expect(argv::forget_peer("pvc-1", 3), ok(""))
        .expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);

    // Both fail for a still connected diskful peer: the del-peer error stands.
    let mut r = Rig::new(
        &lines,
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
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
    let mut lines = healthy("UpToDate");
    lines[0] = "exists resource name:pvc-1 role:Secondary suspended:user".into();
    let mut r = Rig::new(
        &lines,
        FakeHost {
            magic: Mutex::new(vec![true]),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    std::fs::create_dir_all(r.dir.join(".backup")).unwrap();
    std::fs::write(r.dir.join("pvc-1.res"), "resource \"pvc-1\" {}").unwrap();
    std::fs::write(r.dir.join(".backup/pvc-1.res"), "resource \"pvc-1\" {}").unwrap();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::resume_io("pvc-1"), ok(""))
        .expect(argv::down("pvc-1"), ok(""))
        .expect(
            argv::status("pvc-1"),
            code(10, "", "pvc-1: No such resource"),
        )
        .expect(argv::wipe_md(MINOR, DEV), ok(""))
        .expect(lvm::argv::deactivate(&filter(), VG, "pvc-1"), ok(""))
        .expect(lvm::argv::lvremove(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[]);
    let w = r.pass(&[deleting(volume(GIB))], &[]).await;
    let e = entry_of(&w);
    assert!(!e.present && e.last_error.is_none());
    assert_eq!(e.observed_generation, Some(3));
    assert!(r.res_file().is_none() && r.backup().is_none());
}

#[tokio::test]
async fn down_that_does_not_stick_is_an_error() {
    let mut r = Rig::new(&healthy("UpToDate"), FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::down("pvc-1"), ok(""))
        .expect(argv::status("pvc-1"), ok("pvc-1 role:Secondary"));
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: true,
        ..ReplicaStatus::default()
    };
    let w = r
        .pass(&[deleting(with_entries(volume(GIB), vec![prev]))], &[])
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
async fn node_leaving_the_assignment_keeps_its_data() {
    // n1 vanishes from a live Volume: DRBD down, files gone, entry dropped,
    // the LV and its metadata untouched (no wipe-md, no lvremove).
    let mut r = Rig::new(
        &healthy("UpToDate"),
        FakeHost {
            magic: Mutex::new(vec![true]),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    std::fs::create_dir_all(r.dir.join(".backup")).unwrap();
    std::fs::write(r.dir.join("pvc-1.res"), "resource \"pvc-1\" {}").unwrap();
    std::fs::write(r.dir.join(".backup/pvc-1.res"), "resource \"pvc-1\" {}").unwrap();
    let mut v = volume(GIB);
    if let Some(a) = v.spec.assignment.as_mut() {
        a.replicas.retain(|r| r.node != NODE);
    }
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: true,
        ..ReplicaStatus::default()
    };
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::down("pvc-1"), ok(""))
        .expect(argv::status("pvc-1"), code(10, "", ""));
    // The bytes stay, the proof of their metadata goes.
    expect_lv_tags(&x, &[("pvc-1", &[OWN, "r-drbd.md", "other"])]);
    expect_change(&x, "pvc-1", &[], &[OWN, "r-drbd.md"]);
    let w = r.pass(&[with_entries(v.clone(), vec![prev])], &[]).await;
    let gone = w
        .iter()
        .find(|w| matches!(w, StatusWrite::VolumeGone { name } if name == "pvc-1"))
        .expect("entry dropped");
    assert_eq!(gone.body()["status"]["replicas"], serde_json::json!([]));
    assert!(r.res_file().is_none() && r.backup().is_none());
    let seen = x.seen.lock().unwrap().clone();
    assert!(
        !seen
            .iter()
            .any(|a| a.iter().any(|t| t == "lvremove" || t == "wipe-md")),
        "{seen:?}"
    );
    // Gone and forgotten: nothing more to do or write.
    r.tracker
        .lock()
        .unwrap()
        .feed("destroy resource name:pvc-1")
        .unwrap();
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    expect_lv_tags(&x, &[("pvc-1", &["other"])]);
    let w = r.pass(&[v], &[]).await;
    assert!(
        w.iter().all(|w| matches!(w, StatusWrite::Pool { .. })),
        "{w:?}"
    );
}

#[tokio::test]
async fn delete_confirms_down_when_events2_lost_track() {
    // Not tracked (stream restarted): status must say down before wipe and remove.
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::status("pvc-1"), ok("pvc-1 role:Secondary"));
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: true,
        ..ReplicaStatus::default()
    };
    let w = r
        .pass(&[deleting(with_entries(volume(GIB), vec![prev]))], &[])
        .await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("DRBD resource pvc-1 still up after down")
    );
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

/// gross_size_kib(2 GiB) = 2097424, rounded to the extent.
const LV2_KIB: u64 = 2_101_248;

#[tokio::test]
async fn resize_waits_for_every_peer() {
    let mut r = Rig::new(
        &healthy("UpToDate"),
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    std::fs::create_dir_all(&r.dir).unwrap();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(lvm::argv::lvresize(&filter(), VG, "pvc-1", LV2_KIB), ok(""));
    expect_lvs(&x, &[lv(LV2_KIB)]);
    // The first file write is not what this test is about.
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[grown(2 * GIB, 4, Some(LV_KIB))], &[]).await;
    let e = entry_of(&w);
    assert_eq!(e.allocated_bytes, Some(LV2_KIB * 1024));
    assert_eq!(e.observed_generation, Some(3), "waiting on n2");
    assert_eq!(e.last_error, None);
}

#[tokio::test]
async fn resize_runs_once_peers_have_grown() {
    let mut r = Rig::new(
        &healthy("UpToDate"),
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV2_KIB)]);
    x.expect(argv::wait_connect("pvc-1"), ok(""))
        .expect(argv::resize("pvc-1"), ok(""))
        .expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[grown(2 * GIB, 4, Some(LV2_KIB))], &[]).await;
    let e = entry_of(&w);
    assert_eq!(
        (e.observed_generation, e.last_error.as_deref()),
        (Some(4), None)
    );
}

#[tokio::test]
async fn only_the_lowest_node_id_resizes() {
    let mut r = Rig::on(
        "n2",
        &healthy("UpToDate"),
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    let mut pl = (*pool()).clone();
    pl.metadata.name = Some("pool-n2".into());
    pl.spec.node = "n2".into();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(lvm::argv::lvresize(&filter(), VG, "pvc-1", LV2_KIB), ok(""));
    expect_lvs(&x, &[lv(LV2_KIB)]);
    // Its file is new, and the healthy fixture names peers n2 and n3 from n1's view.
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r
        .rec
        .pass(
            &[Arc::new(pl)],
            &[Arc::new(grown(2 * GIB, 4, Some(LV2_KIB)))],
            &[],
        )
        .await;
    x.assert_done();
    let e = entry_of(&w);
    assert_eq!(
        (e.observed_generation, e.last_error.as_deref()),
        (Some(4), None)
    );
}

#[tokio::test]
async fn snapshot_rounds() {
    let lines = healthy("UpToDate");
    let host = || FakeHost {
        sectors: Mutex::new(NET_SECTORS),
        ..FakeHost::default()
    };
    let up = |x: &ScriptedExec| {
        reads(x, &[lv(LV_KIB)]);
        x.expect(argv::adjust("pvc-1"), ok(""));
    };
    let ack = |phase, ok| SnapshotAck {
        node: NODE.into(),
        phase,
        pool: Some(POOL.into()),
        ok,
        error: None,
    };

    let mut r = Rig::new(&lines, host());
    up(&r.x);
    r.x.expect(argv::suspend_io("pvc-1"), ok(""));
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Suspending, true)]);

    // Already suspended by the round: no second suspend, ack stays ok.
    let mut sus = lines.clone();
    sus[0] = "exists resource name:pvc-1 role:Secondary suspended:user".into();
    let mut r = Rig::new(&sus, host());
    up(&r.x);
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Suspending, true)]);

    let mut r = Rig::new(&sus, host());
    up(&r.x);
    expect_tags(&r.x, &["pvc-1"]);
    r.x.expect(
        lvm::argv::snapshot_thin(&filter(), VG, "pvc-1", "pvc-1_snap-1"),
        ok(""),
    );
    expect_change(&r.x, "pvc-1_snap-1", &[], &[OWN]);
    expect_lvs(
        &r.x,
        &[
            lv(LV_KIB),
            lv_row("pvc-1_snap-1", LV_KIB, "Vri-a-tz-k", "pvc-1"),
        ],
    );
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Taking)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Taking, true)]);

    let mut r = Rig::new(&sus, host());
    up(&r.x);
    r.x.expect(argv::resume_io("pvc-1"), ok(""));
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Resuming)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Resuming, true)]);

    let mut r = Rig::new(&lines, host());
    reads(
        &r.x,
        &[
            lv(LV_KIB),
            lv_row("pvc-1_snap-1", LV_KIB, "Vri---tz-k", "pvc-1"),
        ],
    );
    r.x.expect(argv::adjust("pvc-1"), ok(""))
        .expect(lvm::argv::lvremove(&filter(), VG, "pvc-1_snap-1"), ok(""));
    expect_lvs(&r.x, &[lv(LV_KIB)]);
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Deleting)])
        .await;
    assert_eq!(acks(&w), vec![ack(SnapshotPhase::Deleting, true)]);

    // A suspend no round accounts for (snapshot gone mid-round) is resumed.
    let mut r = Rig::new(&sus, host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.x.expect(argv::resume_io("pvc-1"), ok(""))
        .expect(argv::adjust("pvc-1"), ok(""));
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
            "exists device name:pvc-1 volume:0 minor:1000 disk:Diskless client:yes quorum:yes"
                .into(),
        ],
        FakeHost::default(),
    );
    r.x.expect(argv::adjust("pvc-1"), ok(""))
        .expect(argv::suspend_io("pvc-1"), ok(""));
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    let a = acks(&w);
    assert_eq!((a[0].ok, a[0].pool.as_deref()), (true, None));
    // Taking is diskful work only.
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Taking)])
        .await;
    assert_eq!(acks(&w), vec![]);
}

#[tokio::test]
async fn restore_recreates_the_foreign_metadata() {
    // The snapshot LV carries the old resource's metadata: no probe trusts it.
    let mut r = Rig::new(
        &[],
        FakeHost {
            magic: Mutex::new(vec![true]),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    let mut vol = volume(GIB);
    vol.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
    let mut snap = snapshot(SnapshotPhase::Ready);
    snap.spec.volume = "pvc-0".into();
    reads(&x, &[lv_row("pvc-0_snap-1", LV_KIB, "Vri---tz-k", "pvc-0")]);
    x.expect(
        lvm::argv::restore_thin_tagged(&filter(), VG, "pvc-0_snap-1", "pvc-1", Some(NEW)),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    // The copy inherits the source's tags, legacy and per node.
    expect_lv_tags(&x, &[("pvc-1", &[NEW, "r-drbd.md", "r-drbd.md.3"])]);
    expect_change(&x, "pvc-1", &[], &["r-drbd.md", "r-drbd.md.3"]);
    expect_lvs(
        &x,
        &[
            lv_row("pvc-0_snap-1", LV_KIB, "Vri---tz-k", "pvc-0"),
            lv(LV_KIB),
        ],
    );
    expect_new_tags(&x);
    expect_other_md(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    let current = drbd::fresh_gi(RANDOM);
    expect_set_gi(&x, 0, Some(&current));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[vol.clone()], &[snap.clone()]).await;
    assert_eq!(entry_of(&w).last_error, None);

    // Once this replica came up, its metadata is its own.
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: true,
        ..ReplicaStatus::default()
    };
    let mut r = Rig::new(
        &[],
        FakeHost {
            magic: Mutex::new(vec![true]),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &[]);
    x.expect(argv::get_gi(0, MINOR, DEV), ok(&ours()));
    expect_seal(&x, &[]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[with_entries(vol, vec![prev])], &[snap]).await;
}

#[tokio::test]
async fn corrupt_metadata_is_never_overwritten() {
    let mut r = Rig::new(
        &[],
        FakeHost {
            magic: Mutex::new(vec![true]),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &[]);
    x.expect(
        argv::get_gi(0, MINOR, DEV),
        code(255, "", "no valid meta data"),
    );
    let w = r.pass(&[volume(GIB)], &[]).await;
    let e = entry_of(&w);
    assert!(e.last_error.unwrap().starts_with("corrupt DRBD metadata"));
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

fn n2_pool() -> [Arc<Pool>; 1] {
    let mut pl = (*pool()).clone();
    pl.metadata.name = Some("pool-n2".into());
    pl.spec.node = "n2".into();
    [Arc::new(pl)]
}

fn split_brain(survivor_role: &str) -> Vec<String> {
    vec![
        format!("exists resource name:pvc-1 role:{survivor_role} suspended:no"),
        "exists connection name:pvc-1 peer-node-id:0 conn-name:n1 connection:StandAlone".into(),
        "exists connection name:pvc-1 peer-node-id:2 conn-name:n3 connection:Connected".into(),
        "exists device name:pvc-1 volume:0 minor:1000 disk:UpToDate client:no quorum:yes".into(),
    ]
}

#[tokio::test]
async fn resolve_discards_once_per_epoch() {
    let mut r = Rig::on(
        "n2",
        &split_brain("Secondary"),
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    let pl = n2_pool();
    let v = [Arc::new(resolving("n1", true))];
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::disconnect("pvc-1"), ok(""))
        .expect(argv::connect_discard("pvc-1"), ok(""));
    r.rec.pass(&pl, &v, &[]).await;
    x.assert_done();
    // Still StandAlone (events lag, or it failed): no second discard, plain adjust.
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.rec.pass(&pl, &v, &[]).await;
    x.assert_done();
}

#[tokio::test]
async fn resolve_refuses_a_published_replica() {
    let mut r = Rig::on(
        "n2",
        &split_brain("Primary"),
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    let w = r
        .rec
        .pass(&n2_pool(), &[Arc::new(resolving("n1", true))], &[])
        .await;
    x.assert_done();
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("resolve needs the volume unpublished on n2")
    );
}

#[tokio::test]
async fn resolve_needs_split_brain_and_a_diskful_survivor() {
    for v in [resolving("n1", false), resolving("n3", true)] {
        let mut r = Rig::on(
            "n2",
            &split_brain("Secondary"),
            FakeHost {
                sectors: Mutex::new(NET_SECTORS),
                ..FakeHost::default()
            },
        );
        let x = r.x.clone();
        reads(&x, &[lv(LV_KIB)]);
        // Only the normal StandAlone adjust, never a discard.
        x.expect(argv::adjust("pvc-1"), ok(""));
        r.rec.pass(&n2_pool(), &[Arc::new(v)], &[]).await;
        x.assert_done();
    }
}
#[tokio::test]
async fn resolve_survivor_reconnects() {
    let mut lines = split_brain("Secondary");
    lines[1] =
        "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:StandAlone".into();
    let mut r = Rig::new(
        &lines,
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::connect_peer("pvc-1", "n2"), ok(""));
    r.pass(&[resolving("n1", true)], &[]).await;
}

#[tokio::test]
async fn nothing_before_the_initial_dump() {
    let x = Arc::new(ScriptedExec::new());
    let dir = res_dir();
    let env = DrbdEnv::new(
        Some(versions()),
        &dir,
        Arc::new(Mutex::new(Tracker::default())),
    );
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

fn restored() -> (Volume, Snapshot) {
    let mut vol = volume(GIB);
    vol.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
    let mut snap = snapshot(SnapshotPhase::Ready);
    snap.spec.volume = "pvc-0".into();
    (vol, snap)
}

fn magic_host(answers: &[bool]) -> FakeHost {
    FakeHost {
        magic: Mutex::new(answers.to_vec()),
        ..FakeHost::default()
    }
}

#[tokio::test]
async fn restored_replica_survives_a_failed_pass() {
    // It held data and was in use; after a restart a failed pass reports
    // present false. Its own day-0 id on disk keeps create-md away both times.
    let (vol, snap) = restored();
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &[]);
    x.expect(argv::get_gi(0, MINOR, DEV), ok(&ours()));
    expect_seal(&x, &[]);
    x.expect(argv::adjust("pvc-1"), code(10, "", "bad"));
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: true,
        in_use: true,
        ..ReplicaStatus::default()
    };
    let w = r
        .pass(
            &[initialized(with_entries(vol.clone(), vec![prev]))],
            std::slice::from_ref(&snap),
        )
        .await;
    let e1 = entry_of(&w);
    assert!(!e1.present && e1.last_error.is_some());

    // Pass 1 sealed it: its own tag, no probe, no create-md.
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    expect_tags(&x, &["pvc-1"]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r
        .pass(&[initialized(with_entries(vol, vec![e1]))], &[snap])
        .await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn grown_while_attached_then_reboot_refuses_across_passes() {
    // The LV grew under an attached disk, the DRBD resize waited on peers,
    // the node rebooted: metadata is not at the end and check-resize cannot
    // find it. Refused, and the refusal sticks through the next pass.
    let mut r = Rig::new(&[], magic_host(&[false]));
    let x = r.x.clone();
    reads(&x, &[lv(LV2_KIB)]);
    expect_tags(&x, &[]);
    check_resize_fails(&x);
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: true,
        observed_generation: Some(3),
        ..ReplicaStatus::default()
    };
    let mut vol = initialized(volume(2 * GIB));
    vol.metadata.generation = Some(4);
    let w = r.pass(&[with_entries(vol.clone(), vec![prev])], &[]).await;
    let e1 = entry_of(&w);
    assert!(!e1.present);
    assert_eq!(
        e1.last_error.as_deref(),
        Some("metadata not at device end; refusing create-md on a replica that held data")
    );

    // Sticky: no probe, no create-md.
    expect_lvs(&x, &[lv(LV2_KIB)]);
    expect_vgs(&x);
    expect_tags(&x, &[]);
    let w = r.pass(&[with_entries(vol, vec![e1.clone()])], &[]).await;
    assert!(
        w.iter().all(|w| !matches!(w, StatusWrite::Volume { .. })),
        "{w:?}"
    );
}

#[tokio::test]
async fn check_resize_relocated_metadata_is_ours() {
    let mut r = Rig::new(&[], magic_host(&[false, true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV2_KIB)]);
    expect_tags(&x, &[]);
    x.expect(
        argv::check_resize(MINOR, DEV),
        ok("Internal drbd meta data successfully moved."),
    )
    .expect(argv::get_gi(0, MINOR, DEV), ok(&ours()));
    expect_seal(&x, &[]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let vol = with_entries(initialized(volume(2 * GIB)), vec![reported()]);
    let w = r.pass(&[vol], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn fresh_restore_creates_metadata_after_a_failed_pass() {
    // First pass: restore, then create-md fails; the restored LV is dropped.
    let (vol, snap) = restored();
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    let src = lv_row("pvc-0_snap-1", LV_KIB, "Vri---tz-k", "pvc-0");
    let restore = |x: &ScriptedExec| {
        x.expect(
            lvm::argv::restore_thin_tagged(&filter(), VG, "pvc-0_snap-1", "pvc-1", Some(NEW)),
            ok(""),
        )
        .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
        expect_lv_tags(x, &[("pvc-1", &[NEW])]);
    };
    reads(&x, std::slice::from_ref(&src));
    restore(&x);
    expect_lvs(&x, &[src.clone(), lv(LV_KIB)]);
    expect_other_md(&x);
    x.expect(
        argv::create_md("pvc-1", 7, &versions()),
        code(10, "", "io error"),
    )
    .expect(lvm::argv::deactivate(&filter(), VG, "pvc-1"), ok(""))
    .expect(lvm::argv::lvremove(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, std::slice::from_ref(&src));
    let w = r
        .pass(std::slice::from_ref(&vol), std::slice::from_ref(&snap))
        .await;
    let e1 = entry_of(&w);
    assert!(!e1.present && e1.last_error.is_some());

    // Second pass: restored again, metadata recreated as foreign.
    expect_lvs(&x, std::slice::from_ref(&src));
    expect_vgs(&x);
    restore(&x);
    expect_lvs(&x, &[src, lv(LV_KIB)]);
    expect_other_md(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_set_gi(&x, 0, Some(&drbd::fresh_gi(RANDOM)));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[with_entries(vol, vec![e1])], &[snap]).await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn replica_added_to_initialized_volume_creates_metadata() {
    // LV made this pass: just-created metadata, no set-gi; the adjust fails.
    let mut r = Rig::new(&[], magic_host(&[false, true]));
    let x = r.x.clone();
    reads(&x, &[]);
    x.expect(
        lvm::argv::lvcreate_thin_tagged(&filter(), VG, THIN, "pvc-1", LV_KIB, Some(NEW)),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), code(10, "", "bad"));
    let vol = initialized(volume(GIB));
    let w = r.pass(std::slice::from_ref(&vol), &[]).await;
    let e1 = entry_of(&w);
    assert!(!e1.present && e1.last_error.is_some());

    // Next pass: the tag proves the metadata it made; no second create-md.
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    expect_tags(&x, &["pvc-1"]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[with_entries(vol, vec![e1])], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn metadata_of_another_volume_refuses() {
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &[]);
    expect_other_md(&x);
    let vol = volume(GIB);
    let w = r.pass(std::slice::from_ref(&vol), &[]).await;
    let e1 = entry_of(&w);
    assert_eq!(
        e1.last_error.as_deref(),
        Some("metadata of another volume on the LV")
    );
    // Sticky across the next pass, no probe.
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    expect_tags(&x, &[]);
    let w = r.pass(&[with_entries(vol, vec![e1])], &[]).await;
    assert!(
        w.iter().all(|w| !matches!(w, StatusWrite::Volume { .. })),
        "{w:?}"
    );
}

#[test]
fn ids_from_get_gi_and_dump_md() {
    use super::{gi_ids, same_id};
    assert_eq!(
        gi_ids("A1B2C3D4E5F60719:5F3E2D1C0B0A0908:0000000000000000:0000000000000000:1:1:0"),
        vec![0xA1B2_C3D4_E5F6_0719, 0x5F3E_2D1C_0B0A_0908, 0, 0]
    );
    assert_eq!(
        gi_ids("current-uuid 0xA1B2C3D4E5F60718;\n        0x5F3E2D1C0B0A0908; 0x0000000000000000;"),
        vec![0xA1B2_C3D4_E5F6_0718, 0x5F3E_2D1C_0B0A_0908, 0]
    );
    assert!(same_id(0xA1B2_C3D4_E5F6_0719, 0xA1B2_C3D4_E5F6_0718));
}

#[tokio::test]
async fn source_metadata_on_an_initialized_volume_is_refused() {
    // Restore source set, but the LV was not made this pass and the volume
    // already held data: another volume's ids mean refuse, not overwrite.
    let (vol, snap) = restored();
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &[]);
    expect_other_md(&x);
    let w = r.pass(&[initialized(vol)], &[snap]).await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some("metadata of another volume on the LV")
    );
}

#[tokio::test]
async fn tagged_lv_is_ours_without_a_probe() {
    // dump-superblock would show nothing of ours (ids aged out); the tag decides.
    let (vol, snap) = restored();
    let mut r = Rig::new(&[], magic_host(&[false]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_tags(&x, &["pvc-1"]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[initialized(vol)], &[snap]).await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn restored_copy_that_cannot_be_untagged_is_dropped() {
    let (vol, snap) = restored();
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    let src = lv_row("pvc-0_snap-1", LV_KIB, "Vri---tz-k", "pvc-0");
    let retag = storage::lvchange_tags(&filter(), &[], &[OWN], VG, "pvc-1");
    reads(&x, std::slice::from_ref(&src));
    x.expect(
        lvm::argv::restore_thin_tagged(&filter(), VG, "pvc-0_snap-1", "pvc-1", Some(NEW)),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_tags(&x, &["pvc-1"]);
    x.expect(retag.clone(), code(5, "", "failed"));
    // The fresh-filter retry of every LVM command.
    expect_pvdisplay(&x);
    x.expect(retag, code(5, "", "failed"));
    expect_lvs(&x, &[src.clone(), lv(LV_KIB)]);
    x.expect(lvm::argv::deactivate(&filter(), VG, "pvc-1"), ok(""))
        .expect(lvm::argv::lvremove(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[src]);
    let w = r.pass(&[vol], &[snap]).await;
    let e = entry_of(&w).last_error.unwrap();
    assert!(
        e.starts_with("remove inherited metadata tags from vg1/pvc-1"),
        "{e}"
    );
}

#[tokio::test]
async fn tag_clears_a_sticky_refusal() {
    let mut r = Rig::new(&[], magic_host(&[false]));
    let x = r.x.clone();
    reads(&x, &[lv(LV2_KIB)]);
    expect_tags(&x, &["pvc-1"]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let prev = ReplicaStatus {
        node: NODE.into(),
        last_error: Some(
            "metadata not at device end; refusing create-md on a replica that held data".into(),
        ),
        ..ReplicaStatus::default()
    };
    let w = r
        .pass(
            &[initialized(with_entries(volume(2 * GIB), vec![prev]))],
            &[],
        )
        .await;
    assert_eq!(entry_of(&w).last_error, None);
}

/// Captured on a test host (drbd-utils 9.35.0) after create-md and one set-gi;
/// the peers list is cut after two of its entries.
const SUPERBLOCK_JSON: &str = r#"{
  "effective_size": 0,
  "current_uuid": "0xA1B2C3D4E5F60718",
  "device_uuid": "0x0000000000000000",
  "flags": "0x00000091",
  "magic": "0x8374026D",
  "md_size_sect": 528,
  "al_offset": -64,
  "al_nr_extents": 257,
  "bm_offset": -520,
  "bm_bytes_per_bit": 4096,
  "la_peer_max_bio_size": 0,
  "al_stripes": 1,
  "al_stripe_size_4k": 8,
  "peers": [
    { "bitmap_uuid": "0x5F3E2D1C0B0A0908",  "bitmap_dagtag": 0,  "flags": "0x00000000",  "bitmap_index": -1,  "reserved_u32": [ "0x00000000", "0x00000000" ] },
    { "bitmap_uuid": "0x0000000000000000",  "bitmap_dagtag": 0,  "flags": "0x00000000",  "bitmap_index": -1,  "reserved_u32": [ "0x00000000", "0x00000000" ] }
  ]
}"#;

#[test]
fn ids_from_a_real_superblock() {
    use super::gi_ids;
    let ids = gi_ids(SUPERBLOCK_JSON);
    assert_eq!(
        ids,
        vec![0xA1B2_C3D4_E5F6_0718, 0, 0x5F3E_2D1C_0B0A_0908, 0]
    );
}

/// n1's view: n2 StandAlone, n3 Connected.
fn standalone_n2() -> Vec<String> {
    let mut l = healthy("UpToDate");
    l[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:StandAlone".into();
    l
}

fn up_host() -> FakeHost {
    FakeHost {
        sectors: Mutex::new(NET_SECTORS),
        ..FakeHost::default()
    }
}

#[tokio::test]
async fn split_brain_stops_the_reconnect_loop() {
    let mut r = Rig::new(&standalone_n2(), up_host());
    r.rec.drbd.adjust_interval = Duration::ZERO;
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;

    // DRBD reports the split brain: StandAlone is now left to spec.resolve.
    r.feed(&[
        "call helper name:pvc-1 peer-node-id:1 conn-name:n2 helper:split-brain",
        "response helper name:pvc-1 peer-node-id:1 conn-name:n2 helper:split-brain status:0",
        "change connection name:pvc-1 conn-name:n2 connection:StandAlone",
    ]);
    for _ in 0..3 {
        expect_lvs(&x, &[lv(LV_KIB)]);
        expect_vgs(&x);
        r.pass(&[volume(GIB)], &[]).await;
    }

    // Without the helper event the same StandAlone is adjusted every time
    // the interval allows (zero here).
    let mut r = Rig::new(&standalone_n2(), up_host());
    r.rec.drbd.adjust_interval = Duration::ZERO;
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;
}

#[tokio::test]
async fn state_driven_adjust_is_rate_limited() {
    let mut r = Rig::new(&standalone_n2(), up_host());
    r.rec.drbd.adjust_interval = Duration::from_millis(300);
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &[]).await;
    // Same state within the interval: no adjust.
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    r.pass(&[volume(GIB)], &[]).await;
    // A changed resource file is explicit: adjust at once.
    let mut moved = volume(GIB);
    if let Some(a) = moved.spec.assignment.as_mut() {
        a.replicas[1].port = Some(7005);
    }
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(std::slice::from_ref(&moved), &[]).await;
    // Once the interval passed, the state triggers it again.
    tokio::time::sleep(Duration::from_millis(400)).await;
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_vgs(&x);
    x.expect(argv::adjust("pvc-1"), ok(""));
    r.pass(&[moved], &[]).await;
}

#[tokio::test]
async fn resolve_runs_after_a_split_brain() {
    let mut r = Rig::on("n2", &split_brain("Secondary"), up_host());
    let x = r.x.clone();
    r.feed(&["call helper name:pvc-1 peer-node-id:0 conn-name:n1 helper:split-brain"]);
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::disconnect("pvc-1"), ok(""))
        .expect(argv::connect_discard("pvc-1"), ok(""));
    r.rec
        .pass(&n2_pool(), &[Arc::new(resolving("n1", true))], &[])
        .await;
    x.assert_done();
}

/// lvcreate of pvc-1 at the gross size, then the new tag.
fn expect_plain_create(x: &ScriptedExec) {
    x.expect(
        lvm::argv::lvcreate_thin_tagged(&filter(), VG, THIN, "pvc-1", LV_KIB, Some(NEW)),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
}

fn addresses_without_n2() -> BTreeMap<String, IpAddr> {
    [("n1", "10.0.0.1"), ("n3", "10.0.0.3")]
        .into_iter()
        .map(|(n, a)| (n.to_owned(), a.parse().unwrap()))
        .collect()
}

#[tokio::test]
async fn new_lv_cut_short_before_metadata_is_finished_next_pass() {
    // Rebuild: the LV is made and tagged new, then the resource file
    // fails (n2 has no address yet). The next pass creates the metadata
    // instead of refusing it as not at the device end.
    let mut r = Rig::new(&[], FakeHost::default());
    r.rec.set_addresses(addresses_without_n2());
    let x = r.x.clone();
    reads(&x, &[]);
    expect_plain_create(&x);
    expect_lvs(&x, &[lv(LV_KIB)]);
    let vol = with_entries(initialized(volume(GIB)), vec![reported()]);
    let w = r.pass(std::slice::from_ref(&vol), &[]).await;
    let e1 = entry_of(&w);
    let err = e1.last_error.clone().unwrap();
    assert!(
        err.starts_with(crate::reconcile::LV_LOST) && err.ends_with("node n2 has no InternalIP"),
        "{err}"
    );

    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[with_entries(vol.clone(), vec![e1])], &[]).await;
    assert_no_set_gi(&x);
    assert_eq!(entry_of(&w).last_error, None);

    // Late placement: same cut, no report; still never MD_NOT_AT_END.
    let mut r = Rig::new(&[], FakeHost::default());
    r.rec.set_addresses(addresses_without_n2());
    let x = r.x.clone();
    reads(&x, &[]);
    expect_plain_create(&x);
    expect_lvs(&x, &[lv(LV_KIB)]);
    let late = initialized(volume(GIB));
    let w = r.pass(std::slice::from_ref(&late), &[]).await;
    let e1 = entry_of(&w);
    assert_eq!(e1.last_error.as_deref(), Some("node n2 has no InternalIP"));
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let mut late = late;
    if let Some(st) = late.status.as_mut() {
        st.replicas = vec![e1];
    }
    let w = r.pass(&[late], &[]).await;
    assert_no_set_gi(&x);
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn departed_node_placed_again_wipes_its_stale_metadata() {
    // Our day-0 id on an LV with no tag, no report, after initializedAt:
    // the data predates the peers' writes. Wiped, just created, full sync.
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[])]);
    x.expect(argv::get_gi(0, MINOR, DEV), ok(&ours()));
    // Fresh before the wipe.
    expect_change(&x, "pvc-1", &[NEW], &[]);
    x.expect(argv::wipe_md(MINOR, DEV), ok(""))
        .expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[initialized(volume(GIB))], &[]).await;
    assert_no_set_gi(&x);
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn crash_after_a_stale_wipe_leaves_a_fresh_lv() {
    // The wipe ran, create-md failed. The LV carries the new tag, so
    // the next pass creates metadata instead of refusing MD_NOT_AT_END.
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[storage::MD_TAG])]);
    x.expect(argv::get_gi(0, MINOR, DEV), ok(&ours()));
    expect_change(&x, "pvc-1", &[NEW], &[storage::MD_TAG]);
    x.expect(argv::wipe_md(MINOR, DEV), ok("")).expect(
        argv::create_md("pvc-1", 7, &versions()),
        code(10, "", "io error"),
    );
    let vol = initialized(volume(GIB));
    let w = r.pass(std::slice::from_ref(&vol), &[]).await;
    let e1 = entry_of(&w);
    assert!(
        e1.last_error.as_deref().unwrap().starts_with("create-md"),
        "{e1:?}"
    );
    assert!(lv_names_seen(&x, "lvremove").is_empty(), "LV kept");

    let mut r = Rig::new(&[], magic_host(&[false]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let mut vol = vol;
    if let Some(st) = vol.status.as_mut() {
        st.replicas = vec![e1];
    }
    let w = r.pass(&[vol], &[]).await;
    assert_no_set_gi(&x);
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn our_ids_on_a_new_tagged_lv_are_stale() {
    // An LV never attached proves nothing by its ids, ours included
    // (a crash between set-gi and the seal): wiped and created again.
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::get_gi(0, MINOR, DEV), ok(&ours()))
        .expect(argv::wipe_md(MINOR, DEV), ok(""))
        .expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_set_gi(&x, 0, Some(&drbd::fresh_gi(RANDOM)));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    // Not initialized, no entry: the initial node mints a new current id.
    let w = r.pass(&[volume(GIB)], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[test]
fn sticky_refusals_seen_through_a_rebuild_wrap() {
    use crate::reconcile::{rebuild_error, unwrap_rebuild};
    assert_eq!(
        unwrap_rebuild(&rebuild_error(super::MD_NOT_AT_END)),
        super::MD_NOT_AT_END
    );
    assert_eq!(
        unwrap_rebuild(super::MD_OTHER_VOLUME),
        super::MD_OTHER_VOLUME
    );
    assert_eq!(
        unwrap_rebuild(crate::reconcile::LV_LOST),
        crate::reconcile::LV_LOST
    );
}

fn lv_names_seen(x: &ScriptedExec, program: &str) -> Vec<Vec<String>> {
    x.seen
        .lock()
        .unwrap()
        .iter()
        .filter(|a| a[0] == program)
        .cloned()
        .collect()
}

#[tokio::test]
async fn metadata_tag_of_another_node_id_proves_nothing() {
    // r-drbd.md.1 on n1 (node id 0) is treated as no tag at all.
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &["r-drbd.md.1"])]);
    x.expect(argv::get_gi(0, MINOR, DEV), ok(&ours()));
    expect_change(&x, "pvc-1", &[NEW], &["r-drbd.md.1"]);
    x.expect(argv::wipe_md(MINOR, DEV), ok(""))
        .expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r.pass(&[initialized(volume(GIB))], &[]).await;
    assert_no_set_gi(&x);
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn legacy_tag_with_a_report_is_retagged_only() {
    // The legacy tag plus our own report: no probe, no create-md.
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[storage::MD_TAG])]);
    expect_seal(&x, &[storage::MD_TAG]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let vol = with_entries(initialized(volume(GIB)), vec![reported()]);
    let w = r.pass(&[vol], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn legacy_tag_with_an_error_entry_is_resealed_never_wiped() {
    // Present false after a failed adjust, after initializedAt: the
    // entry exists, so the data is ours. Reseal, no probe, no wipe.
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[storage::MD_TAG])]);
    expect_seal(&x, &[storage::MD_TAG]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let prev = ReplicaStatus {
        node: NODE.into(),
        present: false,
        last_error: Some("adjust pvc-1: exit 10: bad".into()),
        ..ReplicaStatus::default()
    };
    let vol = with_entries(initialized(volume(GIB)), vec![prev]);
    let w = r.pass(&[vol], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);
}

#[tokio::test]
async fn untagged_ours_with_an_error_entry_is_adopted() {
    // No tag, probe shows our day-0 id, an error entry exists: adopted.
    let mut r = Rig::new(&[], magic_host(&[true]));
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[])]);
    x.expect(argv::get_gi(0, MINOR, DEV), ok(&ours()));
    expect_seal(&x, &[]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let prev = ReplicaStatus {
        node: NODE.into(),
        last_error: Some("adjust pvc-1: exit 10: bad".into()),
        ..ReplicaStatus::default()
    };
    let vol = with_entries(initialized(volume(GIB)), vec![prev]);
    let w = r.pass(&[vol], &[]).await;
    assert_eq!(entry_of(&w).last_error, None);
    let seen = x.seen.lock().unwrap();
    assert!(!seen.iter().any(|c| c.iter().any(|w| w == "wipe-md")));
}

#[tokio::test]
async fn no_snapshot_of_a_replica_rebuilt_in_the_same_pass() {
    // Taking in the pass that rebuilt the LV acks not ok, copies nothing.
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[]);
    expect_plain_create(&x);
    expect_lvs(&x, &[lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let vol = with_entries(initialized(volume(GIB)), vec![reported()]);
    let w = r.pass(&[vol], &[snapshot(SnapshotPhase::Taking)]).await;
    let a = acks(&w);
    assert!(!a[0].ok, "{a:?}");
    assert!(
        a[0].error.as_deref().unwrap().contains("created this pass"),
        "{a:?}"
    );
    assert!(
        !x.seen
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.iter().any(|w| w == "--snapshot"))
    );
}

#[tokio::test]
async fn no_snapshot_of_a_disk_that_is_not_up_to_date() {
    // A SyncTarget copy is no data either.
    let mut r = Rig::new(
        &healthy("Inconsistent"),
        FakeHost {
            sectors: Mutex::new(NET_SECTORS),
            ..FakeHost::default()
        },
    );
    let x = r.x.clone();
    reads(&x, &[lv(LV_KIB)]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Taking)])
        .await;
    let a = acks(&w);
    assert!(!a[0].ok, "{a:?}");
    assert_eq!(
        a[0].error.as_deref(),
        Some("local DRBD disk is Inconsistent, not UpToDate")
    );
}

#[tokio::test]
async fn failed_rebuild_stays_a_rebuild() {
    // create-md fails after the rebuild; the next pass rebuilds again,
    // plain lvcreate (never a restore), no set-gi, though initializedAt is unset.
    let (vol, snap) = restored();
    let src = lv_row("pvc-0_snap-1", LV_KIB, "Vri---tz-k", "pvc-0");
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, std::slice::from_ref(&src));
    expect_plain_create(&x);
    expect_lvs(&x, &[src.clone(), lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(
        argv::create_md("pvc-1", 7, &versions()),
        code(10, "", "io error"),
    )
    .expect(lvm::argv::deactivate(&filter(), VG, "pvc-1"), ok(""))
    .expect(lvm::argv::lvremove(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, std::slice::from_ref(&src));
    let vol = with_entries(vol, vec![reported()]);
    let w = r
        .pass(std::slice::from_ref(&vol), std::slice::from_ref(&snap))
        .await;
    let e1 = entry_of(&w);
    let err = e1.last_error.clone().unwrap();
    assert!(
        err.starts_with(crate::reconcile::LV_LOST) && err.contains("; rebuild: create-md"),
        "{err}"
    );
    assert!(!e1.present);

    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, std::slice::from_ref(&src));
    expect_plain_create(&x);
    expect_lvs(&x, &[src, lv(LV_KIB)]);
    expect_new_tags(&x);
    x.expect(argv::create_md("pvc-1", 7, &versions()), ok(""));
    expect_seal(&x, &[NEW]);
    x.expect(argv::adjust("pvc-1"), ok(""));
    let mut vol = vol;
    if let Some(st) = vol.status.as_mut() {
        st.replicas = vec![e1];
    }
    let w = r.pass(&[vol], &[snap]).await;
    assert_no_set_gi(&x);
    assert_eq!(entry_of(&w).last_error, None);
}
