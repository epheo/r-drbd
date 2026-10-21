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
const SNAP_UID: &str = "snap-uid-1";
/// The restore's proof that an LV came from snap-1.
const FROM: &str = "r-drbd.from.uid-1.snap-uid-1";
/// The snap tag of the restore source, snap-1 of pvc-0.
const SOURCE_TAG: &str = "r-drbd.snap.snap-uid-1.1073741824.2";
/// The uid and snap tag of snap-1 of pvc-1, the round of these tests.
const ROUND_UID: &str = "round-uid-1";
const ROUND_TAG: &str = "r-drbd.snap.round-uid-1.1073741824.2";
const ALL: Features = Features {
    tiebreaker: true,
    discard_granularity: true,
    suspended_primary_outdated: true,
};

#[test]
fn source_start_warns_only_when_unexpected() {
    use super::{SourceStart, source_start_cause as cause};
    assert_eq!(cause(&[], true, Some(false)), SourceStart::CopiesDiffer);
    assert_eq!(cause(&["n2"], true, Some(false)), SourceStart::CopiesDiffer);
    assert_eq!(cause(&[], true, None), SourceStart::DigestsPending);
    assert_eq!(cause(&["n2"], true, None), SourceStart::NotAllUsable);
    assert_eq!(cause(&["n2"], true, Some(true)), SourceStart::NotAllUsable);
    assert_eq!(cause(&[], false, Some(false)), SourceStart::NotAllUsable);
    assert_eq!(cause(&[], false, Some(true)), SourceStart::NotAllUsable);
}

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

/// Live reads for the tests: by default what the pass was given, the API
/// server agreeing with the cache; a test may script another answer.
#[derive(Debug, Default)]
struct ScriptedLive {
    echo: Mutex<Option<Volume>>,
    scripted: Mutex<Option<Result<Volume, String>>>,
    reads: AtomicU32,
}

#[async_trait::async_trait]
impl crate::reconcile::LiveVolumes for ScriptedLive {
    async fn get(&self, _: &str) -> Result<Volume, String> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if let Some(r) = self.scripted.lock().unwrap().clone() {
            return r;
        }
        self.echo
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "nothing to read".to_owned())
    }
}

struct Rig {
    live: Arc<ScriptedLive>,
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
        let live = Arc::new(ScriptedLive::default());
        let mut rec = Reconciler::new(node, x.clone())
            .with_drbd(env)
            .with_live(live.clone());
        rec.set_addresses(addresses(&["n1", "n2", "n3"]));
        Self {
            live,
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
        *self.live.echo.lock().unwrap() = vols.first().cloned();
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
        snapshot_uid: Some(SNAP_UID.into()),
    });
    v
}

fn source_snapshot() -> Snapshot {
    let mut s = snapshot(SnapshotPhase::Ready);
    s.spec.volume = "pvc-0".into();
    s.spec.volume_uid = Some("uid-0".into());
    s.metadata.uid = Some(SNAP_UID.into());
    let st = s.status.as_mut().unwrap();
    st.size_bytes = Some(GIB);
    st.copies_identical = Some(false);
    st.acks = ["n1", "n2"]
        .map(|n| SnapshotAck {
            node: n.into(),
            phase: SnapshotPhase::Taking,
            taken_pool: Some(format!("pool-{n}")),
            lv_bytes: None,
            allocated_bytes: None,
            lv_created_at: None,
            found: None,
            searched_pools: Vec::new(),
            tag_size_bytes: None,
            tag_source_replicas: None,
            digest: None,
            digest_bytes: None,
            ok: true,
            error: None,
        })
        .to_vec();
    s
}

/// The source as an older version made it Ready: acks in a removed phase.
fn old_source_snapshot() -> Snapshot {
    let mut s = source_snapshot();
    for a in &mut s.status.as_mut().unwrap().acks {
        a.phase = SnapshotPhase::Unknown;
    }
    s
}

/// Copies proven identical, whatever the Snapshot's status says.
#[allow(clippy::unnecessary_wraps)]
fn proven(_: &Snapshot) -> Option<bool> {
    Some(true)
}

/// A peer's entry; `restored` sets restoredFrom to the source.
fn peer_entry(node: &str, restored: bool) -> ReplicaStatus {
    ReplicaStatus {
        node: node.into(),
        present: true,
        disk_state: Some("Inconsistent".into()),
        device_path: Some("/dev/drbd50000".into()),
        restored_from: restored.then(|| "snap-1".into()),
        ..ReplicaStatus::default()
    }
}

/// The restore lvcreate of pvc-1 from this node's copy, with both tags,
/// then the fresh read.
fn expect_restore(x: &ScriptedExec) {
    expect_source_tags(x);
    x.expect(
        lvm::cmd::restore(VG, "pvc-0+snap-1", "pvc-1", &[NEW, FROM]),
        ok(""),
    );
    expect_lv_tags(
        x,
        &[("pvc-0+snap-1", &[SOURCE_TAG]), ("pvc-1", &[NEW, FROM])],
    );
    expect_lvs(x, &[source_lv(), lv(LV_KIB)]);
}

/// The source copy carries its Snapshot's tag, read before a restore.
fn expect_source_tags(x: &ScriptedExec) {
    expect_lv_tags(x, &[("pvc-0+snap-1", &[SOURCE_TAG])]);
}

fn empty_create() -> Vec<String> {
    lvm::cmd::create_thin(VG, THIN, "pvc-1", LV_KIB * 1024, &[NEW])
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
            volume_uid: Some(UID.into()),
        },
    );
    s.metadata.uid = Some(ROUND_UID.into());
    s.status = Some(SnapshotStatus {
        phase: Some(phase),
        size_bytes: Some(GIB),
        source_replicas: Some(2),
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

fn times(x: &ScriptedExec, word: &str) -> usize {
    x.seen
        .lock()
        .unwrap()
        .iter()
        .filter(|a| a.iter().any(|w| w == word))
        .count()
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

/// The chunk comes from this pass's lvs: a Pool status without chunkBytes
/// (a failed probe) leaves the file as it is.
#[tokio::test]
async fn chunk_from_lvs_keeps_the_file() {
    let mut r = Rig::new(&healthy(), up_host());
    let x = r.x.clone();
    let vol = initialized(volume(GIB));
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    r.pass(std::slice::from_ref(&vol), &[]).await;
    let before = r.res_file().unwrap();
    assert!(before.contains("rs-discard-granularity 65536;"));

    let mut p = (*pool()).clone();
    p.status = None;
    reads_again(&x, &[lv(LV_KIB)]);
    dry(&x, "");
    r.pass_pools(&[Arc::new(p)], &[vol], &[]).await;
    assert_eq!(r.res_file().unwrap(), before);
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
async fn restore_every_holder_from_its_copy() {
    // Both diskful members restore from their own snapshot LV; the proof
    // tag survives the metadata tag swap.
    for (node, own) in [("n1", OWN), ("n2", "r-drbd.md.uid-1.1")] {
        let mut r = Rig::on(node, &[], FakeHost::default());
        r.rec.drbd.copies_identical = proven;
        let x = r.x.clone();
        reads(&x, &[source_lv()]);
        expect_restore(&x);
        r.expect_check();
        expect_lv_tags(&x, &[("pvc-1", &[NEW, FROM])]);
        expect_md_as(&x, own, &[NEW]);
        up_plan(&x);
        let w = r
            .pass_pools(
                &[pool_on(node)],
                &[restored(volume(GIB))],
                &[source_snapshot()],
            )
            .await;
        let e = entry_of(&w);
        assert_eq!(e.last_error, None, "{node}");
        assert_eq!(e.restored_from.as_deref(), Some("snap-1"), "{node}");
        assert!(
            !x.seen.lock().unwrap().contains(&empty_create()),
            "{node}: never an empty LV"
        );
        assert!(!seen(&x, "new-current-uuid"), "{node}");
    }
    // The tiebreaker reads no LVM and creates nothing.
    let mut r = Rig::on("n3", &[], FakeHost::default());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    r.expect_check();
    up_plan(&x);
    let w = r.pass(&[restored(volume(GIB))], &[source_snapshot()]).await;
    let e = entry_of(&w);
    assert_eq!((e.last_error, e.restored_from), (None, None));
    assert!(!seen(&x, "lvcreate"));
}

#[tokio::test]
async fn restore_starts_without_sync_when_every_peer_restored() {
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(
        vec![
            "drbdadm".into(),
            "new-current-uuid".into(),
            "--clear-bitmap".into(),
            "pvc-1/0".into(),
        ],
        ok(""),
    );
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
    let w = r.pass(&[vol], &[source_snapshot()]).await;
    assert_eq!(entry_of(&w).restored_from.as_deref(), Some("snap-1"));
    assert!(!seen(&x, "--force-resync"));
    assert!(r.rec.init_started.is_empty());
}

#[tokio::test]
async fn restore_falls_back_to_a_full_sync_when_a_peer_did_not_restore() {
    // n2 is connected and Inconsistent, but its entry has no restoredFrom:
    // it may be empty, so the bitmap is never cleared.
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.drbd.copies_identical = proven;
    r.rec.drbd.init_wait = Duration::from_millis(100);
    let x = r.x.clone();
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", false)]);
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    r.pass(std::slice::from_ref(&vol), &[source_snapshot()])
        .await;
    assert!(!seen(&x, "new-current-uuid"));

    tokio::time::sleep(Duration::from_millis(150)).await;
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(
        vec![
            "drbdadm".into(),
            "new-current-uuid".into(),
            "--force-resync".into(),
            "pvc-1/0".into(),
        ],
        ok(""),
    );
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(!seen(&x, "--clear-bitmap"));
}

#[tokio::test]
async fn restore_waits_for_its_peers_before_any_uuid_step() {
    // n2 restored but is not connected yet; then connected but not yet
    // reporting restoredFrom. Neither moves the UUIDs before init_wait.
    let mut lines = up("Inconsistent", "DUnknown");
    lines[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting role:Unknown".into();
    let mut r = Rig::new(&lines, up_host());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(r.rec.init_started["pvc-1"].is_some());

    r.feed(&[
        "change connection name:pvc-1 conn-name:n2 connection:Connected",
        "change peer-device name:pvc-1 conn-name:n2 volume:0 replication:Established peer-disk:Inconsistent",
    ]);
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", false)]);
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(!seen(&x, "new-current-uuid"));
}

/// The source with its holders' digests not all in yet.
fn pending_snapshot() -> Snapshot {
    let mut s = source_snapshot();
    s.status.as_mut().unwrap().copies_identical = None;
    s
}

fn start_argv(flag: &str) -> Vec<String> {
    ["drbdadm", "new-current-uuid", flag, "pvc-1/0"]
        .map(str::to_owned)
        .to_vec()
}

#[tokio::test]
async fn a_peer_restores_its_copy_while_digests_are_pending() {
    let mut r = Rig::on("n2", &[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    expect_restore(&x);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW, FROM])]);
    expect_md_as(&x, "r-drbd.md.uid-1.1", &[NEW]);
    up_plan(&x);
    let w = r
        .pass_pools(
            &[pool_on("n2")],
            &[restored(volume(GIB))],
            &[pending_snapshot()],
        )
        .await;
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);
    assert_eq!(e.restored_from.as_deref(), Some("snap-1"));
    assert!(!x.seen.lock().unwrap().contains(&empty_create()));
}

/// Every peer proven, digests pending: the initial node waits for them,
/// then past the bound starts as the sync source.
#[tokio::test]
async fn pending_digests_past_their_bound_start_as_the_source() {
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.drbd.digest_wait = Duration::from_millis(100);
    let x = r.x.clone();
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    r.pass(std::slice::from_ref(&vol), &[pending_snapshot()])
        .await;
    assert!(!seen(&x, "new-current-uuid"));
    assert!(r.rec.init_started["pvc-1"].is_some());
    assert_eq!(r.live.reads.load(Ordering::Relaxed), 0, "no proof read yet");

    tokio::time::sleep(Duration::from_millis(150)).await;
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(start_argv("--force-resync"), ok(""));
    r.pass(&[vol], &[pending_snapshot()]).await;
    assert!(seen(&x, "--force-resync") && !seen(&x, "--clear-bitmap"));
    assert!(r.rec.init_started.is_empty());
}

/// Digests that arrive within the bound and match: no sync.
#[tokio::test]
async fn digests_proven_within_their_bound_clear_the_bitmap() {
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    let x = r.x.clone();
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    r.pass(std::slice::from_ref(&vol), &[pending_snapshot()])
        .await;
    assert!(!seen(&x, "new-current-uuid"));

    let mut snap = pending_snapshot();
    snap.status.as_mut().unwrap().copies_identical = Some(true);
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(start_argv("--clear-bitmap"), ok(""));
    r.pass(&[vol], &[snap]).await;
    assert!(seen(&x, "--clear-bitmap") && !seen(&x, "--force-resync"));
    assert_eq!(r.live.reads.load(Ordering::Relaxed), 1, "one proof read");
}

/// Copies that differ: the source at once, without waiting for a peer
/// that has not connected.
#[tokio::test]
async fn differing_copies_start_as_the_source_at_once() {
    let mut lines = up("Inconsistent", "DUnknown");
    lines[1] = "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connecting role:Unknown".into();
    let mut r = Rig::new(&lines, up_host());
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(start_argv("--force-resync"), ok(""));
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(seen(&x, "--force-resync") && !seen(&x, "--clear-bitmap"));
    assert!(r.rec.init_started.is_empty());
}

/// n2's entry: restored, its LV at `kib`.
fn restored_peer(kib: u64) -> ReplicaStatus {
    ReplicaStatus {
        allocated_bytes: Some(kib * 1024),
        ..peer_entry("n2", true)
    }
}

#[tokio::test]
async fn larger_restore_starts_at_the_snapshot_size_then_grows() {
    let mut r = Rig::new(&[], FakeHost::default());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    let vol = |peer_kib| with_entries(restored(volume(2 * GIB)), vec![restored_peer(peer_kib)]);
    // At the snapshot's size: no size on the lvcreate, no lvextend.
    reads(&x, &[source_lv()]);
    expect_restore(&x);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW, FROM])]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    let w = r.pass(&[vol(LV_KIB)], &[source_snapshot()]).await;
    let e = entry_of(&w);
    assert_eq!(e.allocated_bytes, Some(LV_KIB * 1024));
    assert_eq!(
        e.observed_generation, None,
        "not applied at the snapshot size"
    );
    assert!(!seen(&x, "lvextend"));

    // Up, Inconsistent beside a restored n2: the bitmap is cleared at the
    // snapshot size; the resize waits for n2's LV.
    r.feed(
        &up("Inconsistent", "Inconsistent")
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(argv::start_without_sync("pvc-1"), ok(""));
    let w = r.pass(&[vol(LV_KIB)], &[source_snapshot()]).await;
    assert_eq!(entry_of(&w).observed_generation, None);

    // UpToDate: the LV grows to the gross size.
    r.feed(&[
        "change device name:pvc-1 volume:0 disk:UpToDate",
        "change peer-device name:pvc-1 conn-name:n2 volume:0 peer-disk:UpToDate",
    ]);
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    x.expect(lvm::cmd::extend(VG, "pvc-1", LV2_KIB * 1024), ok(""));
    expect_lvs(&x, &[source_lv(), lv(LV2_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    let w = r.pass(&[vol(LV_KIB)], &[source_snapshot()]).await;
    assert_eq!(entry_of(&w).observed_generation, None, "resize waits");

    // n2 grew too: the lowest node id resizes, and the new area syncs.
    reads_again(&x, &[source_lv(), lv(LV2_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    x.expect(argv::resize("pvc-1"), ok(""));
    dry(&x, "");
    let w = r.pass(&[vol(LV2_KIB)], &[source_snapshot()]).await;
    assert_eq!(entry_of(&w).observed_generation, Some(3));
    assert!(!seen(&x, "--assume-clean") && !seen(&x, "--force-resync"));
}

#[tokio::test]
async fn expand_after_initialization_syncs() {
    let mut r = Rig::new(&healthy(), up_host());
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV2_KIB)]);
    r.expect_check();
    r.expect_sweep();
    x.expect(argv::resize("pvc-1"), ok(""));
    dry(&x, "");
    let vol = initialized(with_entries(
        restored(volume(2 * GIB)),
        vec![restored_peer(LV2_KIB)],
    ));
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(!seen(&x, "--assume-clean"));
}

#[tokio::test]
async fn resize_waits_for_the_resync_to_end() {
    let mut lines = healthy();
    lines[4] = "exists peer-device name:pvc-1 peer-node-id:1 conn-name:n2 volume:0 replication:SyncSource peer-disk:Inconsistent".into();
    let mut r = Rig::new(&lines, up_host());
    let x = r.x.clone();
    let vol = || {
        initialized(with_entries(
            restored(volume(2 * GIB)),
            vec![restored_peer(LV2_KIB)],
        ))
    };
    reads(&x, &[source_lv(), lv(LV2_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&x, "");
    let w = r.pass(&[vol()], &[source_snapshot()]).await;
    let e = entry_of(&w);
    assert_eq!((e.observed_generation, e.last_error), (None, None));
    assert!(!seen(&x, "resize"));

    r.feed(&[
        "change peer-device name:pvc-1 peer-node-id:1 conn-name:n2 volume:0 replication:Established peer-disk:UpToDate",
    ]);
    reads_again(&x, &[source_lv(), lv(LV2_KIB)]);
    x.expect(argv::resize("pvc-1"), ok(""));
    dry(&x, "");
    let w = r.pass(&[vol()], &[source_snapshot()]).await;
    assert_eq!(entry_of(&w).last_error, None);
    assert!(seen(&x, "resize"));
}

#[tokio::test]
async fn restore_never_clears_the_bitmap_when_its_lv_is_not_at_the_snapshot_size() {
    // Every peer restored and connected, but this LV is larger than its
    // snapshot LV, or the snapshot LV is not in the report.
    for rows in [vec![source_lv(), lv(LV2_KIB)], vec![lv(LV_KIB)]] {
        let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
        let x = r.x.clone();
        reads(&x, &rows);
        r.expect_check();
        r.expect_sweep();
        expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
        dry(&x, "");
        x.expect(argv::start_as_source("pvc-1"), ok(""));
        let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
        r.pass(&[vol], &[source_snapshot()]).await;
        assert!(!seen(&x, "--clear-bitmap"));
    }
}

/// n2 (not the initial node) with its restored LV up; returns its entry.
async fn restored_from_on_n2(
    vol: Volume,
    snaps: &[Snapshot],
    tags: Option<&[&str]>,
) -> ReplicaStatus {
    let mut lines = up("Inconsistent", "Inconsistent");
    lines[1] = "exists connection name:pvc-1 peer-node-id:0 conn-name:n1 connection:Connected role:Secondary".into();
    lines[4] = "exists peer-device name:pvc-1 peer-node-id:0 conn-name:n1 volume:0 replication:Established peer-disk:Inconsistent".into();
    let mut r = Rig::on("n2", &lines, up_host());
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    if let Some(tags) = tags {
        expect_lv_tags(&x, &[("pvc-1", tags)]);
    }
    dry(&x, "");
    let w = r.pass_pools(&[pool_on("n2")], &[vol], snaps).await;
    entry_of(&w)
}

#[tokio::test]
async fn restored_from_is_reported_from_the_tag_only_while_the_snapshot_exists() {
    const N2: &str = "r-drbd.md.uid-1.1";
    let vol = || restored(volume(GIB));
    let e = restored_from_on_n2(vol(), &[source_snapshot()], Some(&[N2, FROM])).await;
    assert_eq!(e.restored_from.as_deref(), Some("snap-1"));
    // No tag: an LV made empty.
    let e = restored_from_on_n2(vol(), &[source_snapshot()], Some(&[N2])).await;
    assert_eq!(e.restored_from, None);
    // The Snapshot is a new object of the same name: absent, LVM is not read.
    let mut other = source_snapshot();
    other.metadata.uid = Some("snap-uid-2".into());
    let e = restored_from_on_n2(vol(), &[other], None).await;
    assert_eq!(e.restored_from, None);
    // A source without a uid binds no Snapshot.
    let mut legacy = vol();
    legacy.spec.source.as_mut().unwrap().snapshot_uid = None;
    let e = restored_from_on_n2(legacy, &[source_snapshot()], None).await;
    assert_eq!(e.restored_from, None);
    // The Snapshot is gone, or the volume initialized: LVM is not read.
    let e = restored_from_on_n2(vol(), &[], None).await;
    assert_eq!(e.restored_from, None);
    let e = restored_from_on_n2(initialized(vol()), &[source_snapshot()], None).await;
    assert_eq!(e.restored_from, None);
}

#[tokio::test]
async fn restore_holder_without_its_snapshot_lv_reports_an_error_and_creates_nothing() {
    // The initial node: nobody else could hold the data.
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[]);
    expect_lvs(&x, &[]);
    let w = r.pass(&[restored(volume(GIB))], &[source_snapshot()]).await;
    let e = entry_of(&w);
    let err = e.last_error.unwrap();
    assert!(
        err.contains("snapshot LV vg1/pvc-0+snap-1 not found on this node"),
        "{err}"
    );
    assert_eq!(e.restored_from, None);
    assert!(!seen(&x, "lvcreate") && !seen(&x, "create-md"));
    assert!(r.res_file().is_none());
}

#[tokio::test]
async fn restore_peer_without_its_copy_starts_empty_and_full_syncs() {
    // n2 reported its restored copy, then lost its LV and its snapshot LV:
    // an empty LV, no proof tag, and nothing of DRBD before the entry that
    // drops restoredFrom is written.
    let stale = ReplicaStatus {
        node: "n2".into(),
        allocated_bytes: Some(LV_KIB * 1024),
        observed_generation: Some(3),
        ..peer_entry("n2", true)
    };
    let vol = with_entries(restored(volume(GIB)), vec![stale]);
    let mut r = Rig::on("n2", &[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[]);
    expect_create(&x, &[]);
    let w = r
        .pass_pools(
            &[pool_on("n2")],
            std::slice::from_ref(&vol),
            &[source_snapshot()],
        )
        .await;
    let e = entry_of(&w);
    assert_eq!(e.restored_from, None);
    assert!(!seen(&x, "drbdadm") && !seen(&x, "--snapshot"));

    // The next pass carries on: metadata, then up.
    reads_again(&x, &[lv(LV_KIB)]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    expect_md_as(&x, "r-drbd.md.uid-1.1", &[NEW]);
    up_plan(&x);
    // Nothing changed in the entry until DRBD reports: no write.
    let w = r
        .pass_pools(
            &[pool_on("n2")],
            &[with_entries(restored(volume(GIB)), vec![e.clone()])],
            &[source_snapshot()],
        )
        .await;
    assert!(
        !w.iter()
            .any(|w| matches!(w, StatusWrite::Volume { name, .. } if name == "pvc-1")),
        "{w:?}"
    );

    // The initial node sees n2 without restoredFrom: it is the sync source.
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.drbd.init_wait = Duration::ZERO;
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(argv::start_as_source("pvc-1"), ok(""));
    let n2 = ReplicaStatus {
        node: "n2".into(),
        ..e
    };
    let vol = with_entries(restored(volume(GIB)), vec![n2]);
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(!seen(&x, "--clear-bitmap"));
}

/// One pass of n2 restoring pvc-1 from `snaps`, script set by the caller.
async fn n2_restore_pass(r: &mut Rig, snaps: &[Snapshot]) -> ReplicaStatus {
    let w = r
        .pass_pools(&[pool_on("n2")], &[restored(volume(GIB))], snaps)
        .await;
    entry_of(&w)
}

#[tokio::test]
async fn restore_peer_falls_back_on_any_restore_failure_but_not_a_full_pool() {
    let empty = || lvm::cmd::create_thin(VG, THIN, "pvc-1", LV_KIB * 1024, &[NEW]);
    // No Snapshot object.
    let mut r = Rig::on("n2", &[], FakeHost::default());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    expect_create(&x, &[source_lv()]);
    let e = n2_restore_pass(&mut r, &[]).await;
    assert_eq!((e.last_error, e.restored_from), (None, None));

    // The snapshot LV lives in another pool.
    let mut snap = source_snapshot();
    snap.status.as_mut().unwrap().acks = vec![SnapshotAck {
        node: "n2".into(),
        phase: SnapshotPhase::Ready,
        taken_pool: Some("pool-elsewhere".into()),
        lv_bytes: None,
        allocated_bytes: None,
        lv_created_at: None,
        found: None,
        searched_pools: Vec::new(),
        tag_size_bytes: None,
        tag_source_replicas: None,
        digest: None,
        digest_bytes: None,
        ok: true,
        error: None,
    }];
    let mut r = Rig::on("n2", &[], FakeHost::default());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    expect_create(&x, &[source_lv()]);
    let e = n2_restore_pass(&mut r, &[snap]).await;
    assert_eq!(e.last_error, None);

    // The restore lvcreate fails.
    let mut r = Rig::on("n2", &[], FakeHost::default());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    expect_source_tags(&x);
    x.expect(
        lvm::cmd::restore(VG, "pvc-0+snap-1", "pvc-1", &[NEW, FROM]),
        code(5, "", "lvcreate failed"),
    );
    expect_create(&x, &[source_lv()]);
    let e = n2_restore_pass(&mut r, &[source_snapshot()]).await;
    assert_eq!(e.last_error, None);
    assert!(x.seen.lock().unwrap().contains(&empty()));

    // A full pool fails the empty LV too: an error entry.
    let mut r = Rig::on("n2", &[], FakeHost::default());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    reads(&x, &[]);
    x.expect(empty(), code(5, "", "insufficient free space"));
    expect_lvs(&x, &[]);
    let e = n2_restore_pass(&mut r, &[source_snapshot()]).await;
    let err = e.last_error.unwrap();
    assert!(err.contains("the empty LV failed too"), "{err}");
    assert!(!seen(&x, "drbdadm"));
}

/// Holder copies that differ, whichever version took them: one copy, the
/// initial node's, and the other replicas sync in full from it, even when
/// they claim a copy. Never a start without a sync.
#[tokio::test]
async fn a_restore_of_differing_copies_syncs_from_one_copy() {
    for snap in [source_snapshot(), old_source_snapshot()] {
        let mut r = Rig::on("n2", &[], FakeHost::default());
        let x = r.x.clone();
        reads(&x, &[source_lv()]);
        expect_create(&x, &[source_lv()]);
        let e = n2_restore_pass(&mut r, std::slice::from_ref(&snap)).await;
        assert_eq!((e.last_error, e.restored_from), (None, None));
        let used_copy = x
            .seen
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.contains(&"pvc-0+snap-1".to_owned()));
        assert!(!used_copy, "the peer never restores its copy");

        // The initial node: the source at once, no proof awaited.
        let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
        let x = r.x.clone();
        reads(&x, &[source_lv(), lv(LV_KIB)]);
        r.expect_check();
        r.expect_sweep();
        expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
        dry(&x, "");
        x.expect(argv::start_as_source("pvc-1"), ok(""));
        let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
        r.pass(&[vol], &[snap]).await;
        assert!(!seen(&x, "--clear-bitmap"));
        assert_eq!(r.live.reads.load(Ordering::Relaxed), 0, "no proof read");
    }
}

#[tokio::test]
async fn degraded_restore_starts_without_quorum() {
    // The peer and the tiebreaker away: no quorum, yet the initial node's
    // copy is the snapshot's, so after the wait it is the sync source.
    let mut r = Rig::new(&waiting_lines(false), up_host());
    r.rec.drbd.init_wait = Duration::ZERO;
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(argv::start_as_source("pvc-1"), ok(""));
    r.pass(&[restored(volume(GIB))], &[source_snapshot()]).await;
    assert!(!seen(&x, "--clear-bitmap"));
}

#[tokio::test]
async fn initial_node_lost_during_the_grow_starts_as_source() {
    // n1 lost its LV while the grown area synced; n2 is Inconsistent at the
    // grown size. n1 restores its copy again and, the loss still on record,
    // takes the timeout path as the sync source.
    let mut r = Rig::new(&up("Diskless", "Inconsistent"), up_host());
    r.rec.drbd.init_wait = Duration::ZERO;
    let x = r.x.clone();
    let n1 = ReplicaStatus {
        allocated_bytes: Some(LV2_KIB * 1024),
        ..reported()
    };
    let n2 = ReplicaStatus {
        allocated_bytes: Some(LV2_KIB * 1024),
        restored_from: None,
        ..peer_entry("n2", false)
    };
    let vol = |me: ReplicaStatus| with_entries(restored(volume(2 * GIB)), vec![me, n2.clone()]);
    reads(&x, &[source_lv()]);
    expect_restore(&x);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW, FROM])]);
    expect_md(&x, &[NEW]);
    dry(&x, PLAN);
    x.expect(argv::adjust("pvc-1"), code(10, "", "attach failed"));
    let w = r.pass(&[vol(n1)], &[source_snapshot()]).await;
    let e = entry_of(&w);
    assert!(
        e.last_error.as_deref().unwrap().starts_with(LV_LOST),
        "{e:?}"
    );

    r.feed(&["change device name:pvc-1 volume:0 disk:Inconsistent"]);
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(argv::start_as_source("pvc-1"), ok(""));
    r.pass(&[vol(e)], &[source_snapshot()]).await;
    assert!(!seen(&x, "--clear-bitmap"));
}

/// n1 lost its reported LV; n2 is Inconsistent with a device of
/// `peer_bytes`.
fn lost_beside(peer_bytes: u64) -> (Volume, ReplicaStatus) {
    let n1 = ReplicaStatus {
        allocated_bytes: Some(LV2_KIB * 1024),
        ..reported()
    };
    let n2 = ReplicaStatus {
        allocated_bytes: Some(LV2_KIB * 1024),
        size_bytes: Some(peer_bytes),
        ..peer_entry("n2", false)
    };
    (
        with_entries(restored(volume(2 * GIB)), vec![n1, n2.clone()]),
        n2,
    )
}

#[tokio::test]
async fn restored_again_beside_a_grown_peer_grows_first_and_starts_as_source() {
    let mut r = Rig::new(&up("Diskless", "Inconsistent"), up_host());
    r.rec.drbd.init_wait = Duration::ZERO;
    let x = r.x.clone();
    let (vol, n2) = lost_beside(2 * GIB);
    reads(&x, &[source_lv()]);
    expect_restore(&x);
    // Grown before create-md, so the metadata lands at the gross end.
    x.expect(lvm::cmd::extend(VG, "pvc-1", LV2_KIB * 1024), ok(""));
    expect_lvs(&x, &[source_lv(), lv(LV2_KIB)]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW, FROM])]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    let w = r.pass(&[vol], &[source_snapshot()]).await;
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);

    // Up and Inconsistent: not at the snapshot's size, so the sync source.
    r.feed(&["change device name:pvc-1 volume:0 disk:Inconsistent"]);
    reads_again(&x, &[source_lv(), lv(LV2_KIB)]);
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(argv::start_as_source("pvc-1"), ok(""));
    let vol = with_entries(restored(volume(2 * GIB)), vec![e, n2]);
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(!seen(&x, "--clear-bitmap"));
}

#[tokio::test]
async fn restored_again_without_a_grown_peer_stays_at_the_snapshot_size() {
    let mut r = Rig::new(&up("Diskless", "Inconsistent"), up_host());
    let x = r.x.clone();
    let (vol, _) = lost_beside(NET_SECTORS * 512);
    reads(&x, &[source_lv()]);
    expect_restore(&x);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW, FROM])]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    r.pass(&[vol], &[source_snapshot()]).await;
    assert!(!seen(&x, "lvextend"));
}

#[tokio::test]
async fn fallback_peer_attaches_only_once_the_live_entry_drops_restored_from() {
    // n2's empty fallback LV exists; its entry with restoredFrom from before
    // the loss is still in the cache and, the write having failed, in the
    // API server.
    let stale = ReplicaStatus {
        allocated_bytes: Some(LV_KIB * 1024),
        ..peer_entry("n2", true)
    };
    let vol = with_entries(restored(volume(GIB)), vec![stale]);
    let mut r = Rig::on("n2", &[], FakeHost::default());
    let x = r.x.clone();
    *r.live.scripted.lock().unwrap() = Some(Ok(vol.clone()));
    reads(&x, &[lv(LV_KIB)]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    let w = r
        .pass_pools(
            &[pool_on("n2")],
            std::slice::from_ref(&vol),
            &[source_snapshot()],
        )
        .await;
    assert_eq!(entry_of(&w).restored_from, None, "the write goes again");
    assert!(!seen(&x, "create-md") && !seen(&x, "adjust"));

    // A failed live read waits too.
    *r.live.scripted.lock().unwrap() = Some(Err("unreachable".into()));
    reads_again(&x, &[lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    let w = r
        .pass_pools(
            &[pool_on("n2")],
            std::slice::from_ref(&vol),
            &[source_snapshot()],
        )
        .await;
    assert_eq!(entry_of(&w).last_error, None);
    assert!(!seen(&x, "create-md") && !seen(&x, "adjust"));

    // The API server has the entry without restoredFrom: metadata, then up.
    let committed = with_entries(
        restored(volume(GIB)),
        vec![ReplicaStatus {
            restored_from: None,
            ..peer_entry("n2", false)
        }],
    );
    *r.live.scripted.lock().unwrap() = Some(Ok(committed));
    reads_again(&x, &[lv(LV_KIB)]);
    expect_lv_tags(&x, &[("pvc-1", &[NEW])]);
    expect_md_as(&x, "r-drbd.md.uid-1.1", &[NEW]);
    up_plan(&x);
    r.pass_pools(&[pool_on("n2")], &[vol], &[source_snapshot()])
        .await;
}

#[tokio::test]
async fn initial_node_clears_the_bitmap_only_on_the_live_proof() {
    let cached = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
    // The cache shows n2's proof, the API server no longer does.
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.drbd.copies_identical = proven;
    r.rec.drbd.init_wait = Duration::ZERO;
    let x = r.x.clone();
    *r.live.scripted.lock().unwrap() = Some(Ok(with_entries(
        restored(volume(GIB)),
        vec![peer_entry("n2", false)],
    )));
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(argv::start_as_source("pvc-1"), ok(""));
    r.pass(std::slice::from_ref(&cached), &[source_snapshot()])
        .await;
    assert!(!seen(&x, "--clear-bitmap"));

    // A failed live read: no uuid step of either kind.
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.drbd.copies_identical = proven;
    r.rec.drbd.init_wait = Duration::ZERO;
    let x = r.x.clone();
    *r.live.scripted.lock().unwrap() = Some(Err("unreachable".into()));
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    r.pass(std::slice::from_ref(&cached), &[source_snapshot()])
        .await;
    assert!(!seen(&x, "new-current-uuid"));
    assert_eq!(r.live.reads.load(Ordering::Relaxed), 1);

    // Live and cache agree on the proof: the bitmap is cleared.
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(argv::start_without_sync("pvc-1"), ok(""));
    r.pass(&[cached], &[source_snapshot()]).await;
    assert_eq!(r.live.reads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_stale_peer_entry_never_clears_the_bitmap() {
    // restoredFrom without a present entry and a device: written before
    // the LV it speaks of.
    for (present, device) in [(false, true), (true, false)] {
        let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
        r.rec.drbd.init_wait = Duration::ZERO;
        let x = r.x.clone();
        reads(&x, &[source_lv(), lv(LV_KIB)]);
        r.expect_check();
        r.expect_sweep();
        expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
        dry(&x, "");
        x.expect(argv::start_as_source("pvc-1"), ok(""));
        let n2 = ReplicaStatus {
            present,
            device_path: device.then(|| "/dev/drbd50000".into()),
            ..peer_entry("n2", true)
        };
        let vol = with_entries(restored(volume(GIB)), vec![n2]);
        r.pass(&[vol], &[source_snapshot()]).await;
        assert!(!seen(&x, "--clear-bitmap"));
    }
}

#[tokio::test]
async fn stale_lv_with_another_volumes_from_tag_is_made_new_and_not_a_copy() {
    // Left on n2 by a deleted Volume of the same name, restored from the
    // same snapshot: its tags name another Volume uid.
    const OLD_MD: &str = "r-drbd.md.uid-0.1";
    const OLD_FROM: &str = "r-drbd.from.uid-0.snap-uid-1";
    const N2: &str = "r-drbd.md.uid-1.1";
    let mut r = Rig::on("n2", &[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[OLD_MD, OLD_FROM])]);
    expect_change(&x, "pvc-1", &[NEW], &[OLD_MD, OLD_FROM]);
    expect_md_as(&x, N2, &[NEW]);
    up_plan(&x);
    let w = r
        .pass_pools(
            &[pool_on("n2")],
            &[restored(volume(GIB))],
            &[source_snapshot()],
        )
        .await;
    let e = entry_of(&w);
    assert_eq!((e.last_error, e.restored_from), (None, None));
    // Without its report, the initial node force-resyncs: see
    // restore_falls_back_to_a_full_sync_when_a_peer_did_not_restore.
}

#[tokio::test]
async fn restore_peer_larger_than_its_snapshot_reports_no_restored_from() {
    const N2: &str = "r-drbd.md.uid-1.1";
    let mut lines = up("Inconsistent", "Inconsistent");
    lines[1] = "exists connection name:pvc-1 peer-node-id:0 conn-name:n1 connection:Connected role:Secondary".into();
    lines[4] = "exists peer-device name:pvc-1 peer-node-id:0 conn-name:n1 volume:0 replication:Established peer-disk:Inconsistent".into();
    let mut r = Rig::on("n2", &lines, up_host());
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV2_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[N2, FROM])]);
    dry(&x, "");
    let w = r
        .pass_pools(
            &[pool_on("n2")],
            &[restored(volume(GIB))],
            &[source_snapshot()],
        )
        .await;
    assert_eq!(entry_of(&w).restored_from, None);
}

#[tokio::test]
async fn initial_node_with_an_orphan_lv_refuses_until_the_claim_is_deleted() {
    // An LV of a deleted Volume of the same name: another md tag, no from
    // tag of this Volume. Made new, but never started as the restore.
    const OLD_MD: &str = "r-drbd.md.uid-0.0";
    let mut r = Rig::new(&[], FakeHost::default());
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[OLD_MD])]);
    expect_change(&x, "pvc-1", &[NEW], &[OLD_MD]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
    let w = r
        .pass(std::slice::from_ref(&vol), &[source_snapshot()])
        .await;
    assert_eq!(entry_of(&w).restored_from, None);

    r.feed(
        &up("Inconsistent", "Inconsistent")
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    r.rec.drbd.init_wait = Duration::ZERO;
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN])]);
    dry(&x, "");
    let w = r
        .pass(std::slice::from_ref(&vol), &[source_snapshot()])
        .await;
    assert_eq!(
        entry_of(&w).last_error.as_deref(),
        Some(
            "LV vg1/pvc-1 is not this restore's copy of snapshot snap-1; delete the claim and restore again"
        )
    );
    assert!(!seen(&x, "new-current-uuid"));

    // Deleting the claim removes the LV, so the Snapshot guard lets go.
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    x.expect(argv::down("pvc-1"), ok("")).expect(
        argv::status("pvc-1"),
        code(10, "", "pvc-1: No such resource"),
    );
    expect_lvs(&x, &[source_lv(), lv(LV_KIB)]);
    x.expect(lvm::cmd::remove(VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[source_lv()]);
    let w = r.pass(&[deleting(vol)], &[source_snapshot()]).await;
    let e = entry_of(&w);
    assert!(!e.present && e.last_error.is_none(), "{e:?}");
}

#[tokio::test]
async fn a_failed_proof_read_fails_the_pass_and_starts_nothing() {
    let mut r = Rig::new(&up("Inconsistent", "Inconsistent"), up_host());
    r.rec.drbd.init_wait = Duration::ZERO;
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    x.expect(lvm::cmd::lv_tags(VG), code(5, "", "lvs failed"));
    let vol = with_entries(restored(volume(GIB)), vec![peer_entry("n2", true)]);
    let w = r.pass(&[vol], &[source_snapshot()]).await;
    let e = entry_of(&w);
    assert!(e.last_error.unwrap().contains("lvs failed"));
    assert!(!seen(&x, "new-current-uuid"));
}

#[tokio::test]
async fn snapshot_lv_is_kept_while_a_restore_here_is_not_initialized() {
    let mut snap = deleting(source_snapshot());
    snap.status.as_mut().unwrap().acks = vec![SnapshotAck {
        node: NODE.into(),
        phase: SnapshotPhase::Ready,
        taken_pool: Some(POOL.into()),
        lv_bytes: None,
        allocated_bytes: None,
        lv_created_at: None,
        found: None,
        searched_pools: Vec::new(),
        tag_size_bytes: None,
        tag_source_replicas: None,
        digest: None,
        digest_bytes: None,
        ok: true,
        error: None,
    }];
    let mut r = Rig::new(&healthy(), up_host());
    let x = r.x.clone();
    reads(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    let vol = restored(volume(GIB));
    let w = r
        .pass(std::slice::from_ref(&vol), std::slice::from_ref(&snap))
        .await;
    let a = acks(&w);
    assert_eq!((a[0].phase, a[0].ok), (SnapshotPhase::Deleting, false));
    assert!(
        a[0].error
            .as_deref()
            .unwrap()
            .contains("volume pvc-1 restores from it"),
        "{a:?}"
    );
    assert!(!seen(&x, "lvremove"));

    // Initialized: the copy goes.
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    dry(&x, "");
    expect_source_tags(&x);
    expect_lvs(&x, &[source_lv(), lv(LV_KIB)]);
    x.expect(lvm::cmd::remove(VG, "pvc-0+snap-1"), ok(""));
    expect_lvs(&x, &[lv(LV_KIB)]);
    let w = r.pass(&[initialized(vol)], &[snap]).await;
    let a = acks(&w);
    assert_eq!((a[0].phase, a[0].ok), (SnapshotPhase::Deleting, true));
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
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM, "other"])]);
    expect_change(&x, "pvc-1", &[], &[OWN, FROM]);
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

/// A successful take records where the LV lives, its size and the
/// origin's allocation; no other ack of the round names a Pool.
fn round_ack(phase: SnapshotPhase, ok: bool) -> SnapshotAck {
    let taken = phase == SnapshotPhase::Taking && ok;
    SnapshotAck {
        node: NODE.into(),
        phase,
        taken_pool: taken.then(|| POOL.into()),
        lv_bytes: taken.then_some(LV_KIB * 1024),
        allocated_bytes: taken.then_some(0),
        lv_created_at: None,
        found: None,
        searched_pools: Vec::new(),
        tag_size_bytes: None,
        tag_source_replicas: None,
        digest: None,
        digest_bytes: None,
        ok,
        error: None,
    }
}

/// The Snapshot in `phase` with this node's last ack.
fn acked(phase: SnapshotPhase, mine: SnapshotAck) -> Snapshot {
    let mut s = snapshot(phase);
    s.status.as_mut().unwrap().acks = vec![mine];
    s
}

const SUSPENDED: &str = "change resource name:pvc-1 role:Secondary suspended:user";
const RESUMED: &str = "change resource name:pvc-1 role:Secondary suspended:no";

/// A pass in Suspending that suspends IO; the tracker then shows it.
async fn suspend_pass(r: &mut Rig) {
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    r.x.expect(argv::suspend_io(50000), ok(""));
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    assert_eq!(acks(&w), vec![round_ack(SnapshotPhase::Suspending, true)]);
    r.feed(&[SUSPENDED]);
}

fn expect_take(x: &ScriptedExec) {
    expect_lv_tags(x, &[("pvc-1", &[OWN])]);
    x.expect(
        lvm::cmd::snapshot(VG, "pvc-1", "pvc-1+snap-1", &[ROUND_TAG]),
        ok(""),
    );
    expect_change(x, "pvc-1+snap-1", &[], &[OWN]);
    expect_lvs(
        x,
        &[lv(LV_KIB), lv_row("pvc-1+snap-1", LV_KIB, St::Inactive)],
    );
    expect_lv_tags(x, &[("pvc-1", &[OWN]), ("pvc-1+snap-1", &[ROUND_TAG])]);
}

#[tokio::test]
async fn snapshot_rounds() {
    let mut r = Rig::new(&healthy(), up_host());
    suspend_pass(&mut r).await;

    // Acked: no second suspend, no adjust under the frozen IO, no write.
    reads_again(&r.x, &[lv(LV_KIB)]);
    let sus = round_ack(SnapshotPhase::Suspending, true);
    let w = r
        .pass(
            &[volume(GIB)],
            &[acked(SnapshotPhase::Suspending, sus.clone())],
        )
        .await;
    assert_eq!(acks(&w), vec![]);
    assert_eq!(times(&r.x, "--dry-run"), 1, "only before the suspend");

    reads_again(&r.x, &[lv(LV_KIB)]);
    expect_take(&r.x);
    let w = r
        .pass(&[volume(GIB)], &[acked(SnapshotPhase::Taking, sus)])
        .await;
    assert_eq!(acks(&w), vec![round_ack(SnapshotPhase::Taking, true)]);

    // Over: resumed with no word from the controller but the phase.
    reads_again(&r.x, &[lv(LV_KIB)]);
    r.x.expect(argv::resume_io("pvc-1"), ok(""));
    dry(&r.x, "");
    let done = round_ack(SnapshotPhase::Taking, true);
    let w = r
        .pass(&[volume(GIB)], &[acked(SnapshotPhase::Ready, done)])
        .await;
    assert_eq!(acks(&w), vec![]);
    assert_eq!(r.rec.next_deadline(), None);

    // A suspend no round accounts for (snapshot gone mid-round) is resumed.
    let mut sus = healthy();
    sus[0] = "exists resource name:pvc-1 role:Secondary suspended:user".into();
    let mut r = Rig::new(&sus, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.x.expect(argv::resume_io("pvc-1"), ok(""));
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    r.pass(&[volume(GIB)], &[]).await;
}

#[tokio::test(start_paused = true)]
async fn take_refused_past_the_window() {
    let mut r = Rig::new(&healthy(), up_host());
    suspend_pass(&mut r).await;
    tokio::time::advance(model::snapshot::TAKE_WINDOW).await;
    reads_again(&r.x, &[lv(LV_KIB)]);
    let sus = round_ack(SnapshotPhase::Suspending, true);
    let w = r
        .pass(&[volume(GIB)], &[acked(SnapshotPhase::Taking, sus)])
        .await;
    let a = &acks(&w)[0];
    assert_eq!(
        a.error.as_deref(),
        Some("IO not suspended by this agent within the last 120 s")
    );
    assert!(!seen(&r.x, "--snapshot"));
}

#[tokio::test(start_paused = true)]
async fn resume_on_the_local_deadline_without_a_controller() {
    let mut r = Rig::new(&healthy(), up_host());
    let start = tokio::time::Instant::now();
    suspend_pass(&mut r).await;
    let deadline = start + model::snapshot::RESUME_AFTER;
    assert!(r.rec.next_deadline().is_some_and(|d| d >= deadline));
    let sus = round_ack(SnapshotPhase::Suspending, true);
    let stuck = [acked(SnapshotPhase::Suspending, sus)];

    tokio::time::advance(model::snapshot::RESUME_AFTER.saturating_sub(Duration::from_secs(1)))
        .await;
    reads_again(&r.x, &[lv(LV_KIB)]);
    assert_eq!(acks(&r.pass(&[volume(GIB)], &stuck).await), vec![]);
    assert!(!seen(&r.x, "resume-io"));

    tokio::time::advance(Duration::from_secs(1)).await;
    reads_again(&r.x, &[lv(LV_KIB)]);
    r.x.expect(argv::resume_io("pvc-1"), ok(""));
    let w = r.pass(&[volume(GIB)], &stuck).await;
    let a = &acks(&w)[0];
    assert_eq!((a.phase, a.ok), (SnapshotPhase::Suspending, false));
    assert_eq!(
        a.error.as_deref(),
        Some("IO resumed here 240 s after its suspend")
    );
    assert_eq!(r.rec.next_deadline(), None);

    // Still Suspending in the cache: never a second suspend this round.
    r.feed(&[RESUMED]);
    reads_again(&r.x, &[lv(LV_KIB)]);
    dry(&r.x, "");
    let failed = acked(SnapshotPhase::Suspending, a.clone());
    let w = r.pass(&[volume(GIB)], &[failed]).await;
    assert_eq!(acks(&w), vec![]);
    assert_eq!(times(&r.x, "suspend-io"), 1);
}

/// A status lost mid round, or being rebuilt, keeps IO frozen to the
/// agent's own deadline; any other phase ends the freeze at once.
#[tokio::test(start_paused = true)]
async fn a_lost_status_keeps_the_freeze_to_the_deadline() {
    for phase in [None, Some(SnapshotPhase::Discovering)] {
        let mut r = Rig::new(&healthy(), up_host());
        suspend_pass(&mut r).await;
        let mut s = snapshot(SnapshotPhase::Suspending);
        s.status.as_mut().unwrap().phase = phase;
        let lost = [s];
        tokio::time::advance(model::snapshot::RESUME_AFTER.saturating_sub(Duration::from_secs(1)))
            .await;
        // Only a discovery answer, never a failed round ack.
        let outside = |w: &[StatusWrite]| {
            acks(w)
                .iter()
                .all(|a| a.phase == SnapshotPhase::Discovering && a.ok)
        };
        reads_again(&r.x, &[lv(LV_KIB)]);
        dry(&r.x, "");
        assert!(outside(&r.pass(&[volume(GIB)], &lost).await));
        assert!(!seen(&r.x, "resume-io"), "{phase:?}");
        tokio::time::advance(Duration::from_secs(1)).await;
        reads_again(&r.x, &[lv(LV_KIB)]);
        r.x.expect(argv::resume_io("pvc-1"), ok(""));
        dry(&r.x, "");
        let w = r.pass(&[volume(GIB)], &lost).await;
        assert!(outside(&w), "no failed ack outside a round");
    }
    for phase in [
        SnapshotPhase::Ready,
        SnapshotPhase::Failed,
        SnapshotPhase::Lost,
        SnapshotPhase::Pending,
    ] {
        let mut r = Rig::new(&healthy(), up_host());
        suspend_pass(&mut r).await;
        reads_again(&r.x, &[lv(LV_KIB)]);
        r.x.expect(argv::resume_io("pvc-1"), ok(""));
        dry(&r.x, "");
        r.pass(&[volume(GIB)], &[snapshot(phase)]).await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_restart_forgets_the_suspend_and_never_takes() {
    let mut sus = healthy();
    sus[0] = "exists resource name:pvc-1 role:Secondary suspended:user".into();
    let mut r = Rig::new(&sus, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    let before = round_ack(SnapshotPhase::Suspending, true);
    let taking = [acked(SnapshotPhase::Taking, before)];
    let w = r.pass(&[volume(GIB)], &taking).await;
    let a = &acks(&w)[0];
    assert!(
        !a.ok
            && a.error
                .as_deref()
                .unwrap()
                .contains("not suspended by this agent")
    );
    assert!(!seen(&r.x, "--snapshot"));
    // Resumed in time all the same, counted from the first sight.
    tokio::time::advance(model::snapshot::RESUME_AFTER).await;
    reads_again(&r.x, &[lv(LV_KIB)]);
    r.x.expect(argv::resume_io("pvc-1"), ok(""));
    r.pass(&[volume(GIB)], &taking).await;
}

#[tokio::test]
async fn a_copy_that_may_differ_is_refused() {
    let sus = round_ack(SnapshotPhase::Suspending, true);
    let taking = [acked(SnapshotPhase::Taking, sus)];
    // Cut from the diskful peer, both still UpToDate.
    let mut r = Rig::new(&healthy(), up_host());
    suspend_pass(&mut r).await;
    r.feed(&["change connection name:pvc-1 conn-name:n2 connection:Connecting"]);
    reads_again(&r.x, &[lv(LV_KIB)]);
    let w = r.pass(&[volume(GIB)], &taking).await;
    assert_eq!(
        acks(&w)[0].error.as_deref(),
        Some("peer n2 is Connecting, disk UpToDate")
    );
    // Mid resize: the LV grew, the DRBD device did not.
    let mut r = Rig::new(&healthy(), up_host());
    suspend_pass(&mut r).await;
    reads_again(&r.x, &[lv(LV_KIB)]);
    let w = r.pass(&[volume(2 * GIB)], &taking).await;
    let e = acks(&w)[0].error.clone().unwrap();
    assert!(e.contains("a resize is in flight"), "{e}");
    assert!(!seen(&r.x, "--snapshot"));
}

#[tokio::test]
async fn nothing_grows_during_a_round() {
    let round = [snapshot(SnapshotPhase::Suspending)];
    let mut r = Rig::new(&healthy(), up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    r.x.expect(argv::suspend_io(50000), ok(""));
    let w = r.pass(&[grown(2 * GIB, 4, Some(LV_KIB))], &round).await;
    assert_eq!(entry_of(&w).observed_generation, Some(3));
    assert!(!seen(&r.x, "lvextend"));
    // Grown everywhere already: the DRBD resize waits too.
    let mut r = Rig::new(&healthy(), up_host());
    reads(&r.x, &[lv(LV2_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    r.x.expect(argv::suspend_io(50000), ok(""));
    let w = r.pass(&[grown(2 * GIB, 4, Some(LV2_KIB))], &round).await;
    assert_eq!(entry_of(&w).observed_generation, Some(3));
    assert!(!seen(&r.x, "resize"));
}

#[tokio::test]
async fn diskless_replica_keeps_its_freeze_through_taking() {
    let lines = |suspended: &str| {
        vec![
            format!("exists resource name:pvc-1 role:Primary suspended:{suspended}"),
            "exists connection name:pvc-1 peer-node-id:0 conn-name:n1 connection:Connected".into(),
            "exists connection name:pvc-1 peer-node-id:1 conn-name:n2 connection:Connected".into(),
            "exists device name:pvc-1 volume:0 minor:50000 disk:Diskless client:yes quorum:yes"
                .into(),
        ]
    };
    let mine = SnapshotAck {
        node: "n3".into(),
        phase: SnapshotPhase::Suspending,
        taken_pool: None,
        lv_bytes: None,
        allocated_bytes: None,
        lv_created_at: None,
        found: None,
        searched_pools: Vec::new(),
        tag_size_bytes: None,
        tag_source_replicas: None,
        digest: None,
        digest_bytes: None,
        ok: true,
        error: None,
    };
    let taking = acked(SnapshotPhase::Taking, mine);
    let mut r = Rig::on("n3", &lines("user"), FakeHost::default());
    r.expect_check();
    let w = r.pass(&[volume(GIB)], std::slice::from_ref(&taking)).await;
    let frozen = SnapshotAck {
        node: "n3".into(),
        phase: SnapshotPhase::Taking,
        taken_pool: None,
        lv_bytes: None,
        allocated_bytes: None,
        lv_created_at: None,
        found: None,
        searched_pools: Vec::new(),
        tag_size_bytes: None,
        tag_source_replicas: None,
        digest: None,
        digest_bytes: None,
        ok: true,
        error: None,
    };
    assert_eq!(
        acks(&w),
        vec![frozen.clone()],
        "still suspended at ack time"
    );
    // Acked: kept frozen while the phase stays Taking, nothing rewritten.
    let mut held = taking.clone();
    held.status.as_mut().unwrap().acks = vec![frozen];
    let w = r.pass(&[volume(GIB)], &[held]).await;
    assert_eq!(acks(&w), vec![]);
    assert!(!seen(&r.x, "resume-io"));
    // Rebooted mid round: IO is live again, the round must fail.
    let mut r = Rig::on("n3", &lines("no"), FakeHost::default());
    r.expect_check();
    dry(&r.x, "");
    let w = r.pass(&[volume(GIB)], &[taking]).await;
    let a = &acks(&w)[0];
    assert_eq!(
        (a.node.as_str(), a.phase, a.ok),
        ("n3", SnapshotPhase::Taking, false)
    );
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
    r.x.expect(argv::suspend_io(50000), ok(""));
    // A diskless Primary has no disk counter; its peers drained.
    r.x.expect(
        argv::status_stats_json("pvc-1"),
        ok(&stats(None, &[("n1", 0, 0), ("n2", 0, 0)])),
    );
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    let a = acks(&w);
    assert_eq!((a[0].ok, a[0].taken_pool.as_deref()), (true, None));
}

/// `drbdsetup status --json --statistics` of pvc-1: the disk's
/// lower-pending, then (peer, pending, unacked) per connection.
fn stats(lower: Option<u64>, peers: &[(&str, u64, u64)]) -> String {
    let device = lower.map_or(
        serde_json::json!({ "volume": 0 }),
        |l| serde_json::json!({ "volume": 0, "lower-pending": l, "upper-pending": 7 }),
    );
    let conns: Vec<_> = peers
        .iter()
        .map(|(n, p, u)| {
            serde_json::json!({ "name": n, "peer_devices": [
                { "volume": 0, "pending": p, "unacked": u }
            ]})
        })
        .collect();
    serde_json::json!([{ "name": "pvc-1", "devices": [device], "connections": conns }]).to_string()
}

/// A Primary acks only once its writes left: nothing pending on its disk,
/// nothing pending or unacked toward a peer.
#[tokio::test]
async fn a_primary_acks_only_once_drained() {
    let mut lines = healthy();
    lines[0] = "exists resource name:pvc-1 role:Primary suspended:no".into();
    let mut r = Rig::new(&lines, up_host());
    r.rec.drbd.drain_wait = Duration::ZERO;
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    r.x.expect(argv::suspend_io(50000), ok(""));
    r.x.expect(
        argv::status_stats_json("pvc-1"),
        ok(&stats(Some(0), &[("n2", 0, 3), ("n3", 0, 0)])),
    );
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    let a = &acks(&w)[0];
    assert!(!a.ok);
    assert_eq!(
        a.error.as_deref(),
        Some("writes still in flight after the suspend: unacked 3 to n2")
    );

    let mut r = Rig::new(&lines, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    r.x.expect(argv::suspend_io(50000), ok(""));
    r.x.expect(
        argv::status_stats_json("pvc-1"),
        ok(&stats(Some(1), &[("n2", 0, 0), ("n3", 0, 0)])),
    );
    r.x.expect(
        argv::status_stats_json("pvc-1"),
        ok(&stats(Some(0), &[("n2", 0, 0), ("n3", 0, 0)])),
    );
    let w = r
        .pass(&[volume(GIB)], &[snapshot(SnapshotPhase::Suspending)])
        .await;
    assert_eq!(acks(&w), vec![round_ack(SnapshotPhase::Suspending, true)]);
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
    r.rec.drbd.copies_identical = proven;
    let x = r.x.clone();
    reads(&x, &[source_lv()]);
    expect_restore(&x);
    r.expect_check();
    expect_lv_tags(&x, &[("pvc-1", &[NEW, FROM])]);
    expect_md(&x, &[NEW]);
    up_plan(&x);
    let vol = with_entries(
        restored(volume(GIB)),
        vec![reported(), peer_entry("n2", true)],
    );
    let w = r
        .pass(std::slice::from_ref(&vol), &[source_snapshot()])
        .await;
    let e = entry_of(&w);
    assert_eq!(e.last_error, None);
    assert!(
        !x.seen.lock().unwrap().contains(&empty_create()),
        "never an empty LV"
    );
    assert!(!r.rec.rebuilt_seen.contains("pvc-1"));

    // Up and Inconsistent beside a restored peer: the copies are the same
    // snapshot again, nobody syncs.
    let lines = up("Inconsistent", "Inconsistent");
    r.feed(&lines.iter().map(String::as_str).collect::<Vec<_>>());
    *r.host.sectors.lock().unwrap() = NET_SECTORS;
    reads_again(&x, &[source_lv(), lv(LV_KIB)]);
    r.expect_sweep();
    expect_lv_tags(&x, &[("pvc-1", &[OWN, FROM])]);
    dry(&x, "");
    x.expect(argv::start_without_sync("pvc-1"), ok(""));
    r.pass(
        &[with_entries(vol, vec![e, peer_entry("n2", true)])],
        &[source_snapshot()],
    )
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

/// A Snapshot an older version left in a phase this one removed: no round.
/// IO it held suspended is resumed, nothing is taken, nothing written.
#[tokio::test]
async fn a_snapshot_in_an_unknown_phase_is_no_round() {
    let old: Snapshot = serde_json::from_value(serde_json::json!({
        "apiVersion": "r-drbd.io/v1alpha1", "kind": "Snapshot",
        "metadata": { "name": "snap-1" },
        "spec": { "volume": "pvc-1" },
        "status": {
            "phase": "Resuming",
            "acks": [
                { "node": NODE, "phase": "Taken", "pool": POOL, "ok": true },
                { "node": "n2", "phase": "Suspended", "ok": true }
            ]
        }
    }))
    .unwrap();
    assert_eq!(
        old.status.as_ref().unwrap().phase,
        Some(SnapshotPhase::Unknown)
    );
    let mut sus = healthy();
    sus[0] = "exists resource name:pvc-1 role:Secondary suspended:user".into();
    let mut r = Rig::new(&sus, up_host());
    reads(&r.x, &[lv(LV_KIB)]);
    r.x.expect(argv::resume_io("pvc-1"), ok(""));
    r.expect_check();
    r.expect_sweep();
    dry(&r.x, "");
    let w = r.pass(&[volume(GIB)], &[old]).await;
    assert_eq!(acks(&w), vec![]);
    assert!(!seen(&r.x, "--snapshot"));
    assert_eq!(r.rec.next_deadline(), None);
}
