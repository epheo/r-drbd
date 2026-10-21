use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use model::{
    AssignedReplica, Assignment, Pool, PoolSpec, PoolStatus, ReplicaStatus, Role, Snapshot,
    SnapshotAck, SnapshotPhase, SnapshotSpec, SnapshotStatus, Volume, VolumeSource, VolumeSpec,
    VolumeStatus,
};

use super::*;
use crate::exec::scripted::{ScriptedExec, ok};
use crate::storage::fixtures::*;

const NODE: &str = "n1";
const POOL: &str = "pool-n1";
const GIB: u64 = 1 << 30;
const GIB_KIB: u64 = 1 << 20;

fn pool() -> Arc<Pool> {
    pool_in(POOL, VG, THIN)
}

fn pool_in(name: &str, vg: &str, thin: &str) -> Arc<Pool> {
    let mut p = Pool::new(
        name,
        PoolSpec {
            node: NODE.into(),
            tier: "test".into(),
            vg: vg.into(),
            thin_pool: thin.into(),
            overcommit_percent: model::DEFAULT_OVERCOMMIT_PERCENT,
            reserve_percent: model::DEFAULT_RESERVE_PERCENT,
            devices: None,
            chunk_size: None,
            reclaim_policy: None,
        },
    );
    p.metadata.generation = Some(1);
    Arc::new(p)
}

fn volume(name: &str, size: u64, replicas: u32) -> Volume {
    let mut v = Volume::new(
        name,
        VolumeSpec {
            size_bytes: size,
            replicas,
            tier: "test".into(),
            pool: None,
            node: None,
            access_mode: model::AccessMode::default(),
            attachments: vec![],
            evict: vec![],
            source: None,
            resolve: None,
            assignment: Some(Assignment {
                replicas: vec![AssignedReplica {
                    node: NODE.into(),
                    pool: Some(POOL.into()),
                    node_id: 0,
                    port: None,
                    role: Role::Diskful,
                }],
                minor: None,
                secret: None,
                initial_up_to_date_node: None,
            }),
        },
    );
    v.metadata.generation = Some(3);
    v.metadata.uid = Some(format!("uid-{name}"));
    v
}

fn snapshot(name: &str, volume: &str, phase: SnapshotPhase) -> Snapshot {
    let mut s = Snapshot::new(
        name,
        SnapshotSpec {
            volume: volume.into(),
            volume_uid: Some(format!("uid-{volume}")),
        },
    );
    s.metadata.uid = Some(format!("uid-{name}"));
    // Ready: this node took its copy in POOL.
    let acks = (phase == SnapshotPhase::Ready)
        .then(|| taken_ack(SnapshotPhase::Taking, POOL))
        .into_iter()
        .collect();
    s.status = Some(SnapshotStatus {
        phase,
        acks,
        ..SnapshotStatus::default()
    });
    s
}

fn taken_ack(phase: SnapshotPhase, pool: &str) -> SnapshotAck {
    SnapshotAck {
        node: NODE.into(),
        phase,
        taken_pool: Some(pool.into()),
        lv_bytes: Some(GIB),
        allocated_bytes: Some(GIB / 4),
        ok: true,
        error: None,
    }
}

fn source(snapshot: &str) -> VolumeSource {
    VolumeSource {
        snapshot: snapshot.into(),
        snapshot_uid: Some(format!("uid-{snapshot}")),
    }
}

fn deleting<T: kube::Resource>(mut o: T) -> T {
    o.meta_mut().deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
    o
}

fn reads(x: &ScriptedExec, rows: &[String]) {
    expect_lvs(x, rows);
    expect_vgs(x);
}

fn entry(name: &str, kib: u64, generation: Option<i64>) -> ReplicaStatus {
    ReplicaStatus {
        node: NODE.into(),
        present: true,
        disk_state: Some("UpToDate".into()),
        device_path: Some(format!("/dev/{VG}/{name}")),
        allocated_bytes: Some(kib * 1024),
        size_bytes: Some(kib * 1024),
        observed_generation: generation,
        ..ReplicaStatus::default()
    }
}

fn volume_writes(writes: &[StatusWrite]) -> Vec<(String, ReplicaStatus)> {
    writes
        .iter()
        .filter_map(|w| match w {
            StatusWrite::Volume { name, entry } => Some((name.clone(), entry.clone())),
            _ => None,
        })
        .collect()
}

fn snapshot_acks(writes: &[StatusWrite]) -> Vec<(String, SnapshotAck)> {
    writes
        .iter()
        .filter_map(|w| match w {
            StatusWrite::Snapshot { name, ack } => Some((name.clone(), ack.clone())),
            _ => None,
        })
        .collect()
}

async fn run(x: &Arc<ScriptedExec>, vols: Vec<Volume>, snaps: Vec<Snapshot>) -> Vec<StatusWrite> {
    run_in(x, &[pool()], vols, snaps).await
}

async fn run_in(
    x: &Arc<ScriptedExec>,
    pools: &[Arc<Pool>],
    vols: Vec<Volume>,
    snaps: Vec<Snapshot>,
) -> Vec<StatusWrite> {
    let mut r = Reconciler::new(NODE, x.clone());
    let vols: Vec<Arc<Volume>> = vols.into_iter().map(Arc::new).collect();
    let snaps: Vec<Arc<Snapshot>> = snaps.into_iter().map(Arc::new).collect();
    let w = r.pass(pools, &vols, &snaps).await;
    x.assert_done();
    w
}

#[tokio::test]
async fn pool_available_with_capacity() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    let w = run(&x, vec![], vec![]).await;
    assert_eq!(w.len(), 1);
    let body = w[0].body();
    assert_eq!(body["kind"], "Pool");
    assert_eq!(body["status"]["totalBytes"], 52_428_800u64 * 1024);
    assert_eq!(body["status"]["freeBytes"], 47_185_920u64 * 1024);
    assert_eq!(body["status"]["chunkBytes"], 65_536);
    assert_eq!(body["status"]["observedGeneration"], 1);
    assert_eq!(body["status"]["conditions"][0]["type"], "Available");
    assert_eq!(body["status"]["conditions"][0]["status"], "True");
    assert!(body["status"].get("volumes").is_none());
    assert_eq!(body["apiVersion"], "r-drbd.io/v1alpha1");
    assert_eq!(body["metadata"]["name"], POOL);
}

#[tokio::test]
async fn pool_unavailable_carries_stderr_and_drops_capacity() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(
        lvm::cmd::lvs(VG),
        report_fail("Volume group \"vg1\" not found"),
    );
    let w = run(&x, vec![], vec![]).await;
    let body = w[0].body();
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["status"], "False");
    assert_eq!(
        cond["message"],
        "lvs: exit 5: Volume group \"vg1\" not found"
    );
    assert!(body["status"].get("totalBytes").is_none());
}

#[tokio::test]
async fn unsupported_lvm_makes_pools_unavailable() {
    let x = Arc::new(ScriptedExec::new());
    let mut r = Reconciler::new(NODE, x.clone());
    r.set_lvm_unsupported(Some("LVM too old".into()));
    let w = r
        .pass(&[pool()], &[Arc::new(volume("pvc-1", GIB, 1))], &[])
        .await;
    assert!(x.seen.lock().unwrap().is_empty());
    let cond = &w[0].body()["status"]["conditions"][0];
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "LvmUnsupported");
    assert_eq!(cond["message"], "LVM too old");
    assert_eq!(
        volume_writes(&w)[0].1.last_error.as_deref(),
        Some("LVM too old")
    );
}

#[tokio::test]
async fn pool_with_invalid_name_runs_no_lvm_command() {
    for (vg, thin, field) in [("-x", THIN, "vg"), (VG, "..", "thinPool")] {
        // No expectation: any command would panic the scripted exec.
        let x = Arc::new(ScriptedExec::new());
        let mut p = (*pool()).clone();
        p.spec.vg = vg.into();
        p.spec.thin_pool = thin.into();
        let vols = [Arc::new(volume("pvc-1", GIB, 1))];
        let snaps = [Arc::new(snapshot("snap-1", "pvc-1", SnapshotPhase::Taking))];
        let w = Reconciler::new(NODE, x.clone())
            .pass(&[Arc::new(p)], &vols, &snaps)
            .await;
        x.assert_done();
        assert!(x.seen.lock().unwrap().is_empty());
        let body = w[0].body();
        let cond = &body["status"]["conditions"][0];
        assert_eq!(cond["type"], "Available");
        assert_eq!(cond["status"], "False");
        assert_eq!(cond["reason"], "InvalidName");
        assert!(
            cond["message"].as_str().unwrap().starts_with(field),
            "{cond}"
        );
        assert!(body["status"].get("totalBytes").is_none());
        let (_, e) = &volume_writes(&w)[0];
        assert!(!e.present);
        assert_eq!(
            e.last_error.as_deref(),
            Some("pool pool-n1 has an invalid vg or thinPool name")
        );
        let (_, ack) = &snapshot_acks(&w)[0];
        assert!(!ack.ok);
    }
}

/// model::keys::LVM_NAME_PATTERN read by hand (no regex crate): the first
/// byte from one class, up to 126 more from a class that adds `-`; the CEL
/// rule beside it rejects `.` and `..`.
#[test]
fn lvm_name_check_matches_the_model_pattern() {
    assert_eq!(
        model::keys::LVM_NAME_PATTERN,
        "^[A-Za-z0-9+_.][A-Za-z0-9+_.-]{0,126}$"
    );
    let first = |c: u8| c.is_ascii_alphanumeric() || matches!(c, b'+' | b'_' | b'.');
    let rest = |c: u8| first(c) || c == b'-';
    let model_accepts = |n: &str| {
        let b = n.as_bytes();
        b.first().is_some_and(|&c| first(c))
            && b.len() <= 127
            && b[1..].iter().all(|&c| rest(c))
            && n != "."
            && n != ".."
    };
    let long = "a".repeat(127);
    let too_long = "a".repeat(128);
    for n in [
        "thin",
        "nvme-vg-2tb",
        "-foo",
        ".",
        "..",
        "...",
        "a/b",
        "a+b_c.d-e",
        "",
        "caf\u{e9}",
        long.as_str(),
        too_long.as_str(),
    ] {
        assert_eq!(lvm::names::valid_lvm_name(n), model_accepts(n), "{n:?}");
    }
}

#[tokio::test]
async fn pool_unchanged_writes_nothing() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    let mut r = Reconciler::new(NODE, x.clone());
    let w = r.pass(&[pool()], &[], &[]).await;
    let StatusWrite::Pool {
        capacity,
        available,
        ..
    } = &w[0]
    else {
        panic!("pool write expected");
    };
    let cap = capacity.clone().unwrap();
    let mut p = (*pool()).clone();
    p.status = Some(PoolStatus {
        total_bytes: Some(cap.total_bytes),
        free_bytes: Some(cap.free_bytes),
        chunk_bytes: Some(cap.chunk_bytes),
        volumes: vec!["owned-by-controller".into()],
        committed_bytes: None,
        placeable_bytes: None,
        conditions: vec![available.clone()],
        observed_generation: Some(1),
        devices: vec![],
    });
    reads(&x, &[]);
    let w = Reconciler::new(NODE, x.clone())
        .pass(&[Arc::new(p)], &[], &[])
        .await;
    assert!(w.is_empty(), "{w:?}");
    x.assert_done();
}

/// A thin pool LVM flags, or with its metadata nearly full, takes no new
/// volumes; its capacity and chunk stay in the write.
#[tokio::test]
async fn unhealthy_thin_pool_is_unavailable_with_its_capacity() {
    for (extra, why) in [
        (
            r#""metadata_percent":"85.00""#,
            "metadata 85.00 percent used, over 80",
        ),
        (r#""lv_health_status":"out_of_data""#, "health: out_of_data"),
    ] {
        let x = Arc::new(ScriptedExec::new());
        let row = pool_row().replace(
            r#""chunk_size":"65536""#,
            &format!(r#""chunk_size":"65536", {extra}"#),
        );
        x.expect(lvm::cmd::lvs(VG), ok(&json("lv", &[row])));
        expect_vgs(&x);
        let w = Reconciler::new(NODE, x.clone())
            .pass(&[pool()], &[], &[])
            .await;
        x.assert_done();
        let body = w[0].body();
        let cond = &body["status"]["conditions"][0];
        assert_eq!(cond["status"], "False", "{why}");
        assert_eq!(cond["reason"], REASON_UNAVAILABLE);
        assert!(cond["message"].as_str().unwrap().ends_with(why), "{cond}");
        assert_eq!(body["status"]["totalBytes"], POOL_BYTES);
        assert_eq!(body["status"]["chunkBytes"], 65536);
    }
    let x = Arc::new(ScriptedExec::new());
    let row = pool_row().replace(
        r#""chunk_size":"65536""#,
        r#""chunk_size":"65536", "metadata_percent":"80.00""#,
    );
    x.expect(lvm::cmd::lvs(VG), ok(&json("lv", &[row])));
    expect_vgs(&x);
    let w = Reconciler::new(NODE, x.clone())
        .pass(&[pool()], &[], &[])
        .await;
    assert_eq!(w[0].body()["status"]["conditions"][0]["status"], "True");
}

/// A failed probe keeps the chunk the Pool was created with.
#[tokio::test]
async fn failed_probe_keeps_chunk_bytes() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::lvs(VG), report_fail("Volume group not found"));
    let mut p = (*pool()).clone();
    p.status = Some(PoolStatus {
        total_bytes: Some(POOL_BYTES),
        chunk_bytes: Some(65536),
        ..PoolStatus::default()
    });
    let w = Reconciler::new(NODE, x.clone())
        .pass(&[Arc::new(p)], &[], &[])
        .await;
    x.assert_done();
    let body = w[0].body();
    assert_eq!(body["status"]["conditions"][0]["status"], "False");
    assert!(body["status"].get("totalBytes").is_none());
    assert_eq!(body["status"]["chunkBytes"], 65536);
}

#[derive(Debug, Default)]
struct Recorded(std::sync::Mutex<Vec<Vec<String>>>);

#[async_trait::async_trait]
impl Publish for Recorded {
    async fn publish(&self, writes: Vec<StatusWrite>) -> Vec<String> {
        let names = writes.iter().map(|w| w.name().to_owned()).collect();
        self.0.lock().unwrap().push(names);
        Vec::new()
    }
}

/// Each step's writes go out as the step ends, not after the pass.
#[tokio::test]
async fn writes_publish_after_each_step() {
    let x = Arc::new(ScriptedExec::new());
    reads(
        &x,
        &[
            lv_row("pvc-1", GIB_KIB, St::Active),
            lv_row("pvc-2", GIB_KIB, St::Active),
        ],
    );
    let sent = Arc::new(Recorded::default());
    let vols = [
        Arc::new(volume("pvc-1", GIB, 1)),
        Arc::new(volume("pvc-2", GIB, 1)),
    ];
    let left = Reconciler::new(NODE, x.clone())
        .with_publish(sent.clone())
        .pass(&[pool()], &vols, &[])
        .await;
    x.assert_done();
    assert!(left.is_empty(), "{left:?}");
    assert_eq!(
        *sent.0.lock().unwrap(),
        [
            vec![POOL.to_owned()],
            vec!["pvc-1".into()],
            vec!["pvc-2".into()]
        ]
    );
}

#[tokio::test]
async fn create() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    x.expect(lvm::cmd::create_thin(VG, THIN, "pvc-1", GIB, &[]), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
    let w = run(&x, vec![volume("pvc-1", GIB, 1)], vec![]).await;
    assert_eq!(
        volume_writes(&w),
        vec![("pvc-1".into(), entry("pvc-1", GIB_KIB, Some(3)))]
    );
    let body = w[1].body();
    assert_eq!(body["apiVersion"], "r-drbd.io/v1alpha1");
    assert_eq!(body["kind"], "Volume");
    assert_eq!(body["metadata"]["name"], "pvc-1");
    assert_eq!(body["status"]["replicas"][0]["node"], NODE);
    assert_eq!(body["status"]["replicas"][0]["present"], true);
    assert!(body["status"].get("phase").is_none());
}

#[tokio::test]
async fn create_rounds_up_to_extent() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    let want = GIB_KIB + 4096;
    x.expect(
        lvm::cmd::create_thin(VG, THIN, "pvc-1", want * 1024, &[]),
        ok(""),
    );
    expect_lvs(&x, &[lv_row("pvc-1", want, St::Active)]);
    run(&x, vec![volume("pvc-1", GIB + 1, 1)], vec![]).await;
}

#[tokio::test]
async fn restore_then_grow() {
    let x = Arc::new(ScriptedExec::new());
    let snap_lv = lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive);
    reads(&x, std::slice::from_ref(&snap_lv));
    x.expect(lvm::cmd::restore(VG, "pvc-1+snap-1", "pvc-2", &[]), ok(""));
    expect_lvs(&x, &[snap_lv.clone(), lv_row("pvc-2", GIB_KIB, St::Active)]);
    x.expect(lvm::cmd::extend(VG, "pvc-2", 2 * GIB), ok(""));
    expect_lvs(&x, &[snap_lv, lv_row("pvc-2", 2 * GIB_KIB, St::Active)]);
    let mut v = volume("pvc-2", 2 * GIB, 1);
    v.spec.source = Some(source("snap-1"));
    let w = run(
        &x,
        vec![v],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Ready)],
    )
    .await;
    assert_eq!(
        volume_writes(&w),
        vec![("pvc-2".into(), entry("pvc-2", 2 * GIB_KIB, Some(3)))]
    );
}

#[tokio::test]
async fn restore_without_snapshot_lv_is_an_error_not_an_empty_lv() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    expect_lvs(&x, &[]);
    let mut v = volume("pvc-2", GIB, 1);
    v.spec.source = Some(source("snap-1"));
    let w = run(
        &x,
        vec![v],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Ready)],
    )
    .await;
    let (_, e) = &volume_writes(&w)[0];
    assert!(!e.present);
    assert_eq!(
        e.last_error.as_deref(),
        Some("snapshot LV vg1/pvc-1+snap-1 not found on this node")
    );
    assert_eq!(e.observed_generation, None);
}

#[tokio::test]
async fn grow_only() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
    x.expect(lvm::cmd::extend(VG, "pvc-1", 2 * GIB), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", 2 * GIB_KIB, St::Open)]);
    let w = run(&x, vec![volume("pvc-1", 2 * GIB, 1)], vec![]).await;
    let mut want = entry("pvc-1", 2 * GIB_KIB, Some(3));
    want.in_use = true;
    assert_eq!(volume_writes(&w), vec![("pvc-1".into(), want)]);
}

#[tokio::test]
async fn adopt_existing_larger_never_shrinks() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", 2 * GIB_KIB, St::Active)]);
    let w = run(&x, vec![volume("pvc-1", GIB, 1)], vec![]).await;
    assert_eq!(
        volume_writes(&w),
        vec![("pvc-1".into(), entry("pvc-1", 2 * GIB_KIB, Some(3)))]
    );
}

#[tokio::test]
async fn adopt_inactive_activates() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, St::Inactive)]);
    x.expect(lvm::cmd::activate(VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
    let w = run(&x, vec![volume("pvc-1", GIB, 1)], vec![]).await;
    assert_eq!(
        volume_writes(&w),
        vec![("pvc-1".into(), entry("pvc-1", GIB_KIB, Some(3)))]
    );
}

#[tokio::test]
async fn unchanged_entry_writes_nothing() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
    let mut v = volume("pvc-1", GIB, 1);
    v.status = Some(VolumeStatus {
        replicas: vec![entry("pvc-1", GIB_KIB, Some(3))],
        ..VolumeStatus::default()
    });
    let w = run(&x, vec![v], vec![]).await;
    assert_eq!(volume_writes(&w), vec![]);
}

#[tokio::test]
async fn delete() {
    let x = Arc::new(ScriptedExec::new());
    let row = lv_row("pvc-1", GIB_KIB, St::Active);
    reads(&x, std::slice::from_ref(&row));
    expect_lvs(&x, &[row]);
    x.expect(lvm::cmd::remove(VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[]);
    let w = run(&x, vec![deleting(volume("pvc-1", GIB, 1))], vec![]).await;
    assert_eq!(
        volume_writes(&w),
        vec![(
            "pvc-1".into(),
            ReplicaStatus {
                node: NODE.into(),
                observed_generation: Some(3),
                ..ReplicaStatus::default()
            }
        )]
    );
}

#[tokio::test]
async fn failing_create_sets_last_error() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    x.expect(
        lvm::cmd::create_thin(VG, THIN, "pvc-1", GIB, &[]),
        fail("  Insufficient free space"),
    );
    expect_lvs(&x, &[]);
    let w = run(&x, vec![volume("pvc-1", GIB, 1)], vec![]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert!(!e.present);
    assert_eq!(
        e.last_error.as_deref(),
        Some("lvcreate: exit 5: Insufficient free space")
    );
    assert_eq!(e.observed_generation, None);
}

#[tokio::test]
async fn missing_pool_object_sets_last_error() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    let mut v = volume("pvc-1", GIB, 1);
    if let Some(a) = v.spec.assignment.as_mut() {
        a.replicas[0].pool = Some("elsewhere".into());
    }
    let w = run(&x, vec![v], vec![]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert_eq!(
        e.last_error.as_deref(),
        Some("pool elsewhere not found on node n1")
    );
}

#[tokio::test]
async fn replicated_without_drbd_and_foreign_volumes() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    let mut foreign = volume("pvc-9", GIB, 1);
    if let Some(a) = foreign.spec.assignment.as_mut() {
        a.replicas[0].node = "n2".into();
    }
    let w = run(
        &x,
        vec![volume("pvc-1", GIB, 2), foreign],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Taking)],
    )
    .await;
    let v = volume_writes(&w);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].0, "pvc-1");
    assert!(!v[0].1.present);
    assert_eq!(
        v[0].1.last_error.as_deref(),
        Some("drbd-utils or the DRBD module not found on this node")
    );
    assert_eq!(snapshot_acks(&w), vec![]);
}

#[tokio::test]
async fn snapshot_taking() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
    x.expect(lvm::cmd::snapshot(VG, "pvc-1", "pvc-1+snap-1"), ok(""));
    let origin = lv_row("pvc-1", GIB_KIB, St::Active)
        .replace(r#""data_percent":"0.00""#, r#""data_percent":"25.00""#);
    expect_lvs(&x, &[origin, lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive)]);
    let w = run(
        &x,
        vec![volume("pvc-1", GIB, 1)],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Taking)],
    )
    .await;
    assert_eq!(
        snapshot_acks(&w),
        vec![("snap-1".into(), taken_ack(SnapshotPhase::Taking, POOL))]
    );
}

#[tokio::test]
async fn snapshot_taking_existing_lv_is_adopted() {
    let x = Arc::new(ScriptedExec::new());
    reads(
        &x,
        &[
            lv_row("pvc-1", GIB_KIB, St::Active),
            lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive),
        ],
    );
    let w = run(
        &x,
        vec![volume("pvc-1", GIB, 1)],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Taking)],
    )
    .await;
    // A take whose ack was lost acks it on the retry.
    let (_, a) = &snapshot_acks(&w)[0];
    assert!(a.ok);
    assert_eq!(
        (a.taken_pool.as_deref(), a.lv_bytes),
        (Some(POOL), Some(GIB))
    );
}

#[tokio::test]
async fn snapshot_deleting_after_volume_is_gone() {
    let x = Arc::new(ScriptedExec::new());
    let snap_lv = lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive);
    reads(&x, std::slice::from_ref(&snap_lv));
    expect_lvs(&x, &[snap_lv]);
    x.expect(lvm::cmd::remove(VG, "pvc-1+snap-1"), ok(""));
    expect_lvs(&x, &[]);
    let mut s = deleting(snapshot("snap-1", "pvc-1", SnapshotPhase::Ready));
    if let Some(st) = s.status.as_mut() {
        st.acks.push(SnapshotAck {
            node: NODE.into(),
            phase: SnapshotPhase::Taking,
            taken_pool: Some(POOL.into()),
            lv_bytes: None,
            allocated_bytes: None,
            ok: true,
            error: None,
        });
    }
    let w = run(&x, vec![], vec![s]).await;
    let acks = snapshot_acks(&w);
    assert_eq!(acks[0].1.phase, SnapshotPhase::Deleting);
    assert!(acks[0].1.ok);
}

/// The replica moved to another Pool on this node after the take: the
/// delete still goes where takenPool says, and the ack carries it.
#[tokio::test]
async fn snapshot_delete_follows_taken_pool_after_a_pool_move() {
    let x = Arc::new(ScriptedExec::new());
    let thin2 = pool_row().replace(r#""lv_name":"thin""#, r#""lv_name":"thin2""#);
    let rows = [
        thin2,
        lv_in("pvc-1", GIB_KIB, "thin2", St::Active),
        lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive),
    ];
    reads(&x, &rows);
    expect_lvs(&x, &rows);
    x.expect(lvm::cmd::remove(VG, "pvc-1+snap-1"), ok(""));
    expect_lvs(&x, &rows[..2]);
    let mut v = volume("pvc-1", GIB, 1);
    if let Some(a) = v.spec.assignment.as_mut() {
        a.replicas[0].pool = Some("pool-n1b".into());
    }
    let snap = deleting(snapshot("snap-1", "pvc-1", SnapshotPhase::Ready));
    let pools = [pool(), pool_in("pool-n1b", VG, "thin2")];
    let w = run_in(&x, &pools, vec![v], vec![snap]).await;
    assert_eq!(
        snapshot_acks(&w),
        vec![("snap-1".into(), taken_ack(SnapshotPhase::Deleting, POOL))]
    );
}

/// The vgs report of another VG.
fn expect_vgs_of(x: &ScriptedExec, vg: &str) {
    let out = vgs_out().replace(&format!(r#""{VG}""#), &format!(r#""{vg}""#));
    x.expect(lvm::cmd::vgs(vg), ok(&out));
}

/// No takenPool (a lost ack): the delete searches every Pool of the node
/// and acks the Pool it found the LV in.
#[tokio::test]
async fn snapshot_delete_without_taken_pool_searches_the_pools() {
    let x = Arc::new(ScriptedExec::new());
    let snap_lv = lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive);
    let with_lv = lvs_out(std::slice::from_ref(&snap_lv));
    reads(&x, &[]);
    x.expect(lvm::cmd::lvs("vg2"), ok(&with_lv));
    expect_vgs_of(&x, "vg2");
    x.expect(lvm::cmd::lvs("vg2"), ok(&with_lv));
    x.expect(lvm::cmd::remove("vg2", "pvc-1+snap-1"), ok(""));
    x.expect(lvm::cmd::lvs("vg2"), ok(&lvs_out(&[])));
    let pools = [pool(), pool_in("pool-n1b", "vg2", THIN)];
    let snap = snapshot("snap-1", "pvc-1", SnapshotPhase::Deleting);
    let w = run_in(&x, &pools, vec![], vec![snap]).await;
    let (_, a) = &snapshot_acks(&w)[0];
    assert!(a.ok, "{a:?}");
    assert_eq!(a.phase, SnapshotPhase::Deleting);
    assert_eq!(a.taken_pool.as_deref(), Some("pool-n1b"));
    assert_eq!(a.lv_bytes, Some(GIB));
}

/// A node of the round without a Pool, a tiebreaker say, holds nothing:
/// its Deleting ack is ok at once.
#[tokio::test]
async fn snapshot_delete_on_a_node_without_pools_is_ok() {
    let x = Arc::new(ScriptedExec::new());
    let mut snap = snapshot("snap-1", "pvc-1", SnapshotPhase::Deleting);
    snap.status.as_mut().unwrap().acks = vec![SnapshotAck {
        taken_pool: None,
        lv_bytes: None,
        allocated_bytes: None,
        ..taken_ack(SnapshotPhase::Taking, POOL)
    }];
    let w = run_in(&x, &[], vec![], vec![snap]).await;
    let (_, a) = &snapshot_acks(&w)[0];
    assert_eq!(
        (a.phase, a.ok, a.taken_pool.as_deref()),
        (SnapshotPhase::Deleting, true, None)
    );
}

/// A Ready Snapshot whose status lost this node's ack, or one taken
/// before acks named their Pool: the LV found again is acked again.
#[tokio::test]
async fn ready_snapshot_lv_is_rediscovered() {
    let legacy = SnapshotAck {
        taken_pool: None,
        lv_bytes: None,
        allocated_bytes: None,
        ..taken_ack(SnapshotPhase::Taking, POOL)
    };
    for acks in [vec![], vec![legacy]] {
        let x = Arc::new(ScriptedExec::new());
        reads(&x, &[lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive)]);
        let mut snap = snapshot("snap-1", "pvc-1", SnapshotPhase::Ready);
        snap.status.as_mut().unwrap().acks = acks;
        let w = run(&x, vec![], vec![snap]).await;
        let want = SnapshotAck {
            allocated_bytes: None,
            ..taken_ack(SnapshotPhase::Taking, POOL)
        };
        assert_eq!(snapshot_acks(&w), vec![("snap-1".into(), want)]);
    }
}

/// Every holder the source had acked: an LV of that name here is no copy
/// of this Snapshot, and nothing is acked.
#[tokio::test]
async fn a_complete_holder_set_is_never_extended() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive)]);
    let mut snap = snapshot("snap-1", "pvc-1", SnapshotPhase::Ready);
    let st = snap.status.as_mut().unwrap();
    st.source_replicas = Some(2);
    st.acks = ["n2", "n3"]
        .map(|n| SnapshotAck {
            node: n.into(),
            ..taken_ack(SnapshotPhase::Taking, "p")
        })
        .to_vec();
    let w = run(&x, vec![], vec![snap]).await;
    assert_eq!(snapshot_acks(&w), vec![]);
}

#[tokio::test]
async fn snapshot_deleting_not_ours_is_ignored() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    let w = run(
        &x,
        vec![],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Deleting)],
    )
    .await;
    assert_eq!(snapshot_acks(&w), vec![]);
}

#[tokio::test]
async fn snapshot_of_another_volume_uid_takes_nothing() {
    for uid in [Some("uid-old".to_owned()), None] {
        let x = Arc::new(ScriptedExec::new());
        reads(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
        let mut s = snapshot("snap-1", "pvc-1", SnapshotPhase::Taking);
        s.spec.volume_uid = uid;
        let w = run(&x, vec![volume("pvc-1", GIB, 1)], vec![s]).await;
        assert_eq!(snapshot_acks(&w), vec![]);
    }
}

#[tokio::test]
async fn restore_from_a_snapshot_of_another_uid_is_an_error() {
    for uid in [Some("uid-old".to_owned()), None] {
        let x = Arc::new(ScriptedExec::new());
        let snap_lv = lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive);
        reads(&x, std::slice::from_ref(&snap_lv));
        expect_lvs(&x, &[snap_lv]);
        let mut v = volume("pvc-2", GIB, 1);
        v.spec.source = Some(VolumeSource {
            snapshot_uid: uid,
            ..source("snap-1")
        });
        let w = run(
            &x,
            vec![v],
            vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Ready)],
        )
        .await;
        let (_, e) = &volume_writes(&w)[0];
        assert!(!e.present);
        assert_eq!(
            e.last_error.as_deref(),
            Some("source snapshot snap-1 not found")
        );
    }
}

#[tokio::test]
async fn snapshot_deleting_is_kept_only_for_its_own_uid() {
    let rows = [
        lv_row("pvc-2", GIB_KIB, St::Active),
        lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive),
    ];
    let mut s = deleting(snapshot("snap-1", "pvc-1", SnapshotPhase::Ready));
    if let Some(st) = s.status.as_mut() {
        st.acks.push(SnapshotAck {
            node: NODE.into(),
            phase: SnapshotPhase::Taking,
            taken_pool: Some(POOL.into()),
            lv_bytes: None,
            allocated_bytes: None,
            ok: true,
            error: None,
        });
    }
    let restore = |uid: &str| {
        let mut v = volume("pvc-2", GIB, 1);
        v.spec.source = Some(VolumeSource {
            snapshot_uid: Some(uid.into()),
            ..source("snap-1")
        });
        v
    };
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &rows);
    let w = run(&x, vec![restore("uid-snap-1")], vec![s.clone()]).await;
    let acks = snapshot_acks(&w);
    assert!(!acks[0].1.ok);
    assert!(acks[0].1.error.as_deref().unwrap().starts_with("kept"));
    // A restore naming another uid holds nothing.
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &rows);
    expect_lvs(&x, &rows);
    x.expect(lvm::cmd::remove(VG, "pvc-1+snap-1"), ok(""));
    expect_lvs(&x, &rows[..1]);
    let w = run(&x, vec![restore("uid-old")], vec![s]).await;
    assert!(snapshot_acks(&w)[0].1.ok);
}

#[tokio::test]
async fn snapshot_suspend_of_local_volume_is_an_error() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
    let w = run(
        &x,
        vec![volume("pvc-1", GIB, 1)],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Suspending)],
    )
    .await;
    let (_, a) = &snapshot_acks(&w)[0];
    assert!(!a.ok);
    assert_eq!(
        a.error.as_deref(),
        Some("replicas 1 volume: no DRBD to suspend or resume")
    );
}

#[test]
fn replica_entry_shape() {
    let rows = lvm::report::parse_lvs(&json("lv", &[lv_row("pvc-1", 4096, St::Inactive)])).unwrap();
    let e = replica_entry(NODE, rows.first(), Some(7), None);
    assert!(e.present);
    assert_eq!(e.device_path, None, "inactive LV has no device path");
    assert_eq!(e.size_bytes, Some(4096 * 1024));
    let e = replica_entry(NODE, None, Some(7), Some("x".into()));
    assert!(!e.present);
    assert_eq!(e.disk_state, None);
}

fn foreign_row(name: &str, pool_lv: &str) -> String {
    lv_in(name, GIB_KIB, pool_lv, St::Active)
}

#[tokio::test]
async fn pool_without_zeroing_gets_it() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::lvs(VG), ok(&json("lv", &[pool_row_zero(false)])))
        .expect(lvm::cmd::enable_zeroing(VG, THIN), ok(""));
    expect_lvs(&x, &[]);
    expect_vgs(&x);
    let w = run(&x, vec![], vec![]).await;
    assert_eq!(w[0].body()["status"]["conditions"][0]["status"], "True");
}

#[tokio::test]
async fn pool_zeroing_failure_is_unavailable() {
    let x = Arc::new(ScriptedExec::new());
    let lvs_out = json("lv", &[pool_row_zero(false)]);
    x.expect(lvm::cmd::lvs(VG), ok(&lvs_out))
        .expect(lvm::cmd::enable_zeroing(VG, THIN), fail("  denied"));
    // Orphan scan re-reads after the invalidation.
    x.expect(lvm::cmd::lvs(VG), ok(&lvs_out));
    let w = run(&x, vec![], vec![]).await;
    let body = w[0].body();
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "ZeroingDisabled");
    assert_eq!(cond["message"], "lvchange: exit 5: denied");
    assert!(body["status"].get("totalBytes").is_none());
}

#[tokio::test]
async fn foreign_lv_is_never_touched() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[foreign_row("pvc-1", "")]);
    let mut v = volume("pvc-1", 2 * GIB, 1);
    v.status = Some(VolumeStatus {
        replicas: vec![entry("pvc-1", GIB_KIB, Some(2))],
        ..VolumeStatus::default()
    });
    let w = run(
        &x,
        vec![v.clone(), deleting(volume("pvc-2", GIB, 1))],
        vec![],
    )
    .await;
    // The report stays: a stranger's LV says nothing about ours.
    let (_, e) = &volume_writes(&w)[0];
    assert!(e.present);
    assert_eq!(e.observed_generation, Some(2));
    assert_eq!(
        e.last_error.as_deref(),
        Some("LV exists outside thin pool thin")
    );

    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[foreign_row("pvc-1", "otherthin")]);
    let w = run(&x, vec![deleting(v)], vec![]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert_eq!(
        e.last_error.as_deref(),
        Some("LV exists outside thin pool thin")
    );
}

#[tokio::test]
async fn foreign_snapshot_lv_is_never_removed() {
    let x = Arc::new(ScriptedExec::new());
    reads(
        &x,
        &[
            lv_row("pvc-1", GIB_KIB, St::Active),
            foreign_row("pvc-1+snap-1", ""),
        ],
    );
    let w = run(
        &x,
        vec![volume("pvc-1", GIB, 1)],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Deleting)],
    )
    .await;
    let (_, a) = &snapshot_acks(&w)[0];
    assert!(!a.ok);
    assert_eq!(
        a.error.as_deref(),
        Some("LV vg1/pvc-1+snap-1 exists outside the thin pools of this node's Pools")
    );
    assert_eq!(a.taken_pool, None);

    // With takenPool: only that Pool's thin pool counts.
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[foreign_row("pvc-1+snap-1", "otherthin")]);
    let mut snap = snapshot("snap-1", "pvc-1", SnapshotPhase::Deleting);
    if let Some(st) = snap.status.as_mut() {
        st.acks = vec![taken_ack(SnapshotPhase::Taking, POOL)];
    }
    let w = run(&x, vec![], vec![snap]).await;
    let (_, a) = &snapshot_acks(&w)[0];
    assert_eq!(a.error.as_deref(), Some("LV exists outside thin pool thin"));
    assert_eq!(a.taken_pool.as_deref(), Some(POOL));
}

#[tokio::test]
async fn foreign_restore_source_is_an_error() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[foreign_row("pvc-1+snap-1", "")]);
    expect_lvs(&x, &[foreign_row("pvc-1+snap-1", "")]);
    let mut v = volume("pvc-2", GIB, 1);
    v.spec.source = Some(source("snap-1"));
    let w = run(
        &x,
        vec![v],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Ready)],
    )
    .await;
    let (_, e) = &volume_writes(&w)[0];
    assert_eq!(
        e.last_error.as_deref(),
        Some("LV exists outside thin pool thin")
    );
}

#[tokio::test]
async fn restore_across_pools_is_an_error() {
    let x = Arc::new(ScriptedExec::new());
    let snap_lv = lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive);
    reads(&x, std::slice::from_ref(&snap_lv));
    expect_lvs(&x, &[snap_lv]);
    let mut v = volume("pvc-2", GIB, 1);
    v.spec.source = Some(source("snap-1"));
    let mut snap = snapshot("snap-1", "pvc-1", SnapshotPhase::Ready);
    if let Some(st) = snap.status.as_mut() {
        st.acks = vec![taken_ack(SnapshotPhase::Taking, "pool-elsewhere")];
    }
    let w = run(&x, vec![v], vec![snap]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert_eq!(
        e.last_error.as_deref(),
        Some("source snapshot snap-1 lives in pool pool-elsewhere, volume assigned to pool-n1")
    );
}

#[tokio::test(start_paused = true)]
async fn failed_delete_keeps_observed_generation() {
    let x = Arc::new(ScriptedExec::new());
    let row = lv_row("pvc-1", GIB_KIB, St::Open);
    reads(&x, std::slice::from_ref(&row));
    // Every look at the open flag before giving up, then the re-read.
    for _ in 0..5 {
        expect_lvs(&x, std::slice::from_ref(&row));
    }
    let mut v = deleting(volume("pvc-1", GIB, 1));
    v.metadata.generation = Some(4);
    v.status = Some(VolumeStatus {
        replicas: vec![entry("pvc-1", GIB_KIB, Some(3))],
        ..VolumeStatus::default()
    });
    let w = run(&x, vec![v], vec![]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert!(e.present);
    assert_eq!(e.observed_generation, Some(3));
    assert_eq!(
        e.last_error.as_deref(),
        Some("lvremove: vg1/pvc-1 is open: still in use after 3s")
    );
}

#[tokio::test]
async fn phase_deleting_without_deletion_timestamp() {
    let x = Arc::new(ScriptedExec::new());
    reads(
        &x,
        &[
            lv_row("pvc-1", GIB_KIB, St::Active),
            lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive),
        ],
    );
    expect_lvs(&x, &[lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive)]);
    x.expect(lvm::cmd::remove(VG, "pvc-1+snap-1"), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
    let w = run(
        &x,
        vec![volume("pvc-1", GIB, 1)],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Deleting)],
    )
    .await;
    let acks = snapshot_acks(&w);
    assert_eq!(acks[0].1.phase, SnapshotPhase::Deleting);
    assert!(acks[0].1.ok);
    let body = w
        .iter()
        .find(|w| matches!(w, StatusWrite::Snapshot { .. }))
        .unwrap()
        .body();
    assert_eq!(body["apiVersion"], "r-drbd.io/v1alpha1");
    assert_eq!(body["kind"], "Snapshot");
    assert_eq!(body["metadata"]["name"], "snap-1");
}

#[tokio::test]
async fn missing_lv_of_block_volume_is_not_recreated() {
    // No fileSystem record: the report alone proves the loss.
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    expect_lvs(&x, &[]);
    let mut v = volume("pvc-1", GIB, 1);
    v.status = Some(VolumeStatus {
        replicas: vec![entry("pvc-1", GIB_KIB, Some(3))],
        ..VolumeStatus::default()
    });
    let w = run(&x, vec![v], vec![]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert!(!e.present);
    assert_eq!(e.last_error.as_deref(), Some(LV_LOST));
    assert_eq!(e.observed_generation, Some(3));
    assert!(!x.seen.lock().unwrap().iter().any(|a| a[0] == "lvcreate"));
}

#[tokio::test]
async fn missing_lv_of_restored_volume_is_not_recreated() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    expect_lvs(&x, &[]);
    let mut v = volume("pvc-1", GIB, 1);
    v.spec.source = Some(source("snap-1"));
    v.status = Some(VolumeStatus {
        replicas: vec![entry("pvc-1", GIB_KIB, Some(3))],
        ..VolumeStatus::default()
    });
    let snaps = || vec![snapshot("snap-1", "pvc-0", SnapshotPhase::Ready)];
    let snap_lv = lv_row("pvc-0+snap-1", GIB_KIB, St::Inactive);
    let w = run(&x, vec![v.clone()], snaps()).await;
    let (_, e) = &volume_writes(&w)[0];
    assert!(!e.present);
    assert_eq!(e.last_error.as_deref(), Some(LV_LOST));
    assert_eq!(e.observed_generation, Some(3));

    // Pass 2 sees pass 1's entry (present false): still refused, the
    // restore source is right there but no lvcreate runs.
    let x = Arc::new(ScriptedExec::new());
    reads(&x, std::slice::from_ref(&snap_lv));
    expect_lvs(&x, &[snap_lv]);
    if let Some(st) = v.status.as_mut() {
        st.replicas = vec![e.clone()];
    }
    let w = run(&x, vec![v.clone()], snaps()).await;
    // The refusal entry is unchanged, so nothing is written either.
    assert_eq!(volume_writes(&w), vec![]);
    assert!(
        !x.seen.lock().unwrap().iter().any(|a| a[0] == "lvcreate"),
        "no create or restore on the second pass"
    );

    // Deleting still completes.
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    let w = run(&x, vec![deleting(v)], vec![]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert!(!e.present && e.last_error.is_none());
}

#[tokio::test]
async fn failed_restore_stays_retryable() {
    let x = Arc::new(ScriptedExec::new());
    let snap_lv = lv_row("pvc-0+snap-1", GIB_KIB, St::Inactive);
    reads(&x, std::slice::from_ref(&snap_lv));
    x.expect(lvm::cmd::restore(VG, "pvc-0+snap-1", "pvc-1", &[]), ok(""));
    expect_lvs(&x, &[snap_lv.clone(), lv_row("pvc-1", GIB_KIB, St::Active)]);
    let mut v = volume("pvc-1", GIB, 1);
    v.spec.source = Some(source("snap-1"));
    v.status = Some(VolumeStatus {
        file_system: Some("xfs".into()),
        replicas: vec![ReplicaStatus {
            node: NODE.into(),
            last_error: Some("lvcreate: exit 5: Insufficient free space".into()),
            ..ReplicaStatus::default()
        }],
        ..VolumeStatus::default()
    });
    let w = run(
        &x,
        vec![v],
        vec![snapshot("snap-1", "pvc-0", SnapshotPhase::Ready)],
    )
    .await;
    assert!(volume_writes(&w)[0].1.present);
}

#[tokio::test]
async fn seeded_marker_does_not_block_restore() {
    let x = Arc::new(ScriptedExec::new());
    let snap_lv = lv_row("pvc-1+snap-1", GIB_KIB, St::Inactive);
    reads(&x, std::slice::from_ref(&snap_lv));
    x.expect(lvm::cmd::restore(VG, "pvc-1+snap-1", "pvc-2", &[]), ok(""));
    expect_lvs(&x, &[snap_lv, lv_row("pvc-2", GIB_KIB, St::Active)]);
    let mut v = volume("pvc-2", GIB, 1);
    v.spec.source = Some(source("snap-1"));
    v.status = Some(VolumeStatus {
        file_system: Some("xfs".into()),
        ..VolumeStatus::default()
    });
    let w = run(
        &x,
        vec![v],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Ready)],
    )
    .await;
    assert_eq!(
        volume_writes(&w),
        vec![("pvc-2".into(), entry("pvc-2", GIB_KIB, Some(3)))]
    );
}

#[tokio::test]
async fn marker_without_prior_presence_creates() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    x.expect(lvm::cmd::create_thin(VG, THIN, "pvc-1", GIB, &[]), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", GIB_KIB, St::Active)]);
    let mut v = volume("pvc-1", GIB, 1);
    v.status = Some(VolumeStatus {
        file_system: Some("xfs".into()),
        ..VolumeStatus::default()
    });
    let w = run(&x, vec![v], vec![]).await;
    assert!(volume_writes(&w)[0].1.present);
}

#[tokio::test]
async fn lv_lost_survives_a_transient_error() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    expect_lvs(&x, &[]);
    let mut v = volume("pvc-1", GIB, 1);
    v.status = Some(VolumeStatus {
        replicas: vec![entry("pvc-1", GIB_KIB, Some(3))],
        ..VolumeStatus::default()
    });
    let w = run(&x, vec![v.clone()], vec![]).await;
    let (_, lost) = &volume_writes(&w)[0];
    assert!(!lost.present);
    assert_eq!(lost.last_error.as_deref(), Some(LV_LOST));

    // lvs fails: the loss must survive it.
    let x = Arc::new(ScriptedExec::new());
    x.expect(
        lvm::cmd::lvs(VG),
        report_fail("Volume group \"vg1\" not found"),
    );
    if let Some(st) = v.status.as_mut() {
        st.replicas = vec![lost.clone()];
    }
    let w = run(&x, vec![v.clone()], vec![]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert!(!e.present);
    let err = e.last_error.as_deref().unwrap();
    assert!(err.starts_with(LV_LOST), "{err}");
    assert!(err.contains("Volume group \"vg1\" not found"), "{err}");

    // Pass 3: lvs healthy, LV still missing: refused, no lvcreate.
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    expect_lvs(&x, &[]);
    if let Some(st) = v.status.as_mut() {
        st.replicas = vec![e.clone()];
    }
    let w = run(&x, vec![v], vec![]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert!(!e.present);
    assert_eq!(e.last_error.as_deref(), Some(LV_LOST));
    assert!(!x.seen.lock().unwrap().iter().any(|a| a[0] == "lvcreate"));
}

fn lost_entry(error: String) -> ReplicaStatus {
    ReplicaStatus {
        node: NODE.into(),
        last_error: Some(error),
        ..ReplicaStatus::default()
    }
}

#[test]
fn lv_lost_is_never_double_wrapped() {
    let transient = keep_lost(Some(&lost_entry(LV_LOST.into())), "lvs: exit 5".into());
    assert_eq!(transient, format!("{LV_LOST}; lvs: exit 5"));
    let rebuild = rebuild_error("lvcreate: exit 5");
    assert_eq!(rebuild, format!("{LV_LOST}; rebuild: lvcreate: exit 5"));
    assert_eq!(rebuild_error(&transient), transient);
    let again = keep_lost(Some(&lost_entry(transient.clone())), rebuild.clone());
    assert_eq!(again, rebuild);
    for e in [transient, rebuild, again] {
        assert_eq!(e.matches(LV_LOST).count(), 1, "{e}");
    }
    // DRBD recomputes present, so a present lost entry must still wrap.
    let present = ReplicaStatus {
        present: true,
        ..lost_entry(LV_LOST.into())
    };
    assert_eq!(
        keep_lost(Some(&present), "x".into()),
        format!("{LV_LOST}; x")
    );
    let healthy = ReplicaStatus {
        present: true,
        ..lost_entry("lvs: exit 5".into())
    };
    assert_eq!(keep_lost(Some(&healthy), "x".into()), "x");
    assert_eq!(keep_lost(None, "x".into()), "x");
}

/// When the Lease was renewed over 60 s while a pass waits for the disk
/// lock, held by the inventory for `hold`, then runs a 60 s step.
async fn renewals_behind_the_scan(hold: Duration) -> Vec<u64> {
    use crate::node::{Liveness, Progress, renew_loop};
    let progress = Progress::default();
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let scan = lock.clone().lock_owned().await;
    tokio::spawn(async move {
        tokio::time::sleep(hold).await;
        drop(scan);
    });
    let x = Arc::new(ScriptedExec::new());
    let rec = Reconciler::new(NODE, x)
        .with_disk_lock(lock)
        .with_progress(progress.clone());
    // The last await before the lock ended just now.
    progress.within(Duration::ZERO);
    let step = progress.clone();
    let pass = tokio::spawn(async move {
        let _g = rec.lock_disks().await;
        step.within(Duration::from_secs(60));
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let live = Liveness::new(progress, None);
    let base = tokio::time::Instant::now();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let rec_seen = seen.clone();
    let hb = tokio::spawn(renew_loop(
        Duration::from_secs(10),
        k8s_openapi::jiff::Timestamp::now,
        move || live.may_renew(),
        move |_| {
            rec_seen.lock().unwrap().push(base.elapsed().as_secs());
            async { Ok(()) }
        },
    ));
    tokio::time::sleep(Duration::from_secs(65)).await;
    hb.abort();
    pass.abort();
    seen.lock().unwrap().clone()
}

#[tokio::test(start_paused = true)]
async fn waiting_for_the_disk_lock_keeps_the_lease_within_the_scan_bound() {
    let hold = crate::inventory::scan::LOCK_HOLD_MAX;
    assert_eq!(
        renewals_behind_the_scan(hold).await,
        [0, 10, 20, 30, 40, 50, 60]
    );
    // Past the last await's slack, still waiting: the wait is declared.
    let progress = crate::node::Progress::default();
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let scan = lock.clone().lock_owned().await;
    let rec = Reconciler::new(NODE, Arc::new(ScriptedExec::new()))
        .with_disk_lock(lock)
        .with_progress(progress.clone());
    progress.within(Duration::ZERO);
    let waiter = tokio::spawn(async move { drop(rec.lock_disks().await) });
    tokio::time::sleep(hold.saturating_sub(Duration::from_millis(100))).await;
    assert!(progress.alive());
    drop(scan);
    waiter.await.unwrap();
    // A scan hung past its bound: renewals stop after the bound and slack.
    assert_eq!(
        renewals_behind_the_scan(Duration::from_secs(3600)).await,
        [0, 10, 20]
    );
}
