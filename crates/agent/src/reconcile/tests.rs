use std::sync::Arc;

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
                gi: None,
                initial_up_to_date_node: None,
            }),
        },
    );
    v.metadata.generation = Some(3);
    v
}

fn snapshot(name: &str, volume: &str, phase: SnapshotPhase) -> Snapshot {
    let mut s = Snapshot::new(
        name,
        SnapshotSpec {
            volume: volume.into(),
        },
    );
    s.status = Some(SnapshotStatus {
        phase,
        ..SnapshotStatus::default()
    });
    s
}

fn deleting<T: kube::Resource>(mut o: T) -> T {
    o.meta_mut().deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
    o
}

fn reads(x: &ScriptedExec, rows: &[String]) {
    expect_pvdisplay(x);
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
    let mut r = Reconciler::new(NODE, x.clone());
    let vols: Vec<Arc<Volume>> = vols.into_iter().map(Arc::new).collect();
    let snaps: Vec<Arc<Snapshot>> = snaps.into_iter().map(Arc::new).collect();
    let w = r.pass(&[pool()], &vols, &snaps).await;
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
    let none = lvm::PvFilter(None);
    let lvs = lvm::argv::lvs(&none, &[VG.to_owned()]);
    let gone = || fail("  Volume group \"vg1\" not found");
    let empty = || crate::exec::scripted::code(5, "", "");
    x.expect(lvm::argv::pvdisplay_for_vg(VG), empty())
        .expect(lvs.clone(), gone())
        .expect(lvm::argv::pvdisplay_for_vg(VG), empty())
        .expect(lvs, gone());
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
        conditions: vec![available.clone()],
        observed_generation: Some(1),
    });
    reads(&x, &[]);
    let w = Reconciler::new(NODE, x.clone())
        .pass(&[Arc::new(p)], &[], &[])
        .await;
    assert!(w.is_empty(), "{w:?}");
    x.assert_done();
}

#[tokio::test]
async fn create() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[]);
    x.expect(
        lvm::argv::lvcreate_thin(&filter(), VG, THIN, "pvc-1", GIB_KIB),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
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
        lvm::argv::lvcreate_thin(&filter(), VG, THIN, "pvc-1", want),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", want, "Vwi-a-tz--", "")]);
    run(&x, vec![volume("pvc-1", GIB + 1, 1)], vec![]).await;
}

#[tokio::test]
async fn restore_then_grow() {
    let x = Arc::new(ScriptedExec::new());
    let snap_lv = lv_row("pvc-1_snap-1", GIB_KIB, "Vri---tz-k", "pvc-1");
    reads(&x, std::slice::from_ref(&snap_lv));
    x.expect(
        lvm::argv::restore_thin(&filter(), VG, "pvc-1_snap-1", "pvc-2"),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-2"), ok(""));
    expect_lvs(
        &x,
        &[
            snap_lv.clone(),
            lv_row("pvc-2", GIB_KIB, "Vwi-a-tz--", "pvc-1_snap-1"),
        ],
    );
    x.expect(
        lvm::argv::lvresize(&filter(), VG, "pvc-2", 2 * GIB_KIB),
        ok(""),
    );
    expect_lvs(
        &x,
        &[
            snap_lv,
            lv_row("pvc-2", 2 * GIB_KIB, "Vwi-a-tz--", "pvc-1_snap-1"),
        ],
    );
    let mut v = volume("pvc-2", 2 * GIB, 1);
    v.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
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
    v.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
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
        Some("snapshot LV vg1/pvc-1_snap-1 not found on this node")
    );
    assert_eq!(e.observed_generation, None);
}

#[tokio::test]
async fn grow_only() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
    x.expect(
        lvm::argv::lvresize(&filter(), VG, "pvc-1", 2 * GIB_KIB),
        ok(""),
    );
    expect_lvs(&x, &[lv_row("pvc-1", 2 * GIB_KIB, "Vwi-aotz--", "")]);
    let w = run(&x, vec![volume("pvc-1", 2 * GIB, 1)], vec![]).await;
    let mut want = entry("pvc-1", 2 * GIB_KIB, Some(3));
    want.in_use = true;
    assert_eq!(volume_writes(&w), vec![("pvc-1".into(), want)]);
}

#[tokio::test]
async fn adopt_existing_larger_never_shrinks() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", 2 * GIB_KIB, "Vwi-a-tz--", "")]);
    let w = run(&x, vec![volume("pvc-1", GIB, 1)], vec![]).await;
    assert_eq!(
        volume_writes(&w),
        vec![("pvc-1".into(), entry("pvc-1", 2 * GIB_KIB, Some(3)))]
    );
}

#[tokio::test]
async fn adopt_inactive_activates() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi---tz--", "")]);
    x.expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
    let w = run(&x, vec![volume("pvc-1", GIB, 1)], vec![]).await;
    assert_eq!(
        volume_writes(&w),
        vec![("pvc-1".into(), entry("pvc-1", GIB_KIB, Some(3)))]
    );
}

#[tokio::test]
async fn unchanged_entry_writes_nothing() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
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
    reads(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
    x.expect(lvm::argv::deactivate(&filter(), VG, "pvc-1"), ok(""))
        .expect(lvm::argv::lvremove(&filter(), VG, "pvc-1"), ok(""));
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
    let create = lvm::argv::lvcreate_thin(&filter(), VG, THIN, "pvc-1", GIB_KIB);
    x.expect(create.clone(), fail("  Insufficient free space"));
    expect_pvdisplay(&x);
    x.expect(create, fail("  Insufficient free space"));
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
        Some("drbd-utils not found on this node")
    );
    assert_eq!(snapshot_acks(&w), vec![]);
}

#[tokio::test]
async fn snapshot_taking() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
    x.expect(
        lvm::argv::snapshot_thin(&filter(), VG, "pvc-1", "pvc-1_snap-1"),
        ok(""),
    );
    expect_lvs(
        &x,
        &[
            lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", ""),
            lv_row("pvc-1_snap-1", GIB_KIB, "Vri-a-tz-k", "pvc-1"),
        ],
    );
    let w = run(
        &x,
        vec![volume("pvc-1", GIB, 1)],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Taking)],
    )
    .await;
    assert_eq!(
        snapshot_acks(&w),
        vec![(
            "snap-1".into(),
            SnapshotAck {
                node: NODE.into(),
                phase: SnapshotPhase::Taking,
                pool: Some(POOL.into()),
                ok: true,
                error: None,
            }
        )]
    );
}

#[tokio::test]
async fn snapshot_taking_existing_lv_is_adopted() {
    let x = Arc::new(ScriptedExec::new());
    reads(
        &x,
        &[
            lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", ""),
            lv_row("pvc-1_snap-1", GIB_KIB, "Vri---tz-k", "pvc-1"),
        ],
    );
    let w = run(
        &x,
        vec![volume("pvc-1", GIB, 1)],
        vec![snapshot("snap-1", "pvc-1", SnapshotPhase::Taking)],
    )
    .await;
    assert!(snapshot_acks(&w)[0].1.ok);
}

#[tokio::test]
async fn snapshot_deleting_after_volume_is_gone() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1_snap-1", GIB_KIB, "Vri---tz-k", "")]);
    x.expect(lvm::argv::lvremove(&filter(), VG, "pvc-1_snap-1"), ok(""));
    expect_lvs(&x, &[]);
    let mut s = deleting(snapshot("snap-1", "pvc-1", SnapshotPhase::Ready));
    if let Some(st) = s.status.as_mut() {
        st.acks.push(SnapshotAck {
            node: NODE.into(),
            phase: SnapshotPhase::Taking,
            pool: Some(POOL.into()),
            ok: true,
            error: None,
        });
    }
    let w = run(&x, vec![], vec![s]).await;
    let acks = snapshot_acks(&w);
    assert_eq!(acks[0].1.phase, SnapshotPhase::Deleting);
    assert!(acks[0].1.ok);
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
async fn snapshot_suspend_of_local_volume_is_an_error() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
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
    let rows = lvm::report::lvs(&lv_row("pvc-1", 4096, "Vwi---tz--", "")).unwrap();
    let e = replica_entry(NODE, rows.first(), Some(7), None);
    assert!(e.present);
    assert_eq!(e.device_path, None, "inactive LV has no device path");
    assert_eq!(e.size_bytes, Some(4096 * 1024));
    let e = replica_entry(NODE, None, Some(7), Some("x".into()));
    assert!(!e.present);
    assert_eq!(e.disk_state, None);
}

fn foreign_row(name: &str, pool_lv: &str) -> String {
    format!("  {name};/dev/{VG}/{name};{GIB_KIB}.00;{VG};{pool_lv};0.00;-wi-a-----;;0.00;1;\n")
}

#[tokio::test]
async fn pool_without_zeroing_gets_it() {
    let x = Arc::new(ScriptedExec::new());
    expect_pvdisplay(&x);
    x.expect(
        lvm::argv::lvs(&filter(), &[VG.to_owned()]),
        ok(&pool_row().replace("twi-a-tz--", "twi-a-t---")),
    )
    .expect(lvm::argv::thinpool_zero(&filter(), VG, THIN), ok(""));
    expect_lvs(&x, &[]);
    expect_vgs(&x);
    let w = run(&x, vec![], vec![]).await;
    assert_eq!(w[0].body()["status"]["conditions"][0]["status"], "True");
}

#[tokio::test]
async fn pool_zeroing_failure_is_unavailable() {
    let x = Arc::new(ScriptedExec::new());
    expect_pvdisplay(&x);
    let lvs_out = pool_row().replace("twi-a-tz--", "twi-a-t---");
    let zero = lvm::argv::thinpool_zero(&filter(), VG, THIN);
    x.expect(lvm::argv::lvs(&filter(), &[VG.to_owned()]), ok(&lvs_out))
        .expect(zero.clone(), fail("  denied"));
    expect_pvdisplay(&x);
    x.expect(zero, fail("  denied"));
    // Orphan scan re-reads after the invalidation.
    x.expect(lvm::argv::lvs(&filter(), &[VG.to_owned()]), ok(&lvs_out));
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
            lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", ""),
            foreign_row("pvc-1_snap-1", ""),
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
    assert_eq!(a.error.as_deref(), Some("LV exists outside thin pool thin"));
}

#[tokio::test]
async fn foreign_restore_source_is_an_error() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[foreign_row("pvc-1_snap-1", "")]);
    expect_lvs(&x, &[foreign_row("pvc-1_snap-1", "")]);
    let mut v = volume("pvc-2", GIB, 1);
    v.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
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
    let snap_lv = lv_row("pvc-1_snap-1", GIB_KIB, "Vri---tz-k", "pvc-1");
    reads(&x, std::slice::from_ref(&snap_lv));
    expect_lvs(&x, &[snap_lv]);
    let mut v = volume("pvc-2", GIB, 1);
    v.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
    let mut snap = snapshot("snap-1", "pvc-1", SnapshotPhase::Ready);
    if let Some(st) = snap.status.as_mut() {
        st.acks.push(SnapshotAck {
            node: NODE.into(),
            phase: SnapshotPhase::Taking,
            pool: Some("pool-elsewhere".into()),
            ok: true,
            error: None,
        });
    }
    let w = run(&x, vec![v], vec![snap]).await;
    let (_, e) = &volume_writes(&w)[0];
    assert_eq!(
        e.last_error.as_deref(),
        Some("source snapshot snap-1 lives in pool pool-elsewhere, volume assigned to pool-n1")
    );
}

#[tokio::test]
async fn delete_inactive_skips_deactivate() {
    let x = Arc::new(ScriptedExec::new());
    reads(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi---tz--", "")]);
    x.expect(lvm::argv::lvremove(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[]);
    let w = run(&x, vec![deleting(volume("pvc-1", GIB, 1))], vec![]).await;
    assert!(!volume_writes(&w)[0].1.present);
}

#[tokio::test]
async fn failed_delete_keeps_observed_generation() {
    let x = Arc::new(ScriptedExec::new());
    let row = lv_row("pvc-1", GIB_KIB, "Vwi-aotz--", "");
    reads(&x, std::slice::from_ref(&row));
    let deact = lvm::argv::deactivate(&filter(), VG, "pvc-1");
    x.expect(deact.clone(), fail("  Logical volume in use."));
    expect_pvdisplay(&x);
    x.expect(deact, fail("  Logical volume in use."));
    expect_lvs(&x, std::slice::from_ref(&row));
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
        Some("lvchange: exit 5: Logical volume in use.")
    );
}

#[tokio::test]
async fn phase_deleting_without_deletion_timestamp() {
    let x = Arc::new(ScriptedExec::new());
    reads(
        &x,
        &[
            lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", ""),
            lv_row("pvc-1_snap-1", GIB_KIB, "Vri---tz-k", "pvc-1"),
        ],
    );
    x.expect(lvm::argv::lvremove(&filter(), VG, "pvc-1_snap-1"), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
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
    v.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
    v.status = Some(VolumeStatus {
        replicas: vec![entry("pvc-1", GIB_KIB, Some(3))],
        ..VolumeStatus::default()
    });
    let snaps = || vec![snapshot("snap-1", "pvc-0", SnapshotPhase::Ready)];
    let snap_lv = lv_row("pvc-0_snap-1", GIB_KIB, "Vri---tz-k", "pvc-0");
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
    let snap_lv = lv_row("pvc-0_snap-1", GIB_KIB, "Vri---tz-k", "pvc-0");
    reads(&x, std::slice::from_ref(&snap_lv));
    x.expect(
        lvm::argv::restore_thin(&filter(), VG, "pvc-0_snap-1", "pvc-1"),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(
        &x,
        &[
            snap_lv.clone(),
            lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "pvc-0_snap-1"),
        ],
    );
    let mut v = volume("pvc-1", GIB, 1);
    v.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
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
    let snap_lv = lv_row("pvc-1_snap-1", GIB_KIB, "Vri---tz-k", "pvc-1");
    reads(&x, std::slice::from_ref(&snap_lv));
    x.expect(
        lvm::argv::restore_thin(&filter(), VG, "pvc-1_snap-1", "pvc-2"),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-2"), ok(""));
    expect_lvs(
        &x,
        &[
            snap_lv,
            lv_row("pvc-2", GIB_KIB, "Vwi-a-tz--", "pvc-1_snap-1"),
        ],
    );
    let mut v = volume("pvc-2", GIB, 1);
    v.spec.source = Some(VolumeSource {
        snapshot: "snap-1".into(),
    });
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
    x.expect(
        lvm::argv::lvcreate_thin(&filter(), VG, THIN, "pvc-1", GIB_KIB),
        ok(""),
    )
    .expect(lvm::argv::activate(&filter(), VG, "pvc-1"), ok(""));
    expect_lvs(&x, &[lv_row("pvc-1", GIB_KIB, "Vwi-a-tz--", "")]);
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
    let lvs = lvm::argv::lvs(&lvm::PvFilter(None), &[VG.to_owned()]);
    let gone = || fail("  Volume group \"vg1\" not found");
    let empty = || crate::exec::scripted::code(5, "", "");
    x.expect(lvm::argv::pvdisplay_for_vg(VG), empty())
        .expect(lvs.clone(), gone())
        .expect(lvm::argv::pvdisplay_for_vg(VG), empty())
        .expect(lvs, gone());
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

#[test]
fn sticky_md_check_unwraps_a_transient_wrap() {
    let md = "metadata of another volume on the LV";
    assert_eq!(unwrap_rebuild(&format!("{LV_LOST}; {md}")), md);
    assert_eq!(unwrap_rebuild(&format!("{LV_LOST}; rebuild: {md}")), md);
    assert_eq!(unwrap_rebuild(LV_LOST), LV_LOST);
    assert_eq!(unwrap_rebuild(md), md);
}
