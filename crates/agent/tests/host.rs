//! Real commands against the VG `r-drbd-test` (thin pool `thin`) on a test
//! host. Ignored by default; run as root with
//! `R_DRBD_HOST_TEST=1 R_DRBD_HOST_IP=<address> ./host-<hash> --ignored --test-threads=1`.
//! Only LVs named pvc-test-host* and paths under /var/lib/rdrbd-dev are touched.

#![allow(clippy::unnecessary_wraps, clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::Arc;

use agent::csi::{FsMarker, Lookup, NodeService, PUBLISH_DEVICE_PATH, VolumeFacts, VolumeLookup};
use agent::exec::TokioExec;
use agent::reconcile::{Reconciler, StatusWrite};
use agent::storage::Lvm;
use csi::tonic::Request;
use csi::v1::node_server::Node;
use csi::v1::volume_capability::{AccessType, BlockVolume, MountVolume};
use csi::v1::{
    CapacityRange, NodeExpandVolumeRequest, NodeGetVolumeStatsRequest, NodePublishVolumeRequest,
    NodeStageVolumeRequest, NodeUnpublishVolumeRequest, NodeUnstageVolumeRequest, VolumeCapability,
};
use model::{
    AssignedReplica, Assignment, Pool, PoolSpec, Role, Snapshot, SnapshotPhase, SnapshotSpec,
    SnapshotStatus, Volume, VolumeSource, VolumeSpec,
};

const VG: &str = "r-drbd-test";
const THIN: &str = "thin";
const MNT: &str = "/var/lib/rdrbd-dev/mnt";
const GIB: u64 = 1 << 30;
const GIB_KIB: u64 = 1 << 20;

fn enabled() -> bool {
    let on = std::env::var("R_DRBD_HOST_TEST").is_ok_and(|v| v == "1");
    if !on {
        eprintln!("R_DRBD_HOST_TEST != 1: skipped");
    }
    on
}

fn lvm() -> Lvm {
    Lvm::new(Arc::new(TokioExec::default()))
}

async fn lv_names(lvm: &Lvm) -> Vec<String> {
    lvm.lvs(VG)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.lv_name)
        .collect()
}

/// Removes leftovers of an earlier failed run.
async fn clean(lvm: &Lvm, prefix: &str) {
    let mut names = lv_names(lvm).await;
    names.retain(|n| n.starts_with(prefix));
    // Snapshot LVs first: names with '_' are restores' sources.
    names.sort_by_key(|n| std::cmp::Reverse(n.contains('_')));
    for n in names {
        lvm.remove(VG, &n).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "needs root and the r-drbd-test VG"]
async fn lvm_lifecycle() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "pvc-test-host-a").await;
    let cap = lvm.probe_pool(VG, THIN).await.unwrap();
    println!("pool: {cap:?}");
    assert!(cap.total_bytes > 0 && cap.chunk_bytes > 0);

    lvm.create(VG, THIN, "pvc-test-host-a1", GIB_KIB)
        .await
        .unwrap();
    let row = |rows: &[lvm::report::LvRow], n: &str| rows.iter().find(|r| r.lv_name == n).cloned();
    let rows = lvm.lvs(VG).await.unwrap();
    let a1 = row(&rows, "pvc-test-host-a1").unwrap();
    assert_eq!(a1.lv_size, GIB_KIB);
    assert!(a1.is_active());

    lvm.resize(VG, "pvc-test-host-a1", 2 * GIB_KIB)
        .await
        .unwrap();
    lvm.snapshot(VG, "pvc-test-host-a1", "pvc-test-host-a1_snap")
        .await
        .unwrap();
    lvm.restore(VG, "pvc-test-host-a1_snap", "pvc-test-host-a2")
        .await
        .unwrap();
    let rows = lvm.lvs(VG).await.unwrap();
    assert_eq!(row(&rows, "pvc-test-host-a1").unwrap().lv_size, 2 * GIB_KIB);
    let snap = row(&rows, "pvc-test-host-a1_snap").unwrap();
    assert_eq!(snap.origin.as_deref(), Some("pvc-test-host-a1"));
    assert!(
        snap.lv_attr.starts_with("Vr"),
        "read-only snapshot: {}",
        snap.lv_attr
    );
    let a2 = row(&rows, "pvc-test-host-a2").unwrap();
    assert!(
        a2.is_active() && a2.lv_attr.starts_with("Vw"),
        "{}",
        a2.lv_attr
    );
    for r in rows
        .iter()
        .filter(|r| r.lv_name.starts_with("pvc-test-host-a"))
    {
        println!(
            "{} {} {}k origin={:?}",
            r.lv_name, r.lv_attr, r.lv_size, r.origin
        );
    }

    for n in [
        "pvc-test-host-a2",
        "pvc-test-host-a1_snap",
        "pvc-test-host-a1",
    ] {
        lvm.remove(VG, n).await.unwrap();
    }
    let left: Vec<String> = lv_names(&lvm)
        .await
        .into_iter()
        .filter(|n| n.starts_with("pvc-test-host-a"))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

fn pool() -> Arc<Pool> {
    let mut p = Pool::new(
        "r-drbd-test-host",
        PoolSpec {
            node: "host".into(),
            tier: "test".into(),
            vg: VG.into(),
            thin_pool: THIN.into(),
        },
    );
    p.metadata.generation = Some(1);
    Arc::new(p)
}

fn volume(name: &str, size: u64, source: Option<&str>) -> Volume {
    let mut v = Volume::new(
        name,
        VolumeSpec {
            size_bytes: size,
            replicas: 1,
            tier: "test".into(),
            pool: None,
            node: None,
            attachments: vec![],
            access_mode: model::AccessMode::default(),
            evict: vec![],
            source: source.map(|s| VolumeSource { snapshot: s.into() }),
            resolve: None,
            assignment: Some(Assignment {
                replicas: vec![AssignedReplica {
                    node: "host".into(),
                    pool: Some("r-drbd-test-host".into()),
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
    v.metadata.generation = Some(1);
    v
}

fn entry(writes: &[StatusWrite], name: &str) -> model::ReplicaStatus {
    writes
        .iter()
        .find_map(|w| match w {
            StatusWrite::Volume { name: n, entry } if n == name => Some(entry.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no entry for {name} in {writes:?}"))
}

#[tokio::test]
#[ignore = "needs root and the r-drbd-test VG"]
async fn reconcile_decision_table() {
    if !enabled() {
        return;
    }
    clean(&lvm(), "pvc-test-host-r").await;
    let mut rec = Reconciler::new("host", Arc::new(TokioExec::default()));
    let pools = [pool()];

    let v1 = volume("pvc-test-host-r1", GIB, None);
    let w = rec.pass(&pools, &[Arc::new(v1.clone())], &[]).await;
    let e = entry(&w, "pvc-test-host-r1");
    println!("create: {e:?}");
    assert!(e.present && e.last_error.is_none());
    assert_eq!(e.size_bytes, Some(GIB));
    assert_eq!(
        e.device_path.as_deref(),
        Some("/dev/r-drbd-test/pvc-test-host-r1")
    );

    let mut grown = v1.clone();
    grown.spec.size_bytes = 2 * GIB;
    grown.metadata.generation = Some(2);
    let w = rec.pass(&pools, &[Arc::new(grown.clone())], &[]).await;
    let e = entry(&w, "pvc-test-host-r1");
    println!("grow: {e:?}");
    assert_eq!(
        (e.size_bytes, e.observed_generation),
        (Some(2 * GIB), Some(2))
    );

    let mut snap = Snapshot::new(
        "snapshot-test-host",
        SnapshotSpec {
            volume: "pvc-test-host-r1".into(),
        },
    );
    snap.status = Some(SnapshotStatus {
        phase: SnapshotPhase::Taking,
        ..SnapshotStatus::default()
    });
    let w = rec
        .pass(
            &pools,
            &[Arc::new(grown.clone())],
            &[Arc::new(snap.clone())],
        )
        .await;
    let ack = w
        .iter()
        .find_map(|w| match w {
            StatusWrite::Snapshot { ack, .. } => Some(ack.clone()),
            _ => None,
        })
        .unwrap();
    println!("take: {ack:?}");
    assert!(ack.ok);

    let v2 = volume("pvc-test-host-r2", 3 * GIB, Some("snapshot-test-host"));
    let w = rec
        .pass(
            &pools,
            &[Arc::new(grown.clone()), Arc::new(v2.clone())],
            &[Arc::new(snap.clone())],
        )
        .await;
    let e = entry(&w, "pvc-test-host-r2");
    println!("restore: {e:?}");
    assert!(e.present && e.last_error.is_none());
    assert_eq!(e.size_bytes, Some(3 * GIB));

    let mut del1 = grown.clone();
    let mut del2 = v2.clone();
    let now =
        k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(k8s_openapi::jiff::Timestamp::now());
    del1.metadata.deletion_timestamp = Some(now.clone());
    del2.metadata.deletion_timestamp = Some(now.clone());
    snap.metadata.deletion_timestamp = Some(now);
    let w = rec
        .pass(&pools, &[Arc::new(del1), Arc::new(del2)], &[Arc::new(snap)])
        .await;
    for n in ["pvc-test-host-r1", "pvc-test-host-r2"] {
        let e = entry(&w, n);
        println!("delete {n}: {e:?}");
        assert!(!e.present && e.last_error.is_none());
    }
    let left: Vec<String> = lv_names(&lvm())
        .await
        .into_iter()
        .filter(|n| n.starts_with("pvc-test-host-r"))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

/// Devices come from publish_context; the marker lives in memory.
#[derive(Clone, Default)]
struct NoLookup(Arc<std::sync::Mutex<Option<String>>>);

impl VolumeLookup for NoLookup {
    fn lookup(&self, _: &str, _: &str) -> Lookup {
        Lookup::VolumeMissing
    }

    fn facts(&self, _: &str) -> Option<VolumeFacts> {
        Some(VolumeFacts {
            file_system: self.0.lock().unwrap().clone(),
            restored: false,
        })
    }
}

#[async_trait::async_trait]
impl FsMarker for NoLookup {
    async fn write(&self, _: &str, fs: &str) -> Result<(), String> {
        *self.0.lock().unwrap() = Some(fs.to_owned());
        Ok(())
    }

    async fn read_live(&self, _: &str) -> Result<Option<String>, String> {
        Ok(self.0.lock().unwrap().clone())
    }
}

fn node_service() -> NodeService {
    let state = NoLookup::default();
    NodeService::new(
        "host".into(),
        Arc::new(TokioExec::default()),
        Arc::new(state.clone()),
        Arc::new(state),
    )
}

/// Unmounts (deepest first) and removes the LV when the test ends, also on
/// panic, so a failed run leaves no mounts behind on the host.
struct Cleanup {
    mounts: Vec<String>,
    lv: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        for m in self.mounts.iter().rev() {
            let _ = std::process::Command::new("umount").arg(m).output();
        }
        let _ = std::process::Command::new("lvremove")
            .args(["-f", &format!("{VG}/{}", self.lv)])
            .output();
    }
}

fn mount_cap() -> Option<VolumeCapability> {
    Some(VolumeCapability {
        access_mode: None,
        access_type: Some(AccessType::Mount(MountVolume::default())),
    })
}

fn block_cap() -> Option<VolumeCapability> {
    Some(VolumeCapability {
        access_mode: None,
        access_type: Some(AccessType::Block(BlockVolume {})),
    })
}

#[tokio::test]
#[ignore = "needs root and the r-drbd-test VG"]
async fn csi_node_cycle() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "pvc-test-host-c").await;
    let lv = "pvc-test-host-c1";
    lvm.create(VG, THIN, lv, GIB_KIB).await.unwrap();
    let dev = format!("/dev/{VG}/{lv}");
    let svc = node_service();
    let ctx = HashMap::from([(PUBLISH_DEVICE_PATH.to_owned(), dev.clone())]);
    let staging = format!("{MNT}/staging/{lv}");
    let target = format!("{MNT}/pods/{lv}/mount");
    let block_target = format!("{MNT}/pods/{lv}/block");
    let _cleanup = Cleanup {
        mounts: vec![staging.clone(), target.clone(), block_target.clone()],
        lv: lv.to_owned(),
    };

    let stage = || NodeStageVolumeRequest {
        volume_id: lv.into(),
        publish_context: ctx.clone(),
        staging_target_path: staging.clone(),
        volume_capability: mount_cap(),
        ..NodeStageVolumeRequest::default()
    };
    svc.node_stage_volume(Request::new(stage())).await.unwrap();
    svc.node_stage_volume(Request::new(stage())).await.unwrap();
    svc.node_publish_volume(Request::new(NodePublishVolumeRequest {
        volume_id: lv.into(),
        staging_target_path: staging.clone(),
        target_path: target.clone(),
        volume_capability: mount_cap(),
        ..NodePublishVolumeRequest::default()
    }))
    .await
    .unwrap();
    std::fs::write(format!("{target}/hello"), b"r-drbd").unwrap();

    let stats = svc
        .node_get_volume_stats(Request::new(NodeGetVolumeStatsRequest {
            volume_id: lv.into(),
            volume_path: target.clone(),
            staging_target_path: staging.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    println!("stats: {:?}", stats.usage);
    assert!(stats.usage[0].total > 800 << 20);

    lvm.resize(VG, lv, 2 * GIB_KIB).await.unwrap();
    let got = svc
        .node_expand_volume(Request::new(NodeExpandVolumeRequest {
            volume_id: lv.into(),
            volume_path: target.clone(),
            capacity_range: Some(CapacityRange {
                required_bytes: i64::try_from(2 * GIB).unwrap(),
                limit_bytes: 0,
            }),
            volume_capability: mount_cap(),
            ..NodeExpandVolumeRequest::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let stats = svc
        .node_get_volume_stats(Request::new(NodeGetVolumeStatsRequest {
            volume_id: lv.into(),
            volume_path: target.clone(),
            staging_target_path: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    println!(
        "expanded to {}, fs total {}",
        got.capacity_bytes, stats.usage[0].total
    );
    assert!(stats.usage[0].total > 1800 << 20);

    let unpublish = || NodeUnpublishVolumeRequest {
        volume_id: lv.into(),
        target_path: target.clone(),
    };
    svc.node_unpublish_volume(Request::new(unpublish()))
        .await
        .unwrap();
    svc.node_unpublish_volume(Request::new(unpublish()))
        .await
        .unwrap();
    assert!(!std::path::Path::new(&target).exists());
    let unstage = || NodeUnstageVolumeRequest {
        volume_id: lv.into(),
        staging_target_path: staging.clone(),
    };
    svc.node_unstage_volume(Request::new(unstage()))
        .await
        .unwrap();
    svc.node_unstage_volume(Request::new(unstage()))
        .await
        .unwrap();

    // A second stage must find the file system and keep the data.
    svc.node_stage_volume(Request::new(stage())).await.unwrap();
    let data = std::fs::read(format!("{staging}/hello")).unwrap();
    assert_eq!(data, b"r-drbd");
    svc.node_unstage_volume(Request::new(unstage()))
        .await
        .unwrap();

    // Block: nothing at stage, the device bound onto a file at publish.
    svc.node_stage_volume(Request::new(NodeStageVolumeRequest {
        volume_capability: block_cap(),
        ..stage()
    }))
    .await
    .unwrap();
    svc.node_publish_volume(Request::new(NodePublishVolumeRequest {
        volume_id: lv.into(),
        publish_context: ctx.clone(),
        staging_target_path: staging.clone(),
        target_path: block_target.clone(),
        volume_capability: block_cap(),
        ..NodePublishVolumeRequest::default()
    }))
    .await
    .unwrap();
    let stats = svc
        .node_get_volume_stats(Request::new(NodeGetVolumeStatsRequest {
            volume_id: lv.into(),
            volume_path: block_target.clone(),
            staging_target_path: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    println!("block stats: {:?}", stats.usage);
    assert_eq!(stats.usage[0].total, i64::try_from(2 * GIB).unwrap());
    svc.node_unpublish_volume(Request::new(NodeUnpublishVolumeRequest {
        volume_id: lv.into(),
        target_path: block_target.clone(),
    }))
    .await
    .unwrap();
    assert!(!std::path::Path::new(&block_target).exists());

    let _ = std::fs::remove_dir_all(format!("{MNT}/pods/{lv}"));
    let _ = std::fs::remove_dir(&staging);
    lvm.remove(VG, lv).await.unwrap();
    let left: Vec<String> = lv_names(&lvm)
        .await
        .into_iter()
        .filter(|n| n == lv)
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

/// An empty msdos label: what `parted mklabel msdos` leaves.
fn write_msdos_label(dev: &str) {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().write(true).open(dev).unwrap();
    f.seek(SeekFrom::Start(440)).unwrap();
    f.write_all(&[0x12, 0x34, 0x56, 0x78]).unwrap();
    f.seek(SeekFrom::Start(510)).unwrap();
    f.write_all(&[0x55, 0xAA]).unwrap();
    f.sync_all().unwrap();
}

#[tokio::test]
#[ignore = "needs root and the r-drbd-test VG"]
async fn stage_refuses_partition_table() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "pvc-test-host-p").await;
    let lv = "pvc-test-host-p1";
    lvm.create(VG, THIN, lv, GIB_KIB).await.unwrap();
    let staging = format!("{MNT}/staging/{lv}");
    let _cleanup = Cleanup {
        mounts: vec![staging.clone()],
        lv: lv.to_owned(),
    };
    let dev = format!("/dev/{VG}/{lv}");
    write_msdos_label(&dev);
    let svc = node_service();
    let e = svc
        .node_stage_volume(Request::new(NodeStageVolumeRequest {
            volume_id: lv.into(),
            publish_context: HashMap::from([(PUBLISH_DEVICE_PATH.to_owned(), dev.clone())]),
            staging_target_path: staging.clone(),
            volume_capability: mount_cap(),
            ..NodeStageVolumeRequest::default()
        }))
        .await
        .unwrap_err();
    println!(
        "stage on a partitioned device: {:?} {}",
        e.code(),
        e.message()
    );
    assert_eq!(e.code(), csi::tonic::Code::FailedPrecondition);
    let out = std::process::Command::new("blkid")
        .args(["-p", "-o", "export", &dev])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("PTTYPE=dos") && !text.contains("\nTYPE="),
        "{text}"
    );
    let _ = std::fs::remove_dir(&staging);
}

// Replicated volume on the single test host, with a peer "ghost" that never
// answers. Only DRBD resources named pvc-test-*, minors 1900 to 1999 and
// ports 7900 to 7999 are used; the resource file goes to /var/lib/r-drbd.

const DRBD_VOL: &str = "pvc-test-host-d1";
const DRBD_SNAP: &str = "snap-test-host-d1";
const DRBD_MINOR: u32 = 1900;
const DRBD_PORT: u16 = 7900;
const RES_DIR: &str = "/var/lib/r-drbd";

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .unwrap()
        .trim()
        .to_owned()
}

async fn sh(parts: &[&str]) -> agent::Output {
    use agent::Exec;
    TokioExec::default()
        .run(&agent::exec::argv(parts), agent::exec::DEFAULT_TIMEOUT)
        .await
        .unwrap()
}

/// Leftovers of an earlier failed run: the resource, its files, its LVs.
async fn drbd_clean() {
    let _ = sh(&["drbdadm", "resume-io", DRBD_VOL]).await;
    let _ = sh(&["drbdsetup", "down", DRBD_VOL]).await;
    for p in [
        format!("{RES_DIR}/{DRBD_VOL}.res"),
        format!("{RES_DIR}/.backup/{DRBD_VOL}.res"),
    ] {
        let _ = std::fs::remove_file(p);
    }
    clean(&lvm(), DRBD_VOL).await;
}

async fn lv_tags(lv: &str) -> Vec<String> {
    lvm().tags(VG).await.unwrap().remove(lv).unwrap_or_default()
}

fn drbd_pool(node: &str) -> Arc<Pool> {
    let mut p = (*pool()).clone();
    node.clone_into(&mut p.spec.node);
    Arc::new(p)
}

fn drbd_volume(node: &str, size: u64, generation: i64, ghost: bool) -> Volume {
    let mut replicas = vec![AssignedReplica {
        node: node.to_owned(),
        pool: Some("r-drbd-test-host".into()),
        node_id: 0,
        port: Some(DRBD_PORT),
        role: Role::Diskful,
    }];
    if ghost {
        replicas.push(AssignedReplica {
            node: "ghost".into(),
            pool: Some("ghost-pool".into()),
            node_id: 1,
            port: Some(DRBD_PORT),
            role: Role::Diskful,
        });
    }
    let mut v = Volume::new(
        DRBD_VOL,
        VolumeSpec {
            size_bytes: size,
            replicas: 2,
            tier: "test".into(),
            pool: None,
            node: None,
            attachments: vec![],
            access_mode: model::AccessMode::default(),
            evict: vec![],
            source: None,
            resolve: None,
            assignment: Some(Assignment {
                replicas,
                minor: Some(DRBD_MINOR),
                secret: Some("dGVzdHNlY3JldHRlc3Rz".into()),
                gi: Some("5F3E2D1C0B0A0908".into()),
                initial_up_to_date_node: Some(node.to_owned()),
            }),
        },
    );
    v.metadata.generation = Some(generation);
    v
}

/// Carries the entries written so far, as the API server would.
fn with_status(mut v: Volume, entry: Option<&model::ReplicaStatus>) -> Volume {
    v.status = Some(model::VolumeStatus {
        replicas: entry.cloned().into_iter().collect(),
        ..model::VolumeStatus::default()
    });
    v
}

async fn wait_for<F>(tracker: &std::sync::Mutex<agent::events2::Tracker>, what: &str, f: F)
where
    F: Fn(&agent::events2::Tracker) -> bool,
{
    for _ in 0..100 {
        if f(&tracker.lock().unwrap()) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for events2: {what}");
}

fn sectors(minor: u32) -> u64 {
    std::fs::read_to_string(format!("/sys/block/drbd{minor}/size"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn snap_ack(writes: &[StatusWrite]) -> model::SnapshotAck {
    writes
        .iter()
        .find_map(|w| match w {
            StatusWrite::Snapshot { ack, .. } => Some(ack.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no ack in {writes:?}"))
}

fn drbd_snapshot(phase: SnapshotPhase) -> Snapshot {
    let mut s = Snapshot::new(
        DRBD_SNAP,
        SnapshotSpec {
            volume: DRBD_VOL.into(),
        },
    );
    s.status = Some(SnapshotStatus {
        phase,
        ..SnapshotStatus::default()
    });
    s
}

#[tokio::test]
#[ignore = "needs root, DRBD 9 and the r-drbd-test VG"]
async fn drbd_replicated_lifecycle() {
    use agent::drbd::{self, Notify};
    use agent::events2::Tracker;
    use agent::reconcile::DrbdEnv;
    use agent::watch::Trigger;

    if !enabled() {
        return;
    }
    let node = hostname();
    let host_ip: std::net::IpAddr = std::env::var("R_DRBD_HOST_IP")
        .expect("R_DRBD_HOST_IP: the host's DRBD address")
        .parse()
        .unwrap();
    drbd_clean().await;

    let exec: Arc<dyn agent::Exec> = Arc::new(TokioExec::default());
    let tools = agent::node::probe(exec.as_ref()).await;
    let versions = tools.drbd_versions().expect("DRBD tools on the host");
    let tracker = Arc::new(std::sync::Mutex::new(Tracker::default()));
    let events = tokio::spawn(drbd::events2(
        None,
        tracker.clone(),
        Notify {
            changes: Arc::new(Trigger::default()),
            pass: Arc::new(Trigger::default()),
        },
    ));
    wait_for(&tracker, "initial dump", Tracker::is_available).await;

    let mut rec = Reconciler::new(node.clone(), exec).with_drbd(DrbdEnv::new(
        Some(versions),
        RES_DIR,
        tracker.clone(),
    ));
    rec.set_addresses(
        [
            (node.clone(), host_ip),
            ("ghost".to_owned(), "192.0.2.1".parse().unwrap()),
        ]
        .into_iter()
        .collect(),
    );
    let pools = [drbd_pool(&node)];

    // Creation: LV of gross size, metadata, day-0 ids, adjust.
    let v = drbd_volume(&node, GIB, 1, true);
    let w = rec.pass(&pools, &[Arc::new(v.clone())], &[]).await;
    let e = entry(&w, DRBD_VOL);
    println!("create: {e:?}");
    assert_eq!(e.last_error, None);
    assert_eq!(e.allocated_bytes, Some(1_052_672 * 1024));
    let rows = lvm().lvs(VG).await.unwrap();
    let row = rows.iter().find(|r| r.lv_name == DRBD_VOL).unwrap();
    assert_eq!(row.lv_size, 1_052_672);
    assert_eq!(
        lv_tags(DRBD_VOL).await,
        ["r-drbd.md.0"],
        "own metadata tag, new tag gone after create-md"
    );
    let res_path = format!("{RES_DIR}/{DRBD_VOL}.res");
    let file = std::fs::read_to_string(&res_path).unwrap();
    println!("{file}");
    assert!(file.contains("host \"ghost\" address ipv4 192.0.2.1:7900;"));

    let status = sh(&["drbdsetup", "status", "--json", DRBD_VOL]).await;
    assert_eq!(status.exit_code, Some(0), "{}", status.stderr);
    let json: serde_json::Value = serde_json::from_str(&status.stdout).unwrap();
    let r0 = &json[0];
    assert_eq!(r0["role"], "Secondary");
    assert_eq!(r0["devices"][0]["disk-state"], "UpToDate", "winner");
    assert_eq!(r0["connections"][0]["name"], "ghost");
    assert_eq!(r0["connections"][0]["connection-state"], "Connecting");
    println!(
        "drbdsetup status: role {} disk {} ghost {}",
        r0["role"], r0["devices"][0]["disk-state"], r0["connections"][0]["connection-state"]
    );

    wait_for(&tracker, "resource up and Connecting", |t| {
        t.resource(DRBD_VOL).is_some_and(|r| {
            r.devices.get(&0).and_then(|d| d.disk.as_deref()) == Some("UpToDate")
                && r.connections.get("ghost").map(|c| c.state.as_str()) == Some("Connecting")
        })
    })
    .await;
    let w = rec
        .refresh(&[Arc::new(with_status(v.clone(), Some(&e)))])
        .await;
    let e = entry(&w, DRBD_VOL);
    println!("events2: {e:?}");
    assert!(e.present && !e.in_use);
    assert_eq!(e.disk_state.as_deref(), Some("UpToDate"));
    assert_eq!(e.peers.get("ghost").map(String::as_str), Some("Connecting"));
    assert_eq!(e.device_path.as_deref(), Some("/dev/drbd1900"));
    assert_eq!(e.size_bytes, Some(1_052_408 * 1024));
    assert_eq!(e.quorum, Some(false), "one of two nodes");

    // Second pass: nothing changes, the file is byte stable.
    let w = rec
        .pass(&pools, &[Arc::new(with_status(v.clone(), Some(&e)))], &[])
        .await;
    assert!(
        w.iter().all(|w| !matches!(w, StatusWrite::Volume { .. })),
        "{w:?}"
    );
    assert_eq!(std::fs::read_to_string(&res_path).unwrap(), file);

    // Snapshot rounds.
    let vols = [Arc::new(with_status(v.clone(), Some(&e)))];
    for phase in [
        SnapshotPhase::Suspending,
        SnapshotPhase::Taking,
        SnapshotPhase::Resuming,
    ] {
        let w = rec
            .pass(&pools, &vols, &[Arc::new(drbd_snapshot(phase))])
            .await;
        let ack = snap_ack(&w);
        println!("{phase:?}: {ack:?}");
        assert!(ack.ok && ack.phase == phase, "{ack:?}");
        let want_suspended = phase != SnapshotPhase::Resuming;
        wait_for(&tracker, "suspend state", |t| {
            t.resource(DRBD_VOL)
                .is_some_and(|r| r.suspended_by_user() == want_suspended)
        })
        .await;
    }
    let snap_lv = format!("{DRBD_VOL}_{DRBD_SNAP}");
    assert!(lv_names(&lvm()).await.contains(&snap_lv));
    let w = rec
        .pass(
            &pools,
            &vols,
            &[Arc::new(drbd_snapshot(SnapshotPhase::Deleting))],
        )
        .await;
    assert!(snap_ack(&w).ok);
    assert!(!lv_names(&lvm()).await.contains(&snap_lv));

    // Resize: the LV grows, the device waits for the ghost that never grows.
    let before = sectors(DRBD_MINOR);
    let v2 = with_status(drbd_volume(&node, 2 * GIB, 2, true), Some(&e));
    let w = rec.pass(&pools, &[Arc::new(v2)], &[]).await;
    let e2 = entry(&w, DRBD_VOL);
    println!("grow, gated: {e2:?}");
    assert_eq!(e2.allocated_bytes, Some(2_101_248 * 1024));
    assert_eq!(e2.observed_generation, Some(1), "waits on the ghost");
    assert_eq!(e2.last_error, None);
    assert_eq!(sectors(DRBD_MINOR), before);

    // The ghost leaves: this node is the lowest id alone and resizes.
    let v3 = with_status(drbd_volume(&node, 2 * GIB, 3, false), Some(&e2));
    let w = rec.pass(&pools, &[Arc::new(v3.clone())], &[]).await;
    let e3 = entry(&w, DRBD_VOL);
    println!("grow, alone: {e3:?}");
    assert_eq!(e3.last_error, None);
    assert_eq!(e3.observed_generation, Some(3));
    let after = sectors(DRBD_MINOR);
    println!("device sectors {before} -> {after}");
    assert!(after >= 2 * GIB / 512, "{after}");
    assert!(
        !std::fs::read_to_string(&res_path)
            .unwrap()
            .contains("ghost")
    );
    wait_for(&tracker, "ghost connection gone", |t| {
        t.resource(DRBD_VOL)
            .is_some_and(|r| r.connections.is_empty())
    })
    .await;

    // This node drops out of a live Volume: DRBD down, files gone, data kept.
    let mut away = v3.clone();
    if let Some(a) = away.spec.assignment.as_mut() {
        a.replicas[0].node = "elsewhere".into();
    }
    let w = rec.pass(&pools, &[Arc::new(away)], &[]).await;
    println!("departed: {w:?}");
    assert!(
        w.iter()
            .any(|w| matches!(w, StatusWrite::VolumeGone { name } if name == DRBD_VOL))
    );
    assert!(!std::path::Path::new(&res_path).exists());
    assert_ne!(
        sh(&["drbdsetup", "status", DRBD_VOL]).await.exit_code,
        Some(0)
    );
    assert!(
        lv_names(&lvm()).await.contains(&DRBD_VOL.to_owned()),
        "LV kept"
    );
    assert_eq!(
        lv_tags(DRBD_VOL).await,
        Vec::<String>::new(),
        "tags dropped"
    );
    wait_for(&tracker, "resource destroyed", |t| {
        t.resource(DRBD_VOL).is_none()
    })
    .await;

    // It comes back with no entry left after initializedAt: its
    // metadata is stale, so it is wiped and created again, no generation
    // ids; alone it stays Inconsistent (a peer would give a full sync).
    let mut rejoin = v3.clone();
    rejoin.status = Some(model::VolumeStatus {
        initialized_at: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
            k8s_openapi::jiff::Timestamp::now(),
        )),
        ..model::VolumeStatus::default()
    });
    let w = rec.pass(&pools, &[Arc::new(rejoin)], &[]).await;
    let back = entry(&w, DRBD_VOL);
    println!("rejoined: {back:?}");
    assert_eq!(lv_tags(DRBD_VOL).await, ["r-drbd.md.0"], "sealed again");
    assert_eq!(back.last_error, None);
    wait_for(&tracker, "rejoined Inconsistent", |t| {
        t.resource(DRBD_VOL)
            .and_then(|r| r.devices.get(&0))
            .and_then(|d| d.disk.as_deref())
            == Some("Inconsistent")
    })
    .await;
    assert!(sectors(DRBD_MINOR) >= 2 * GIB / 512);

    // Delete: not tracked any more, so status confirms down; metadata wiped,
    // LV removed, files removed.
    let mut del = v3;
    del.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        k8s_openapi::jiff::Timestamp::now(),
    ));
    let w = rec.pass(&pools, &[Arc::new(del)], &[]).await;
    let e4 = entry(&w, DRBD_VOL);
    println!("delete: {e4:?}");
    assert!(!e4.present && e4.last_error.is_none());
    assert!(!std::path::Path::new(&res_path).exists());
    assert!(!std::path::Path::new(&format!("{RES_DIR}/.backup/{DRBD_VOL}.res")).exists());
    assert!(
        !lv_names(&lvm())
            .await
            .iter()
            .any(|n| n.starts_with(DRBD_VOL))
    );
    let st = sh(&["drbdsetup", "status", DRBD_VOL]).await;
    assert_ne!(st.exit_code, Some(0), "{}", st.stdout);
    assert!(tracker.lock().unwrap().resource(DRBD_VOL).is_none());
    events.abort();
    let _ = events.await;
}

/// A restored volume without its marker yet.
#[derive(Clone, Default)]
struct Restored(Arc<std::sync::Mutex<Option<String>>>);

impl VolumeLookup for Restored {
    fn lookup(&self, _: &str, _: &str) -> Lookup {
        Lookup::VolumeMissing
    }

    fn facts(&self, _: &str) -> Option<VolumeFacts> {
        Some(VolumeFacts {
            file_system: self.0.lock().unwrap().clone(),
            restored: true,
        })
    }
}

#[async_trait::async_trait]
impl FsMarker for Restored {
    async fn write(&self, _: &str, fs: &str) -> Result<(), String> {
        *self.0.lock().unwrap() = Some(fs.to_owned());
        Ok(())
    }

    async fn read_live(&self, _: &str) -> Result<Option<String>, String> {
        Ok(self.0.lock().unwrap().clone())
    }
}

struct Unwind {
    mounts: Vec<String>,
    lvs: Vec<String>,
}

impl Drop for Unwind {
    fn drop(&mut self) {
        for m in self.mounts.iter().rev() {
            let _ = std::process::Command::new("umount").arg(m).output();
        }
        for lv in &self.lvs {
            let _ = std::process::Command::new("lvremove")
                .args(["-f", &format!("{VG}/{lv}")])
                .output();
        }
    }
}

fn fs_uuid(dev: &str) -> String {
    let out = std::process::Command::new("blkid")
        .args(["-p", "-s", "UUID", "-o", "value", dev])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// A restore staged on the node where its source is mounted.
#[tokio::test]
#[ignore = "needs root and the r-drbd-test VG"]
async fn restored_xfs_mounts_next_to_its_source() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "pvc-test-host-u").await;
    let (src, snap, copy) = (
        "pvc-test-host-u1",
        "pvc-test-host-u1_snap",
        "pvc-test-host-u2",
    );
    let staging1 = format!("{MNT}/staging/{src}");
    let staging2 = format!("{MNT}/staging/{copy}");
    let _unwind = Unwind {
        mounts: vec![staging1.clone(), staging2.clone()],
        lvs: vec![copy.to_owned(), snap.to_owned(), src.to_owned()],
    };
    lvm.create(VG, THIN, src, GIB_KIB).await.unwrap();
    let stage = |svc: &NodeService, lv: &str, staging: &str| {
        let req = NodeStageVolumeRequest {
            volume_id: lv.into(),
            publish_context: HashMap::from([(
                PUBLISH_DEVICE_PATH.to_owned(),
                format!("/dev/{VG}/{lv}"),
            )]),
            staging_target_path: staging.to_owned(),
            volume_capability: mount_cap(),
            ..NodeStageVolumeRequest::default()
        };
        let svc = svc.clone();
        async move { svc.node_stage_volume(Request::new(req)).await }
    };
    stage(&node_service(), src, &staging1).await.unwrap();
    std::fs::write(format!("{staging1}/hello"), b"source").unwrap();
    // Data on disk, the log still dirty: what a snapshot of a live volume sees.
    std::process::Command::new("sync").status().unwrap();

    // Snapshot of a mounted file system: dirty log, same UUID as the source.
    lvm.snapshot(VG, src, snap).await.unwrap();
    lvm.restore(VG, snap, copy).await.unwrap();
    let copy_dev = format!("/dev/{VG}/{copy}");
    let before = fs_uuid(&copy_dev);
    assert_eq!(before, fs_uuid(&format!("/dev/{VG}/{src}")));

    let marker = Restored::default();
    let svc = NodeService::new(
        "host".into(),
        Arc::new(TokioExec::default()),
        Arc::new(marker.clone()),
        Arc::new(marker.clone()),
    );
    stage(&svc, copy, &staging2).await.unwrap();
    let after = fs_uuid(&copy_dev);
    println!("restored xfs UUID {before} -> {after}");
    assert_ne!(after, before);
    assert_eq!(
        std::fs::read(format!("{staging2}/hello")).unwrap(),
        b"source"
    );
    assert_eq!(marker.0.lock().unwrap().as_deref(), Some("xfs"));
    // Staged again: the marker says done, the UUID stays.
    stage(&svc, copy, &staging2).await.unwrap();
    assert_eq!(fs_uuid(&copy_dev), after);
}

fn fs_label(dev: &str) -> String {
    let out = std::process::Command::new("blkid")
        .args(["-p", "-s", "LABEL", "-o", "value", dev])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// A file system whose record was lost is adopted with its data; only
/// the mkfs label proves an unfinished format.
#[tokio::test]
#[ignore = "needs root and the r-drbd-test VG"]
async fn stage_adopts_xfs_without_a_record() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "pvc-test-host-f").await;
    let lv = "pvc-test-host-f1";
    let dev = format!("/dev/{VG}/{lv}");
    let staging = format!("{MNT}/staging/{lv}");
    let _cleanup = Cleanup {
        mounts: vec![staging.clone()],
        lv: lv.to_owned(),
    };
    lvm.create(VG, THIN, lv, GIB_KIB).await.unwrap();
    let state = NoLookup::default();
    // A new service each time: an agent restart, nothing remembered in memory.
    let svc = || {
        NodeService::new(
            "host".into(),
            Arc::new(TokioExec::default()),
            Arc::new(state.clone()),
            Arc::new(state.clone()),
        )
    };
    let stage_req = NodeStageVolumeRequest {
        volume_id: lv.into(),
        publish_context: HashMap::from([(PUBLISH_DEVICE_PATH.to_owned(), dev.clone())]),
        staging_target_path: staging.clone(),
        volume_capability: mount_cap(),
        ..NodeStageVolumeRequest::default()
    };
    let unstage_req = NodeUnstageVolumeRequest {
        volume_id: lv.into(),
        staging_target_path: staging.clone(),
    };

    svc()
        .node_stage_volume(Request::new(stage_req.clone()))
        .await
        .unwrap();
    assert_eq!(state.0.lock().unwrap().as_deref(), Some("xfs"));
    assert_eq!(fs_label(&dev), "", "mkfs label cleared");
    let uuid = fs_uuid(&dev);
    std::fs::write(format!("{staging}/hello"), b"keep me").unwrap();
    svc()
        .node_unstage_volume(Request::new(unstage_req.clone()))
        .await
        .unwrap();

    // The Volume status lost fileSystem: adopted, no mkfs, data intact.
    *state.0.lock().unwrap() = None;
    svc()
        .node_stage_volume(Request::new(stage_req.clone()))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(format!("{staging}/hello")).unwrap(),
        b"keep me"
    );
    assert_eq!(fs_uuid(&dev), uuid, "same file system");
    assert_eq!(state.0.lock().unwrap().as_deref(), Some("xfs"), "re-marked");
    println!("adopted xfs {uuid} with its data");
    svc()
        .node_unstage_volume(Request::new(unstage_req.clone()))
        .await
        .unwrap();

    // An mkfs cut short before the label clear: formatted again.
    let out = std::process::Command::new("mkfs.xfs")
        .args(["-q", "-f", "-K", "-L", agent::csi::MKFS_LABEL, &dev])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    *state.0.lock().unwrap() = None;
    let labelled = fs_uuid(&dev);
    svc()
        .node_stage_volume(Request::new(stage_req.clone()))
        .await
        .unwrap();
    assert_ne!(fs_uuid(&dev), labelled, "formatted again");
    assert_eq!(fs_label(&dev), "");
    assert_eq!(state.0.lock().unwrap().as_deref(), Some("xfs"));
    assert!(!std::path::Path::new(&format!("{staging}/hello")).exists());
    svc()
        .node_unstage_volume(Request::new(unstage_req))
        .await
        .unwrap();
    let _ = std::fs::remove_dir(&staging);
    lvm.remove(VG, lv).await.unwrap();
}

/// A reported replica whose LV vanished is rebuilt empty with
/// just-created metadata, never day 0 and never the old current id.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the r-drbd-test VG"]
async fn drbd_lost_lv_is_rebuilt_with_fresh_metadata() {
    use agent::drbd::{self, Notify};
    use agent::events2::Tracker;
    use agent::reconcile::DrbdEnv;
    use agent::watch::Trigger;

    if !enabled() {
        return;
    }
    let node = hostname();
    let host_ip: std::net::IpAddr = std::env::var("R_DRBD_HOST_IP")
        .expect("R_DRBD_HOST_IP: the host's DRBD address")
        .parse()
        .unwrap();
    drbd_clean().await;
    let exec: Arc<dyn agent::Exec> = Arc::new(TokioExec::default());
    let tools = agent::node::probe(exec.as_ref()).await;
    let versions = tools.drbd_versions().expect("DRBD tools on the host");
    let tracker = Arc::new(std::sync::Mutex::new(Tracker::default()));
    let events = tokio::spawn(drbd::events2(
        None,
        tracker.clone(),
        Notify {
            changes: Arc::new(Trigger::default()),
            pass: Arc::new(Trigger::default()),
        },
    ));
    wait_for(&tracker, "initial dump", Tracker::is_available).await;
    let mut rec = Reconciler::new(node.clone(), exec).with_drbd(DrbdEnv::new(
        Some(versions),
        RES_DIR,
        tracker.clone(),
    ));
    rec.set_addresses([(node.clone(), host_ip)].into_iter().collect());
    let pools = [drbd_pool(&node)];
    let dev = format!("/dev/{VG}/{DRBD_VOL}");
    let minor = DRBD_MINOR.to_string();
    let get_gi = || {
        let (minor, dev) = (minor.clone(), dev.clone());
        async move {
            let out = sh(&[
                "drbdmeta",
                &minor,
                "v09",
                &dev,
                "internal",
                "get-gi",
                "--node-id",
                "0",
            ])
            .await;
            assert_eq!(out.exit_code, Some(0), "{out:?}");
            out.stdout.trim().to_owned()
        }
    };
    let down = || async {
        let out = sh(&["drbdsetup", "down", DRBD_VOL]).await;
        assert_eq!(out.exit_code, Some(0), "{out:?}");
        wait_for(&tracker, "resource down", |t| {
            t.resource(DRBD_VOL).is_none()
        })
        .await;
    };

    // Normal creation on one node: this node mints the current id.
    let v = drbd_volume(&node, GIB, 1, false);
    let w = rec.pass(&pools, &[Arc::new(v.clone())], &[]).await;
    assert_eq!(entry(&w, DRBD_VOL).last_error, None);
    wait_for(&tracker, "created UpToDate", |t| {
        t.resource(DRBD_VOL)
            .and_then(|r| r.devices.get(&0))
            .and_then(|d| d.disk.as_deref())
            == Some("UpToDate")
    })
    .await;
    let created = entry(&w, DRBD_VOL);
    // The pass may already have seen it up: then the refresh has nothing new.
    let w = rec
        .refresh(&[Arc::new(with_status(v.clone(), Some(&created)))])
        .await;
    let reported = if w.is_empty() {
        created
    } else {
        entry(&w, DRBD_VOL)
    };
    assert!(reported.present, "{reported:?}");
    down().await;
    let first = get_gi().await;
    println!("created: get-gi {first}");
    let day0 = "5F3E2D1C0B0A0908";

    // The LV vanishes behind the agent's back.
    lvm().remove(VG, DRBD_VOL).await.unwrap();
    let mut lost = with_status(v.clone(), Some(&reported));
    if let Some(st) = lost.status.as_mut() {
        st.initialized_at = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
            k8s_openapi::jiff::Timestamp::now(),
        ));
    }
    let w = rec.pass(&pools, &[Arc::new(lost.clone())], &[]).await;
    let e = entry(&w, DRBD_VOL);
    println!("rebuilt: {e:?}");
    assert_eq!(e.last_error, None);
    let rows = lvm().lvs(VG).await.unwrap();
    let row = rows
        .iter()
        .find(|r| r.lv_name == DRBD_VOL)
        .expect("LV back");
    assert_eq!(row.lv_size, 1_052_672);
    wait_for(&tracker, "rebuilt replica attached", |t| {
        t.resource(DRBD_VOL)
            .and_then(|r| r.devices.get(&0))
            .is_some_and(|d| d.disk.is_some())
    })
    .await;
    let disk = tracker
        .lock()
        .unwrap()
        .resource(DRBD_VOL)
        .and_then(|r| r.devices.get(&0))
        .and_then(|d| d.disk.clone());
    println!("rebuilt disk state: {disk:?}");
    assert_eq!(
        disk.as_deref(),
        Some("Inconsistent"),
        "no peer to sync from"
    );
    down().await;
    let rebuilt = get_gi().await;
    println!("rebuilt: get-gi {rebuilt}");
    assert_eq!(lv_tags(DRBD_VOL).await, ["r-drbd.md.0"], "new tag swapped");
    let current = rebuilt.split(':').next().unwrap_or_default();
    assert_eq!(current, "0000000000000004", "just-created current id");
    assert!(!rebuilt.to_uppercase().contains(day0), "no day-0 id");
    assert!(
        !rebuilt.contains(first.split(':').next().unwrap_or_default()),
        "not the old current id"
    );

    // Delete: wipe-md, LV removed, files removed.
    let mut del = lost;
    del.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        k8s_openapi::jiff::Timestamp::now(),
    ));
    let w = rec.pass(&pools, &[Arc::new(del)], &[]).await;
    assert_eq!(entry(&w, DRBD_VOL).last_error, None);
    assert!(
        !lv_names(&lvm())
            .await
            .iter()
            .any(|n| n.starts_with(DRBD_VOL))
    );
    events.abort();
    let _ = events.await;
    drbd_clean().await;
}

/// The host ships drbd-graceful-down.service; read-only query, nothing installed.
#[tokio::test]
#[ignore = "needs a host with drbd-graceful-down.service"]
async fn shutdown_guard_unit_is_loaded() {
    if !enabled() {
        return;
    }
    let argv = agent::node::shutdown_guard_argv();
    let parts: Vec<&str> = argv.iter().map(String::as_str).collect();
    let out = sh(&parts).await;
    println!("{}", out.stdout.trim());
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    assert!(agent::node::shutdown_guard_loaded(&out.stdout), "{out:?}");
}
