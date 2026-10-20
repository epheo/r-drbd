#![allow(clippy::unnecessary_wraps)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ::csi::v1::volume_capability::{BlockVolume, MountVolume};
use ::csi::v1::{CapacityRange, VolumeCapability};

use super::*;
use crate::exec::scripted::{ScriptedExec, code, ok};

#[derive(Default)]
struct Lookup(HashMap<String, String>);

impl Lookup {
    fn lookup(&self, volume: &str, node: &str) -> super::Lookup {
        assert_eq!(node, "n1");
        match self.0.get(volume) {
            None => super::Lookup::VolumeMissing,
            Some(d) if d.is_empty() => super::Lookup::NoDevice,
            Some(d) => super::Lookup::Device(d.clone()),
        }
    }
}

/// Volume facts plus a recording marker writer; a successful write updates
/// the facts as the apiserver would.
#[derive(Debug)]
struct FsState {
    facts: std::sync::Mutex<VolumeFacts>,
    writes: std::sync::Mutex<Vec<(String, String)>>,
    fail: std::sync::atomic::AtomicBool,
    /// What the API server holds, which the cache (`facts`) may lag behind.
    server: std::sync::Mutex<Result<Option<String>, String>>,
    /// Commands the exec had run at each marker write, for ordering checks.
    exec: Option<Arc<ScriptedExec>>,
    commands_at_write: std::sync::Mutex<Vec<usize>>,
}

impl Default for FsState {
    fn default() -> Self {
        Self {
            facts: std::sync::Mutex::default(),
            writes: std::sync::Mutex::default(),
            fail: std::sync::atomic::AtomicBool::default(),
            server: std::sync::Mutex::new(Ok(None)),
            exec: None,
            commands_at_write: std::sync::Mutex::default(),
        }
    }
}

impl FsState {
    fn server(&self, r: Result<Option<&str>, &str>) {
        *self.server.lock().unwrap() = r.map(|o| o.map(str::to_owned)).map_err(str::to_owned);
    }

    fn set(&self, file_system: Option<&str>, restored: bool) {
        *self.facts.lock().unwrap() = VolumeFacts {
            file_system: file_system.map(str::to_owned),
            restored,
        };
    }

    fn writes(&self) -> Vec<(String, String)> {
        self.writes.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl FsMarker for FsState {
    async fn write(&self, volume: &str, fs: &str) -> Result<(), String> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("apiserver unreachable".into());
        }
        if let Some(x) = &self.exec {
            let n = x.seen.lock().unwrap().len();
            self.commands_at_write.lock().unwrap().push(n);
        }
        self.writes
            .lock()
            .unwrap()
            .push((volume.to_owned(), fs.to_owned()));
        self.facts.lock().unwrap().file_system = Some(fs.to_owned());
        *self.server.lock().unwrap() = Ok(Some(fs.to_owned()));
        Ok(())
    }

    async fn read_live(&self, _: &str) -> Result<Option<String>, String> {
        self.server.lock().unwrap().clone()
    }
}

struct Combined {
    lookup: Lookup,
    fs: Arc<FsState>,
}

impl VolumeLookup for Combined {
    fn lookup(&self, volume: &str, node: &str) -> super::Lookup {
        self.lookup.lookup(volume, node)
    }

    fn facts(&self, _: &str) -> Option<VolumeFacts> {
        Some(self.fs.facts.lock().unwrap().clone())
    }
}

struct Env {
    dir: PathBuf,
    dev: String,
    x: Arc<ScriptedExec>,
    fs: Arc<FsState>,
}

impl Env {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("agent-csi-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dev = dir.join("dev").display().to_string();
        std::fs::write(&dev, b"").unwrap();
        let x = Arc::new(ScriptedExec::new());
        Self {
            dir,
            dev,
            fs: Arc::new(FsState {
                exec: Some(x.clone()),
                ..FsState::default()
            }),
            x,
        }
    }

    fn path(&self, p: &str) -> String {
        self.dir.join(p).display().to_string()
    }

    fn svc(&self, lookup: Lookup) -> NodeService {
        let combined = Combined {
            lookup,
            fs: self.fs.clone(),
        };
        NodeService::new(
            "n1".into(),
            self.x.clone(),
            Arc::new(combined),
            self.fs.clone(),
        )
        .with_device_wait(Duration::from_millis(300))
    }

    fn ctx(&self) -> HashMap<String, String> {
        HashMap::from([(PUBLISH_DEVICE_PATH.to_owned(), self.dev.clone())])
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn mount_cap(fs: &str, flags: &[&str]) -> Option<VolumeCapability> {
    Some(VolumeCapability {
        access_mode: None,
        access_type: Some(AccessType::Mount(MountVolume {
            fs_type: fs.into(),
            mount_flags: flags.iter().map(|&f| f.to_owned()).collect(),
            volume_mount_group: String::new(),
        })),
    })
}

fn block_cap() -> Option<VolumeCapability> {
    Some(VolumeCapability {
        access_mode: None,
        access_type: Some(AccessType::Block(BlockVolume {})),
    })
}

fn stage_req(env: &Env, cap: Option<VolumeCapability>) -> NodeStageVolumeRequest {
    NodeStageVolumeRequest {
        volume_id: "pvc-1".into(),
        publish_context: env.ctx(),
        staging_target_path: env.path("staging"),
        volume_capability: cap,
        ..NodeStageVolumeRequest::default()
    }
}

/// Regular files stand in for devices in these tests: their rdev is 0:0.
const MAJMIN: &str = "0:0";

fn findmnt(x: &ScriptedExec, path: &str, mounted: bool) {
    findmnt_dev(x, path, mounted.then_some(MAJMIN));
}

fn findmnt_dev(x: &ScriptedExec, path: &str, majmin: Option<&str>) {
    x.expect_s(
        &["findmnt", "-n", "-o", "MAJ:MIN", "--mountpoint", path],
        match majmin {
            Some(m) => ok(&format!("{m}\n")),
            None => code(1, "", ""),
        },
    );
}

fn blkid(x: &ScriptedExec, dev: &str, out: Result<crate::Output, String>) {
    x.expect_s(
        &[
            "blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-s", "LABEL", "-o", "export", dev,
        ],
        out,
    );
}

/// blkid finds nothing and the first block reads.
fn blank(x: &ScriptedExec, dev: &str) {
    blkid(x, dev, code(2, "", ""));
    x.expect(direct_read(dev), ok(""));
}

fn direct_read(dev: &str) -> Vec<String> {
    crate::exec::argv(&[
        "dd",
        &format!("if={dev}"),
        "of=/dev/null",
        "bs=4096",
        "count=1",
        "iflag=direct",
        "status=none",
    ])
}

/// blkid exits 2 also when it cannot open the device: a failed read is
/// Unavailable, never an empty device to format.
#[tokio::test]
async fn unreadable_device_is_never_formatted() {
    let env = Env::new("stage-unreadable");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, code(2, "", ""));
    env.x
        .expect(direct_read(&env.dev), code(1, "", "Wrong medium type"));
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    assert!(
        !env.x
            .seen
            .lock()
            .unwrap()
            .iter()
            .any(|a| a[0] == "mkfs.xfs")
    );
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

#[tokio::test]
async fn stage_formats_empty_device() {
    let env = Env::new("stage-empty");
    let staging = env.path("staging");
    findmnt(&env.x, &staging, false);
    blank(&env.x, &env.dev);
    env.x
        .expect_s(
            &["mkfs.xfs", "-q", "-K", "-L", MKFS_LABEL, &env.dev],
            ok(""),
        )
        .expect_s(&["xfs_admin", "-L", "--", &env.dev], ok(""))
        .expect_s(&["mount", "-t", "xfs", &env.dev, &staging], ok(""))
        .expect_s(&["xfs_growfs", &staging], ok(""));
    env.svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap();
    assert!(PathBuf::from(&staging).is_dir());
    assert_eq!(
        env.fs.writes(),
        vec![("pvc-1".to_owned(), "xfs".to_owned())]
    );
    // findmnt, blkid, dd, mkfs, xfs_admin, then the marker, then mount.
    assert_eq!(*env.fs.commands_at_write.lock().unwrap(), vec![5]);
    env.x.assert_done();
}

#[tokio::test]
async fn failed_label_clear_is_internal_and_unmarked() {
    let env = Env::new("stage-label-fail");
    findmnt(&env.x, &env.path("staging"), false);
    blank(&env.x, &env.dev);
    env.x
        .expect_s(
            &["mkfs.xfs", "-q", "-K", "-L", MKFS_LABEL, &env.dev],
            ok(""),
        )
        .expect_s(
            &["xfs_admin", "-L", "--", &env.dev],
            code(1, "", "io error"),
        );
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Internal);
    assert!(
        e.message().starts_with("clear mkfs label"),
        "{}",
        e.message()
    );
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

#[tokio::test]
async fn stage_never_formats_existing_fs_and_passes_flags() {
    let env = Env::new("stage-existing");
    env.fs.set(Some("xfs"), false);
    let staging = env.path("staging");
    findmnt(&env.x, &staging, false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    env.x.expect_s(
        &[
            "mount",
            "-t",
            "xfs",
            "-o",
            "noatime,nodiscard",
            &env.dev,
            &staging,
        ],
        ok(""),
    );
    env.x.expect_s(&["xfs_growfs", &staging], ok(""));
    env.svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("xfs", &["noatime", "nodiscard"])))
        .await
        .unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn stage_partition_table_only_never_formats() {
    let env = Env::new("stage-pttype");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("PTTYPE=gpt\n"));
    let e = env
        .svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert!(e.message().contains("PTTYPE=gpt"), "{}", e.message());
    env.x.assert_done();
}

#[tokio::test]
async fn stage_exit0_without_type_never_formats() {
    let env = Env::new("stage-notype");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok(""));
    let e = env
        .svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    env.x.assert_done();
}

#[tokio::test]
async fn stage_other_fs_is_failed_precondition() {
    let env = Env::new("stage-ext4");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=ext4\n"));
    let e = env
        .svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    env.x.assert_done();
}

#[tokio::test]
async fn stage_is_idempotent() {
    let env = Env::new("stage-idem");
    findmnt(&env.x, &env.path("staging"), true);
    env.svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn stage_retry_succeeds_before_device_check() {
    let env = Env::new("stage-retry");
    findmnt(&env.x, &env.path("staging"), true);
    let mut req = stage_req(&env, mount_cap("", &[]));
    req.publish_context.clear();
    env.svc(Lookup::default()).stage(req).await.unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn stage_other_device_mounted_is_already_exists() {
    let env = Env::new("stage-other");
    findmnt_dev(&env.x, &env.path("staging"), Some("253:7"));
    let e = env
        .svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::AlreadyExists);
    env.x.assert_done();
}

#[tokio::test]
async fn stage_blkid_failure_never_formats() {
    let env = Env::new("stage-blkid");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, code(4, "", "io error"));
    let e = env
        .svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Internal);
    assert!(e.message().starts_with("blkid"), "{}", e.message());
    env.x.assert_done();
}

#[tokio::test]
async fn stage_device_from_volume_status() {
    let env = Env::new("stage-lookup");
    env.fs.set(Some("xfs"), false);
    let staging = env.path("staging");
    findmnt(&env.x, &staging, false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    env.x
        .expect_s(&["mount", "-t", "xfs", &env.dev, &staging], ok(""))
        .expect_s(&["xfs_growfs", &staging], ok(""));
    let mut req = stage_req(&env, mount_cap("", &[]));
    req.publish_context.clear();
    env.svc(Lookup(HashMap::from([("pvc-1".into(), env.dev.clone())])))
        .stage(req)
        .await
        .unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn stage_without_device() {
    let env = Env::new("stage-nodev");
    let staging = env.path("staging");
    let svc = env.svc(Lookup(HashMap::from([("pvc-1".into(), String::new())])));
    let mut req = stage_req(&env, mount_cap("", &[]));
    req.publish_context.clear();
    findmnt(&env.x, &staging, false);
    let e = svc.stage(req.clone()).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);

    req.volume_id = "pvc-gone".into();
    findmnt(&env.x, &staging, false);
    let e = svc.stage(req).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);

    let mut req = stage_req(&env, mount_cap("", &[]));
    req.publish_context
        .insert(PUBLISH_DEVICE_PATH.into(), env.path("never"));
    findmnt(&env.x, &staging, false);
    let e = svc.stage(req).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    env.x.assert_done();
}

#[tokio::test]
async fn stage_block_does_nothing() {
    let env = Env::new("stage-block");
    env.svc(Lookup::default())
        .stage(stage_req(&env, block_cap()))
        .await
        .unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn concurrent_rpc_on_same_volume_is_aborted() {
    let env = Env::new("concurrent");
    let svc = env.svc(Lookup::default());
    let guard = svc.inflight.begin("pvc-1").unwrap();
    let e = svc
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Aborted);
    assert!(!svc.inflight().is_idle());
    drop(guard);
    assert!(svc.inflight().is_idle());
    svc.stage(stage_req(&env, block_cap())).await.unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn operation_survives_client_cancel() {
    let env = Env::new("cancel");
    let staging = env.path("staging");
    findmnt(&env.x, &staging, false);
    blank(&env.x, &env.dev);
    env.x
        .expect_s(
            &["mkfs.xfs", "-q", "-K", "-L", MKFS_LABEL, &env.dev],
            ok(""),
        )
        .expect_s(&["xfs_admin", "-L", "--", &env.dev], ok(""))
        .expect_s(&["mount", "-t", "xfs", &env.dev, &staging], ok(""))
        .expect_s(&["xfs_growfs", &staging], ok(""));
    let svc = env.svc(Lookup::default());
    let inflight = svc.inflight();
    // Dropped at its first await point, as a cancelled gRPC handler is.
    let fut = svc.stage(stage_req(&env, mount_cap("", &[])));
    let _ = tokio::time::timeout(Duration::ZERO, fut).await;
    assert!(inflight.wait_idle(Duration::from_secs(5)).await);
    env.x.assert_done();
}

#[tokio::test]
async fn publish_mount_readonly_then_unpublish() {
    let env = Env::new("publish-mount");
    let staging = env.path("staging");
    let target = env.path("pods/uid/mount");
    findmnt(&env.x, &staging, true);
    findmnt(&env.x, &target, false);
    env.x
        .expect_s(&["mount", "-o", "bind,ro", &staging, &target], ok(""));
    let svc = env.svc(Lookup::default());
    svc.publish(NodePublishVolumeRequest {
        volume_id: "pvc-1".into(),
        staging_target_path: staging.clone(),
        target_path: target.clone(),
        volume_capability: mount_cap("", &[]),
        readonly: true,
        ..NodePublishVolumeRequest::default()
    })
    .await
    .unwrap();
    assert!(PathBuf::from(&target).is_dir());

    findmnt(&env.x, &target, true);
    env.x.expect_s(&["umount", &target], ok(""));
    svc.unpublish(NodeUnpublishVolumeRequest {
        volume_id: "pvc-1".into(),
        target_path: target.clone(),
    })
    .await
    .unwrap();
    assert!(!PathBuf::from(&target).exists());

    findmnt(&env.x, &target, false);
    svc.unpublish(NodeUnpublishVolumeRequest {
        volume_id: "pvc-1".into(),
        target_path: target,
    })
    .await
    .unwrap();
    env.x.assert_done();
}

fn publish_req(env: &Env) -> NodePublishVolumeRequest {
    NodePublishVolumeRequest {
        volume_id: "pvc-1".into(),
        staging_target_path: env.path("staging"),
        target_path: env.path("pods/uid/mount"),
        volume_capability: mount_cap("", &[]),
        ..NodePublishVolumeRequest::default()
    }
}

#[tokio::test]
async fn publish_requires_staged_volume() {
    let env = Env::new("publish-unstaged");
    findmnt(&env.x, &env.path("staging"), false);
    let e = env
        .svc(Lookup::default())
        .publish(publish_req(&env))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    env.x.assert_done();
}

#[tokio::test]
async fn publish_idempotent_and_other_mount_is_already_exists() {
    let env = Env::new("publish-other");
    let svc = env.svc(Lookup::default());
    findmnt_dev(&env.x, &env.path("staging"), Some("253:3"));
    findmnt_dev(&env.x, &env.path("pods/uid/mount"), Some("253:3"));
    svc.publish(publish_req(&env)).await.unwrap();
    findmnt_dev(&env.x, &env.path("staging"), Some("253:3"));
    findmnt_dev(&env.x, &env.path("pods/uid/mount"), Some("253:9"));
    let e = svc.publish(publish_req(&env)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::AlreadyExists);
    env.x.assert_done();
}

#[tokio::test]
async fn publish_block_binds_device_onto_file() {
    let env = Env::new("publish-block");
    let target = env.path("pods/uid/dev/pvc-1");
    findmnt(&env.x, &target, false);
    env.x
        .expect_s(&["mount", "-o", "bind", &env.dev, &target], ok(""));
    let svc = env.svc(Lookup::default());
    svc.publish(NodePublishVolumeRequest {
        volume_id: "pvc-1".into(),
        publish_context: env.ctx(),
        staging_target_path: env.path("staging"),
        target_path: target.clone(),
        volume_capability: block_cap(),
        ..NodePublishVolumeRequest::default()
    })
    .await
    .unwrap();
    assert!(PathBuf::from(&target).is_file());

    findmnt(&env.x, &target, true);
    env.x.expect_s(&["umount", &target], ok(""));
    svc.unpublish(NodeUnpublishVolumeRequest {
        volume_id: "pvc-1".into(),
        target_path: target.clone(),
    })
    .await
    .unwrap();
    assert!(!PathBuf::from(&target).exists());
    env.x.assert_done();
}

#[tokio::test]
async fn unstage_idempotent() {
    let env = Env::new("unstage");
    let staging = env.path("staging");
    let svc = env.svc(Lookup::default());
    let req = || NodeUnstageVolumeRequest {
        volume_id: "pvc-1".into(),
        staging_target_path: staging.clone(),
    };
    findmnt(&env.x, &staging, true);
    env.x.expect_s(&["umount", &staging], ok(""));
    svc.unstage(req()).await.unwrap();
    findmnt(&env.x, &staging, false);
    svc.unstage(req()).await.unwrap();
    env.x.assert_done();
}

fn expand_req(path: &str, want: i64, cap: Option<VolumeCapability>) -> NodeExpandVolumeRequest {
    NodeExpandVolumeRequest {
        volume_id: "pvc-1".into(),
        volume_path: path.into(),
        capacity_range: Some(CapacityRange {
            required_bytes: want,
            limit_bytes: 0,
        }),
        volume_capability: cap,
        ..NodeExpandVolumeRequest::default()
    }
}

#[tokio::test]
async fn expand_mount_grows_xfs() {
    let env = Env::new("expand");
    let target = env.path("mnt");
    env.x
        .expect_s(
            &["findmnt", "-n", "-o", "SOURCE", "--mountpoint", &target],
            ok("/dev/mapper/vg1-pvc--1\n"),
        )
        .expect_s(
            &["blockdev", "--getsize64", "/dev/mapper/vg1-pvc--1"],
            ok("2147483648\n"),
        )
        .expect_s(&["xfs_growfs", &target], ok(""));
    let got = env
        .svc(Lookup::default())
        .expand(expand_req(&target, 2 << 30, mount_cap("", &[])))
        .await
        .unwrap();
    assert_eq!(got, 2 << 30);
    env.x.assert_done();
}

#[tokio::test]
async fn expand_waits_for_lv() {
    let env = Env::new("expand-wait");
    let target = env.path("mnt");
    env.x
        .expect_s(
            &["findmnt", "-n", "-o", "SOURCE", "--mountpoint", &target],
            ok("/dev/mapper/vg1-pvc--1\n"),
        )
        .expect_s(
            &["blockdev", "--getsize64", "/dev/mapper/vg1-pvc--1"],
            ok("1073741824\n"),
        );
    let e = env
        .svc(Lookup::default())
        .expand(expand_req(&target, 2 << 30, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    env.x.assert_done();
}

#[tokio::test]
async fn expand_block_only_checks_size() {
    let env = Env::new("expand-block");
    let target = env.path("dev");
    env.x
        .expect_s(&["blockdev", "--getsize64", &target], ok("2147483648\n"));
    let got = env
        .svc(Lookup::default())
        .expand(expand_req(&target, 2 << 30, block_cap()))
        .await
        .unwrap();
    assert_eq!(got, 2 << 30);
    env.x.assert_done();
}

#[tokio::test]
async fn stats_of_a_directory() {
    let env = Env::new("stats");
    let usage = env
        .svc(Lookup::default())
        .stats(NodeGetVolumeStatsRequest {
            volume_id: "pvc-1".into(),
            volume_path: env.dir.display().to_string(),
            staging_target_path: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(usage.len(), 2);
    assert!(usage[0].total > 0 && usage[0].used <= usage[0].total);
    let e = env
        .svc(Lookup::default())
        .stats(NodeGetVolumeStatsRequest {
            volume_id: "pvc-1".into(),
            volume_path: env.path("missing"),
            staging_target_path: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn info_and_capabilities() {
    let env = Env::new("info");
    let svc = env.svc(Lookup::default());
    let info = svc
        .node_get_info(Request::new(NodeGetInfoRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.node_id, "n1");
    assert_eq!(
        info.accessible_topology
            .unwrap()
            .segments
            .get("r-drbd.io/node"),
        Some(&"n1".to_owned())
    );
    let caps = svc
        .node_get_capabilities(Request::new(NodeGetCapabilitiesRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(caps.capabilities.len(), 3);
    let plugin = IdentityService
        .get_plugin_info(Request::new(GetPluginInfoRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(plugin.name, "r-drbd.io");
}

fn mount_step(env: &Env) {
    let staging = env.path("staging");
    env.x
        .expect_s(&["mount", "-t", "xfs", &env.dev, &staging], ok(""))
        .expect_s(&["xfs_growfs", &staging], ok(""));
}

async fn stage_mount(env: &Env) -> Result<(), Status> {
    env.svc(Lookup::default())
        .stage(stage_req(env, mount_cap("", &[])))
        .await
}

#[tokio::test]
async fn marker_matches_mounts_without_mkfs() {
    let env = Env::new("fs-match");
    env.fs.set(Some("xfs"), false);
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    mount_step(&env);
    stage_mount(&env).await.unwrap();
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

fn uuid_steps(env: &Env, xfs_admin: Result<crate::Output, String>) {
    let staging = env.path("staging");
    env.x
        .expect_s(
            &["mount", "-t", "xfs", "-o", "nouuid", &env.dev, &staging],
            ok(""),
        )
        .expect_s(&["umount", &staging], ok(""))
        .expect_s(&["xfs_admin", "-U", "generate", &env.dev], xfs_admin);
}

#[tokio::test]
async fn restored_without_marker_is_adopted() {
    // The clone carries its source's XFS UUID: log replay, new UUID, then
    // the marker, then the real mount.
    let env = Env::new("fs-restored");
    env.fs.set(None, true);
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    uuid_steps(&env, ok("Clearing log and setting UUID\n"));
    mount_step(&env);
    stage_mount(&env).await.unwrap();
    assert_eq!(
        env.fs.writes(),
        vec![("pvc-1".to_owned(), "xfs".to_owned())]
    );
    env.x.assert_done();
}

#[tokio::test]
async fn labelled_unfinished_mkfs_is_redone() {
    let env = Env::new("fs-interrupted");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("LABEL=r-drbd-mkfs\nTYPE=xfs\n"));
    env.x
        .expect_s(
            &["mkfs.xfs", "-q", "-f", "-K", "-L", MKFS_LABEL, &env.dev],
            ok(""),
        )
        .expect_s(&["xfs_admin", "-L", "--", &env.dev], ok(""));
    mount_step(&env);
    stage_mount(&env).await.unwrap();
    assert_eq!(
        env.fs.writes(),
        vec![("pvc-1".to_owned(), "xfs".to_owned())]
    );
    env.x.assert_done();
}

#[tokio::test]
async fn unlabelled_fs_without_marker_is_adopted() {
    // The Volume lost its status: the data stays, only the record is redone.
    let env = Env::new("fs-adopt");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    mount_step(&env);
    stage_mount(&env).await.unwrap();
    assert_eq!(
        env.fs.writes(),
        vec![("pvc-1".to_owned(), "xfs".to_owned())]
    );
    assert!(
        !env.x
            .seen
            .lock()
            .unwrap()
            .iter()
            .any(|a| a[0] == "mkfs.xfs")
    );
    env.x.assert_done();

    // Another label is no proof either.
    let env = Env::new("fs-adopt-label");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("LABEL=data\nTYPE=xfs\n"));
    mount_step(&env);
    stage_mount(&env).await.unwrap();
    assert_eq!(env.fs.writes().len(), 1);
    env.x.assert_done();
}

#[tokio::test]
async fn marker_mismatch_is_failed_precondition() {
    let env = Env::new("fs-mismatch");
    env.fs.set(Some("ext4"), false);
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

#[tokio::test]
async fn empty_device_with_marker_is_failed_precondition() {
    let env = Env::new("fs-empty-marked");
    env.fs.set(Some("xfs"), false);
    findmnt(&env.x, &env.path("staging"), false);
    blank(&env.x, &env.dev);
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert_eq!(e.message(), "volume records xfs but the device is empty");
    env.x.assert_done();
}

#[tokio::test]
async fn restored_empty_device_is_failed_precondition() {
    let env = Env::new("fs-restored-empty");
    env.fs.set(None, true);
    findmnt(&env.x, &env.path("staging"), false);
    blank(&env.x, &env.dev);
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert_eq!(e.message(), "restored volume has no file system");
    env.x.assert_done();
}

#[tokio::test]
async fn stale_cache_with_live_marker_mounts_without_mkfs() {
    let env = Env::new("fs-stale-cache");
    env.fs.server(Ok(Some("xfs")));
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("LABEL=r-drbd-mkfs\nTYPE=xfs\n"));
    mount_step(&env);
    stage_mount(&env).await.unwrap();
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

#[tokio::test]
async fn live_read_error_is_unavailable_and_never_formats() {
    let env = Env::new("fs-live-error");
    env.fs.server(Err("connection refused"));
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("LABEL=r-drbd-mkfs\nTYPE=xfs\n"));
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    env.x.assert_done();
}

#[tokio::test]
async fn own_marker_guards_even_when_cache_and_api_fail() {
    let env = Env::new("fs-own-marker");
    let svc = env.svc(Lookup::default());
    findmnt(&env.x, &env.path("staging"), false);
    blank(&env.x, &env.dev);
    env.x
        .expect_s(
            &["mkfs.xfs", "-q", "-K", "-L", MKFS_LABEL, &env.dev],
            ok(""),
        )
        .expect_s(&["xfs_admin", "-L", "--", &env.dev], ok(""));
    mount_step(&env);
    svc.stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap();

    // Cache lost the marker and the API server is unreachable: a label
    // that asks for a format again is overruled by our own write.
    env.fs.set(None, false);
    env.fs.server(Err("watch in backoff"));
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("LABEL=r-drbd-mkfs\nTYPE=xfs\n"));
    mount_step(&env);
    svc.stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap();

    // Adopt never trusts the in-process set: the record may have been wiped.
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    let e = svc
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    assert_eq!(env.fs.writes().len(), 1);
    env.x.assert_done();
}

/// Formats and marks a fresh device, unstages, then wipes the record in the
/// cache and on the API server (an etcd restore or a re-apply).
/// Returns how many commands ran before the second stage.
async fn mark_then_wipe(env: &Env, svc: &NodeService) -> usize {
    let staging = env.path("staging");
    findmnt(&env.x, &staging, false);
    blank(&env.x, &env.dev);
    env.x
        .expect_s(
            &["mkfs.xfs", "-q", "-K", "-L", MKFS_LABEL, &env.dev],
            ok(""),
        )
        .expect_s(&["xfs_admin", "-L", "--", &env.dev], ok(""));
    mount_step(env);
    svc.stage(stage_req(env, mount_cap("", &[]))).await.unwrap();
    findmnt(&env.x, &staging, true);
    env.x.expect_s(&["umount", &staging], ok(""));
    svc.unstage(NodeUnstageVolumeRequest {
        volume_id: "pvc-1".into(),
        staging_target_path: staging,
    })
    .await
    .unwrap();
    env.fs.set(None, false);
    env.fs.server(Ok(None));
    env.x.seen.lock().unwrap().len()
}

fn mkfs_since(env: &Env, from: usize) -> bool {
    env.x.seen.lock().unwrap()[from..]
        .iter()
        .any(|a| a[0] == "mkfs.xfs")
}

#[tokio::test]
async fn marked_volume_with_wiped_record_is_adopted_again() {
    let env = Env::new("fs-wiped-adopt");
    let svc = env.svc(Lookup::default());
    let from = mark_then_wipe(&env, &svc).await;
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    mount_step(&env);
    svc.stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap();
    assert!(!mkfs_since(&env, from));
    let w = ("pvc-1".to_owned(), "xfs".to_owned());
    assert_eq!(env.fs.writes(), vec![w.clone(), w]);
    env.x.assert_done();
}

#[tokio::test]
async fn marked_volume_with_wiped_record_and_empty_device_is_refused() {
    let env = Env::new("fs-wiped-empty");
    let svc = env.svc(Lookup::default());
    let from = mark_then_wipe(&env, &svc).await;
    findmnt(&env.x, &env.path("staging"), false);
    blank(&env.x, &env.dev);
    let e = svc
        .stage(stage_req(&env, mount_cap("", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert!(e.message().contains("device is empty"), "{}", e.message());
    assert!(!mkfs_since(&env, from));
    assert_eq!(env.fs.writes().len(), 1);
    env.x.assert_done();
}

#[tokio::test]
async fn marker_write_failure_means_no_mount() {
    let env = Env::new("fs-write-fail");
    env.fs.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    findmnt(&env.x, &env.path("staging"), false);
    blank(&env.x, &env.dev);
    env.x
        .expect_s(
            &["mkfs.xfs", "-q", "-K", "-L", MKFS_LABEL, &env.dev],
            ok(""),
        )
        .expect_s(&["xfs_admin", "-L", "--", &env.dev], ok(""));
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    // No mount was scripted: ScriptedExec would have panicked on one.
    env.x.assert_done();

    let env = Env::new("fs-write-fail-restored");
    env.fs.set(None, true);
    env.fs.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    // Only the log-replay mount, already undone before the marker write.
    uuid_steps(&env, ok(""));
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    env.x.assert_done();
}

#[tokio::test]
async fn block_volumes_never_touch_the_marker() {
    let env = Env::new("fs-block");
    env.fs.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    env.svc(Lookup::default())
        .stage(stage_req(&env, block_cap()))
        .await
        .unwrap();
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

#[test]
fn decision_table() {
    let f = |m: Option<&str>, restored| VolumeFacts {
        file_system: m.map(str::to_owned),
        restored,
    };
    let p = |t: &str, label: Option<&str>| {
        Some(Probe {
            type_: t.to_owned(),
            label: label.map(str::to_owned),
        })
    };
    let xfs = || p("xfs", None);
    let ok = |found: Option<Probe>, facts: VolumeFacts| {
        fs_decision(found.as_ref(), &facts, "xfs").unwrap()
    };
    let err = |found: Option<Probe>, facts: VolumeFacts| {
        fs_decision(found.as_ref(), &facts, "xfs")
            .unwrap_err()
            .code()
    };
    // No signature.
    assert_eq!(ok(None, f(None, false)), FsAction::FormatMarkMount);
    assert_eq!(
        err(None, f(Some("xfs"), false)),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(err(None, f(None, true)), tonic::Code::FailedPrecondition);
    // Type differs from the marker or the request.
    assert_eq!(
        fs_decision(xfs().as_ref(), &f(Some("xfs"), false), "ext4")
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        err(xfs(), f(Some("ext4"), false)),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        err(p("ext4", None), f(None, false)),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        err(p("ext4", None), f(None, true)),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        err(p("ext4", None), f(Some("ext4"), false)),
        tonic::Code::FailedPrecondition
    );
    // Marker set: mount, whatever the label.
    assert_eq!(ok(xfs(), f(Some("xfs"), false)), FsAction::Mount);
    assert_eq!(
        ok(p("xfs", Some(MKFS_LABEL)), f(Some("xfs"), false)),
        FsAction::Mount
    );
    // No marker, restored.
    assert_eq!(ok(xfs(), f(None, true)), FsAction::MarkThenMount);
    assert_eq!(
        ok(p("xfs", Some(MKFS_LABEL)), f(None, true)),
        FsAction::MarkThenMount
    );
    // No marker, not restored: only our label formats.
    assert_eq!(
        ok(p("xfs", Some(MKFS_LABEL)), f(None, false)),
        FsAction::FormatMarkMount
    );
    assert_eq!(ok(xfs(), f(None, false)), FsAction::Adopt);
    assert_eq!(ok(p("xfs", Some("data")), f(None, false)), FsAction::Adopt);
    // Only xfs, the type we make, is ever adopted.
    let e = fs_decision(p("ext4", None).as_ref(), &f(None, false), "ext4").unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        e.message(),
        "device holds ext4 without a record; only xfs is adopted"
    );
}

#[tokio::test]
async fn non_xfs_without_a_record_is_never_adopted() {
    let env = Env::new("fs-adopt-ext4");
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=ext4\n"));
    let e = env
        .svc(Lookup::default())
        .stage(stage_req(&env, mount_cap("ext4", &[])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

#[tokio::test]
async fn empty_device_with_a_live_marker_only_is_failed_precondition() {
    // The cache lost the marker, the API server still has it: the data
    // is gone, and a fresh file system would hide that.
    let env = Env::new("fs-empty-live");
    env.fs.server(Ok(Some("xfs")));
    findmnt(&env.x, &env.path("staging"), false);
    blank(&env.x, &env.dev);
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert_eq!(e.message(), "volume records xfs but the device is empty");
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

#[test]
fn marker_body_shape() {
    let b = fs_marker_body("pvc-1", "xfs");
    assert_eq!(
        b,
        serde_json::json!({
            "apiVersion": "r-drbd.io/v1alpha1",
            "kind": "Volume",
            "metadata": { "name": "pvc-1" },
            "status": { "fileSystem": "xfs" },
        })
    );
    assert_eq!(model::keys::FS_FIELD_MANAGER, "r-drbd-fs");
}

#[tokio::test]
async fn growfs_failure_still_stages() {
    let env = Env::new("growfs-fail");
    env.fs.set(Some("xfs"), false);
    let staging = env.path("staging");
    findmnt(&env.x, &staging, false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    env.x
        .expect_s(&["mount", "-t", "xfs", &env.dev, &staging], ok(""))
        .expect_s(&["xfs_growfs", &staging], code(1, "", "growfs failed"));
    stage_mount(&env).await.unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn restored_uuid_failure_is_internal_and_unmarked() {
    let env = Env::new("fs-restored-uuid-fail");
    env.fs.set(None, true);
    findmnt(&env.x, &env.path("staging"), false);
    blkid(&env.x, &env.dev, ok("TYPE=xfs\n"));
    uuid_steps(&env, code(1, "", "xfs_admin: log is dirty"));
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), csi::tonic::Code::Internal);
    assert!(e.message().contains("new xfs UUID"), "{}", e.message());
    assert_eq!(env.fs.writes(), vec![]);
    env.x.assert_done();
}

#[tokio::test]
async fn half_done_restored_stage_is_not_success() {
    // Staging already mounted from a stage cut short before the marker.
    let env = Env::new("fs-restored-half");
    env.fs.set(None, true);
    findmnt(&env.x, &env.path("staging"), true);
    let e = stage_mount(&env).await.unwrap_err();
    assert_eq!(e.code(), csi::tonic::Code::FailedPrecondition);
    assert_eq!(
        e.message(),
        format!(
            "restored volume mounted without its marker; unmount {} to retry",
            env.path("staging")
        )
    );
    env.x.assert_done();

    // Once marked, the same mount is the idempotent success.
    env.fs.set(Some("xfs"), true);
    findmnt(&env.x, &env.path("staging"), true);
    stage_mount(&env).await.unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn plain_volume_keeps_its_uuid() {
    // A plain volume being formatted never runs the UUID step.
    let env = Env::new("fs-plain");
    findmnt(&env.x, &env.path("staging"), false);
    blank(&env.x, &env.dev);
    env.x
        .expect_s(
            &["mkfs.xfs", "-q", "-K", "-L", MKFS_LABEL, &env.dev],
            ok(""),
        )
        .expect_s(&["xfs_admin", "-L", "--", &env.dev], ok(""));
    mount_step(&env);
    stage_mount(&env).await.unwrap();
    env.x.assert_done();
}

#[tokio::test]
async fn closed_gate_refuses_opens_and_serves_releases() {
    use ::csi::v1::node_server::Node as _;
    let env = Env::new("gate");
    let staging = env.path("staging");
    let target = env.path("pods/uid/mount");
    let svc = env.svc(Lookup::default());
    svc.open_gate().close();

    let e = svc
        .node_stage_volume(Request::new(stage_req(&env, mount_cap("", &[]))))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);
    assert_eq!(e.message(), "node shutting down");
    let e = svc
        .node_publish_volume(Request::new(publish_req(&env)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unavailable);

    findmnt(&env.x, &target, true);
    env.x.expect_s(&["umount", &target], ok(""));
    svc.node_unpublish_volume(Request::new(NodeUnpublishVolumeRequest {
        volume_id: "pvc-1".into(),
        target_path: target,
    }))
    .await
    .unwrap();
    findmnt(&env.x, &staging, true);
    env.x.expect_s(&["umount", &staging], ok(""));
    svc.node_unstage_volume(Request::new(NodeUnstageVolumeRequest {
        volume_id: "pvc-1".into(),
        staging_target_path: staging,
    }))
    .await
    .unwrap();
    env.x.assert_done();
}
