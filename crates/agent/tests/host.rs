//! Real commands against a test VG with a thin pool on a test host.
//! Ignored by default; run as root with
//! `R_DRBD_HOST_TEST=1 R_DRBD_HOST_IP=<address> ./host-<hash> --ignored --test-threads=1`.
//! The VG, thin pool and scratch directory come from `R_DRBD_TEST_VG`,
//! `R_DRBD_TEST_THIN` and `R_DRBD_TEST_DIR` at run time, so one static
//! binary serves any host.
//! With `R_DRBD_HOST_NS` set, as in the agent's pod, host tools run through
//! the host-exec of the r-drbd binary (`R_DRBD_HOST_EXEC`, default /r-drbd)
//! and drbd-utils from the image, as the agent runs them; unset, everything
//! runs from this process's PATH.
//! Only LVs named rw-* or pvc-test-host* and paths under the scratch directory are touched;
//! the DRBD volume tests live in host/drbd.rs.

#![allow(clippy::unnecessary_wraps, clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

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

/// A setting read from the environment once. Used as `&Setting`, it
/// passes where a `&str` goes and formats as one, so the tests keep
/// writing `VG` as they did when it was a constant.
struct Setting {
    var: &'static str,
    default: &'static str,
    value: OnceLock<String>,
}

impl Setting {
    const fn new(var: &'static str, default: &'static str) -> Self {
        Self {
            var,
            default,
            value: OnceLock::new(),
        }
    }
}

impl std::ops::Deref for Setting {
    type Target = str;

    fn deref(&self) -> &str {
        self.value
            .get_or_init(|| std::env::var(self.var).unwrap_or_else(|_| self.default.to_owned()))
    }
}

impl std::fmt::Display for Setting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self)
    }
}

impl From<&Setting> for String {
    fn from(s: &Setting) -> Self {
        (**s).to_owned()
    }
}

static VG_SETTING: Setting = Setting::new("R_DRBD_TEST_VG", "vg-test");
static THIN_SETTING: Setting = Setting::new("R_DRBD_TEST_THIN", "thin");
static TEST_DIR_SETTING: Setting = Setting::new("R_DRBD_TEST_DIR", "/var/tmp/r-drbd-hosttest");
static VG: &Setting = &VG_SETTING;
static THIN: &Setting = &THIN_SETTING;
static TEST_DIR: &Setting = &TEST_DIR_SETTING;
const GIB: u64 = 1 << 30;

fn enabled() -> bool {
    let on = std::env::var("R_DRBD_HOST_TEST").is_ok_and(|v| v == "1");
    if !on {
        eprintln!("R_DRBD_HOST_TEST != 1: skipped");
    }
    on
}

/// The agent's own prefix when `R_DRBD_HOST_NS` is set.
fn host_prefix() -> Option<Vec<String>> {
    static PREFIX: OnceLock<Option<Vec<String>>> = OnceLock::new();
    PREFIX
        .get_or_init(|| {
            let ns = std::env::var("R_DRBD_HOST_NS").ok()?;
            let exe = std::env::var("R_DRBD_HOST_EXEC").unwrap_or_else(|_| "/r-drbd".to_owned());
            Some(agent::exec::host_exec_prefix(&exe, &ns))
        })
        .clone()
}

fn exec() -> TokioExec {
    TokioExec::new(host_prefix())
}

/// A host tool run directly rather than through Exec: the image has none.
fn host_command(program: &str) -> std::process::Command {
    match host_prefix() {
        Some(p) => {
            let mut c = std::process::Command::new(&p[0]);
            c.args(&p[1..]).arg(program);
            c
        }
        None => std::process::Command::new(program),
    }
}

fn lvm() -> Lvm {
    Lvm::new(Arc::new(exec()))
}

async fn lv_names(lvm: &Lvm) -> Vec<String> {
    lvm.lvs(VG)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.name)
        .collect()
}

async fn sh(parts: &[&str]) -> agent::Output {
    use agent::Exec;
    exec()
        .run(&agent::exec::argv(parts), agent::exec::DEFAULT_TIMEOUT)
        .await
        .unwrap()
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .unwrap()
        .trim()
        .to_owned()
}

/// Removes leftovers of an earlier failed run.
async fn clean(lvm: &Lvm, prefix: &str) {
    let mut names = lv_names(lvm).await;
    names.retain(|n| n.starts_with(prefix));
    // Restores first, then snapshot LVs, then their origins.
    names.sort_by_key(|n| std::cmp::Reverse(n.contains('+')));
    for n in names {
        lvm.remove(VG, &n).await.unwrap();
    }
}

/// One report field of one LV, straight from lvs.
async fn lv_field(lv: &str, field: &str) -> String {
    let target = format!("{VG}/{lv}");
    let out = sh(&["lvs", "--noheadings", "-o", field, &target]).await;
    assert_eq!(out.exit_code, Some(0), "{}", out.stderr);
    out.stdout.trim().to_owned()
}

#[tokio::test]
#[ignore = "needs root and the test VG"]
async fn lvm_lifecycle() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "rw-l-a").await;
    let cap = lvm.probe_pool(VG, THIN).await.unwrap();
    println!("pool: {cap:?}");
    assert!(cap.total_bytes > 0 && cap.chunk_bytes > 0 && cap.free_bytes <= cap.total_bytes);

    lvm.create(VG, THIN, "rw-l-a1", GIB, &["rw-l.t1"])
        .await
        .unwrap();
    let row = |rows: &[lvm::report::Lv], n: &str| rows.iter().find(|r| r.name == n).cloned();
    let rows = lvm.lvs(VG).await.unwrap();
    let a1 = row(&rows, "rw-l-a1").unwrap();
    assert_eq!(a1.size_bytes, GIB);
    assert!(a1.active && !a1.open);
    assert_eq!(a1.path, format!("/dev/{VG}/rw-l-a1"));
    assert_eq!(a1.pool.as_deref(), Some(&**THIN));
    assert_eq!(lvm.tags(VG).await.unwrap()["rw-l-a1"], ["rw-l.t1"]);

    lvm.extend(VG, "rw-l-a1", 2 * GIB).await.unwrap();
    let e = lvm.extend(VG, "rw-l-a1", GIB).await.unwrap_err();
    println!("shrink refused: {e}");
    lvm.snapshot(VG, "rw-l-a1", "rw-l-a1+snap").await.unwrap();
    lvm.restore(VG, "rw-l-a1+snap", "rw-l-a2", &["rw-l.t2"])
        .await
        .unwrap();
    let rows = lvm.lvs(VG).await.unwrap();
    assert_eq!(row(&rows, "rw-l-a1").unwrap().size_bytes, 2 * GIB);
    let snap = row(&rows, "rw-l-a1+snap").unwrap();
    assert!(!snap.active, "a snapshot is never activated");
    assert_eq!(
        lv_field("rw-l-a1+snap", "lv_permissions").await,
        "read-only"
    );
    assert_eq!(lv_field("rw-l-a1+snap", "origin").await, "rw-l-a1");
    let a2 = row(&rows, "rw-l-a2").unwrap();
    assert!(a2.active, "a restore is active on return");
    assert_eq!(lv_field("rw-l-a2", "lv_permissions").await, "writeable");
    let tags = lvm.tags(VG).await.unwrap();
    assert!(tags["rw-l-a1+snap"].is_empty(), "snapshots copy no tags");
    assert_eq!(tags["rw-l-a2"], ["rw-l.t2"]);
    lvm.change_tags(VG, "rw-l-a2", &["rw-l.t3"], &["rw-l.t2"])
        .await
        .unwrap();
    assert_eq!(lvm.tags(VG).await.unwrap()["rw-l-a2"], ["rw-l.t3"]);

    // lvremove of an open LV: the report shows it open, remove says so.
    let mut sleep = host_command("sleep");
    sleep.arg("30");
    let held = tokio::process::Command::from(sleep)
        .stdin(std::fs::File::open(format!("/dev/{VG}/rw-l-a2")).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let rows = lvm.lvs(VG).await.unwrap();
    assert!(row(&rows, "rw-l-a2").unwrap().open);
    let e = lvm.remove(VG, "rw-l-a2").await.unwrap_err();
    assert_eq!(
        e.to_string(),
        format!("lvremove: {VG}/rw-l-a2 is open: still in use after 3s")
    );
    drop(held);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    lvm.deactivate(VG, "rw-l-a2").await.unwrap();
    lvm.activate(VG, "rw-l-a2").await.unwrap();
    for n in ["rw-l-a2", "rw-l-a1+snap", "rw-l-a1"] {
        lvm.remove(VG, n).await.unwrap();
    }
    // Already gone is done.
    lvm.remove(VG, "rw-l-a1").await.unwrap();
    let left: Vec<String> = lv_names(&lvm)
        .await
        .into_iter()
        .filter(|n| n.starts_with("rw-l-a"))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

const GUEST_RES: &str = "rw-l-d";
const GUEST_MINOR: u32 = 99_989;
/// Test-only settings: no backup files under /etc/lvm, no devices file
/// entry for the test PVs.
const NO_BACKUP: &str = "backup { backup = 0 archive = 0 }";

fn guest_res() -> String {
    format!("{TEST_DIR}/l2/{GUEST_RES}.res")
}

/// The test resource on `disk`, alone, so it comes up without a peer.
fn guest_res_file(disk: &str) -> String {
    format!(
        "resource \"{GUEST_RES}\" {{\n    on {} {{\n        node-id 0;\n        volume 0 {{ device minor {GUEST_MINOR}; disk {disk}; meta-disk internal; }}\n        address 127.0.0.1:11989;\n    }}\n}}\n",
        hostname()
    )
}

/// Every rw-* DRBD resource the kernel has and every rw-* LV, whichever
/// test left them: a failed run leaves them holding each other open, and
/// any one of them fails the next run.
async fn clean_rw(lvm: &Lvm) {
    let out = sh(&["drbdsetup", "status", "--json"]).await;
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    let status: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    for r in status.as_array().into_iter().flatten() {
        let Some(name) = r["name"].as_str().filter(|n| n.starts_with("rw-")) else {
            continue;
        };
        for d in r["devices"].as_array().into_iter().flatten() {
            if let Some(minor) = d["minor"].as_u64() {
                let _ = sh(&["drbdsetup", "resume-io", &minor.to_string()]).await;
            }
        }
        let down = sh(&["drbdsetup", "down", name]).await;
        assert_eq!(down.exit_code, Some(0), "down {name}: {down:?}");
    }
    let _ = std::fs::remove_dir_all(format!("{TEST_DIR}/l2"));
    clean(lvm, "rw-").await;
}

async fn guest_clean(lvm: &Lvm) {
    clean_rw(lvm).await;
}

/// A PV and a VG `vg` on `dev`, written with a config that sees only `dev`.
/// A guest writes its label whatever the host's LVM config says, so the
/// host's global_filter is opened up too.
async fn guest_vg(dev: &str, vg: &str, scan_lvs: u8) {
    let config = format!(
        "devices {{ use_devicesfile = 0 scan_lvs = {scan_lvs} global_filter = [ \"a|.*|\" ] filter = [ \"a|^{dev}$|\", \"r|.*|\" ] }} {NO_BACKUP}"
    );
    for cmd in [
        vec!["pvcreate", "--config", &config, dev],
        vec!["vgcreate", "--config", &config, vg, dev],
    ] {
        let out = sh(&cmd).await;
        assert_eq!(out.exit_code, Some(0), "{cmd:?}: {}", out.stderr);
    }
}

/// `vg pv` of every VG the config shows, and stderr with the JSON log.
async fn seen(config: &str) -> (Vec<String>, String) {
    let out = sh(&[
        "vgs",
        "--config",
        config,
        "--reportformat",
        "json",
        "-o",
        "vg_name,pv_name",
    ])
    .await;
    assert_eq!(out.exit_code, Some(0), "{}", out.stderr);
    let doc: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    let names = doc["report"][0]["vg"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| format!("{} {}", r["vg_name"], r["pv_name"]))
        .collect();
    (names, format!("{}{}", out.stderr, doc["log"]))
}

fn duplicate_pv_warning(text: &str) -> bool {
    text.contains("Not using device") || text.contains("prefers device")
}

fn guests(names: &[String]) -> Vec<String> {
    names
        .iter()
        .filter(|n| n.contains("rw-l-guest"))
        .cloned()
        .collect()
}

async fn guest_drbdadm(args: &[&str]) {
    let res = guest_res();
    let mut cmd = vec!["drbdadm", "-c", &res];
    cmd.extend_from_slice(args);
    cmd.push(GUEST_RES);
    let out = sh(&cmd).await;
    assert_eq!(out.exit_code, Some(0), "{cmd:?}: {}", out.stderr);
}

/// A guest's LVM on a volume, directly or through DRBD, stays invisible to
/// the agent's lvm commands, with no duplicate PV warning.
#[tokio::test]
#[ignore = "needs root, drbd-utils and the test VG"]
async fn guest_signatures_stay_hidden() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    guest_clean(&lvm).await;
    for lv in ["rw-l-g1", "rw-l-g2"] {
        lvm.create(VG, THIN, lv, 64 << 20, &[]).await.unwrap();
    }
    // A guest PV straight on a volume.
    guest_vg(&format!("/dev/{VG}/rw-l-g1"), "rw-l-guest1", 1).await;

    // A guest PV through DRBD: the backing LV holds the same signature.
    std::fs::create_dir_all(format!("{TEST_DIR}/l2")).unwrap();
    std::fs::write(guest_res(), guest_res_file(&format!("/dev/{VG}/rw-l-g2"))).unwrap();
    guest_drbdadm(&["--force", "create-md"]).await;
    guest_drbdadm(&["up"]).await;
    guest_drbdadm(&["primary", "--force"]).await;
    guest_vg(&format!("/dev/drbd{GUEST_MINOR}"), "rw-l-guest2", 0).await;
    guest_drbdadm(&["secondary"]).await;

    // The agent's commands, the host's devices file in force.
    let (names, text) = seen(lvm::cmd::CONFIG).await;
    println!("ours: {names:?}");
    assert!(guests(&names).is_empty(), "{names:?}");
    assert!(!duplicate_pv_warning(&text), "{text}");
    let argv = lvm::cmd::lvs(VG);
    let out = sh(&argv.iter().map(String::as_str).collect::<Vec<_>>()).await;
    assert_eq!(out.exit_code, Some(0));
    assert!(!duplicate_pv_warning(&out.stdout) && !duplicate_pv_warning(&out.stderr));
    lvm.lvs(VG).await.unwrap();
    lvm.vg(VG).await.unwrap();

    // The same rules on a host without a devices file.
    let no_file = format!("devices {{ {} use_devicesfile = 0 }}", lvm::device_rules!());
    let (names, text) = seen(&no_file).await;
    println!("ours without a devices file: {names:?}");
    assert!(guests(&names).is_empty(), "{names:?}");
    assert!(!duplicate_pv_warning(&text), "{text}");

    // Control: without the rules both guests show, the DRBD one twice.
    let (names, text) =
        seen("devices { use_devicesfile = 0 scan_lvs = 1 global_filter = [ \"a|.*|\" ] }").await;
    println!("control: {:?}", guests(&names));
    assert_eq!(guests(&names).len(), 2, "{names:?}");
    assert!(
        text.contains(&format!("Not using device /dev/{VG}/rw-l-g2")),
        "{text}"
    );

    guest_drbdadm(&["down"]).await;
    guest_clean(&lvm).await;
}

fn pool() -> Arc<Pool> {
    let mut p = Pool::new(
        "test-host-pool",
        PoolSpec {
            node: "host".into(),
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
            source: source.map(|s| VolumeSource {
                snapshot: s.into(),
                snapshot_uid: Some(format!("uid-{s}")),
            }),
            resolve: None,
            assignment: Some(Assignment {
                replicas: vec![AssignedReplica {
                    node: "host".into(),
                    pool: Some("test-host-pool".into()),
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
    v.metadata.generation = Some(1);
    v.metadata.uid = Some(format!("uid-{name}"));
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
#[ignore = "needs root and the test VG"]
async fn reconcile_decision_table() {
    if !enabled() {
        return;
    }
    clean(&lvm(), "rw-l-r").await;
    let mut rec = Reconciler::new("host", Arc::new(exec()));
    let pools = [pool()];

    let v1 = volume("rw-l-r1", GIB, None);
    let w = rec.pass(&pools, &[Arc::new(v1.clone())], &[]).await;
    let e = entry(&w, "rw-l-r1");
    println!("create: {e:?}");
    assert!(e.present && e.last_error.is_none());
    assert_eq!(e.size_bytes, Some(GIB));
    assert_eq!(
        e.device_path.as_deref(),
        Some(format!("/dev/{VG}/rw-l-r1").as_str())
    );

    let mut grown = v1.clone();
    grown.spec.size_bytes = 2 * GIB;
    grown.metadata.generation = Some(2);
    let w = rec.pass(&pools, &[Arc::new(grown.clone())], &[]).await;
    let e = entry(&w, "rw-l-r1");
    println!("grow: {e:?}");
    assert_eq!(
        (e.size_bytes, e.observed_generation),
        (Some(2 * GIB), Some(2))
    );

    let mut snap = Snapshot::new(
        "snapshot-test-host",
        SnapshotSpec {
            volume: "rw-l-r1".into(),
            volume_uid: Some("uid-rw-l-r1".into()),
        },
    );
    snap.metadata.uid = Some("uid-snapshot-test-host".into());
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
    // The restore finds the copy through the ack's takenPool.
    snap.status.as_mut().unwrap().acks = vec![ack];

    let v2 = volume("rw-l-r2", 3 * GIB, Some("snapshot-test-host"));
    let w = rec
        .pass(
            &pools,
            &[Arc::new(grown.clone()), Arc::new(v2.clone())],
            &[Arc::new(snap.clone())],
        )
        .await;
    let e = entry(&w, "rw-l-r2");
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
    for n in ["rw-l-r1", "rw-l-r2"] {
        let e = entry(&w, n);
        println!("delete {n}: {e:?}");
        assert!(!e.present && e.last_error.is_none());
    }
    let left: Vec<String> = lv_names(&lvm())
        .await
        .into_iter()
        .filter(|n| n.starts_with("rw-l-r"))
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
        Arc::new(exec()),
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
            let _ = host_command("umount").arg(m).output();
        }
        let _ = host_command("lvremove")
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
#[ignore = "needs root and the test VG"]
async fn csi_node_cycle() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "rw-l-c").await;
    let lv = "rw-l-c1";
    lvm.create(VG, THIN, lv, GIB, &[]).await.unwrap();
    let dev = format!("/dev/{VG}/{lv}");
    let svc = node_service();
    let ctx = HashMap::from([(PUBLISH_DEVICE_PATH.to_owned(), dev.clone())]);
    let staging = format!("{TEST_DIR}/mnt/staging/{lv}");
    let target = format!("{TEST_DIR}/mnt/pods/{lv}/mount");
    let block_target = format!("{TEST_DIR}/mnt/pods/{lv}/block");
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

    lvm.extend(VG, lv, 2 * GIB).await.unwrap();
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

    let _ = std::fs::remove_dir_all(format!("{TEST_DIR}/mnt/pods/{lv}"));
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
#[ignore = "needs root and the test VG"]
async fn stage_refuses_partition_table() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "rw-l-p").await;
    let lv = "rw-l-p1";
    lvm.create(VG, THIN, lv, GIB, &[]).await.unwrap();
    let staging = format!("{TEST_DIR}/mnt/staging/{lv}");
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
    let out = host_command("blkid")
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
            let _ = host_command("umount").arg(m).output();
        }
        for lv in &self.lvs {
            let _ = host_command("lvremove")
                .args(["-f", &format!("{VG}/{lv}")])
                .output();
        }
    }
}

fn fs_uuid(dev: &str) -> String {
    let out = host_command("blkid")
        .args(["-p", "-s", "UUID", "-o", "value", dev])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// A restore staged on the node where its source is mounted.
#[tokio::test]
#[ignore = "needs root and the test VG"]
async fn restored_xfs_mounts_next_to_its_source() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "rw-l-u").await;
    let (src, snap, copy) = ("rw-l-u1", "rw-l-u1+snap", "rw-l-u2");
    let staging1 = format!("{TEST_DIR}/mnt/staging/{src}");
    let staging2 = format!("{TEST_DIR}/mnt/staging/{copy}");
    let _unwind = Unwind {
        mounts: vec![staging1.clone(), staging2.clone()],
        lvs: vec![copy.to_owned(), snap.to_owned(), src.to_owned()],
    };
    lvm.create(VG, THIN, src, GIB, &[]).await.unwrap();
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
    host_command("sync").status().unwrap();

    // Snapshot of a mounted file system: dirty log, same UUID as the source.
    lvm.snapshot(VG, src, snap).await.unwrap();
    lvm.restore(VG, snap, copy, &[]).await.unwrap();
    let copy_dev = format!("/dev/{VG}/{copy}");
    let before = fs_uuid(&copy_dev);
    assert_eq!(before, fs_uuid(&format!("/dev/{VG}/{src}")));

    let marker = Restored::default();
    let svc = NodeService::new(
        "host".into(),
        Arc::new(exec()),
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
    let out = host_command("blkid")
        .args(["-p", "-s", "LABEL", "-o", "value", dev])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// A file system whose record was lost is adopted with its data; only
/// the mkfs label proves an unfinished format.
#[tokio::test]
#[ignore = "needs root and the test VG"]
async fn stage_adopts_xfs_without_a_record() {
    if !enabled() {
        return;
    }
    let lvm = lvm();
    clean(&lvm, "rw-l-f").await;
    let lv = "rw-l-f1";
    let dev = format!("/dev/{VG}/{lv}");
    let staging = format!("{TEST_DIR}/mnt/staging/{lv}");
    let _cleanup = Cleanup {
        mounts: vec![staging.clone()],
        lv: lv.to_owned(),
    };
    lvm.create(VG, THIN, lv, GIB, &[]).await.unwrap();
    let state = NoLookup::default();
    // A new service each time: an agent restart, nothing remembered in memory.
    let svc = || {
        NodeService::new(
            "host".into(),
            Arc::new(exec()),
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
    let out = host_command("mkfs.xfs")
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

#[path = "host/drbd.rs"]
mod drbd;

#[path = "host/shutdown.rs"]
mod shutdown;
