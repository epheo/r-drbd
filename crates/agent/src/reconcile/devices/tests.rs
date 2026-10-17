#![allow(clippy::many_single_char_names)]

use std::sync::Arc;

use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use model::{DeviceState, Pool, PoolSpec, PoolStatus, ReclaimPolicy};

use super::super::*;
use crate::devices::fake::{FakeDisks, WIPEFS_BLANK, WIPEFS_XFS, disk};
use crate::devices::wipefs_argv;
use crate::exec::scripted::{ScriptedExec, ok};
use crate::storage::fixtures::{EXTENT, St, THIN, VG, fail, json, lv_row};

const NODE: &str = "n1";
const POOL: &str = "p1";
const TAG: &str = "r-drbd.pool.p1";
const A: &str = "/dev/disk/by-id/virtio-a";
const B: &str = "/dev/disk/by-id/virtio-b";
const C: &str = "/dev/disk/by-id/virtio-c";
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

/// The devices file check before vgcreate and vgextend: in use, present.
trait DevicesFile {
    fn expect_devices_file(&self) -> &Self;
}

impl DevicesFile for ScriptedExec {
    fn expect_devices_file(&self) -> &Self {
        self.expect(
            lvm::cmd::host_setting("devices/use_devicesfile"),
            ok("use_devicesfile=1\n"),
        )
        .expect(
            lvm::cmd::host_setting("devices/devicesfile"),
            ok("devicesfile=\"system.devices\"\n"),
        )
        .expect(
            crate::storage::devices_file_argv("/etc/lvm/devices/system.devices"),
            ok(""),
        )
    }
}

fn pool(devices: &[&str]) -> Pool {
    let mut p = Pool::new(
        POOL,
        PoolSpec {
            node: NODE.into(),
            tier: "test".into(),
            vg: VG.into(),
            thin_pool: THIN.into(),
            overcommit_percent: model::DEFAULT_OVERCOMMIT_PERCENT,
            reserve_percent: model::DEFAULT_RESERVE_PERCENT,
            devices: Some(devices.iter().map(|&d| d.to_owned()).collect()),
            chunk_size: None,
            reclaim_policy: None,
        },
    );
    p.metadata.generation = Some(1);
    p.metadata.uid = Some("uid-p1".into());
    p
}

fn disks() -> Arc<FakeDisks> {
    let d = FakeDisks::default();
    d.set(A, disk("vdd"))
        .set(B, disk("vde"))
        .set(C, disk("vdf"));
    Arc::new(d)
}

fn vgs(rows: &[(u64, u64, &str)]) -> Result<crate::Output, String> {
    let rows: Vec<String> = rows
        .iter()
        .map(|(size, free, tags)| {
            format!(
                r#"{{"vg_name":"{VG}", "vg_extent_size":"{EXTENT}", "vg_size":"{size}", "vg_free":"{free}", "vg_tags":"{tags}", "vg_missing_pv_count":"0"}}"#
            )
        })
        .collect();
    ok(&json("vg", &rows))
}

fn pvs(rows: &[(&str, &str)]) -> Result<crate::Output, String> {
    let rows: Vec<String> = rows
        .iter()
        .map(|(n, vg)| format!(r#"{{"pv_name":"{n}", "vg_name":"{vg}"}}"#))
        .collect();
    ok(&json("pv", &rows))
}

fn thin_pools(pool: Option<(u64, u64)>) -> Result<crate::Output, String> {
    let rows: Vec<String> = pool
        .into_iter()
        .map(|(data, meta)| {
            format!(
                r#"{{"lv_name":"{THIN}", "segtype":"thin-pool", "lv_size":"{data}", "lv_metadata_size":"{meta}", "chunk_size":"65536"}}"#
            )
        })
        .collect();
    ok(&json("lv", &rows))
}

/// The steady probe of a ready pool: lvs then vgs of the VG.
fn probe(x: &ScriptedExec) {
    crate::storage::fixtures::expect_lvs(x, &[]);
    crate::storage::fixtures::expect_vgs(x);
}

async fn run(x: &Arc<ScriptedExec>, d: Arc<FakeDisks>, p: Pool) -> (Vec<StatusWrite>, Reconciler) {
    let mut r = Reconciler::new(NODE, x.clone()).with_disks(d);
    let w = r.pass(&[Arc::new(p)], &[], &[]).await;
    x.assert_done();
    (w, r)
}

fn status(w: &[StatusWrite]) -> serde_json::Value {
    w.iter()
        .find(|w| matches!(w, StatusWrite::Pool { .. }))
        .expect("pool write")
        .body()["status"]
        .clone()
}

fn available(s: &serde_json::Value) -> (String, String, String) {
    let c = s["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Available")
        .unwrap();
    (
        c["status"].as_str().unwrap().into(),
        c["reason"].as_str().unwrap().into(),
        c["message"].as_str().unwrap().into(),
    )
}

#[tokio::test]
async fn blank_disks_build_vg_then_thin_pool() {
    let x = Arc::new(ScriptedExec::new());
    let free = 8 * GIB - 2 * EXTENT;
    // Look 1: nothing yet, both disks blank.
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[]))
        .expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_BLANK))
        .expect(wipefs_argv("/dev/vde"), ok(WIPEFS_BLANK))
        .expect_devices_file()
        .expect(lvm::cmd::vgcreate(VG, TAG, &[A.into(), B.into()]), ok(""));
    // Look 2: the tagged VG, members need no wipefs.
    x.expect(lvm::cmd::vgs_all(), vgs(&[(free, free, TAG)]))
        .expect(lvm::cmd::thin_pools(VG), thin_pools(None))
        .expect(
            lvm::cmd::pvs_all(),
            pvs(&[("/dev/vdd", VG), ("/dev/vde", VG)]),
        )
        .expect(
            lvm::cmd::create_thin_pool(VG, THIN, 64 << 10, 72 * MIB),
            ok(""),
        );
    // Look 3: ready, then the usual probe.
    x.expect(lvm::cmd::vgs_all(), vgs(&[(free, 0, TAG)]))
        .expect(
            lvm::cmd::thin_pools(VG),
            thin_pools(Some((free - 144 * MIB, 72 * MIB))),
        )
        .expect(
            lvm::cmd::pvs_all(),
            pvs(&[("/dev/vdd", VG), ("/dev/vde", VG)]),
        );
    probe(&x);
    let (w, _) = run(&x, disks(), pool(&[A, B])).await;
    let s = status(&w);
    assert_eq!(available(&s).0, "True");
    assert_eq!(s["chunkBytes"], 65_536);
    let rows = s["devices"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["path"], A);
    assert_eq!(rows[0]["state"], "Member");
    assert_eq!(rows[0]["serial"], "serial-vdd");
    assert_eq!(rows[0]["sizeBytes"], 4u64 << 30);
}

#[tokio::test]
async fn one_signature_blocks_everything() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[]))
        .expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_BLANK))
        .expect(wipefs_argv("/dev/vde"), ok(WIPEFS_XFS));
    let (w, _) = run(&x, disks(), pool(&[A, B])).await;
    let s = status(&w);
    assert_eq!(
        available(&s),
        (
            "False".into(),
            "DeviceNotEmpty".into(),
            format!("{B} is NotEmpty: signatures: xfs at 0x0")
        )
    );
    assert_eq!(s["devices"][0]["state"], "Blank");
    assert_eq!(s["devices"][1]["state"], "NotEmpty");
    assert!(s.get("totalBytes").is_none());
}

#[tokio::test]
async fn busy_and_missing_disks_block() {
    let d = disks();
    let mut held = disk("vdd");
    held.mounts = vec!["/mnt/x".into()];
    held.exclusive = Err("Device or resource busy (os error 16)".into());
    d.set(A, held);
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[]));
    let (w, _) = run(&x, d, pool(&[A, "/dev/disk/by-id/gone"])).await;
    let s = status(&w);
    let (st, reason, msg) = available(&s);
    assert_eq!((st.as_str(), reason.as_str()), ("False", "DeviceMissing"));
    assert!(msg.contains("is Busy: mounted on /mnt/x"), "{msg}");
}

#[tokio::test]
async fn foreign_vg_is_never_touched() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[(4 * GIB, 4 * GIB, "")]))
        .expect(lvm::cmd::thin_pools(VG), thin_pools(None))
        .expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", VG)]));
    let (w, _) = run(&x, disks(), pool(&[A])).await;
    let (st, reason, _) = available(&status(&w));
    assert_eq!(
        (st.as_str(), reason.as_str()),
        ("False", "VolumeGroupNotOwned")
    );
    // Reports only.
    for argv in x.seen.lock().unwrap().iter() {
        assert!(
            ["vgs", "lvs", "pvs"].contains(&argv[0].as_str()),
            "{argv:?}"
        );
    }
}

#[tokio::test]
async fn growth_extends_then_grows_and_warns_once() {
    let d = disks();
    let x = Arc::new(ScriptedExec::new());
    let data = 4 * GIB - 136 * MIB;
    // A member, a blank new disk, and one that cannot join.
    x.expect(lvm::cmd::vgs_all(), vgs(&[(4 * GIB, 0, TAG)]))
        .expect(lvm::cmd::thin_pools(VG), thin_pools(Some((data, 68 * MIB))))
        .expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", VG)]))
        .expect(wipefs_argv("/dev/vde"), ok(WIPEFS_BLANK))
        .expect(wipefs_argv("/dev/vdf"), ok(WIPEFS_XFS))
        .expect_devices_file()
        .expect(lvm::cmd::vgextend(VG, &[B.into()]), ok(""));
    let both = [("/dev/vdd", VG), ("/dev/vde", VG)];
    x.expect(lvm::cmd::vgs_all(), vgs(&[(8 * GIB, 4 * GIB, TAG)]))
        .expect(lvm::cmd::thin_pools(VG), thin_pools(Some((data, 68 * MIB))))
        .expect(lvm::cmd::pvs_all(), pvs(&both))
        .expect(wipefs_argv("/dev/vdf"), ok(WIPEFS_XFS))
        .expect(lvm::cmd::extend_pool_metadata(VG, THIN, 72 * MIB), ok(""));
    x.expect(
        lvm::cmd::vgs_all(),
        vgs(&[(8 * GIB, 4 * GIB - 8 * MIB, TAG)]),
    )
    .expect(lvm::cmd::thin_pools(VG), thin_pools(Some((data, 72 * MIB))))
    .expect(lvm::cmd::pvs_all(), pvs(&both))
    .expect(wipefs_argv("/dev/vdf"), ok(WIPEFS_XFS))
    .expect(lvm::cmd::extend_pool_data(VG, THIN), ok(""));
    x.expect(lvm::cmd::vgs_all(), vgs(&[(8 * GIB, 0, TAG)]))
        .expect(
            lvm::cmd::thin_pools(VG),
            thin_pools(Some((8 * GIB - 144 * MIB, 72 * MIB))),
        )
        .expect(lvm::cmd::pvs_all(), pvs(&both))
        .expect(wipefs_argv("/dev/vdf"), ok(WIPEFS_XFS));
    probe(&x);
    let p = pool(&[A, B, C]);
    let mut r = Reconciler::new(NODE, x.clone()).with_disks(d);
    let w = r.pass(&[Arc::new(p.clone())], &[], &[]).await;
    x.assert_done();
    assert_eq!(available(&status(&w)).0, "True");
    let events: Vec<&StatusWrite> = w
        .iter()
        .filter(|w| matches!(w, StatusWrite::Event { .. }))
        .collect();
    assert_eq!(events.len(), 1);
    let ev = events[0].body();
    assert_eq!(ev["reason"], "DeviceNotUsable");
    assert_eq!(ev["involvedObject"]["kind"], "Pool");
    assert_eq!(ev["involvedObject"]["uid"], "uid-p1");
    assert_eq!(
        ev["message"],
        format!("not added to the pool: {C} is NotEmpty: signatures: xfs at 0x0")
    );

    // Next pass, same state: no second Event.
    x.expect(lvm::cmd::vgs_all(), vgs(&[(8 * GIB, 0, TAG)]))
        .expect(
            lvm::cmd::thin_pools(VG),
            thin_pools(Some((8 * GIB - 144 * MIB, 72 * MIB))),
        )
        .expect(lvm::cmd::pvs_all(), pvs(&both))
        .expect(wipefs_argv("/dev/vdf"), ok(WIPEFS_XFS));
    probe(&x);
    let w = r.pass(&[Arc::new(p)], &[], &[]).await;
    x.assert_done();
    assert!(!w.iter().any(|w| matches!(w, StatusWrite::Event { .. })));
}

#[tokio::test]
async fn failed_step_reports_building() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[]))
        .expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_BLANK))
        .expect_devices_file()
        .expect(
            lvm::cmd::vgcreate(VG, TAG, &[A.into()]),
            fail("WARNING: xfs signature detected on /dev/vdd at offset 0. Wipe it? [y/n]: [n]\n  Aborted wiping of xfs.\n"),
        );
    let (w, _) = run(&x, disks(), pool(&[A])).await;
    let (st, reason, msg) = available(&status(&w));
    assert_eq!((st.as_str(), reason.as_str()), ("False", "Building"));
    assert!(
        msg.starts_with(&format!("vgcreate on {A}: vgcreate: exit 5:")),
        "{msg}"
    );
}

#[tokio::test]
async fn bad_chunk_size_runs_nothing() {
    let x = Arc::new(ScriptedExec::new());
    let mut p = pool(&[A]);
    p.spec.chunk_size = Some(Quantity("96Ki".into()));
    let (w, _) = run(&x, disks(), p).await;
    let (st, _, msg) = available(&status(&w));
    assert_eq!(st, "False");
    assert_eq!(msg, "chunkSize 96Ki is not a power of two from 64Ki to 1Gi");
}

fn deleting(mut p: Pool, policy: ReclaimPolicy, members: &[&str]) -> Pool {
    p.metadata.deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
    p.spec.reclaim_policy = Some(policy);
    p.status = Some(PoolStatus {
        devices: members
            .iter()
            .map(|&m| model::DeviceStatus {
                path: m.into(),
                state: DeviceState::Member,
                size_bytes: None,
                model: None,
                serial: None,
                message: None,
            })
            .collect(),
        ..PoolStatus::default()
    });
    p
}

fn released(s: &serde_json::Value) -> Option<(String, String, String)> {
    let c = s["conditions"]
        .as_array()?
        .iter()
        .find(|c| c["type"] == "Released")?;
    Some((
        c["status"].as_str()?.into(),
        c["reason"].as_str()?.into(),
        c["message"].as_str()?.into(),
    ))
}

#[tokio::test]
async fn delete_removes_pool_vg_and_labels() {
    let x = Arc::new(ScriptedExec::new());
    let look = |x: &ScriptedExec, vg: bool, pv_vg: &str| {
        if vg {
            x.expect(lvm::cmd::vgs_all(), vgs(&[(4 * GIB, 0, TAG)]))
                .expect(lvm::cmd::thin_pools(VG), thin_pools(Some((GIB, 68 * MIB))));
        } else {
            x.expect(lvm::cmd::vgs_all(), vgs(&[]));
        }
        x.expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", pv_vg)]));
    };
    let pool_lv = |active: St| ok(&json("lv", &[pool_row(active)]));
    look(&x, true, VG);
    x.expect(lvm::cmd::lvs(VG), pool_lv(St::Active))
        .expect(lvm::cmd::deactivate(VG, THIN), ok(""));
    look(&x, true, VG);
    x.expect(lvm::cmd::lvs(VG), pool_lv(St::Inactive))
        .expect(lvm::cmd::remove_inactive(VG, THIN), ok(""));
    look(&x, true, VG);
    x.expect(lvm::cmd::lvs(VG), ok(&json("lv", &[])))
        .expect(lvm::cmd::vgremove(VG), ok(""));
    // The orphan PV is cleared because the status had it as a member.
    look(&x, false, "");
    x.expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_BLANK))
        .expect(lvm::cmd::pvremove(&[A.into()]), ok(""));
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[]))
        .expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_BLANK));
    let p = deleting(pool(&[A]), ReclaimPolicy::Delete, &[A]);
    let (w, _) = run(&x, disks(), p).await;
    let s = status(&w);
    assert_eq!(
        released(&s),
        Some((
            "True".into(),
            "Removed".into(),
            "thin pool, volume group vg1 and PV labels removed".into()
        ))
    );
    assert_eq!(available(&s).0, "False");
    assert_eq!(s["devices"][0]["state"], "Blank");
}

fn pool_row(st: St) -> String {
    let (active, open) = match st {
        St::Inactive => ("0", "-1"),
        _ => ("1", "0"),
    };
    format!(
        r#"{{"lv_name":"{THIN}", "lv_path":"", "lv_size":"{GIB}", "segtype":"thin-pool", "pool_lv":"", "lv_active_locally":"{active}", "lv_device_open":"{open}", "zero":"1", "data_percent":"0.00", "chunk_size":"65536"}}"#
    )
}

#[tokio::test]
async fn delete_with_another_lv_removes_nothing() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[(4 * GIB, 0, TAG)]))
        .expect(lvm::cmd::thin_pools(VG), thin_pools(Some((GIB, 68 * MIB))))
        .expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", VG)]))
        .expect(
            lvm::cmd::lvs(VG),
            ok(&json(
                "lv",
                &[pool_row(St::Active), lv_row("keep", 1024, St::Active)],
            )),
        );
    // The pool is still there: the usual probe, on the lvs read above.
    crate::storage::fixtures::expect_vgs(&x);
    let p = deleting(pool(&[A]), ReclaimPolicy::Delete, &[A]);
    let (w, _) = run(&x, disks(), p).await;
    let s = status(&w);
    assert_eq!(
        released(&s),
        Some((
            "False".into(),
            "RemoveBlocked".into(),
            "volume group vg1 holds LV keep: nothing removed".into()
        ))
    );
    assert_eq!(available(&s).0, "True");
}

#[tokio::test]
async fn deleting_never_builds() {
    // Retain: look only, no Released.
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[]))
        .expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_BLANK));
    x.expect(
        lvm::cmd::lvs(VG),
        crate::storage::fixtures::report_fail("Volume group \"vg1\" not found"),
    );
    let p = deleting(pool(&[A]), ReclaimPolicy::Retain, &[]);
    let (w, _) = run(&x, disks(), p).await;
    let s = status(&w);
    assert_eq!(released(&s), None);
    assert_eq!(available(&s).0, "False");

    // Delete with volumes still listed: nothing runs beyond the look.
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[]))
        .expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_BLANK))
        .expect(
            lvm::cmd::lvs(VG),
            crate::storage::fixtures::report_fail("Volume group \"vg1\" not found"),
        );
    let mut p = deleting(pool(&[A]), ReclaimPolicy::Delete, &[A]);
    p.status.as_mut().unwrap().volumes = vec!["pvc-1".into()];
    let (w, _) = run(&x, disks(), p).await;
    let (st, reason, msg) = released(&status(&w)).unwrap();
    assert_eq!((st.as_str(), reason.as_str()), ("False", "RemoveBlocked"));
    assert_eq!(msg, "volumes still placed here: pvc-1: nothing removed");
}

#[tokio::test]
async fn ready_pool_writes_nothing_when_unchanged() {
    let x = Arc::new(ScriptedExec::new());
    let ready = |x: &ScriptedExec| {
        x.expect(lvm::cmd::vgs_all(), vgs(&[(4 * GIB, 0, TAG)]))
            .expect(lvm::cmd::thin_pools(VG), thin_pools(Some((GIB, 68 * MIB))))
            .expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", VG)]));
        probe(x);
    };
    ready(&x);
    let mut p = pool(&[A]);
    let (w, mut r) = run(&x, disks(), p.clone()).await;
    let StatusWrite::Pool {
        capacity,
        available,
        devices,
        ..
    } = &w[0]
    else {
        panic!("pool write expected");
    };
    let cap = capacity.clone().unwrap();
    p.status = Some(PoolStatus {
        total_bytes: Some(cap.total_bytes),
        free_bytes: Some(cap.free_bytes),
        chunk_bytes: Some(cap.chunk_bytes),
        conditions: vec![available.clone()],
        observed_generation: Some(1),
        devices: devices.clone().unwrap(),
        ..PoolStatus::default()
    });
    ready(&x);
    let w = r.pass(&[Arc::new(p)], &[], &[]).await;
    x.assert_done();
    assert!(w.is_empty(), "{w:?}");
}

#[tokio::test]
async fn no_lv_in_a_vg_that_is_not_ours() {
    use model::{AssignedReplica, Assignment, Role, Volume, VolumeSpec};
    let mut v = Volume::new(
        "pvc-1",
        VolumeSpec {
            size_bytes: GIB,
            replicas: 1,
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
    v.metadata.generation = Some(1);
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[(4 * GIB, 4 * GIB, "")]))
        .expect(lvm::cmd::thin_pools(VG), thin_pools(None))
        .expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", VG)]));
    let mut r = Reconciler::new(NODE, x.clone()).with_disks(disks());
    let w = r.pass(&[Arc::new(pool(&[A]))], &[Arc::new(v)], &[]).await;
    x.assert_done();
    let err = w
        .iter()
        .find_map(|w| match w {
            StatusWrite::Volume { entry, .. } => entry.last_error.clone(),
            _ => None,
        })
        .unwrap();
    assert!(
        err.starts_with("pool p1 is not ready: volume group vg1 exists without tag"),
        "{err}"
    );
}

#[tokio::test]
async fn deleting_a_pool_on_a_foreign_vg_runs_reports_only() {
    for policy in [ReclaimPolicy::Delete, ReclaimPolicy::Retain] {
        let x = Arc::new(ScriptedExec::new());
        x.expect(lvm::cmd::vgs_all(), vgs(&[(4 * GIB, 4 * GIB, "")]))
            .expect(lvm::cmd::thin_pools(VG), thin_pools(Some((GIB, 68 * MIB))))
            .expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", VG)]));
        let p = deleting(pool(&[A]), policy, &[A]);
        let (w, _) = run(&x, disks(), p).await;
        let s = status(&w);
        let (st, reason, _) = available(&s);
        assert_eq!(
            (st.as_str(), reason.as_str()),
            ("False", "VolumeGroupNotOwned")
        );
        if policy == ReclaimPolicy::Delete {
            assert_eq!(released(&s).unwrap().1, "RemoveBlocked");
        }
        for argv in x.seen.lock().unwrap().iter() {
            assert!(
                ["vgs", "lvs", "pvs"].contains(&argv[0].as_str()),
                "{argv:?}"
            );
        }
    }
}

#[tokio::test]
async fn no_vgcreate_without_the_devices_file_in_use() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[]))
        .expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_BLANK))
        .expect(
            lvm::cmd::host_setting("devices/use_devicesfile"),
            ok("use_devicesfile=1\n"),
        )
        .expect(
            lvm::cmd::host_setting("devices/devicesfile"),
            ok("devicesfile=\"system.devices\"\n"),
        )
        .expect(
            crate::storage::devices_file_argv("/etc/lvm/devices/system.devices"),
            crate::exec::scripted::code(1, "", ""),
        );
    let (w, _) = run(&x, disks(), pool(&[A])).await;
    let (st, reason, msg) = available(&status(&w));
    assert_eq!((st.as_str(), reason.as_str()), ("False", "Building"));
    assert!(
        msg.contains("/etc/lvm/devices/system.devices does not exist"),
        "{msg}"
    );
    assert!(msg.contains("vgimportdevices --all"), "{msg}");
}

/// A Pool deleted under Delete whose VG is gone and whose disk A still
/// carries its PV label.
fn orphan_look(x: &ScriptedExec) {
    x.expect(lvm::cmd::vgs_all(), vgs(&[]))
        .expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", "")]))
        .expect(
            wipefs_argv("/dev/vdd"),
            ok(r#"{"signatures": [{"device": "vdd", "offset": "0x218", "type": "LVM2_member", "uuid": "x", "label": null}]}"#),
        );
}

#[tokio::test]
async fn failed_pvremove_is_retried_never_released() {
    let mut p = deleting(pool(&[A]), ReclaimPolicy::Delete, &[A]);
    let x = Arc::new(ScriptedExec::new());
    orphan_look(&x);
    x.expect(lvm::cmd::pvremove(&[A.into()]), fail("  denied\n"));
    x.expect(
        lvm::cmd::lvs(VG),
        crate::storage::fixtures::report_fail("Volume group \"vg1\" not found"),
    );
    let (w, mut r) = run(&x, disks(), p.clone()).await;
    let s = status(&w);
    let (st, reason, _) = released(&s).unwrap();
    assert_eq!((st.as_str(), reason.as_str()), ("False", "RemoveFailed"));
    // The row stays Member, so the next pass knows the label is ours.
    assert_eq!(s["devices"][0]["state"], "Member");

    p.status.as_mut().unwrap().devices = serde_json::from_value(s["devices"].clone()).unwrap();
    orphan_look(&x);
    x.expect(lvm::cmd::pvremove(&[A.into()]), fail("  denied\n"));
    x.expect(
        lvm::cmd::lvs(VG),
        crate::storage::fixtures::report_fail("Volume group \"vg1\" not found"),
    );
    let w = r.pass(&[Arc::new(p)], &[], &[]).await;
    x.assert_done();
    let (st, reason, _) = released(&status(&w)).unwrap();
    assert_eq!((st.as_str(), reason.as_str()), ("False", "RemoveFailed"));
}

#[tokio::test]
async fn an_orphan_pv_never_a_member_is_left_alone() {
    let x = Arc::new(ScriptedExec::new());
    orphan_look(&x);
    let p = deleting(pool(&[A]), ReclaimPolicy::Delete, &[]);
    let (w, _) = run(&x, disks(), p).await;
    let s = status(&w);
    assert_eq!(released(&s).unwrap().0, "True");
    assert_eq!(s["devices"][0]["state"], "NotEmpty");
    assert!(!x.seen.lock().unwrap().iter().any(|a| a[0] == "pvremove"));
}

#[tokio::test]
async fn failed_growth_keeps_the_pool_serving() {
    use model::{AssignedReplica, Assignment, Role, Volume, VolumeSpec};
    let mut v = Volume::new(
        "pvc-1",
        VolumeSpec {
            size_bytes: GIB,
            replicas: 1,
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
    v.metadata.generation = Some(1);
    let x = Arc::new(ScriptedExec::new());
    let data = 4 * GIB - 136 * MIB;
    x.expect(lvm::cmd::vgs_all(), vgs(&[(4 * GIB, 0, TAG)]))
        .expect(lvm::cmd::thin_pools(VG), thin_pools(Some((data, 68 * MIB))))
        .expect(lvm::cmd::pvs_all(), pvs(&[("/dev/vdd", VG)]))
        .expect(wipefs_argv("/dev/vde"), ok(WIPEFS_BLANK))
        .expect_devices_file()
        .expect(
            lvm::cmd::vgextend(VG, &[B.into()]),
            fail("  Device /dev/vde excluded by a filter.\n"),
        );
    // The usual probe, then the volume finds its LV in the pool.
    x.expect(
        lvm::cmd::lvs(VG),
        ok(&crate::storage::fixtures::lvs_out(&[lv_row(
            "pvc-1",
            1 << 20,
            St::Active,
        )])),
    );
    crate::storage::fixtures::expect_vgs(&x);
    let mut r = Reconciler::new(NODE, x.clone()).with_disks(disks());
    let w = r
        .pass(&[Arc::new(pool(&[A, B]))], &[Arc::new(v)], &[])
        .await;
    x.assert_done();
    let s = status(&w);
    let (st, _, msg) = available(&s);
    assert_eq!(st, "True");
    assert!(
        msg.starts_with(&format!("vgextend with {B}: vgextend: exit 5:")),
        "{msg}"
    );
    assert!(
        s["devices"][1]["message"]
            .as_str()
            .unwrap()
            .contains("excluded by a filter")
    );
    let ev: Vec<_> = w
        .iter()
        .filter(|w| matches!(w, StatusWrite::Event { .. }))
        .map(|w| w.body()["reason"].clone())
        .collect();
    assert_eq!(ev, ["PoolNotGrown"]);
    let entry = w
        .iter()
        .find_map(|w| match w {
            StatusWrite::Volume { entry, .. } => Some(entry.clone()),
            _ => None,
        })
        .unwrap();
    assert!(entry.present && entry.last_error.is_none(), "{entry:?}");
}

#[tokio::test]
async fn unreadable_host_keeps_a_built_pool_on_the_usual_probe() {
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), fail("  lvmlockd busy\n"));
    probe(&x);
    let mut p = pool(&[A]);
    p.status = Some(PoolStatus {
        devices: vec![model::DeviceStatus {
            path: A.into(),
            state: DeviceState::Member,
            size_bytes: None,
            model: None,
            serial: None,
            message: None,
        }],
        ..PoolStatus::default()
    });
    let (w, r) = run(&x, disks(), p).await;
    assert_eq!(available(&status(&w)).0, "True");
    assert!(r.unready_pools.is_empty());
    // Never built: not even the probe.
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::vgs_all(), fail("  lvmlockd busy\n"));
    let (w, r) = run(&x, disks(), pool(&[A])).await;
    assert_eq!(available(&status(&w)).0, "False");
    assert!(r.unready_pools.contains_key(POOL));
}
