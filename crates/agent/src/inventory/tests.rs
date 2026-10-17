use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use model::{Pool, PoolSpec};

use super::*;
use crate::devices::fake::{FakeDisks, WIPEFS_BLANK, WIPEFS_XFS, disk};
use crate::devices::{HostDisks, Part, wipefs_argv};
use crate::exec::scripted::{ScriptedExec, code, ok};
use crate::storage::fixtures::json;

const NODE: &str = "n1";

/// `wipefs --no-act --json` on a PV.
const WIPEFS_PV: &str = r#"{"signatures": [{"device": "vdb", "offset": "0x218", "type": "LVM2_member", "uuid": "x", "label": null}]}"#;

fn links(names: &[&str]) -> Vec<String> {
    names
        .iter()
        .map(|n| format!("/dev/disk/by-id/{n}"))
        .collect()
}

fn pool(name: &str, vg: &str, devices: bool) -> Arc<Pool> {
    Arc::new(Pool::new(
        name,
        PoolSpec {
            node: NODE.into(),
            tier: "t".into(),
            vg: vg.into(),
            thin_pool: "thin".into(),
            overcommit_percent: model::DEFAULT_OVERCOMMIT_PERCENT,
            reserve_percent: model::DEFAULT_RESERVE_PERCENT,
            devices: devices.then(|| vec!["/dev/disk/by-id/virtio-a".into()]),
            chunk_size: None,
            reclaim_policy: None,
        },
    ))
}

fn pv(name: &str, vg: Option<&str>) -> lvm::report::Pv {
    lvm::report::Pv {
        name: name.into(),
        vg: vg.map(str::to_owned),
    }
}

fn vg(name: &str, tags: &[&str]) -> lvm::report::VgFull {
    lvm::report::VgFull {
        name: name.into(),
        extent_bytes: 4 << 20,
        size_bytes: 0,
        free_bytes: 0,
        tags: tags.iter().map(|&t| t.to_owned()).collect(),
        missing_pvs: 0,
    }
}

fn pvs_out(rows: &[(&str, &str)]) -> Result<crate::Output, String> {
    let rows: Vec<String> = rows
        .iter()
        .map(|(n, vg)| format!(r#"{{"pv_name":"{n}", "vg_name":"{vg}"}}"#))
        .collect();
    ok(&json("pv", &rows))
}

fn vgs_out(rows: &[(&str, &str)]) -> Result<crate::Output, String> {
    let rows: Vec<String> = rows
        .iter()
        .map(|(name, tags)| {
            format!(
                r#"{{"vg_name":"{name}", "vg_extent_size":"4194304", "vg_size":"0", "vg_free":"0", "vg_tags":"{tags}", "vg_missing_pv_count":"0"}}"#
            )
        })
        .collect();
    ok(&json("vg", &rows))
}

#[test]
fn path_prefers_wwn_then_bus() {
    let l = links(&["nvme-Some_SSD_S7KH", "nvme-eui.0025", "wwn-0x5002"]);
    assert_eq!(preferred(&l, "nvme0n1"), "/dev/disk/by-id/wwn-0x5002");
    let l = links(&["lvm-pv-uuid-abc", "nvme-eui.0025", "nvme-Some_SSD_S7KH"]);
    assert_eq!(
        preferred(&l, "nvme0n1"),
        "/dev/disk/by-id/nvme-Some_SSD_S7KH"
    );
    let l = links(&["nvme-eui.0025"]);
    assert_eq!(preferred(&l, "nvme0n1"), "/dev/disk/by-id/nvme-eui.0025");
    assert_eq!(preferred(&[], "vdz"), "/dev/vdz");
}

#[test]
fn vg_owners() {
    let pools = [
        pool("p1", "vg1", true),
        pool("p3", "vg2", false),
        pool("p2", "vg2", false),
        pool("p4", "vg5", true),
    ];
    let l = Lvm::new(
        &[pv("/dev/vdb", Some("vg1")), pv("/dev/vde", None)],
        &[
            vg("vg1", &["other", "r-drbd.pool.p1"]),
            vg("vg2", &[]),
            vg("vg3", &["r-drbd.pool.gone"]),
            vg("vg4", &["r-drbd.pool.b", "r-drbd.pool.a"]),
            vg("vg5", &[]),
            vg("data", &[]),
        ],
        NODE,
        &pools,
    );
    assert_eq!(l.owners["vg1"], Ok(("p1".into(), None)));
    // vg mode: the first by name, the others in the note.
    assert_eq!(
        l.owners["vg2"],
        Ok(("p2".into(), Some("VG vg2 also used by Pools p3".into())))
    );
    assert_eq!(
        l.owners["vg3"],
        Err("VG vg3 tagged for Pool gone, no such Pool on this node".into())
    );
    assert_eq!(l.owners["vg4"], Err("VG vg4 tagged for Pools a, b".into()));
    // A devices Pool never owns an untagged VG of its name.
    assert!(!l.owners.contains_key("vg5"));
    assert!(!l.owners.contains_key("data"));
    assert_eq!(
        l.pvs
            .get(std::path::Path::new("/dev/vde"))
            .map(String::as_str),
        Some("")
    );
    // A Pool of the same name on another node is not this VG's.
    let mut elsewhere = (*pool("p1", "vg1", true)).clone();
    elsewhere.spec.node = "n2".into();
    let l = Lvm::new(
        &[],
        &[vg("vg1", &["r-drbd.pool.p1"])],
        NODE,
        &[Arc::new(elsewhere)],
    );
    assert!(l.owners["vg1"].is_err());
}

fn scanner(x: &Arc<ScriptedExec>, d: &Arc<FakeDisks>, pools: Vec<Arc<Pool>>) -> Scanner {
    let disks: Arc<dyn HostDisks> = d.clone();
    Scanner::new(
        NODE.into(),
        x.clone(),
        disks,
        Arc::default(),
        Box::new(move || pools.clone()),
        DiskWatch::Uevent,
    )
}

fn summary(s: &NodeDisksStatus) -> Vec<String> {
    s.disks
        .iter()
        .map(|r| {
            format!(
                "{} {} {:?} {} {}",
                r.name,
                r.path,
                r.state,
                r.pool.as_deref().unwrap_or("-"),
                r.message.as_deref().unwrap_or("-")
            )
        })
        .collect()
}

fn lvm_reports(x: &ScriptedExec) {
    x.expect(
        lvm::cmd::pvs_all(),
        pvs_out(&[("/dev/vdb", "data"), ("/dev/vdd", "vg1"), ("/dev/vdg", "")]),
    )
    .expect(
        lvm::cmd::vgs_all(),
        vgs_out(&[("data", ""), ("vg1", "r-drbd.pool.p1")]),
    );
}

/// One disk per state.
fn host() -> Arc<FakeDisks> {
    let d = Arc::new(FakeDisks::default());
    let mut root = disk("vda");
    root.mounts = vec!["/sysroot".into()];
    root.sys.as_mut().unwrap().parts = vec![Part {
        name: "vda4".into(),
        dev: "252:4".into(),
        holders: vec![],
    }];
    let mut member = disk("vdd");
    member.sys.as_mut().unwrap().holders = vec!["dm-3".into()];
    let mut foreign = disk("vdb");
    foreign.sys.as_mut().unwrap().holders = vec!["dm-0".into()];
    let mut ro = disk("vdh");
    ro.sys.as_mut().unwrap().read_only = true;
    let mut held = disk("vdi");
    held.exclusive = Err("Device or resource busy (os error 16)".into());
    d.set("/dev/vda", root)
        .set("/dev/vdb", foreign)
        .set("/dev/vdd", member.clone())
        .set("/dev/disk/by-id/virtio-d", member)
        .set("/dev/disk/by-id/wwn-0x5d", disk("vdd"))
        .set("/dev/vde", disk("vde"))
        .set("/dev/disk/by-id/virtio-e", disk("vde"))
        .set("/dev/vdf", disk("vdf"))
        .set("/dev/vdg", disk("vdg"))
        .set("/dev/vdh", ro)
        .set("/dev/vdi", held);
    d
}

#[tokio::test]
async fn first_pass_reads_every_disk() {
    let d = host();
    let x = Arc::new(ScriptedExec::new());
    x.expect(wipefs_argv("/dev/vda"), ok(WIPEFS_BLANK))
        .expect(wipefs_argv("/dev/vdb"), ok(WIPEFS_PV))
        .expect(wipefs_argv("/dev/vdd"), ok(WIPEFS_PV))
        .expect(wipefs_argv("/dev/vde"), ok(WIPEFS_BLANK))
        .expect(wipefs_argv("/dev/vdf"), ok(WIPEFS_XFS))
        .expect(wipefs_argv("/dev/vdg"), ok(WIPEFS_PV))
        .expect(wipefs_argv("/dev/vdi"), ok(WIPEFS_BLANK));
    lvm_reports(&x);
    let mut s = scanner(&x, &d, vec![pool("p1", "vg1", true)]);
    let p = s.pass(Dirty::everything()).await;
    x.assert_done();
    assert_eq!(
        summary(&p.status),
        [
            "vda /dev/vda Busy - mounted on /sysroot",
            "vdb /dev/vdb NotEmpty - PV of VG data",
            "vdd /dev/disk/by-id/wwn-0x5d Member p1 -",
            "vde /dev/disk/by-id/virtio-e Blank - -",
            "vdf /dev/vdf NotEmpty - signatures: xfs at 0x0",
            "vdg /dev/vdg NotEmpty - PV without a VG",
            "vdh /dev/vdh NotADisk - vdh is read-only",
            "vdi /dev/vdi Busy - exclusive open failed: Device or resource busy (os error 16)",
        ]
    );
    // Only disks every read found blank were opened.
    assert_eq!(
        *d.opened.lock().unwrap(),
        [PathBuf::from("/dev/vde"), PathBuf::from("/dev/vdi")]
    );
    assert_eq!(p.status.disks[2].links, links(&["virtio-d", "wwn-0x5d"]));
    assert_eq!(p.status.disks[3].size_bytes, Some(4 << 30));
    assert_eq!(p.status.disks[3].serial.as_deref(), Some("serial-vde"));
    assert_eq!(p.status.blank, 1);
    assert_eq!(p.status.watch, Some(DiskWatch::Uevent));
    assert!(p.unchecked.is_empty());
}

#[tokio::test]
async fn later_passes_read_only_what_changed() {
    let d = host();
    let x = Arc::new(ScriptedExec::new());
    for (dev, out) in [
        ("vda", WIPEFS_BLANK),
        ("vdb", WIPEFS_PV),
        ("vdd", WIPEFS_PV),
        ("vde", WIPEFS_BLANK),
        ("vdf", WIPEFS_XFS),
        ("vdg", WIPEFS_PV),
        ("vdi", WIPEFS_BLANK),
    ] {
        x.expect(wipefs_argv(&format!("/dev/{dev}")), ok(out));
    }
    lvm_reports(&x);
    let mut s = scanner(&x, &d, vec![pool("p1", "vg1", true)]);
    let first = s.pass(Dirty::everything()).await;

    // Nothing dirty, nothing changed: no command, no open, same rows.
    d.opened.lock().unwrap().clear();
    let again = s.pass(Dirty::default()).await;
    x.assert_done();
    assert_eq!(again.status, first.status);
    assert!(d.opened.lock().unwrap().is_empty());

    // A uevent on vde, and vdf mounted: only those two are read.
    let mut mounted = disk("vdf");
    mounted.mounts = vec!["/mnt/x".into()];
    d.set("/dev/vdf", mounted);
    x.expect(wipefs_argv("/dev/vde"), ok(WIPEFS_XFS))
        .expect(wipefs_argv("/dev/vdf"), ok(WIPEFS_XFS));
    let mut dirty = Dirty::default();
    dirty.add(Wake::Disk("vde".into()));
    let next = s.pass(dirty).await;
    x.assert_done();
    assert_eq!(next.status.disks[3].state, DeviceState::NotEmpty);
    assert_eq!(
        next.status.disks[4].message.as_deref(),
        Some("mounted on /mnt/x")
    );
    assert!(d.opened.lock().unwrap().is_empty());

    // A Pool device change: LVM reports only.
    lvm_reports(&x);
    let mut dirty = Dirty::default();
    dirty.add(Wake::Lvm);
    s.pass(dirty).await;
    x.assert_done();
}

#[tokio::test]
async fn a_new_label_rereads_lvm() {
    let d = Arc::new(FakeDisks::default());
    d.set("/dev/vde", disk("vde"));
    let x = Arc::new(ScriptedExec::new());
    x.expect(wipefs_argv("/dev/vde"), ok(WIPEFS_BLANK))
        .expect(lvm::cmd::pvs_all(), pvs_out(&[]))
        .expect(lvm::cmd::vgs_all(), vgs_out(&[]));
    let mut s = scanner(&x, &d, vec![]);
    assert_eq!(s.pass(Dirty::everything()).await.status.blank, 1);
    // pvcreate by hand: its uevent, an LVM signature LVM did not report.
    x.expect(wipefs_argv("/dev/vde"), ok(WIPEFS_PV))
        .expect(lvm::cmd::pvs_all(), pvs_out(&[("/dev/vde", "")]))
        .expect(lvm::cmd::vgs_all(), vgs_out(&[]));
    let mut dirty = Dirty::default();
    dirty.add(Wake::Disk("vde".into()));
    let p = s.pass(dirty).await;
    x.assert_done();
    assert_eq!(
        summary(&p.status),
        ["vde /dev/vde NotEmpty - PV without a VG"]
    );
}

#[tokio::test]
async fn without_lvm_reports_a_pv_reads_by_its_signature_and_is_never_opened() {
    let d = Arc::new(FakeDisks::default());
    d.set("/dev/vdb", disk("vdb"));
    let x = Arc::new(ScriptedExec::new());
    x.expect(wipefs_argv("/dev/vdb"), ok(WIPEFS_PV))
        .expect(lvm::cmd::pvs_all(), code(5, "", "lvm not found"));
    let mut s = scanner(&x, &d, vec![]);
    let p = s.pass(Dirty::everything()).await;
    x.assert_done();
    assert_eq!(
        summary(&p.status),
        ["vdb /dev/vdb NotEmpty - signatures: LVM2_member at 0x218"]
    );
    assert!(d.opened.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unreadable_signatures_are_not_blank() {
    let d = Arc::new(FakeDisks::default());
    d.set("/dev/vde", disk("vde"));
    let x = Arc::new(ScriptedExec::new());
    x.expect(wipefs_argv("/dev/vde"), code(1, "", "wipefs: denied"))
        .expect(lvm::cmd::pvs_all(), pvs_out(&[]))
        .expect(lvm::cmd::vgs_all(), vgs_out(&[]));
    let p = scanner(&x, &d, vec![]).pass(Dirty::everything()).await;
    assert_eq!(p.status.disks[0].state, DeviceState::NotEmpty);
    assert!(d.opened.lock().unwrap().is_empty());
}

#[tokio::test]
async fn out_of_time_disks_are_named_not_listed() {
    let d = Arc::new(FakeDisks::default());
    let mut ro = disk("vdh");
    ro.sys.as_mut().unwrap().read_only = true;
    d.set("/dev/vde", disk("vde")).set("/dev/vdh", ro);
    let x = Arc::new(ScriptedExec::new());
    x.expect(lvm::cmd::pvs_all(), pvs_out(&[]))
        .expect(lvm::cmd::vgs_all(), vgs_out(&[]));
    let p = scanner(&x, &d, vec![])
        .with_max(Duration::ZERO)
        .pass(Dirty::everything())
        .await;
    x.assert_done();
    assert_eq!(p.unchecked, BTreeSet::from(["vde".to_owned()]));
    assert_eq!(
        summary(&p.status),
        ["vdh /dev/vdh NotADisk - vdh is read-only"]
    );
    assert_eq!(
        p.status.message.as_deref(),
        Some("scan stopped after 120 s, not checked yet: vde")
    );
}

fn row(name: &str, state: DeviceState) -> DiskStatus {
    DiskStatus {
        path: format!("/dev/{name}"),
        name: name.into(),
        links: vec![],
        state,
        pool: None,
        size_bytes: None,
        model: None,
        serial: None,
        message: None,
    }
}

fn st(rows: Vec<DiskStatus>) -> NodeDisksStatus {
    status(rows, &BTreeSet::new(), DiskWatch::Uevent)
}

#[test]
fn rows_are_capped() {
    let rows: Vec<DiskStatus> = (0..MAX_ROWS + 3)
        .map(|i| row(&format!("sd{i}"), DeviceState::Blank))
        .collect();
    let s = st(rows);
    assert_eq!(s.disks.len(), MAX_ROWS);
    assert_eq!(s.blank as usize, MAX_ROWS);
    assert_eq!(
        s.message.as_deref(),
        Some("515 disks, only the first 512 listed")
    );
}

#[test]
fn written_only_on_change() {
    let now = st(vec![
        row("vdd", DeviceState::Blank),
        row("vde", DeviceState::Blank),
    ]);
    assert_eq!(now.blank, 2);
    let mut o = object("n1", "uid-n1");
    assert!(changed(None, &now), "no object yet");
    assert!(changed(Some(&o), &now), "no status yet");
    o.status = Some(now.clone());
    assert!(!changed(Some(&o), &now));
    let mut later = now.clone();
    later.disks[1].state = DeviceState::Member;
    later.disks[1].pool = Some("p1".into());
    assert!(changed(Some(&o), &st(later.disks)));
    let mut unplugged = now.clone();
    unplugged.disks.pop();
    assert!(changed(Some(&o), &st(unplugged.disks)));
}

#[test]
fn owned_by_the_node() {
    let o = object("n1", "uid-n1");
    let r = &o.metadata.owner_references.as_ref().unwrap()[0];
    assert_eq!(
        (r.api_version.as_str(), r.kind.as_str(), r.name.as_str()),
        ("v1", "Node", "n1")
    );
    assert_eq!(r.uid, "uid-n1");
    assert_eq!(o.metadata.name.as_deref(), Some("n1"));
}

/// Counts passes and keeps what each was asked to read.
struct CountingSource(Arc<StdMutex<Vec<(Instant, Dirty)>>>);

#[async_trait::async_trait]
impl Source for CountingSource {
    async fn pass(&mut self, dirty: Dirty) -> Pass {
        self.0.lock().unwrap().push((Instant::now(), dirty));
        Pass {
            status: st(vec![]),
            unchecked: BTreeSet::new(),
        }
    }
}

/// Fails its first `fails` writes; keeps when each came.
struct FlakySink {
    fails: AtomicUsize,
    at: Arc<StdMutex<Vec<Instant>>>,
}

#[async_trait::async_trait]
impl Sink for FlakySink {
    async fn write(&self, _: &NodeDisksStatus) -> Result<bool, String> {
        self.at.lock().unwrap().push(Instant::now());
        if self.fails.load(Ordering::SeqCst) > 0 {
            self.fails.fetch_sub(1, Ordering::SeqCst);
            return Err("forbidden".into());
        }
        Ok(true)
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_write_is_retried_alone_with_backoff() {
    let passes = Arc::new(StdMutex::new(Vec::new()));
    let writes = Arc::new(StdMutex::new(Vec::new()));
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let t0 = Instant::now();
    let task = tokio::spawn(run_loop(
        CountingSource(passes.clone()),
        FlakySink {
            fails: AtomicUsize::new(3),
            at: writes.clone(),
        },
        rx,
        FULL_EVERY,
        stop_rx,
    ));
    tokio::time::sleep(Duration::from_secs(400)).await;
    let secs = |v: &[Instant]| -> Vec<u64> { v.iter().map(|t| (*t - t0).as_secs()).collect() };
    // 30 s doubling; no scan in between.
    assert_eq!(secs(&writes.lock().unwrap()), [0, 30, 90, 210]);
    assert_eq!(passes.lock().unwrap().len(), 1);
    assert!(passes.lock().unwrap()[0].1.all);

    // A uevent: one pass after the debounce, for that disk only.
    tx.send(Wake::Disk("vdd".into())).unwrap();
    tx.send(Wake::Refresh).unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    {
        let p = passes.lock().unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!((p[1].0 - t0).as_secs(), 402);
        assert_eq!(p[1].1.disks, BTreeSet::from(["vdd".to_owned()]));
        assert!(!p[1].1.all);
    }

    // The hourly full pass.
    tokio::time::sleep(FULL_EVERY).await;
    {
        let p = passes.lock().unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!((p[2].0 - t0).as_secs(), 3600);
        assert!(p[2].1.all);
    }
    stop_tx.send(true).unwrap();
    task.await.unwrap();
}
