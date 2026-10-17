//! Checks on the disks a Pool lists, before the agent writes to any of them,
//! and on every whole disk of the node for its NodeDisks.
//!
//! Each listed path gets one [`DeviceState`]. When several apply, the first
//! of Missing, NotADisk, Member, Busy, NotEmpty, Blank wins.
//! Facts come from the by-id link, sysfs, the host mount table and an
//! exclusive open from this process; signatures from the host's wipefs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use model::{DeviceState, DeviceStatus};

use crate::Exec;
use crate::exec::{DEFAULT_TIMEOUT, run_ok};

/// Kernel name prefixes of block devices that are not whole disks, with
/// what to call them. Multipath maps are dm devices: out of scope.
const NOT_DISKS: [(&str, &str); 12] = [
    ("loop", "a loop device"),
    ("ram", "a RAM disk"),
    ("zram", "a zram device"),
    ("dm-", "a device-mapper device"),
    ("md", "an md RAID device"),
    ("drbd", "a DRBD device"),
    ("sr", "an optical drive"),
    ("nbd", "a network block device"),
    ("rbd", "a Ceph RBD device"),
    ("zd", "a ZFS volume"),
    ("nullb", "a null block device"),
    ("fd", "a floppy drive"),
];

/// A partition of the disk, from sysfs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Part {
    pub name: String,
    /// `major:minor`.
    pub dev: String,
    pub holders: Vec<String>,
}

/// What sysfs says about one block device.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sys {
    /// `major:minor`.
    pub dev: String,
    pub partition: bool,
    pub read_only: bool,
    pub size_bytes: u64,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub holders: Vec<String>,
    pub parts: Vec<Part>,
}

/// Everything about one listed path but its signatures and LVM membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    /// The resolved device node; `Err` with why when the path is Missing.
    pub dev: Result<PathBuf, String>,
    /// `Err` when the node is not a block device or sysfs is unreadable.
    pub sys: Result<Sys, String>,
    /// Mount points of the disk and its partitions.
    pub mounts: Vec<String>,
    /// `Err` with the error when the exclusive open failed; tried only on
    /// a disk that may be claimed, see [`check`].
    pub exclusive: Result<(), String>,
    /// udev marks it a path of a multipath map, claimed or about to be.
    pub multipath: bool,
}

/// The host's disks as the agent sees them; a trait so tests need no disks.
pub trait HostDisks: Send + Sync + std::fmt::Debug {
    /// Everything but the exclusive open, left `Ok`.
    fn facts(&self, path: &str) -> Facts;
    fn exclusive(&self, node: &Path) -> Result<(), String>;
    /// Kernel names of the whole disks, sorted: no partition, none of [`NOT_DISKS`].
    fn whole_disks(&self) -> Vec<String>;
    /// The by-id links, sorted, by the kernel name they resolve to.
    fn by_id(&self) -> BTreeMap<String, Vec<String>>;
    /// Whether the host's udev database can be read, which multipath
    /// path detection needs.
    fn udev_readable(&self) -> bool;
}

/// What a kernel name says the device is when it is not a whole disk.
fn not_a_disk(name: &str) -> Option<&'static str> {
    // mmcblk0boot0 is no partition in sysfs, yet a boot area of mmcblk0.
    if name.starts_with("mmcblk") && name.contains("boot") {
        return Some("an eMMC boot partition");
    }
    NOT_DISKS
        .iter()
        .find(|(p, _)| name.starts_with(p))
        .map(|(_, what)| *what)
}

/// Not a kernel name of a whole disk, whatever sysfs says.
pub fn not_a_disk_name(name: &str) -> bool {
    not_a_disk(name).is_some()
}

/// The exclusive open on a blocking thread: an open can wait on the device.
pub async fn open_exclusive_blocking(
    disks: &std::sync::Arc<dyn HostDisks>,
    node: &Path,
) -> Result<(), String> {
    let (d, n) = (disks.clone(), node.to_owned());
    tokio::task::spawn_blocking(move || d.exclusive(&n))
        .await
        .unwrap_or_else(|e| Err(e.to_string()))
}

/// `E:DM_MULTIPATH_DEVICE_PATH=1` in a udev database entry.
pub fn udev_multipath(entry: &str) -> bool {
    entry
        .lines()
        .any(|l| l.trim() == "E:DM_MULTIPATH_DEVICE_PATH=1")
}

/// The real host: /dev and /sys are the host's in the agent pod, and with
/// hostPID init's mount table is the host's.
#[derive(Debug, Clone)]
pub struct SysDisks {
    pub sys: PathBuf,
    pub mountinfo: PathBuf,
    pub by_id: PathBuf,
    /// The host's udev database, through init's root: not mounted in the pod.
    pub udev: PathBuf,
}

impl Default for SysDisks {
    fn default() -> Self {
        Self {
            sys: "/sys".into(),
            mountinfo: "/proc/1/mountinfo".into(),
            by_id: "/dev/disk/by-id".into(),
            udev: "/proc/1/root/run/udev/data".into(),
        }
    }
}

impl HostDisks for SysDisks {
    fn facts(&self, path: &str) -> Facts {
        let dev = std::fs::canonicalize(path).map_err(|e| e.to_string());
        let Ok(node) = &dev else {
            return Facts {
                dev,
                sys: Err(String::new()),
                mounts: Vec::new(),
                exclusive: Ok(()),
                multipath: false,
            };
        };
        let sys = block_name(node).and_then(|name| read_sys(&self.sys, &name));
        let mounts = match (&sys, std::fs::read_to_string(&self.mountinfo)) {
            (Ok(s), Ok(table)) => mounts_of(&table, s),
            // Without the table a mount still fails the exclusive open.
            _ => Vec::new(),
        };
        let multipath = sys.as_ref().is_ok_and(|s| {
            std::fs::read_to_string(self.udev.join(format!("b{}", s.dev)))
                .is_ok_and(|e| udev_multipath(&e))
        });
        Facts {
            dev,
            sys,
            mounts,
            exclusive: Ok(()),
            multipath,
        }
    }

    fn exclusive(&self, node: &Path) -> Result<(), String> {
        open_exclusive(node)
    }

    /// Hidden devices too are left out: NVMe multipath paths under their
    /// shared namespace disk.
    fn whole_disks(&self) -> Vec<String> {
        let base = self.sys.join("class/block");
        dir_names(&base)
            .into_iter()
            .filter(|n| {
                !not_a_disk_name(n)
                    && !base.join(n).join("partition").exists()
                    && read_trim(&base.join(n).join("hidden")).as_deref() != Some("1")
            })
            .collect()
    }

    fn udev_readable(&self) -> bool {
        std::fs::read_dir(&self.udev).is_ok()
    }

    fn by_id(&self) -> BTreeMap<String, Vec<String>> {
        let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for link in dir_names(&self.by_id) {
            let path = self.by_id.join(&link);
            let Some(name) = std::fs::canonicalize(&path)
                .ok()
                .and_then(|t| t.file_name().and_then(|n| n.to_str()).map(str::to_owned))
            else {
                continue;
            };
            out.entry(name)
                .or_default()
                .push(path.display().to_string());
        }
        out
    }
}

/// A PV name as the agent's /dev resolves it, so it compares with a
/// resolved by-id link.
pub fn resolved(name: &str) -> PathBuf {
    std::fs::canonicalize(name).unwrap_or_else(|_| PathBuf::from(name))
}

/// The kernel name of a block device node.
fn block_name(node: &Path) -> Result<String, String> {
    use std::os::unix::fs::FileTypeExt;
    let meta = std::fs::metadata(node).map_err(|e| e.to_string())?;
    if !meta.file_type().is_block_device() {
        return Err(format!("{} is not a block device", node.display()));
    }
    node.file_name()
        .and_then(|n| n.to_str())
        .map(str::to_owned)
        .ok_or_else(|| format!("{} has no name", node.display()))
}

/// The kernel refuses O_EXCL on a block device while anything holds it: a
/// mount, a dm or md map, an active LV, another exclusive opener.
fn open_exclusive(node: &Path) -> Result<(), String> {
    use rustix::fs::{Mode, OFlags, open};
    open(
        node,
        OFlags::RDONLY | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map(drop)
    .map_err(|e| e.to_string())
}

fn read_trim(p: &Path) -> Option<String> {
    std::fs::read_to_string(p)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn dir_names(p: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(p)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.file_name().to_str().map(str::to_owned))
        .collect();
    v.sort();
    v
}

/// sysfs facts of block device `name` under `sys` (normally /sys).
pub fn read_sys(sys: &Path, name: &str) -> Result<Sys, String> {
    let base = sys.join("class/block").join(name);
    let dev = read_trim(&base.join("dev"))
        .ok_or_else(|| format!("{} unreadable", base.join("dev").display()))?;
    let sectors: u64 = read_trim(&base.join("size"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Disks name their serial in different places: virtio-blk and NVMe
    // next to the disk, SCSI and ATA under device/; a wwid when no serial.
    let serial = ["serial", "device/serial", "wwid", "device/wwid"]
        .iter()
        .find_map(|f| read_trim(&base.join(f)));
    let parts = dir_names(&base)
        .into_iter()
        .filter(|n| n.starts_with(name) && base.join(n).join("partition").exists())
        .map(|n| Part {
            dev: read_trim(&base.join(&n).join("dev")).unwrap_or_default(),
            holders: dir_names(&base.join(&n).join("holders")),
            name: n,
        })
        .collect();
    Ok(Sys {
        dev,
        partition: base.join("partition").exists(),
        read_only: read_trim(&base.join("ro")).as_deref() == Some("1"),
        size_bytes: sectors.saturating_mul(512),
        model: read_trim(&base.join("device/model")),
        serial,
        holders: dir_names(&base.join("holders")),
        parts,
    })
}

/// Mount points in a mountinfo table of the disk or any of its partitions.
pub fn mounts_of(table: &str, sys: &Sys) -> Vec<String> {
    let devs: Vec<&str> = std::iter::once(sys.dev.as_str())
        .chain(sys.parts.iter().map(|p| p.dev.as_str()))
        .collect();
    let mut out: Vec<String> = table
        .lines()
        .filter_map(|l| {
            let mut f = l.split(' ');
            let dev = f.nth(2)?;
            let point = f.nth(1)?;
            devs.contains(&dev).then(|| unescape(point))
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// mountinfo escapes space, tab, newline and backslash as `\ooo`.
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\'
            && let Some(oct) = b.get(i + 1..i + 4)
            && let Ok(v) = u8::from_str_radix(std::str::from_utf8(oct).unwrap_or("x"), 8)
        {
            out.push(v);
            i += 4;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One signature wipefs found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub kind: String,
    pub offset: String,
}

/// `wipefs --no-act --json <dev>`: reads only, whatever wipefs version.
pub fn wipefs_argv(dev: &str) -> Vec<String> {
    ["wipefs", "--no-act", "--json", dev]
        .map(str::to_owned)
        .to_vec()
}

/// The signatures in wipefs JSON output; empty output also means none
/// (older util-linux prints nothing for a blank device).
pub fn parse_wipefs(stdout: &str) -> Result<Vec<Signature>, String> {
    #[derive(serde::Deserialize)]
    struct Doc {
        signatures: Vec<Raw>,
    }
    #[derive(serde::Deserialize)]
    struct Raw {
        #[serde(rename = "type")]
        kind: Option<String>,
        offset: Option<String>,
    }
    if stdout.trim().is_empty() {
        return Ok(Vec::new());
    }
    let d: Doc = serde_json::from_str(stdout).map_err(|e| format!("wipefs output: {e}"))?;
    Ok(d.signatures
        .into_iter()
        .map(|r| Signature {
            kind: r.kind.unwrap_or_else(|| "unknown".into()),
            offset: r.offset.unwrap_or_default(),
        })
        .collect())
}

/// The state settled before signatures are read; `None` means wipefs decides.
pub fn classify_early(
    f: &Facts,
    pv_vg: Option<&str>,
    vg: &str,
) -> Option<(DeviceState, Option<String>)> {
    let node = match &f.dev {
        Ok(n) => n,
        Err(e) => return Some((DeviceState::Missing, Some(e.clone()))),
    };
    let sys = match &f.sys {
        Ok(s) => s,
        Err(e) => return Some((DeviceState::NotADisk, Some(e.clone()))),
    };
    let name = node.file_name().and_then(|n| n.to_str()).unwrap_or("");
    if sys.partition {
        return Some((
            DeviceState::NotADisk,
            Some(format!("{name} is a partition")),
        ));
    }
    if let Some(what) = not_a_disk(name) {
        return Some((DeviceState::NotADisk, Some(format!("{name} is {what}"))));
    }
    if sys.read_only {
        return Some((DeviceState::NotADisk, Some(format!("{name} is read-only"))));
    }
    if sys.size_bytes == 0 {
        return Some((DeviceState::NotADisk, Some(format!("{name} has no medium"))));
    }
    if pv_vg == Some(vg) {
        return Some((DeviceState::Member, None));
    }
    let mut busy = Vec::new();
    if !f.mounts.is_empty() {
        busy.push(format!("mounted on {}", f.mounts.join(", ")));
    }
    let holders: Vec<&str> = sys
        .holders
        .iter()
        .chain(sys.parts.iter().flat_map(|p| &p.holders))
        .map(String::as_str)
        .collect();
    if !holders.is_empty() {
        busy.push(format!("held by {}", holders.join(", ")));
    }
    if f.multipath {
        busy.push("a multipath path".into());
    }
    if let Err(e) = &f.exclusive {
        busy.push(format!("exclusive open failed: {e}"));
    }
    if !busy.is_empty() {
        return Some((DeviceState::Busy, Some(busy.join("; "))));
    }
    None
}

/// NotEmpty or Blank, from the signatures wipefs read.
pub fn classify_signatures(
    sys: &Sys,
    pv_vg: Option<&str>,
    signatures: &Result<Vec<Signature>, String>,
) -> (DeviceState, Option<String>) {
    let mut found = Vec::new();
    match signatures {
        Ok(s) if !s.is_empty() => {
            let list: Vec<String> = s
                .iter()
                .map(|g| format!("{} at {}", g.kind, g.offset))
                .collect();
            found.push(format!("signatures: {}", list.join(", ")));
        }
        Ok(_) => {}
        // Not knowing is not blank.
        Err(e) => found.push(format!("signatures unknown: {e}")),
    }
    if !sys.parts.is_empty() {
        let names: Vec<&str> = sys.parts.iter().map(|p| p.name.as_str()).collect();
        found.push(format!("partitions: {}", names.join(", ")));
    }
    match pv_vg {
        Some("") | None => {}
        Some(other) => found.push(format!("PV of VG {other}")),
    }
    if found.is_empty() {
        (DeviceState::Blank, None)
    } else {
        (DeviceState::NotEmpty, Some(found.join("; ")))
    }
}

/// One checked path: its status row and the node the commands use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    pub row: DeviceStatus,
    pub node: Option<PathBuf>,
}

/// Checks every path. `pvs` maps a resolved device node to its VG name
/// (empty for an orphan PV).
pub async fn check(
    exec: &dyn Exec,
    disks: &std::sync::Arc<dyn HostDisks>,
    paths: &[String],
    pvs: &BTreeMap<PathBuf, String>,
    vg: &str,
) -> Vec<Checked> {
    let mut out: Vec<Checked> = Vec::with_capacity(paths.len());
    for path in paths {
        let mut f = disks.facts(path);
        // Two by-id names of one disk (wwn- and nvme-): only the first counts.
        let first = f.dev.as_ref().ok().and_then(|n| {
            out.iter()
                .find(|c| c.node.as_ref() == Some(n))
                .map(|c| c.row.path.clone())
        });
        if let Some(first) = first {
            f.sys = Err(format!("the same disk as {first}"));
        }
        let pv_vg = f
            .dev
            .as_ref()
            .ok()
            .and_then(|n| pvs.get(n))
            .map(String::as_str);
        // Never on a member: while its VG is inactive, our brief exclusive
        // hold could make an activation's claim on the disk fail.
        let settled = matches!(
            classify_early(&f, pv_vg, vg),
            Some((
                DeviceState::Missing | DeviceState::NotADisk | DeviceState::Member,
                _
            ))
        );
        if !settled && let Ok(node) = &f.dev {
            f.exclusive = open_exclusive_blocking(disks, node).await;
        }
        let early = classify_early(&f, pv_vg, vg);
        let (state, message) = if let Some(s) = early {
            s
        } else {
            let node = f
                .dev
                .as_ref()
                .map(|n| n.display().to_string())
                .unwrap_or_default();
            let sigs = run_ok(exec, &wipefs_argv(&node), DEFAULT_TIMEOUT)
                .await
                .map_err(|e| e.to_string())
                .and_then(|o| parse_wipefs(&o.stdout));
            let sys = f.sys.as_ref().cloned().unwrap_or_default();
            classify_signatures(&sys, pv_vg, &sigs)
        };
        let sys = f.sys.as_ref().ok();
        out.push(Checked {
            row: DeviceStatus {
                path: path.clone(),
                state,
                size_bytes: sys.map(|s| s.size_bytes).filter(|b| *b > 0),
                model: sys.and_then(|s| s.model.clone()),
                serial: sys.and_then(|s| s.serial.clone()),
                message,
            },
            node: f.dev.ok(),
        });
    }
    out
}

#[cfg(test)]
pub(crate) mod fake {
    //! Scripted host disks.

    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    use super::{Facts, HostDisks, Sys};

    /// `wipefs --no-act --json` from util-linux 2.40.2 on a blank disk
    /// and on one after mkfs.xfs.
    pub(crate) const WIPEFS_BLANK: &str = "{\n   \"signatures\": [\n\n   ]\n}\n";
    pub(crate) const WIPEFS_XFS: &str = r#"{
   "signatures": [
      {
         "device": "virtio-disk1",
         "offset": "0x0",
         "type": "xfs",
         "uuid": "d4dcb573-7fc4-4551-8b64-317016445265",
         "label": null
      }
   ]
}
"#;

    #[derive(Debug, Default)]
    pub(crate) struct FakeDisks {
        pub(crate) facts: Mutex<HashMap<String, Facts>>,
        /// Nodes the exclusive open was tried on, in order.
        pub(crate) opened: Mutex<Vec<std::path::PathBuf>>,
    }

    /// A blank whole disk `name` of 4 GiB behind `path`.
    pub(crate) fn disk(name: &str) -> Facts {
        Facts {
            dev: Ok(format!("/dev/{name}").into()),
            sys: Ok(Sys {
                dev: "252:48".into(),
                size_bytes: 4 << 30,
                serial: Some(format!("serial-{name}")),
                ..Sys::default()
            }),
            mounts: Vec::new(),
            exclusive: Ok(()),
            multipath: false,
        }
    }

    impl FakeDisks {
        pub(crate) fn set(&self, path: &str, f: Facts) -> &Self {
            self.facts.lock().unwrap().insert(path.to_owned(), f);
            self
        }
    }

    impl HostDisks for FakeDisks {
        fn facts(&self, path: &str) -> Facts {
            self.facts
                .lock()
                .unwrap()
                .get(path)
                .cloned()
                .unwrap_or(Facts {
                    dev: Err("No such file or directory (os error 2)".into()),
                    sys: Err(String::new()),
                    mounts: Vec::new(),
                    exclusive: Ok(()),
                    multipath: false,
                })
        }

        /// The `exclusive` of the scripted facts of that node.
        fn exclusive(&self, node: &std::path::Path) -> Result<(), String> {
            self.opened.lock().unwrap().push(node.to_owned());
            self.facts
                .lock()
                .unwrap()
                .values()
                .find(|f| f.dev.as_deref().ok() == Some(node))
                .map_or(Ok(()), |f| f.exclusive.clone())
        }

        fn udev_readable(&self) -> bool {
            true
        }

        /// The scripted paths directly under /dev.
        fn whole_disks(&self) -> Vec<String> {
            let mut v: Vec<String> = self
                .facts
                .lock()
                .unwrap()
                .keys()
                .filter_map(|p| p.strip_prefix("/dev/"))
                .filter(|n| !n.contains('/'))
                .map(str::to_owned)
                .collect();
            v.sort();
            v
        }

        /// The scripted by-id paths, by the node their facts resolve to.
        fn by_id(&self) -> BTreeMap<String, Vec<String>> {
            let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for (path, f) in self.facts.lock().unwrap().iter() {
                if !path.starts_with("/dev/disk/by-id/") {
                    continue;
                }
                if let Some(name) = f
                    .dev
                    .as_ref()
                    .ok()
                    .and_then(|n| n.file_name())
                    .and_then(|n| n.to_str())
                {
                    out.entry(name.to_owned()).or_default().push(path.clone());
                }
            }
            for v in out.values_mut() {
                v.sort();
            }
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::{FakeDisks, WIPEFS_BLANK, WIPEFS_XFS, disk};
    use super::*;
    use crate::exec::scripted::{ScriptedExec, code, ok};

    /// `wipefs --no-act --json` from util-linux 2.40.2 on a disk with a GPT.
    const WIPEFS_GPT: &str = r#"{
   "signatures": [
      {
         "device": "virtio-disk1",
         "offset": "0x200",
         "type": "gpt",
         "uuid": null,
         "label": null
      },{
         "device": "virtio-disk1",
         "offset": "0xfffffe00",
         "type": "gpt",
         "uuid": null,
         "label": null
      },{
         "device": "virtio-disk1",
         "offset": "0x1fe",
         "type": "PMBR",
         "uuid": null,
         "label": null
      }
   ]
}
"#;

    #[test]
    fn wipefs_shapes() {
        assert_eq!(parse_wipefs(WIPEFS_BLANK).unwrap(), []);
        assert_eq!(parse_wipefs("").unwrap(), []);
        assert_eq!(
            parse_wipefs(WIPEFS_XFS).unwrap(),
            [Signature {
                kind: "xfs".into(),
                offset: "0x0".into()
            }]
        );
        let gpt = parse_wipefs(WIPEFS_GPT).unwrap();
        assert_eq!(gpt.len(), 3);
        assert_eq!(gpt[2].kind, "PMBR");
        assert!(parse_wipefs("not json").is_err());
    }

    #[test]
    fn sysfs_tree() {
        let t = std::env::temp_dir().join(format!("r-drbd-sysfs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&t);
        let d = t.join("class/block/vdd");
        for (p, v) in [
            ("dev", "252:48\n"),
            ("size", "8388608\n"),
            ("ro", "0\n"),
            ("serial", "disk1\n"),
            ("vdd1/partition", "1\n"),
            ("vdd1/dev", "252:49\n"),
            ("vdd1/holders/dm-3", ""),
            ("queue/x", ""),
        ] {
            std::fs::create_dir_all(d.join(p).parent().unwrap()).unwrap();
            std::fs::write(d.join(p), v).unwrap();
        }
        std::fs::create_dir_all(d.join("holders")).unwrap();
        let s = read_sys(&t, "vdd").unwrap();
        assert_eq!(
            s,
            Sys {
                dev: "252:48".into(),
                partition: false,
                read_only: false,
                size_bytes: 4 << 30,
                model: None,
                serial: Some("disk1".into()),
                holders: vec![],
                parts: vec![Part {
                    name: "vdd1".into(),
                    dev: "252:49".into(),
                    holders: vec!["dm-3".into()],
                }],
            }
        );
        // An NVMe disk keeps model and serial under device/.
        let n = t.join("class/block/nvme0n1");
        for (p, v) in [
            ("dev", "259:0\n"),
            ("size", "100\n"),
            ("ro", "1\n"),
            ("device/model", "Some SSD 2TB              \n"),
            ("device/serial", "  S7KH123  \n"),
        ] {
            std::fs::create_dir_all(n.join(p).parent().unwrap()).unwrap();
            std::fs::write(n.join(p), v).unwrap();
        }
        let s = read_sys(&t, "nvme0n1").unwrap();
        assert_eq!(s.model.as_deref(), Some("Some SSD 2TB"));
        assert_eq!(s.serial.as_deref(), Some("S7KH123"));
        assert!(s.read_only);
        assert!(read_sys(&t, "sdz").is_err());
        let _ = std::fs::remove_dir_all(&t);
    }

    #[test]
    fn whole_disks_and_their_links() {
        let t = std::env::temp_dir().join(format!("r-drbd-disks-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&t);
        let block = t.join("sys/class/block");
        for n in [
            "vda",
            "nvme0n1",
            "loop0",
            "dm-0",
            "sr0",
            "zram0",
            "fd0",
            "nvme1c1n1",
        ] {
            std::fs::create_dir_all(block.join(n)).unwrap();
        }
        std::fs::write(block.join("nvme1c1n1/hidden"), "1\n").unwrap();
        std::fs::write(block.join("nvme0n1/hidden"), "0\n").unwrap();
        // In real sysfs a partition is also listed at the top level.
        std::fs::create_dir_all(block.join("vda1")).unwrap();
        std::fs::write(block.join("vda1/partition"), "1\n").unwrap();
        let dev = t.join("dev");
        let by_id = dev.join("disk/by-id");
        std::fs::create_dir_all(&by_id).unwrap();
        for n in ["vda", "vda1", "nvme0n1"] {
            std::fs::write(dev.join(n), "").unwrap();
        }
        for (link, target) in [
            ("virtio-a", "../../vda"),
            ("virtio-a-part1", "../../vda1"),
            ("wwn-0x1", "../../nvme0n1"),
            ("nvme-SSD_1", "../../nvme0n1"),
            ("dangling", "../../nope"),
        ] {
            std::os::unix::fs::symlink(target, by_id.join(link)).unwrap();
        }
        let d = SysDisks {
            sys: t.join("sys"),
            mountinfo: t.join("mountinfo"),
            by_id: by_id.clone(),
            udev: t.join("udev"),
        };
        assert_eq!(d.whole_disks(), ["nvme0n1", "vda"]);
        let l = |n: &str| by_id.join(n).display().to_string();
        assert_eq!(
            d.by_id(),
            BTreeMap::from([
                ("nvme0n1".to_owned(), vec![l("nvme-SSD_1"), l("wwn-0x1")]),
                ("vda".to_owned(), vec![l("virtio-a")]),
                ("vda1".to_owned(), vec![l("virtio-a-part1")]),
            ])
        );
        let _ = std::fs::remove_dir_all(&t);
    }

    #[test]
    fn mount_table() {
        let table = "22 1 252:4 / /sysroot rw - xfs /dev/vda4 rw\n\
                     90 22 252:49 / /mnt/a\\040b rw - xfs /dev/vdd1 rw\n\
                     91 22 252:48 / /data rw - xfs /dev/vdd rw\n";
        let sys = Sys {
            dev: "252:48".into(),
            parts: vec![Part {
                name: "vdd1".into(),
                dev: "252:49".into(),
                holders: vec![],
            }],
            ..Sys::default()
        };
        assert_eq!(mounts_of(table, &sys), ["/data", "/mnt/a b"]);
    }

    fn early(f: &Facts, pv: Option<&str>) -> Option<(DeviceState, Option<String>)> {
        classify_early(f, pv, "vg1")
    }

    #[test]
    fn state_table() {
        // Missing first, even for a path that would be a member.
        let mut f = disk("vdd");
        f.dev = Err("No such file".into());
        assert_eq!(early(&f, Some("vg1")).unwrap().0, DeviceState::Missing);

        // NotADisk: partitions, names, read-only, not a block device.
        let mut f = disk("vdd1");
        f.sys.as_mut().unwrap().partition = true;
        assert_eq!(
            early(&f, None),
            Some((DeviceState::NotADisk, Some("vdd1 is a partition".into())))
        );
        for (name, what) in [
            ("dm-3", "a device-mapper device"),
            ("md127", "an md RAID device"),
            ("loop0", "a loop device"),
            ("drbd50000", "a DRBD device"),
            ("zram0", "a zram device"),
            ("sr0", "an optical drive"),
            ("rbd0", "a Ceph RBD device"),
            ("zd16", "a ZFS volume"),
            ("nullb0", "a null block device"),
        ] {
            let f = disk(name);
            assert_eq!(
                early(&f, Some("vg1")),
                Some((DeviceState::NotADisk, Some(format!("{name} is {what}"))))
            );
        }
        let mut f = disk("vdd");
        f.sys.as_mut().unwrap().read_only = true;
        assert_eq!(early(&f, None).unwrap().0, DeviceState::NotADisk);
        let mut f = disk("vdd");
        f.sys = Err("/dev/vdd is not a block device".into());
        assert_eq!(early(&f, None).unwrap().0, DeviceState::NotADisk);

        // Member before Busy: our own active LVs hold the disk.
        let mut f = disk("vdd");
        f.exclusive = Err("Device or resource busy (os error 16)".into());
        assert_eq!(early(&f, Some("vg1")), Some((DeviceState::Member, None)));

        // Busy: names what holds it.
        let mut f = disk("vdd");
        f.mounts = vec!["/mnt/x".into()];
        f.sys.as_mut().unwrap().parts = vec![Part {
            name: "vdd1".into(),
            dev: "252:49".into(),
            holders: vec!["dm-3".into()],
        }];
        f.exclusive = Err("Device or resource busy (os error 16)".into());
        assert_eq!(
            early(&f, Some("other")),
            Some((
                DeviceState::Busy,
                Some(
                    "mounted on /mnt/x; held by dm-3; exclusive open failed: Device or resource busy (os error 16)"
                        .into()
                )
            ))
        );

        // No medium, an eMMC boot area, a floppy.
        let mut f = disk("sdz");
        f.sys.as_mut().unwrap().size_bytes = 0;
        assert_eq!(
            early(&f, None),
            Some((DeviceState::NotADisk, Some("sdz has no medium".into())))
        );
        for (name, what) in [
            ("mmcblk0boot0", "an eMMC boot partition"),
            ("fd0", "a floppy drive"),
        ] {
            assert_eq!(
                early(&disk(name), None),
                Some((DeviceState::NotADisk, Some(format!("{name} is {what}"))))
            );
        }
        assert!(!not_a_disk_name("mmcblk0"));

        // A multipath path is Busy before any open.
        let mut f = disk("sdc");
        f.multipath = true;
        assert_eq!(
            early(&f, None),
            Some((DeviceState::Busy, Some("a multipath path".into())))
        );
        assert!(udev_multipath(
            "S:disk/by-id/x\nE:DM_MULTIPATH_DEVICE_PATH=1\nE:ID_SERIAL=x\n"
        ));
        assert!(!udev_multipath("E:DM_MULTIPATH_DEVICE_PATH=0\n"));

        // Anything else needs the signatures.
        assert_eq!(early(&disk("vdd"), None), None);
        assert_eq!(early(&disk("nvme0n1"), Some("")), None);
    }

    #[test]
    fn signatures_decide_blank() {
        let sys = Sys::default();
        assert_eq!(
            classify_signatures(&sys, None, &Ok(vec![])),
            (DeviceState::Blank, None)
        );
        let xfs = parse_wipefs(WIPEFS_XFS);
        assert_eq!(
            classify_signatures(&sys, None, &xfs),
            (DeviceState::NotEmpty, Some("signatures: xfs at 0x0".into()))
        );
        let lvm = Ok(vec![Signature {
            kind: "LVM2_member".into(),
            offset: "0x218".into(),
        }]);
        assert_eq!(
            classify_signatures(&sys, Some("other"), &lvm).1.as_deref(),
            Some("signatures: LVM2_member at 0x218; PV of VG other")
        );
        let with_parts = Sys {
            parts: vec![Part {
                name: "vdd1".into(),
                ..Part::default()
            }],
            ..Sys::default()
        };
        let (s, m) = classify_signatures(&with_parts, None, &parse_wipefs(WIPEFS_GPT));
        assert_eq!(s, DeviceState::NotEmpty);
        assert_eq!(
            m.as_deref(),
            Some("signatures: gpt at 0x200, gpt at 0xfffffe00, PMBR at 0x1fe; partitions: vdd1")
        );
        let (s, m) = classify_signatures(&sys, None, &Err("wipefs: exit 1: denied".into()));
        assert_eq!(s, DeviceState::NotEmpty);
        assert_eq!(
            m.as_deref(),
            Some("signatures unknown: wipefs: exit 1: denied")
        );
    }

    #[tokio::test]
    async fn an_alias_of_a_listed_disk_is_not_a_second_disk() {
        let disks = FakeDisks::default();
        disks
            .set("/dev/disk/by-id/nvme-x", disk("nvme0n1"))
            .set("/dev/disk/by-id/wwn-x", disk("nvme0n1"));
        let x = ScriptedExec::new();
        x.expect(wipefs_argv("/dev/nvme0n1"), ok(WIPEFS_BLANK));
        let paths = ["/dev/disk/by-id/nvme-x", "/dev/disk/by-id/wwn-x"].map(str::to_owned);
        let dyn_disks: std::sync::Arc<dyn HostDisks> = std::sync::Arc::new(disks);
        let rows = check(&x, &dyn_disks, &paths, &BTreeMap::new(), "vg1").await;
        x.assert_done();
        assert_eq!(rows[0].row.state, DeviceState::Blank);
        assert_eq!(rows[1].row.state, DeviceState::NotADisk);
        assert_eq!(
            rows[1].row.message.as_deref(),
            Some("the same disk as /dev/disk/by-id/nvme-x")
        );
    }

    #[tokio::test]
    async fn check_runs_wipefs_only_where_it_decides() {
        let disks = std::sync::Arc::new(FakeDisks::default());
        disks
            .set("/dev/disk/by-id/a", disk("vdd"))
            .set("/dev/disk/by-id/b", disk("vde"))
            .set("/dev/disk/by-id/c", disk("vdf"));
        let x = ScriptedExec::new();
        x.expect(wipefs_argv("/dev/vde"), ok(WIPEFS_BLANK))
            .expect(wipefs_argv("/dev/vdf"), code(1, "", "wipefs: error"));
        let pvs = BTreeMap::from([(PathBuf::from("/dev/vdd"), "vg1".to_owned())]);
        let paths = ["a", "b", "c", "gone"].map(|p| format!("/dev/disk/by-id/{p}"));
        let dyn_disks: std::sync::Arc<dyn HostDisks> = disks.clone();
        let rows = check(&x, &dyn_disks, &paths, &pvs, "vg1").await;
        x.assert_done();
        // Never on the member, nor on a path that does not resolve.
        assert_eq!(
            *disks.opened.lock().unwrap(),
            [PathBuf::from("/dev/vde"), PathBuf::from("/dev/vdf")]
        );
        let states: Vec<_> = rows.iter().map(|r| r.row.state).collect();
        assert_eq!(
            states,
            [
                DeviceState::Member,
                DeviceState::Blank,
                DeviceState::NotEmpty,
                DeviceState::Missing
            ]
        );
        assert_eq!(rows[1].row.size_bytes, Some(4 << 30));
        assert_eq!(rows[1].row.serial.as_deref(), Some("serial-vde"));
        assert_eq!(rows[1].node.as_deref(), Some(Path::new("/dev/vde")));
        assert_eq!(rows[3].row.size_bytes, None);
        assert_eq!(rows[3].node, None);
    }
}
