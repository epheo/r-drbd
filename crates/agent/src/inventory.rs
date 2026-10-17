//! The node's NodeDisks: every whole disk with its state, so the admin
//! picks a Pool's disks from kubectl.
//!
//! Same facts as the Pool checks, with one difference: the exclusive open
//! runs last, only on a disk every read already finds blank. A disk in use
//! is never opened, and one with a signature never needs to be.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Patch, PatchParams, PostParams};
use kube::{Api, Client};
use model::{DeviceState, DiskStatus, NodeDisks, NodeDisksSpec, NodeDisksStatus};

use crate::Exec;
use crate::devices::{self, HostDisks};
use crate::exec::{DEFAULT_TIMEOUT, run_ok};

pub const EVERY: Duration = Duration::from_secs(300);

/// by-id names that do not name the disk itself: a PV, a dm or md map
/// on it, or an NVMe id with no model or serial in it.
const NOT_BUS: [&str; 6] = [
    "lvm-pv-uuid-",
    "dm-",
    "md-",
    "nvme-eui.",
    "nvme-nvme.",
    "nvme-uuid.",
];

/// The path a Pool should list: `wwn-`, else a bus id (`ata-`, `nvme-`,
/// `scsi-`, `virtio-`...), else any link, else the kernel name.
pub fn preferred(links: &[String], name: &str) -> String {
    let base = |l: &String| {
        Path::new(l)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_owned()
    };
    links
        .iter()
        .find(|l| base(l).starts_with("wwn-"))
        .or_else(|| {
            links
                .iter()
                .find(|l| !NOT_BUS.iter().any(|p| base(l).starts_with(p)))
        })
        .or_else(|| links.first())
        .cloned()
        .unwrap_or_else(|| format!("/dev/{name}"))
}

/// What LVM says about the node's disks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Lvm {
    /// Resolved PV device node to VG name, empty for an orphan PV.
    pub pvs: BTreeMap<PathBuf, String>,
    /// VG name to the Pool named by its `r-drbd.pool.<name>` tag.
    pub pools: BTreeMap<String, String>,
}

impl Lvm {
    pub fn from_reports(pvs: &[lvm::report::Pv], vgs: &[lvm::report::VgFull]) -> Self {
        let prefix = model::keys::pool_vg_tag("");
        Self {
            pvs: pvs
                .iter()
                .map(|p| (devices::resolved(&p.name), p.vg.clone().unwrap_or_default()))
                .collect(),
            pools: vgs
                .iter()
                .filter_map(|v| {
                    let pool = v.tags.iter().find_map(|t| t.strip_prefix(&prefix))?;
                    Some((v.name.clone(), pool.to_owned()))
                })
                .collect(),
        }
    }
}

/// One row per whole disk, in kernel name order.
pub async fn scan(exec: &dyn Exec, disks: &dyn HostDisks, lvm: &Lvm) -> Vec<DiskStatus> {
    let links = disks.by_id();
    let mut out = Vec::new();
    for name in disks.whole_disks() {
        let links = links.get(&name).cloned().unwrap_or_default();
        let mut f = disks.facts(&format!("/dev/{name}"));
        // The open comes last, in [`state`].
        f.exclusive = Ok(());
        let (state, pool, message) = state(exec, disks, lvm, &f).await;
        let sys = f.sys.as_ref().ok();
        out.push(DiskStatus {
            path: preferred(&links, &name),
            name,
            links,
            state,
            pool,
            size_bytes: sys.map(|s| s.size_bytes).filter(|b| *b > 0),
            model: sys.and_then(|s| s.model.clone()),
            serial: sys.and_then(|s| s.serial.clone()),
            message,
        });
    }
    out
}

/// Missing, NotADisk, Member, then any PV of a VG that is not a Pool's as
/// NotEmpty naming it, then Busy, NotEmpty, Blank.
async fn state(
    exec: &dyn Exec,
    disks: &dyn HostDisks,
    lvm: &Lvm,
    f: &devices::Facts,
) -> (DeviceState, Option<String>, Option<String>) {
    let early = devices::classify_early(f, None, "");
    if let Some((s @ (DeviceState::Missing | DeviceState::NotADisk), m)) = early {
        return (s, None, m);
    }
    let Ok(node) = &f.dev else {
        return (DeviceState::Missing, None, None);
    };
    match lvm.pvs.get(node).map(String::as_str) {
        Some("") => return (DeviceState::NotEmpty, None, Some("PV without a VG".into())),
        Some(vg) => {
            return match lvm.pools.get(vg) {
                Some(pool) => (DeviceState::Member, Some(pool.clone()), None),
                None => (DeviceState::NotEmpty, None, Some(format!("PV of VG {vg}"))),
            };
        }
        None => {}
    }
    if let Some((s, m)) = early {
        return (s, None, m);
    }
    let sigs = run_ok(
        exec,
        &devices::wipefs_argv(&node.display().to_string()),
        DEFAULT_TIMEOUT,
    )
    .await
    .map_err(|e| e.to_string())
    .and_then(|o| devices::parse_wipefs(&o.stdout));
    let sys = f.sys.as_ref().cloned().unwrap_or_default();
    let (s, m) = devices::classify_signatures(&sys, None, &sigs);
    if s != DeviceState::Blank {
        return (s, None, m);
    }
    match disks.exclusive(node) {
        Ok(()) => (DeviceState::Blank, None, None),
        Err(e) => (
            DeviceState::Busy,
            None,
            Some(format!("exclusive open failed: {e}")),
        ),
    }
}

pub fn status(disks: Vec<DiskStatus>) -> NodeDisksStatus {
    let blank = disks
        .iter()
        .filter(|d| d.state == DeviceState::Blank)
        .count();
    NodeDisksStatus {
        blank: u32::try_from(blank).unwrap_or(u32::MAX),
        disks,
    }
}

/// A new status only when it differs from the written one.
pub fn changed(written: Option<&NodeDisks>, now: &NodeDisksStatus) -> bool {
    written.and_then(|o| o.status.as_ref()) != Some(now)
}

/// The object for `node`, owned by its Node so it goes with the node.
pub fn object(node: &str, node_uid: &str) -> NodeDisks {
    NodeDisks {
        metadata: ObjectMeta {
            name: Some(node.to_owned()),
            owner_references: Some(vec![OwnerReference {
                api_version: "v1".into(),
                kind: "Node".into(),
                name: node.to_owned(),
                uid: node_uid.to_owned(),
                ..OwnerReference::default()
            }]),
            ..ObjectMeta::default()
        },
        spec: NodeDisksSpec {},
        status: None,
    }
}

/// Creates the object if needed and patches its status if it changed;
/// true when it wrote.
pub async fn write(client: &Client, node: &str, now: &NodeDisksStatus) -> Result<bool, String> {
    let api: Api<NodeDisks> = Api::all(client.clone());
    let current = if let Some(o) = api.get_opt(node).await.map_err(|e| e.to_string())? {
        o
    } else {
        let uid = Api::<Node>::all(client.clone())
            .get(node)
            .await
            .map_err(|e| format!("read node {node}: {e}"))?
            .metadata
            .uid
            .ok_or_else(|| format!("node {node} has no uid"))?;
        api.create(&PostParams::default(), &object(node, &uid))
            .await
            .map_err(|e| format!("create: {e}"))?
    };
    if !changed(Some(&current), now) {
        return Ok(false);
    }
    // A merge patch replaces the whole list: rows of unplugged disks go.
    api.patch_status(
        node,
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({ "status": now })),
    )
    .await
    .map_err(|e| format!("patch status: {e}"))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::devices::fake::{FakeDisks, WIPEFS_BLANK, WIPEFS_XFS, disk};
    use crate::devices::{Part, wipefs_argv};
    use crate::exec::scripted::{ScriptedExec, ok};

    fn links(names: &[&str]) -> Vec<String> {
        names
            .iter()
            .map(|n| format!("/dev/disk/by-id/{n}"))
            .collect()
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
    fn pools_come_from_vg_tags() {
        let pvs = [
            lvm::report::Pv {
                name: "/dev/vdb".into(),
                vg: Some("data".into()),
            },
            lvm::report::Pv {
                name: "/dev/vdd".into(),
                vg: Some("r-drbd-dev".into()),
            },
            lvm::report::Pv {
                name: "/dev/vde".into(),
                vg: None,
            },
        ];
        let vg = |name: &str, tags: &[&str]| lvm::report::VgFull {
            name: name.into(),
            extent_bytes: 4 << 20,
            size_bytes: 0,
            free_bytes: 0,
            tags: tags.iter().map(|&t| t.to_owned()).collect(),
            missing_pvs: 0,
        };
        let l = Lvm::from_reports(
            &pvs,
            &[
                vg("data", &["other"]),
                vg("r-drbd-dev", &["x", "r-drbd.pool.devn1"]),
            ],
        );
        assert_eq!(
            l.pools,
            BTreeMap::from([("r-drbd-dev".to_owned(), "devn1".to_owned())])
        );
        assert_eq!(
            l.pvs.get(Path::new("/dev/vde")).map(String::as_str),
            Some("")
        );
        assert_eq!(l.pvs.len(), 3);
    }

    /// One disk per state; each by-id link maps to its disk.
    #[tokio::test]
    async fn every_whole_disk_gets_a_row() {
        let d = FakeDisks::default();
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
        let x = ScriptedExec::new();
        x.expect(wipefs_argv("/dev/vde"), ok(WIPEFS_BLANK))
            .expect(wipefs_argv("/dev/vdf"), ok(WIPEFS_XFS))
            .expect(wipefs_argv("/dev/vdi"), ok(WIPEFS_BLANK));
        let lvm = Lvm {
            pvs: BTreeMap::from([
                (PathBuf::from("/dev/vdb"), "data".to_owned()),
                (PathBuf::from("/dev/vdd"), "r-drbd-dev".to_owned()),
                (PathBuf::from("/dev/vdg"), String::new()),
            ]),
            pools: BTreeMap::from([("r-drbd-dev".to_owned(), "devn1".to_owned())]),
        };
        let rows = scan(&x, &d, &lvm).await;
        let got: Vec<String> = rows
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
            .collect();
        assert_eq!(
            got,
            [
                "vda /dev/vda Busy - mounted on /sysroot",
                "vdb /dev/vdb NotEmpty - PV of VG data",
                "vdd /dev/disk/by-id/wwn-0x5d Member devn1 -",
                "vde /dev/disk/by-id/virtio-e Blank - -",
                "vdf /dev/vdf NotEmpty - signatures: xfs at 0x0",
                "vdg /dev/vdg NotEmpty - PV without a VG",
                "vdh /dev/vdh NotADisk - vdh is read-only",
                "vdi /dev/vdi Busy - exclusive open failed: Device or resource busy (os error 16)",
            ]
        );
        x.assert_done();
        // Only disks every read found blank were opened.
        assert_eq!(
            *d.opened.lock().unwrap(),
            [PathBuf::from("/dev/vde"), PathBuf::from("/dev/vdi")]
        );
        assert_eq!(
            rows[2].links,
            links(&["virtio-d", "wwn-0x5d"]),
            "every link of the disk"
        );
        assert_eq!(rows[3].size_bytes, Some(4 << 30));
        assert_eq!(rows[3].serial.as_deref(), Some("serial-vde"));
        assert_eq!(status(rows).blank, 1);
    }

    #[tokio::test]
    async fn unreadable_signatures_are_not_blank() {
        let d = FakeDisks::default();
        d.set("/dev/vde", disk("vde"));
        let x = ScriptedExec::new();
        x.expect(
            wipefs_argv("/dev/vde"),
            crate::exec::scripted::code(1, "", "wipefs: denied"),
        );
        let rows = scan(&x, &d, &Lvm::default()).await;
        assert_eq!(rows[0].state, DeviceState::NotEmpty);
        assert!(d.opened.lock().unwrap().is_empty());
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

    #[test]
    fn written_only_on_change() {
        let now = status(vec![
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
        assert!(changed(Some(&o), &status(later.disks)));
        let mut unplugged = now.clone();
        unplugged.disks.pop();
        assert!(changed(Some(&o), &status(unplugged.disks)));
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
}
