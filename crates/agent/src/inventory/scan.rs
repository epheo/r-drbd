//! One inventory pass over the host's disks, reading only what changed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kube::ResourceExt;
use model::{DeviceState, DiskStatus, DiskWatch, Pool};
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::{Dirty, Pass, SCAN_MAX, Source, preferred, status};
use crate::devices::{self, Facts, HostDisks, Signature, Udev};
use crate::exec::run_ok;
use crate::{Exec, ExecMode};

/// A disk that does not answer wipefs in this time reads NotEmpty.
const WIPEFS_WAIT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(10),
    term: Duration::from_secs(2),
    kill: Duration::from_secs(2),
};

/// The longest the scan holds the disk lock at once: one wipefs or one
/// exclusive open. The lock is fair, so a waiter waits one hold at most.
pub(crate) const LOCK_HOLD_MAX: Duration = {
    let ExecMode::Timeout { wait, term, kill } = WIPEFS_WAIT else {
        unreachable!()
    };
    let wipefs = wait.saturating_add(term).saturating_add(kill);
    if wipefs.as_nanos() > devices::OPEN_WAIT.as_nanos() {
        wipefs
    } else {
        devices::OPEN_WAIT
    }
};

const LVM_SIGNATURE: &str = "LVM2_member";

/// What LVM says about the node's disks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Lvm {
    /// Resolved PV device node to VG name, empty for an orphan PV.
    pub pvs: BTreeMap<PathBuf, String>,
    /// VG name to its Pool and a note, or why no Pool owns it. A VG
    /// absent here belongs to no Pool.
    pub owners: BTreeMap<String, Result<(String, Option<String>), String>>,
}

impl Lvm {
    /// The tag `r-drbd.pool.<name>` names the Pool; without it, a Pool
    /// on this node with that `vg` and no `devices`.
    pub fn new(
        pvs: &[lvm::report::Pv],
        vgs: &[lvm::report::VgFull],
        node: &str,
        pools: &[Arc<Pool>],
    ) -> Self {
        let prefix = model::keys::pool_vg_tag("");
        let here: Vec<&Arc<Pool>> = pools.iter().filter(|p| p.spec.node == node).collect();
        let mut owners = BTreeMap::new();
        for vg in vgs {
            let mut tagged: Vec<&str> = vg
                .tags
                .iter()
                .filter_map(|t| t.strip_prefix(&prefix))
                .collect();
            tagged.sort_unstable();
            let owner = match tagged.as_slice() {
                [] => {
                    let mut names: Vec<String> = here
                        .iter()
                        .filter(|p| p.spec.devices.is_none() && p.spec.vg == vg.name)
                        .map(|p| p.name_any())
                        .collect();
                    names.sort();
                    let Some(first) = names.first().cloned() else {
                        continue;
                    };
                    let others = (names.len() > 1).then(|| {
                        format!(
                            "VG {} also used by Pools {}",
                            vg.name,
                            names[1..].join(", ")
                        )
                    });
                    Ok((first, others))
                }
                [one] if here.iter().any(|p| p.name_any() == *one) => Ok(((*one).to_owned(), None)),
                [one] => Err(format!(
                    "VG {} tagged for Pool {one}, no such Pool on this node",
                    vg.name
                )),
                many => Err(format!(
                    "VG {} tagged for Pools {}",
                    vg.name,
                    many.join(", ")
                )),
            };
            owners.insert(vg.name.clone(), owner);
        }
        Self {
            pvs: pvs
                .iter()
                .map(|p| (devices::resolved(&p.name), p.vg.clone().unwrap_or_default()))
                .collect(),
            owners,
        }
    }
}

/// What the last reads found on one disk.
#[derive(Debug, Clone)]
struct Entry {
    /// Cheap: sysfs, mounts, udev; read every pass.
    facts: Facts,
    /// wipefs, read when the disk is dirty or its facts changed.
    sigs: Option<Result<Vec<Signature>, String>>,
    /// The exclusive open, tried once a disk reads blank.
    open: Option<Result<(), String>>,
}

/// The state from the reads, or that the open must decide.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    Final(DeviceState, Option<String>, Option<String>),
    Open,
}

/// Missing, NotADisk, then a PV: Member of its Pool or NotEmpty naming
/// its VG, then Busy, NotEmpty, then the open for Blank.
fn decide(f: &Facts, sigs: Option<&Result<Vec<Signature>, String>>, lvm: &Lvm) -> Decision {
    let early = devices::classify_early(f, None, "");
    if let Some((s @ (DeviceState::Missing | DeviceState::NotADisk), m)) = early {
        return Decision::Final(s, None, m);
    }
    let Ok(node) = &f.dev else {
        return Decision::Final(DeviceState::Missing, None, None);
    };
    match lvm.pvs.get(node).map(String::as_str) {
        Some("") => {
            return Decision::Final(DeviceState::NotEmpty, None, Some("PV without a VG".into()));
        }
        Some(vg) => {
            return match lvm.owners.get(vg) {
                Some(Ok((owner, others))) => {
                    Decision::Final(DeviceState::Member, Some(owner.clone()), others.clone())
                }
                Some(Err(why)) => Decision::Final(DeviceState::NotEmpty, None, Some(why.clone())),
                None => {
                    Decision::Final(DeviceState::NotEmpty, None, Some(format!("PV of VG {vg}")))
                }
            };
        }
        None => {}
    }
    if let Some((s, m)) = early {
        return Decision::Final(s, None, m);
    }
    let unread = Err("signatures not read".to_owned());
    let sys = f.sys.as_ref().cloned().unwrap_or_default();
    match devices::classify_signatures(&sys, None, sigs.unwrap_or(&unread)) {
        (DeviceState::Blank, _) => Decision::Open,
        (s, m) => Decision::Final(s, None, m),
    }
}

/// wipefs is needed only when the cheap facts settle nothing: never on a
/// Busy disk (held, mounted, a multipath path), where a read may hang or
/// log I/O errors on a standby path.
fn needs_signatures(f: &Facts) -> bool {
    devices::classify_early(f, None, "").is_none()
}

/// Whole-disk holders only: a PV on a partition never names the disk's row.
fn held_unknown_pv(f: &Facts, lvm: &Lvm) -> bool {
    let held = f.sys.as_ref().is_ok_and(|s| !s.holders.is_empty());
    held && f.dev.as_ref().is_ok_and(|n| !lvm.pvs.contains_key(n))
}

fn has_lvm_signature(e: &Entry) -> bool {
    matches!(&e.sigs, Some(Ok(s)) if s.iter().any(|g| g.kind == LVM_SIGNATURE))
}

/// The host's disks, read through `exec` and `disks`, with what the last
/// pass found.
pub struct Scanner {
    node: String,
    exec: Arc<dyn Exec>,
    disks: Arc<dyn HostDisks>,
    lock: Arc<Mutex<()>>,
    pools: Box<dyn Fn() -> Vec<Arc<Pool>> + Send + Sync>,
    watch: DiskWatch,
    stop: tokio::sync::watch::Receiver<bool>,
    cache: BTreeMap<String, Entry>,
    lvm: Lvm,
    lvm_warned: bool,
    cut_warned: bool,
    max: Duration,
}

impl Scanner {
    pub fn new(
        node: String,
        exec: Arc<dyn Exec>,
        disks: Arc<dyn HostDisks>,
        lock: Arc<Mutex<()>>,
        pools: Box<dyn Fn() -> Vec<Arc<Pool>> + Send + Sync>,
        watch: DiskWatch,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            node,
            exec,
            disks,
            lock,
            pools,
            watch,
            stop,
            cache: BTreeMap::new(),
            lvm: Lvm::default(),
            lvm_warned: false,
            cut_warned: false,
            max: SCAN_MAX,
        }
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn watch(&self) -> DiskWatch {
        self.watch
    }

    pub fn udev_readable(&self) -> bool {
        self.disks.udev_readable()
    }

    fn stopping(&self) -> bool {
        *self.stop.borrow()
    }

    #[cfg(test)]
    pub(crate) fn with_max(mut self, max: Duration) -> Self {
        self.max = max;
        self
    }

    async fn read_lvm(&mut self) {
        let lvm = crate::storage::Lvm::new(self.exec.clone());
        let reports =
            async { Ok::<_, crate::exec::CmdError>((lvm.pvs_all().await?, lvm.vgs_all().await?)) };
        match reports.await {
            Ok((pvs, vgs)) => {
                self.lvm_warned = false;
                self.lvm = Lvm::new(&pvs, &vgs, &self.node, &(self.pools)());
            }
            Err(e) => {
                if self.lvm_warned {
                    tracing::debug!(error = %e, "disk inventory without LVM reports");
                } else {
                    tracing::warn!(error = %e, "disk inventory without LVM reports: PVs read by their signature");
                    self.lvm_warned = true;
                }
                self.lvm = Lvm::default();
            }
        }
    }

    async fn wipefs(&self, node: &std::path::Path) -> Result<Vec<Signature>, String> {
        let _held = self.lock.lock().await;
        run_ok(
            self.exec.as_ref(),
            &devices::wipefs_argv(&node.display().to_string()),
            WIPEFS_WAIT,
        )
        .await
        .map_err(|e| e.to_string())
        .and_then(|o| devices::parse_wipefs(&o.stdout))
    }

    async fn open(&self, node: &std::path::Path) -> Result<(), String> {
        let _held = self.lock.lock().await;
        devices::open_exclusive_blocking(&self.disks, node).await
    }
}

impl std::fmt::Debug for Scanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scanner")
            .field("node", &self.node)
            .field("watch", &self.watch)
            .finish_non_exhaustive()
    }
}

impl Scanner {
    /// Cheap facts of every disk; returns the disks to read again: dirty,
    /// new, or whose facts changed.
    async fn refresh(&mut self, names: &[String], dirty: &Dirty) -> Vec<String> {
        self.cache.retain(|n, _| names.contains(n));
        let table = self.disks.mounts();
        let mut deep = Vec::new();
        for name in names {
            if self.stopping() {
                break;
            }
            if dirty.disks.contains(name) {
                self.disks.udev_restart(name);
            }
            let (d, t) = (self.disks.clone(), table.clone());
            let path = format!("/dev/{name}");
            let Ok(mut f) = tokio::task::spawn_blocking(move || d.facts(&path, &t)).await else {
                continue;
            };
            f.exclusive = Ok(());
            let same = self.cache.get(name).is_some_and(|e| e.facts == f);
            if dirty.all || dirty.disks.contains(name) || dirty.again.contains(name) || !same {
                self.cache.insert(
                    name.clone(),
                    Entry {
                        facts: f,
                        sigs: None,
                        open: None,
                    },
                );
                deep.push(name.clone());
            }
        }
        deep
    }

    /// Signatures of `deep` within the time cap; returns the disks left
    /// out and whether LVM's view is stale.
    async fn read_signatures(&mut self, deep: &[String]) -> (BTreeSet<String>, bool) {
        let deadline = Instant::now() + self.max;
        let mut unchecked = BTreeSet::new();
        let mut stale = false;
        for name in deep {
            let Some(e) = self.cache.get(name) else {
                continue;
            };
            if !needs_signatures(&e.facts) {
                // LVs activated before wipefs saw the label hold the disk,
                // and wipefs stays off a held disk: only LVM can name its VG.
                stale |= held_unknown_pv(&e.facts, &self.lvm);
                continue;
            }
            if Instant::now() >= deadline || self.stopping() {
                unchecked.insert(name.clone());
                continue;
            }
            let Ok(node) = e.facts.dev.clone() else {
                continue;
            };
            let sigs = self.wipefs(&node).await;
            if let Some(e) = self.cache.get_mut(name) {
                e.sigs = Some(sigs);
                // A label appeared or went.
                stale |= has_lvm_signature(e) != self.lvm.pvs.contains_key(&node);
            }
        }
        for name in &unchecked {
            self.cache.remove(name);
        }
        (unchecked, stale)
    }

    /// The row of a cached disk; the open only once every read finds it blank.
    async fn row(&mut self, name: &str, links: &[String]) -> Option<DiskStatus> {
        let e = self.cache.get(name)?;
        let mut d = decide(&e.facts, e.sigs.as_ref(), &self.lvm);
        if d == Decision::Open {
            let open = if let Some(o) = &e.open {
                o.clone()
            } else {
                let node = e.facts.dev.clone().unwrap_or_default();
                let o = self.open(&node).await;
                if let Some(e) = self.cache.get_mut(name) {
                    e.open = Some(o.clone());
                }
                o
            };
            d = match open {
                Ok(()) => Decision::Final(DeviceState::Blank, None, None),
                Err(err) => Decision::Final(
                    DeviceState::Busy,
                    None,
                    Some(format!("exclusive open failed: {err}")),
                ),
            };
        }
        let Decision::Final(state, pool, message) = d else {
            return None;
        };
        let sys = self.cache.get(name)?.facts.sys.as_ref().ok();
        Some(DiskStatus {
            path: preferred(links, name),
            name: name.to_owned(),
            links: links.to_vec(),
            state,
            pool,
            size_bytes: sys.map(|s| s.size_bytes).filter(|b| *b > 0),
            model: sys.and_then(|s| s.model.clone()),
            serial: sys.and_then(|s| s.serial.clone()),
            message,
        })
    }
}

#[async_trait::async_trait]
impl Source for Scanner {
    async fn pass(&mut self, dirty: Dirty) -> Pass {
        let names = self.disks.whole_disks();
        let links = self.disks.by_id();
        let deep = self.refresh(&names, &dirty).await;
        let (unchecked, stale) = self.read_signatures(&deep).await;
        if (dirty.all || dirty.lvm || stale) && !self.stopping() {
            self.read_lvm().await;
        }
        let mut rows = Vec::new();
        for name in &names {
            if self.stopping() {
                break;
            }
            let l = links.get(name).cloned().unwrap_or_default();
            if let Some(r) = self.row(name, &l).await {
                rows.push(r);
            }
        }
        // udevd was not done with these: read again soon, no uevent will say.
        let unsettled: BTreeSet<String> = self
            .cache
            .iter()
            .filter(|(_, e)| e.facts.udev == Udev::Unsettled)
            .map(|(n, _)| n.clone())
            .collect();
        let s = status(rows, &unchecked, self.watch);
        if let Some(m) = &s.message
            && !self.cut_warned
        {
            tracing::warn!(message = m, "NodeDisks incomplete");
            self.cut_warned = true;
        }
        Pass {
            status: s,
            unchecked,
            unsettled,
        }
    }

    fn set_watch(&mut self, watch: DiskWatch) {
        self.watch = watch;
    }
}
