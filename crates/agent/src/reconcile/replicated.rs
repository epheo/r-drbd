//! Replicated volumes (spec.replicas >= 2) on this node: backing LV of
//! gross size, resource file, metadata with the day-0 generation id,
//! adjust, peers leaving, resize, teardown, snapshot suspend and resume,
//! split-brain resolution, status from events2.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drbd_config::{MetaParams, ToolVersions};
use kube::ResourceExt;
use lvm::report::LvRow;
use model::{
    AssignedReplica, Assignment, Pool, ReplicaStatus, Resolve, Role, Snapshot, SnapshotPhase,
    Volume,
};

use super::{
    Place, Reads, Reconciler, StatusWrite, find_in_pool, is_lost, lost_after_report, rebuild_error,
    unwrap_rebuild,
};
use crate::ExecMode;
use crate::drbd::{self, Facts, FileInputs, Host, RealHost, ResFiles, VOLUME, argv};
use crate::events2::{Resource, Tracker};
use crate::exec::{DEFAULT_TIMEOUT, failure, run_any, run_ok};
use crate::storage;

const NO_DRBD: &str = "drbd-utils not found on this node";
const MD_NOT_AT_END: &str =
    "metadata not at device end; refusing create-md on a replica that held data";
const MD_OTHER_VOLUME: &str = "metadata of another volume on the LV";
const RESYNC_BUSY: &str = "Resize not allowed during resync";
const POLL: Duration = Duration::from_millis(100);

/// Everything DRBD work needs besides the executor.
#[derive(Debug, Clone)]
pub struct DrbdEnv {
    /// None when drbd-utils or the module are missing on this node.
    pub versions: Option<ToolVersions>,
    pub files: ResFiles,
    pub host: Arc<dyn Host>,
    pub tracker: Arc<Mutex<Tracker>>,
    /// Bound on the events2 destroy after `drbdsetup down`.
    pub down_wait: Duration,
    /// Sleeps between adjust retries while a resync blocks it.
    pub adjust_retry: [Duration; 3],
    /// Least time between two adjusts driven by events2 state alone.
    pub adjust_interval: Duration,
}

impl DrbdEnv {
    pub fn new(
        versions: Option<ToolVersions>,
        res_dir: impl Into<PathBuf>,
        tracker: Arc<Mutex<Tracker>>,
    ) -> Self {
        Self {
            versions,
            files: ResFiles::new(res_dir),
            host: Arc::new(RealHost),
            tracker,
            down_wait: Duration::from_secs(5),
            adjust_interval: Duration::from_secs(30),
            adjust_retry: [
                Duration::from_millis(500),
                Duration::from_millis(1000),
                Duration::from_millis(2000),
            ],
        }
    }
}

fn meta_params() -> MetaParams {
    MetaParams {
        peer_slots: model::keys::PEER_SLOTS,
        ..MetaParams::default()
    }
}

/// Net size the caller asked for, in KiB.
fn net_kib(vol: &Volume) -> u64 {
    vol.spec.size_bytes.div_ceil(1024)
}

fn gross_kib(vol: &Volume) -> Result<u64, String> {
    drbd_config::gross_size_kib(net_kib(vol), meta_params()).map_err(|e| e.to_string())
}

/// Phases during which a user suspend of the volume is expected.
fn snapshot_round_active(snapshots: &[Arc<Snapshot>], volume: &str) -> bool {
    snapshots.iter().any(|s| {
        s.spec.volume == volume
            && s.metadata.deletion_timestamp.is_none()
            && s.status.as_ref().is_some_and(|st| {
                matches!(
                    st.phase,
                    SnapshotPhase::Suspending
                        | SnapshotPhase::Suspended
                        | SnapshotPhase::Taking
                        | SnapshotPhase::Taken
                        | SnapshotPhase::Resuming
                )
            })
    })
}

/// Whose metadata sits on the backing LV, from its generation ids.
enum MdIdentity {
    /// Our day-0 id appears: never recreated.
    Ours,
    /// create-md ran but no id was set yet (current 4, nothing else): no
    /// data identity, so creating it again is safe.
    Unfinished,
    /// Another resource's metadata: the snapshot source's, or a stranger's.
    Other,
}

/// The 64-bit ids in get-gi output (current, bitmap, two history) or in
/// dump-superblock output (current, every bitmap and history id).
fn gi_ids(text: &str) -> Vec<u64> {
    text.split(|c: char| matches!(c, ':' | ';' | ',' | '"') || c.is_whitespace())
        .filter_map(|t| {
            let h = t.strip_prefix("0x").unwrap_or(t);
            (h.len() == 16)
                .then(|| u64::from_str_radix(h, 16).ok())
                .flatten()
        })
        .collect()
}

/// DRBD keeps a role flag in bit 0 of an id; day-0 ids have it cleared.
fn same_id(a: u64, b: u64) -> bool {
    a & !1 == b & !1
}

/// The backing LV after [`Reconciler::ensure_drbd_lv`].
struct Backing {
    row: LvRow,
    /// False while a needed growth waits for the attach.
    grown: bool,
    /// This pass created the LV.
    created: bool,
    /// Created in place of an LV this node had reported: empty, never a restore.
    rebuilt: bool,
}

enum Resize {
    NotNeeded,
    /// Grown: adjust follows.
    Done,
    /// This node resizes once every diskful peer has grown its LV.
    Waiting,
}

impl Reconciler {
    pub(super) fn drbd_available(&self) -> bool {
        self.drbd.versions.is_some() && self.drbd.tracker.lock().is_ok_and(|t| t.is_available())
    }

    pub(super) fn tracked(&self, name: &str) -> Option<Resource> {
        self.drbd
            .tracker
            .lock()
            .ok()
            .and_then(|t| t.resource(name).cloned())
    }

    fn prev_entry(&self, vol: &Volume) -> Option<ReplicaStatus> {
        vol.status
            .as_ref()
            .and_then(|s| s.replicas.iter().find(|r| r.node == self.node))
            .cloned()
    }

    async fn drbd_cmd(&self, argv: &[String], mode: ExecMode) -> Result<(), String> {
        run_ok(self.lvm.exec(), argv, mode)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub(super) async fn drbd_volume(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snapshots: &[Arc<Snapshot>],
        vol: &Volume,
    ) -> Option<StatusWrite> {
        let name = vol.name_any();
        let assignment = vol.spec.assignment.as_ref()?;
        let prev = self.prev_entry(vol);
        let mine = assignment
            .replicas
            .iter()
            .find(|r| r.node == self.node)
            .cloned();
        let Some(versions) = self.drbd.versions.clone() else {
            mine.as_ref()?;
            let e = self.error_entry(prev.as_ref(), NO_DRBD.to_owned());
            return (prev.as_ref() != Some(&e)).then_some(StatusWrite::Volume { name, entry: e });
        };
        let res = {
            let t = self.drbd.tracker.lock().ok()?;
            if !t.is_available() {
                // No DRBD work before the initial events2 dump.
                return None;
            }
            t.resource(&name).cloned()
        };
        match mine {
            Some(m) if vol.metadata.deletion_timestamp.is_none() => {
                self.drbd_up(
                    reads, pools, snapshots, vol, assignment, &m, &versions, res, prev,
                )
                .await
            }
            mine => {
                self.drbd_remove(reads, pools, vol, mine.as_ref(), res.as_ref(), prev)
                    .await
            }
        }
    }

    /// Whole-volume delete, or this node left the assignment.
    async fn drbd_remove(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        vol: &Volume,
        mine: Option<&AssignedReplica>,
        res: Option<&Resource>,
        prev: Option<ReplicaStatus>,
    ) -> Option<StatusWrite> {
        let name = vol.name_any();
        let lv = lvm::names::volume_lv(&name);
        let minor = vol.spec.assignment.as_ref().and_then(|a| a.minor);
        self.facts.remove(&name);
        self.adjusted.remove(&name);
        if mine.is_none() && vol.metadata.deletion_timestamp.is_none() {
            return self.drbd_depart(reads, pools, &name, &lv, res, prev).await;
        }
        let found = match mine {
            Some(m) if m.role == Role::Diskful => match self.place(pools, m.pool.as_deref()) {
                Ok(p) => reads
                    .find(&self.lvm, &p.vg, &lv, &p.thin)
                    .await
                    .map(|row| row.map(|r| (p, r))),
                Err(e) => Err(e),
            },
            Some(_) => Ok(None),
            None => Ok(self.find_lv(reads, pools, &lv).await),
        };
        let result = match found {
            Ok(found) => {
                let local = res.is_some()
                    || found.is_some()
                    || self.drbd.files.exists(&name)
                    || self.drbd.files.backup_path(&name).exists();
                if local {
                    self.teardown(reads, &name, res, minor, found.as_ref())
                        .await
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(e),
        };
        if let Err(e) = &result {
            tracing::warn!(volume = name, error = %e, "replica teardown failed");
        }
        match (mine, result) {
            (None, Ok(())) => prev.map(|_| StatusWrite::VolumeGone { name }),
            (None, Err(e)) => {
                let p = prev?;
                let entry = self.error_entry(Some(&p), e);
                (p != entry).then_some(StatusWrite::Volume { name, entry })
            }
            (Some(_), r) => {
                let entry = match r {
                    Ok(()) => ReplicaStatus {
                        node: self.node.clone(),
                        observed_generation: vol.metadata.generation,
                        ..ReplicaStatus::default()
                    },
                    Err(e) => self.error_entry(prev.as_ref(), e),
                };
                (prev.as_ref() != Some(&entry)).then_some(StatusWrite::Volume { name, entry })
            }
        }
    }

    /// This node left the assignment of a live Volume. An entry can vanish by
    /// an edit or a bug, so the data stays: DRBD down, files removed, entry
    /// dropped, the LV and its metadata left alone as an orphan.
    async fn drbd_depart(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        name: &str,
        lv: &str,
        res: Option<&Resource>,
        prev: Option<ReplicaStatus>,
    ) -> Option<StatusWrite> {
        let mut result = self.depart(name, res).await;
        if let Some((place, _)) = self.find_lv(reads, pools, lv).await {
            let key = format!("{}/{lv}", place.vg);
            if self.orphans_logged.insert(key.clone()) {
                tracing::warn!(
                    lv = key,
                    volume = name,
                    "node left the assignment: LV kept as an orphan"
                );
            }
            // If placed here again, this data must earn adoption anew.
            if result.is_ok() {
                result = self.drop_md_tags(reads, &place, lv).await;
            }
        }
        match result {
            Ok(()) => prev.map(|_| StatusWrite::VolumeGone {
                name: name.to_owned(),
            }),
            Err(e) => {
                tracing::warn!(volume = name, error = %e, "leaving the assignment failed");
                let p = prev?;
                let entry = self.error_entry(Some(&p), e);
                (p != entry).then(|| StatusWrite::Volume {
                    name: name.to_owned(),
                    entry,
                })
            }
        }
    }

    async fn drop_md_tags(&self, reads: &mut Reads, place: &Place, lv: &str) -> Result<(), String> {
        let tags = reads.tags(&self.lvm, &place.vg, lv).await?;
        let del: Vec<&str> = tags
            .iter()
            .map(String::as_str)
            .filter(|t| storage::is_md_tag(t))
            .collect();
        if del.is_empty() {
            return Ok(());
        }
        let r = self.lvm.change_tags(&place.vg, lv, &[], &del).await;
        reads.tags.remove(&place.vg);
        r.map_err(|e| e.to_string())
    }

    async fn depart(&self, name: &str, res: Option<&Resource>) -> Result<(), String> {
        if let Some(r) = res {
            self.down(name, r).await?;
        }
        self.drbd.files.remove(name)
    }

    async fn down(&self, name: &str, r: &Resource) -> Result<(), String> {
        if r.suspended_by_user() {
            tracing::info!(volume = name, "resuming IO before down");
            self.drbd_cmd(&argv::resume_io(name), DEFAULT_TIMEOUT)
                .await?;
        }
        tracing::info!(volume = name, "taking DRBD resource down");
        self.drbd_cmd(&argv::down(name), DEFAULT_TIMEOUT).await?;
        self.wait_down(name).await
    }

    /// The LV of that name in one of this node's thin pools.
    async fn find_lv(
        &self,
        reads: &mut Reads,
        pools: &[&Pool],
        lv: &str,
    ) -> Option<(Place, LvRow)> {
        for p in pools {
            let place = Place {
                pool: p.name_any(),
                vg: p.spec.vg.clone(),
                thin: p.spec.thin_pool.clone(),
            };
            if let Ok(Some(row)) = reads.find(&self.lvm, &place.vg, lv, &place.thin).await {
                return Some((place, row));
            }
        }
        None
    }

    /// Down (resumed first if a snapshot round left it suspended), wipe the
    /// metadata, remove the LV, remove the res file and its backup.
    async fn teardown(
        &self,
        reads: &mut Reads,
        name: &str,
        res: Option<&Resource>,
        minor: Option<u32>,
        found: Option<&(Place, LvRow)>,
    ) -> Result<(), String> {
        match res {
            Some(r) => self.down(name, r).await?,
            // Not tracked is not proof of down: wipe-md and lvremove follow.
            None => self.confirm_down(name).await?,
        }
        if let Some((place, row)) = found {
            if let Some(minor) = minor
                && row.is_active()
                && self.md_magic(&row.lv_path).await?
            {
                self.drbd_cmd(&argv::wipe_md(minor, &row.lv_path), DEFAULT_TIMEOUT)
                    .await?;
            }
            let removed = self.remove_volume(&place.vg, row).await;
            reads.invalidate(&place.vg);
            removed?;
        }
        self.drbd.files.remove(name)
    }

    /// The events2 destroy for `name`, bounded; `drbdsetup status` decides
    /// either way, since a lagging or restarted stream proves nothing.
    async fn wait_down(&self, name: &str) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + self.drbd.down_wait;
        while tokio::time::Instant::now() < deadline && self.tracked(name).is_some() {
            tokio::time::sleep(POLL).await;
        }
        self.confirm_down(name).await
    }

    /// Exit 0 of `drbdsetup status <res>` means still up.
    async fn confirm_down(&self, name: &str) -> Result<(), String> {
        let out = run_any(self.lvm.exec(), &argv::status(name), DEFAULT_TIMEOUT)
            .await
            .map_err(|e| e.to_string())?;
        if out.exit_code == Some(0) {
            Err(format!("DRBD resource {name} still up after down"))
        } else {
            Ok(())
        }
    }

    async fn md_magic(&self, dev: &str) -> Result<bool, String> {
        let host = self.drbd.host.clone();
        let path = dev.to_owned();
        tokio::task::spawn_blocking(move || host.read_tail(&path))
            .await
            .map_err(|e| e.to_string())?
            .map(|t| drbd::has_md_magic(&t))
            .map_err(|e| format!("read metadata area of {dev}: {e}"))
    }

    async fn sectors(&self, minor: u32) -> std::io::Result<u64> {
        let host = self.drbd.host.clone();
        tokio::task::spawn_blocking(move || host.device_sectors(minor))
            .await
            .map_err(std::io::Error::other)?
    }

    /// The entry from the current events2 state.
    async fn current_entry(&self, name: &str, facts: &Facts) -> ReplicaStatus {
        let now = self.tracked(name);
        let fallback = self.sync_fallback(name, now.as_ref()).await;
        let sectors = match &now {
            Some(r) if r.devices.contains_key(&VOLUME) => self.sectors(facts.minor).await.ok(),
            _ => None,
        };
        drbd::entry(&self.node, now.as_ref(), facts, sectors, fallback)
    }

    #[allow(clippy::too_many_arguments)]
    async fn drbd_up(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snapshots: &[Arc<Snapshot>],
        vol: &Volume,
        a: &Assignment,
        mine: &AssignedReplica,
        versions: &ToolVersions,
        res: Option<Resource>,
        prev: Option<ReplicaStatus>,
    ) -> Option<StatusWrite> {
        let name = vol.name_any();
        let Some(minor) = a.minor else {
            let e = self.error_entry(prev.as_ref(), "assignment has no minor".to_owned());
            return (prev.as_ref() != Some(&e)).then_some(StatusWrite::Volume { name, entry: e });
        };
        let mut facts = Facts {
            minor,
            diskful: mine.role == Role::Diskful,
            allocated_bytes: self
                .facts
                .get(&name)
                .and_then(|f| f.allocated_bytes)
                .or_else(|| prev.as_ref().and_then(|p| p.allocated_bytes)),
            observed_generation: prev.as_ref().and_then(|p| p.observed_generation),
            last_error: None,
        };
        let step = Step {
            vol,
            a,
            mine,
            versions,
            minor,
            res: res.as_ref(),
            prev: prev.as_ref(),
        };
        match self
            .drbd_apply(reads, pools, snapshots, &step, &mut facts)
            .await
        {
            Ok(true) => facts.observed_generation = vol.metadata.generation,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(volume = name, error = %e, "replicated volume reconcile failed");
                facts.last_error = Some(e);
            }
        }
        let entry = self.current_entry(&name, &facts).await;
        self.facts.insert(name.clone(), facts);
        (prev.as_ref() != Some(&entry)).then_some(StatusWrite::Volume { name, entry })
    }

    /// The pass for one replica; Ok(false) while a resize waits on peers.
    async fn drbd_apply(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snapshots: &[Arc<Snapshot>],
        s: &Step<'_>,
        facts: &mut Facts,
    ) -> Result<bool, String> {
        // Until a pass completes, every error of a rebuild keeps the
        // loss on record, so the next pass rebuilds again (never restores,
        // never sets day-0 ids).
        let prev_lost = s
            .prev
            .and_then(|p| p.last_error.as_deref())
            .is_some_and(is_lost);
        let mut rebuilt = false;
        self.drbd_apply_steps(reads, pools, snapshots, s, facts, &mut rebuilt)
            .await
            .map_err(|e| {
                if rebuilt || prev_lost {
                    rebuild_error(&e)
                } else {
                    e
                }
            })
    }

    async fn drbd_apply_steps(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snapshots: &[Arc<Snapshot>],
        s: &Step<'_>,
        facts: &mut Facts,
        rebuilt: &mut bool,
    ) -> Result<bool, String> {
        let name = s.vol.name_any();
        let diskful = s.mine.role == Role::Diskful;
        let mut complete = true;
        let backing = if diskful {
            let place = self.place(pools, s.mine.pool.as_deref())?;
            let b = self.ensure_drbd_lv(reads, snapshots, s, &place).await?;
            *rebuilt = b.rebuilt;
            complete &= b.grown;
            facts.allocated_bytes = Some(b.row.lv_size * 1024);
            let chunk = pools
                .iter()
                .find(|p| p.name_any() == place.pool)
                .and_then(|p| p.status.as_ref()?.chunk_bytes);
            Some((b, chunk, place))
        } else {
            None
        };

        let file = drbd::resource_file(&FileInputs {
            node: &self.node,
            volume: &name,
            assignment: s.a,
            backing: backing.as_ref().map(|(b, ..)| b.row.lv_path.as_str()),
            addresses: &self.addresses,
            versions: s.versions,
            replicas: s.vol.spec.replicas,
            chunk_bytes: backing.as_ref().and_then(|(_, c, ..)| *c),
            access_mode: s.vol.spec.access_mode,
        })?;
        // Explicit triggers adjust at once; state alone at most once per
        // adjust_interval, so a peer that keeps failing is not hammered.
        let written = self
            .drbd
            .files
            .write_if_differs(&name, &drbd_config::render(&file))?;
        let mut adjust = written || s.res.is_none();
        let peers: Vec<&str> =
            s.a.replicas
                .iter()
                .filter(|r| r.node != self.node && drbd::connects(s.mine, r))
                .map(|r| r.node.as_str())
                .collect();
        let state_adjust = drbd::needs_adjust(s.res, &peers, diskful);

        adjust |= self.remove_departed(s, &name, diskful).await?;

        if let Some((b, _, place)) = &backing {
            match self.ensure_metadata(reads, s, place, b).await {
                Ok(made) => adjust |= made,
                Err(e) => {
                    if b.created {
                        // An LV made this pass holds no data; without it the
                        // next pass starts over instead of finding half-made
                        // metadata it must refuse.
                        tracing::warn!(volume = name, "removing the LV created this pass");
                        let removed = self.remove_volume(&place.vg, &b.row).await;
                        reads.invalidate(&place.vg);
                        if let Err(re) = removed {
                            tracing::warn!(volume = name, error = %re, "LV removal failed");
                        }
                    }
                    return Err(e);
                }
            }
        }

        if let Some(r) = s.res
            && r.suspended_by_user()
            && !snapshot_round_active(snapshots, &name)
        {
            tracing::info!(
                volume = name,
                "IO suspended without a snapshot round: resuming"
            );
            self.drbd_cmd(&argv::resume_io(&name), DEFAULT_TIMEOUT)
                .await?;
        }

        let mut skip_adjust = false;
        if diskful && let (Some(rv), Some(r)) = (&s.vol.spec.resolve, s.res) {
            skip_adjust = self.resolve(s, rv, r).await?;
        }

        if diskful && let Some(r) = s.res {
            match self.resize_step(s, r).await? {
                Resize::NotNeeded => {}
                Resize::Done => adjust = true,
                Resize::Waiting => complete = false,
            }
        }

        let due = self
            .adjusted
            .get(&name)
            .is_none_or(|t| t.elapsed() >= self.drbd.adjust_interval);
        if (adjust || (state_adjust && due)) && !skip_adjust {
            self.adjusted
                .insert(name.clone(), tokio::time::Instant::now());
            if s.res.is_none() {
                self.probe_port(s.mine).await?;
            }
            self.adjust(&name).await?;
        }
        Ok(complete)
    }

    /// Create, restore, activate or grow the backing LV to the DRBD gross
    /// size rounded to the extent. Growth moves the end of
    /// the device where internal metadata lives, so an existing LV grows
    /// only under an attached disk (DRBD relocates its metadata online) or
    /// right after this pass created it (a restore of a smaller snapshot).
    /// An LV this node had reported is rebuilt empty: its peers hold the
    /// data, and a restore would bring back data older than theirs.
    async fn ensure_drbd_lv(
        &self,
        reads: &mut Reads,
        snapshots: &[Arc<Snapshot>],
        s: &Step<'_>,
        place: &Place,
    ) -> Result<Backing, String> {
        let (vol, prev) = (s.vol, s.prev);
        let attached = s.res.and_then(|r| r.devices.get(&VOLUME)).is_some_and(|d| {
            matches!(
                d.disk.as_deref(),
                Some("UpToDate" | "Consistent" | "Outdated" | "Inconsistent")
            )
        });
        let lv = lvm::names::volume_lv(&vol.name_any());
        let vg = place.vg.as_str();
        let rows = reads.lvs(&self.lvm, vg).await?;
        let mut row = find_in_pool(&rows, &lv, &place.thin)?.cloned();
        let extent = reads
            .vgs(&self.lvm, vg)
            .await?
            .first()
            .map(|r| r.vg_extent_size)
            .ok_or_else(|| format!("volume group {vg} not found"))?;
        let desired = storage::desired_kib(gross_kib(vol)? * 1024, extent);
        let rebuilt = row.is_none() && lost_after_report(prev);
        if rebuilt {
            tracing::warn!(lv, vg, kib = desired, "rebuilding lost backing LV");
            let created = self
                .lvm
                .create_tagged(vg, &place.thin, &lv, desired, Some(storage::NEW_TAG))
                .await;
            reads.invalidate(vg);
            created.map_err(|e| rebuild_error(&e.to_string()))?;
            row = reads.find(&self.lvm, vg, &lv, &place.thin).await?;
        } else if row.is_none() {
            let created = self
                .create_lv(&rows, snapshots, place, vol, &lv, desired)
                .await;
            reads.invalidate(vg);
            created?;
            if vol.spec.source.is_some() {
                self.strip_inherited(reads, place, &lv).await?;
            }
            row = reads.find(&self.lvm, vg, &lv, &place.thin).await?;
        }
        let created = row.is_some() && rows.iter().all(|r| r.lv_name != lv);
        if created {
            reads.created.insert(format!("{vg}/{lv}"));
        }
        let may_grow = attached || created;
        let r = row.ok_or_else(|| format!("LV {vg}/{lv} missing after create"))?;
        let mut changed = false;
        if !r.is_active() {
            changed = true;
            self.lvm
                .activate(vg, &lv)
                .await
                .map_err(|e| e.to_string())?;
        }
        let mut grown = true;
        if r.lv_size < desired && !may_grow {
            tracing::info!(lv, "backing LV grows once the DRBD disk is attached");
            grown = false;
        } else if r.lv_size < desired {
            tracing::info!(
                lv,
                from = r.lv_size,
                to = desired,
                "growing DRBD backing LV"
            );
            changed = true;
            self.lvm
                .resize(vg, &lv, desired)
                .await
                .map_err(|e| e.to_string())?;
        }
        let row = if changed {
            reads.invalidate(vg);
            reads
                .find(&self.lvm, vg, &lv, &place.thin)
                .await?
                .ok_or_else(|| format!("LV {vg}/{lv} vanished"))?
        } else {
            r
        };
        Ok(Backing {
            row,
            grown,
            created,
            rebuilt,
        })
    }

    /// Decided from what is on the disk, never from
    /// status (a failed pass reports present false while data is there).
    /// Returns whether metadata was created.
    async fn ensure_metadata(
        &self,
        reads: &mut Reads,
        s: &Step<'_>,
        place: &Place,
        b: &Backing,
    ) -> Result<bool, String> {
        let row = &b.row;
        let attached = s
            .res
            .and_then(|r| r.devices.get(&VOLUME))
            .is_some_and(|d| drbd::disk_has_metadata(d.disk.as_deref()));
        if attached {
            return Ok(false);
        }
        let dev = row.lv_path.as_str();
        let name = s.vol.name_any();
        let mut tags = reads.tags(&self.lvm, &place.vg, &row.lv_name).await?;
        let own = storage::md_tag(s.mine.node_id);
        let has = |t: &str| tags.iter().any(|x| x == t);
        // Made by us and never attached, in this pass or one cut short.
        let fresh_lv = b.created || has(storage::NEW_TAG);
        let reported = lost_after_report(s.prev);
        // An entry goes only through our own departure or the controller's
        // cleanup of a stale node: no entry is the departure signal.
        let has_entry = s.prev.is_some();
        let rebuilt = b.rebuilt || (fresh_lv && reported);
        let restored = fresh_lv && !rebuilt && s.vol.spec.source.is_some();
        if has(&own) {
            if restored {
                return Err(format!(
                    "restored LV still carries the {own} tag of its source"
                ));
            }
            return Ok(false);
        }
        if has(storage::MD_TAG) && has_entry && !fresh_lv {
            // Legacy tag plus any entry of ours: ours, retag only.
            self.seal(reads, place, &row.lv_name, &tags, &own).await?;
            return Ok(false);
        }
        // Sticky refusals: a later pass finds them in the entry, also inside
        // an LV_LOST-wrapped error, and refuses again without a probe. The
        // tag above, or a fresh LV, clears them.
        let prev_error = s
            .prev
            .and_then(|p| p.last_error.as_deref())
            .map(unwrap_rebuild);
        if !fresh_lv && let Some(e @ (MD_NOT_AT_END | MD_OTHER_VOLUME)) = prev_error {
            return Err(e.to_owned());
        }
        let day0 = s.a.gi.as_deref().ok_or("assignment has no gi")?;
        let day0_id = u64::from_str_radix(day0, 16)
            .map_err(|_| format!("assignment gi {day0} is not hex"))?;
        let mut magic = self.md_magic(dev).await?;
        if !magic && !fresh_lv {
            magic = self.check_resize(s, dev).await?;
        }
        let initialized = s
            .vol
            .status
            .as_ref()
            .is_some_and(|st| st.initialized_at.is_some());
        let foreign = if magic {
            match self.md_identity(s, dev, day0_id).await? {
                // Only a node with no entry at all on an initialized
                // volume is a departed node placed again; an error never costs data.
                MdIdentity::Ours if !fresh_lv && (has_entry || !initialized) => {
                    if let Err(e) = self.seal(reads, place, &row.lv_name, &tags, &own).await {
                        tracing::warn!(volume = name, error = %e, "tagging adopted metadata failed");
                    }
                    return Ok(false);
                }
                MdIdentity::Unfinished => false,
                // The source's metadata only on a copy we made, or before
                // any replica ever held data.
                MdIdentity::Other if s.vol.spec.source.is_some() && (fresh_lv || !initialized) => {
                    true
                }
                MdIdentity::Other if !fresh_lv => return Err(MD_OTHER_VOLUME.to_owned()),
                // An LV of ours never attached proves nothing by its ids;
                // so a departed node's stale metadata lands here too.
                MdIdentity::Ours | MdIdentity::Other => {
                    tags = self.wipe_stale(reads, place, s, dev, &tags).await?;
                    false
                }
            }
        } else {
            let never_held = !initialized && !s.prev.is_some_and(|p| p.present);
            if !fresh_lv && !never_held {
                return Err(MD_NOT_AT_END.to_owned());
            }
            false
        };
        // Nobody writes before initializedAt, so only a replica created
        // before it by a node that never reported may claim day-0 data.
        // Any other keeps just-created metadata and gets a full sync.
        let fresh = !initialized && !reported && !rebuilt;
        tracing::info!(volume = name, dev, foreign, fresh, "creating DRBD metadata");
        self.drbd_cmd(
            &argv::create_md(&name, model::keys::PEER_SLOTS, s.versions),
            drbd::CREATE_MD_TIMEOUT,
        )
        .await
        .map_err(|e| format!("create-md {name}/{VOLUME}: {e}"))?;
        if fresh {
            self.set_day0_gi(s, dev, day0).await?;
        }
        self.seal(reads, place, &row.lv_name, &tags, &own).await?;
        Ok(true)
    }

    /// Moves internal metadata after an offline grow, from the last known
    /// device size drbd-utils keeps under /var/lib/drbd. True when the magic
    /// is at the end afterwards.
    async fn check_resize(&self, s: &Step<'_>, dev: &str) -> Result<bool, String> {
        let argv = argv::check_resize(s.minor, dev);
        match run_any(self.lvm.exec(), &argv, DEFAULT_TIMEOUT).await {
            Ok(o) if o.exit_code == Some(0) => return self.md_magic(dev).await,
            Ok(o) => {
                tracing::debug!(volume = s.vol.name_any(), error = %failure(&argv, &o), "check-resize");
            }
            Err(e) => tracing::debug!(volume = s.vol.name_any(), error = %e, "check-resize"),
        }
        Ok(false)
    }

    /// The metadata tags give way to the new tag first, in one lvchange,
    /// so a crash after the wipe leaves a fresh LV, never one that is
    /// untagged or legacy-tagged without metadata. Returns the tags now on
    /// the LV.
    async fn wipe_stale(
        &self,
        reads: &mut Reads,
        place: &Place,
        s: &Step<'_>,
        dev: &str,
        tags: &[String],
    ) -> Result<Vec<String>, String> {
        let name = s.vol.name_any();
        tracing::warn!(volume = name, dev, "stale metadata: wiping");
        let del: Vec<&str> = tags
            .iter()
            .map(String::as_str)
            .filter(|t| storage::is_md_tag(t))
            .collect();
        let has_new = tags.iter().any(|t| t == storage::NEW_TAG);
        let mut now: Vec<String> = tags
            .iter()
            .filter(|t| !storage::is_md_tag(t))
            .cloned()
            .collect();
        if !has_new || !del.is_empty() {
            let add: &[&str] = if has_new { &[] } else { &[storage::NEW_TAG] };
            let r = self
                .lvm
                .change_tags(&place.vg, &lvm::names::volume_lv(&name), add, &del)
                .await;
            reads.tags.remove(&place.vg);
            r.map_err(|e| e.to_string())?;
            if !has_new {
                now.push(storage::NEW_TAG.to_owned());
            }
        }
        self.drbd_cmd(&argv::wipe_md(s.minor, dev), DEFAULT_TIMEOUT)
            .await?;
        Ok(now)
    }

    /// Metadata proven for this node id: `own` in, the new tag and every
    /// other metadata tag out, in one lvchange.
    async fn seal(
        &self,
        reads: &mut Reads,
        place: &Place,
        lv: &str,
        tags: &[String],
        own: &str,
    ) -> Result<(), String> {
        let del: Vec<&str> = tags
            .iter()
            .map(String::as_str)
            .filter(|t| *t != own && (*t == storage::NEW_TAG || storage::is_md_tag(t)))
            .collect();
        let r = self.lvm.change_tags(&place.vg, lv, &[own], &del).await;
        reads.tags.remove(&place.vg);
        r.map_err(|e| e.to_string())
    }

    /// A restored copy carries its snapshot's tags: the metadata tags go,
    /// the new tag lvcreate set stays. A copy that cannot be cleaned is
    /// removed; it holds nothing of this volume yet.
    async fn strip_inherited(
        &self,
        reads: &mut Reads,
        place: &Place,
        lv: &str,
    ) -> Result<(), String> {
        let vg = place.vg.as_str();
        let res = match reads.tags(&self.lvm, vg, lv).await {
            Ok(tags) => {
                let del: Vec<&str> = tags
                    .iter()
                    .map(String::as_str)
                    .filter(|t| storage::is_md_tag(t))
                    .collect();
                if del.is_empty() {
                    return Ok(());
                }
                let r = self
                    .lvm
                    .change_tags(vg, lv, &[], &del)
                    .await
                    .map_err(|e| e.to_string());
                reads.tags.remove(vg);
                r
            }
            Err(e) => Err(e),
        };
        let Err(e) = res else { return Ok(()) };
        reads.invalidate(vg);
        if let Ok(Some(row)) = reads.find(&self.lvm, vg, lv, &place.thin).await {
            let removed = self.remove_volume(vg, &row).await;
            reads.invalidate(vg);
            if let Err(re) = removed {
                tracing::warn!(lv, error = %re, "removing the restored copy");
            }
        }
        Err(format!(
            "remove inherited metadata tags from {vg}/{lv}: {e}"
        ))
    }

    /// The initial node mints the current id, the
    /// others take day 0, so the first sync is a bitmap sync of nothing.
    async fn set_day0_gi(&self, s: &Step<'_>, dev: &str, day0: &str) -> Result<(), String> {
        let winner = s.a.initial_up_to_date_node.as_deref() == Some(self.node.as_str());
        let current = if winner {
            let r = self
                .drbd
                .host
                .random()
                .map_err(|e| format!("random generation id: {e}"))?;
            Some(drbd::fresh_gi(r))
        } else {
            None
        };
        for id in 0..=model::keys::NODE_ID_MAX {
            let gi = drbd::gi_value(id, s.mine.node_id, day0, current.as_deref());
            self.drbd_cmd(&argv::set_gi(id, s.minor, dev, &gi), DEFAULT_TIMEOUT)
                .await?;
        }
        Ok(())
    }

    /// get-gi of this node's slot; when day0 is not among its two history
    /// ids, dump-superblock with the full history decides.
    async fn md_identity(&self, s: &Step<'_>, dev: &str, day0: u64) -> Result<MdIdentity, String> {
        let argv = argv::get_gi(s.mine.node_id, s.minor, dev);
        let out = run_any(self.lvm.exec(), &argv, DEFAULT_TIMEOUT)
            .await
            .map_err(|e| e.to_string())?;
        match out.exit_code {
            Some(0) => {}
            Some(1 | 255) => {
                return Err(format!(
                    "corrupt DRBD metadata on {dev}; check that drbd-utils match the kernel module"
                ));
            }
            _ => return Err(failure(&argv, &out).to_string()),
        }
        let ids = gi_ids(out.stdout.lines().next().unwrap_or_default());
        if ids.iter().any(|i| same_id(*i, day0)) {
            return Ok(MdIdentity::Ours);
        }
        if ids.first() == Some(&4) && ids.iter().skip(1).all(|i| *i == 0) {
            return Ok(MdIdentity::Unfinished);
        }
        let argv = argv::dump_superblock(s.minor, dev);
        let out = run_ok(self.lvm.exec(), &argv, drbd::SUPERBLOCK_TIMEOUT)
            .await
            .map_err(|e| e.to_string())?;
        Ok(if gi_ids(&out.stdout).iter().any(|i| same_id(*i, day0)) {
            MdIdentity::Ours
        } else {
            MdIdentity::Other
        })
    }

    /// Connections to peers no longer in the assignment; true when any was removed.
    async fn remove_departed(
        &self,
        s: &Step<'_>,
        name: &str,
        diskful: bool,
    ) -> Result<bool, String> {
        let Some(r) = s.res else { return Ok(false) };
        let mut removed = false;
        for (conn, c) in &r.connections {
            if s.a.replicas.iter().any(|x| &x.node == conn) {
                continue;
            }
            let Some(id) = c.peer_node_id else { continue };
            let peer_diskless = r
                .peer_devices
                .get(&(conn.clone(), VOLUME))
                .is_some_and(|p| {
                    p.peer_client == Some(true) || p.peer_disk.as_deref() == Some("Diskless")
                });
            self.forget_peer(name, conn, id, diskful, peer_diskless)
                .await?;
            removed = true;
        }
        Ok(removed)
    }

    /// On the remaining nodes: del-peer, then forget-peer on a diskful replica.
    async fn forget_peer(
        &self,
        name: &str,
        peer: &str,
        id: u8,
        local_diskful: bool,
        peer_diskless: bool,
    ) -> Result<(), String> {
        tracing::info!(volume = name, peer, id, "removing departed peer");
        let del = self
            .drbd_cmd(&argv::del_peer(name, id), DEFAULT_TIMEOUT)
            .await
            .err();
        if !local_diskful {
            return del.map_or(Ok(()), Err);
        }
        match self
            .drbd_cmd(&argv::forget_peer(name, id), DEFAULT_TIMEOUT)
            .await
        {
            Ok(()) => Ok(()),
            Err(_) if peer_diskless => Ok(()),
            Err(fe) => match del {
                // del-peer removed the connection, so the slot error is moot.
                None => {
                    tracing::warn!(volume = name, peer, error = %fe, "forget-peer failed after del-peer");
                    Ok(())
                }
                Some(de) => Err(de),
            },
        }
    }

    /// Only the diskful replica with the lowest node id resizes,
    /// once every diskful peer reports an LV of the new gross size.
    async fn resize_step(&self, s: &Step<'_>, r: &Resource) -> Result<Resize, String> {
        if !r.devices.contains_key(&VOLUME) {
            return Ok(Resize::NotNeeded);
        }
        let current_kib = self
            .sectors(s.minor)
            .await
            .map_err(|e| format!("size of {}: {e}", drbd::device_path(s.minor)))?
            / 2;
        if current_kib >= net_kib(s.vol) {
            return Ok(Resize::NotNeeded);
        }
        let diskful = || s.a.replicas.iter().filter(|x| x.role == Role::Diskful);
        if diskful().map(|x| x.node_id).min() != Some(s.mine.node_id) {
            return Ok(Resize::NotNeeded);
        }
        let gross = gross_kib(s.vol)? * 1024;
        let entries = s
            .vol
            .status
            .as_ref()
            .map(|st| st.replicas.as_slice())
            .unwrap_or_default();
        let peers_grown = diskful().filter(|x| x.node != self.node).all(|x| {
            entries
                .iter()
                .any(|e| e.node == x.node && e.allocated_bytes.is_some_and(|b| b >= gross))
        });
        let up_to_date = r
            .devices
            .get(&VOLUME)
            .is_some_and(|d| d.disk.as_deref() == Some("UpToDate"));
        if !peers_grown || !up_to_date {
            return Ok(Resize::Waiting);
        }
        let name = s.vol.name_any();
        tracing::info!(volume = name, from_kib = current_kib, "growing DRBD device");
        self.drbd_cmd(&argv::wait_connect(&name), DEFAULT_TIMEOUT)
            .await?;
        self.drbd_cmd(&argv::resize(&name), DEFAULT_TIMEOUT).await?;
        Ok(Resize::Done)
    }

    /// Spec.resolve: the non-survivor discards its data once per epoch; the
    /// survivor reconnects its StandAlone diskful connections. Returns true
    /// when this pass must not adjust.
    async fn resolve(&mut self, s: &Step<'_>, rv: &Resolve, r: &Resource) -> Result<bool, String> {
        let name = s.vol.name_any();
        // Only while the controller's latched SplitBrain holds: a resolve
        // left over from an earlier split must not discard a later good side.
        let split = s.vol.status.as_ref().is_some_and(|st| {
            model::condition::is_true(&st.conditions, model::condition::types::SPLIT_BRAIN)
        });
        if !split {
            return Ok(false);
        }
        let survivor_diskful =
            s.a.replicas
                .iter()
                .any(|x| x.node == rv.survivor && x.role == Role::Diskful);
        if !survivor_diskful {
            tracing::warn!(
                volume = name,
                survivor = rv.survivor,
                "resolve survivor is not a diskful replica: ignored"
            );
            return Ok(false);
        }
        if rv.survivor == self.node {
            let mut connected = false;
            for (peer, c) in &r.connections {
                let diskful_peer =
                    s.a.replicas
                        .iter()
                        .any(|x| &x.node == peer && x.role == Role::Diskful);
                if diskful_peer && c.state == "StandAlone" {
                    tracing::info!(volume = name, peer, "survivor reconnecting");
                    self.drbd_cmd(&argv::connect_peer(&name, peer), DEFAULT_TIMEOUT)
                        .await?;
                    connected = true;
                }
            }
            return Ok(connected);
        }
        let standalone = r
            .connections
            .get(&rv.survivor)
            .is_some_and(|c| c.state == "StandAlone");
        let key = (
            name.clone(),
            rv.survivor.clone(),
            s.vol.metadata.generation.unwrap_or_default(),
        );
        if !standalone || self.resolved.contains(&key) {
            return Ok(false);
        }
        if r.is_primary() || s.prev.is_some_and(|p| p.in_use) {
            return Err(format!(
                "resolve needs the volume unpublished on {}",
                self.node
            ));
        }
        tracing::warn!(
            volume = name,
            survivor = rv.survivor,
            "discarding local data to resolve split brain"
        );
        self.drbd_cmd(&argv::disconnect(&name), DEFAULT_TIMEOUT)
            .await?;
        self.drbd_cmd(&argv::connect_discard(&name), DEFAULT_TIMEOUT)
            .await?;
        self.resolved.insert(key);
        Ok(true)
    }

    async fn probe_port(&self, mine: &AssignedReplica) -> Result<(), String> {
        let addr = self
            .addresses
            .get(&self.node)
            .ok_or_else(|| format!("node {} has no InternalIP", self.node))?;
        let port = mine.port.ok_or("assignment has no port for this node")?;
        let (host, ip) = (self.drbd.host.clone(), *addr);
        let busy = tokio::task::spawn_blocking(move || host.port_in_use(ip, port))
            .await
            .map_err(|e| e.to_string())?;
        if busy {
            return Err(format!("DRBD port {port} on {addr} is in use"));
        }
        Ok(())
    }

    /// Retried while a resync blocks it; the backup
    /// file follows a success and is restored after a failure.
    async fn adjust(&self, name: &str) -> Result<(), String> {
        let argv = argv::adjust(name);
        let mut attempt = 0;
        let result = loop {
            match run_any(self.lvm.exec(), &argv, DEFAULT_TIMEOUT).await {
                Ok(o) if o.exit_code == Some(0) => break Ok(()),
                Ok(o)
                    if attempt < self.drbd.adjust_retry.len() && o.stderr.contains(RESYNC_BUSY) =>
                {
                    tokio::time::sleep(self.drbd.adjust_retry[attempt]).await;
                    attempt += 1;
                }
                Ok(o) => break Err(failure(&argv, &o).to_string()),
                Err(e) => break Err(e.to_string()),
            }
        };
        match result {
            Ok(()) => self.drbd.files.backup(name),
            Err(e) => {
                if let Err(re) = self.drbd.files.restore_backup(name) {
                    tracing::warn!(volume = name, error = %re, "backup restore failed");
                }
                Err(format!("adjust {name}: {e}"))
            }
        }
    }

    /// `done` only comes with events2 statistics; ask the status command
    /// while syncing without it.
    async fn sync_fallback(&self, name: &str, res: Option<&Resource>) -> Option<u32> {
        if !drbd::derive(res).sync_unknown {
            return None;
        }
        let argv = crate::exec::argv(&["drbdsetup", "status", "--json", name]);
        let out = run_ok(self.lvm.exec(), &argv, DEFAULT_TIMEOUT).await.ok()?;
        drbd::json_sync_percent(&out.stdout)
    }

    pub(super) async fn suspend_resume(
        &self,
        volume: &str,
        phase: SnapshotPhase,
    ) -> Result<(), String> {
        let r = self
            .tracked(volume)
            .ok_or_else(|| format!("DRBD resource {volume} is not up"))?;
        match phase {
            SnapshotPhase::Suspending if !r.suspended_by_user() => {
                tracing::info!(volume, "suspending IO for a snapshot");
                self.drbd_cmd(&argv::suspend_io(volume), DEFAULT_TIMEOUT)
                    .await
            }
            SnapshotPhase::Resuming if r.suspended_by_user() => {
                tracing::info!(volume, "resuming IO after a snapshot");
                self.drbd_cmd(&argv::resume_io(volume), DEFAULT_TIMEOUT)
                    .await
            }
            _ => Ok(()),
        }
    }

    /// Status of the replicated volumes events2 touched since the last call,
    /// from the last pass facts; no host work besides the sysfs size.
    pub async fn refresh(&mut self, volumes: &[Arc<Volume>]) -> Vec<StatusWrite> {
        let dirty = match self.drbd.tracker.lock() {
            Ok(mut t) if t.is_available() => t.take_dirty(),
            _ => return Vec::new(),
        };
        let mut writes = Vec::new();
        for name in dirty {
            let Some(vol) = volumes.iter().find(|v| v.name_any() == name) else {
                continue;
            };
            let Some(facts) = self.facts.get(&name).cloned() else {
                continue;
            };
            if vol.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let prev = self.prev_entry(vol);
            let entry = self.current_entry(&name, &facts).await;
            if prev.as_ref() != Some(&entry) {
                writes.push(StatusWrite::Volume { name, entry });
            }
        }
        writes
    }
}

/// The inputs of one replica's pass.
struct Step<'a> {
    vol: &'a Volume,
    a: &'a Assignment,
    mine: &'a AssignedReplica,
    versions: &'a ToolVersions,
    minor: u32,
    res: Option<&'a Resource>,
    prev: Option<&'a ReplicaStatus>,
}

#[cfg(test)]
mod tests;
