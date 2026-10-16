//! One reconciliation pass: pools, then volumes in name order, then
//! snapshots. Host work first; the pass returns the status writes, which
//! the caller applies last.
//!
//! Rule: an LV is removed only when its Volume has a deletionTimestamp, or,
//! for a snapshot LV, when its Snapshot is Deleting. A replicated volume
//! that drops this node from its assignment without a deletionTimestamp only
//! takes DRBD down: an assignment entry can vanish by an edit or a bug, so
//! the LV and its metadata stay as an orphan.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, ResourceExt};
use lvm::report::{LvRow, VgRow};
use model::condition::{self, Condition};
use model::{
    AssignedReplica, Pool, ReplicaStatus, Role, Snapshot, SnapshotAck, SnapshotPhase, Volume,
};
use serde_json::json;

use crate::Exec;
use crate::drbd::Facts;
use crate::events2::Tracker;
use crate::storage::{self, Lvm, PoolCapacity};

mod replicated;
pub use replicated::DrbdEnv;

const DISK_UP_TO_DATE: &str = "UpToDate";
const REASON_READY: &str = "ThinPoolReady";
const REASON_UNAVAILABLE: &str = "ThinPoolUnavailable";
const REASON_ZEROING_DISABLED: &str = "ZeroingDisabled";
const REASON_INVALID_NAME: &str = "InvalidName";
const NO_DRBD_LOCAL: &str = "replicas 1 volume: no DRBD to suspend or resume";
pub(crate) const LV_LOST: &str = model::keys::LV_LOST;

/// One server-side apply on a status subresource, this node's part only.
#[derive(Debug, Clone, PartialEq)]
pub enum StatusWrite {
    Pool {
        name: String,
        generation: Option<i64>,
        capacity: Result<PoolCapacity, String>,
        available: Condition,
    },
    Volume {
        name: String,
        entry: ReplicaStatus,
    },
    /// This node left the assignment: an empty list drops the entry it owns.
    VolumeGone {
        name: String,
    },
    Snapshot {
        name: String,
        ack: SnapshotAck,
    },
}

impl StatusWrite {
    pub fn name(&self) -> &str {
        match self {
            Self::Pool { name, .. }
            | Self::Volume { name, .. }
            | Self::VolumeGone { name }
            | Self::Snapshot { name, .. } => name,
        }
    }

    /// The apply body. Only fields this agent owns appear: never
    /// status.volumes of a Pool, never another node's entry.
    pub fn body(&self) -> serde_json::Value {
        let api_version = format!("{}/{}", model::GROUP, model::VERSION);
        match self {
            Self::Pool {
                name,
                generation,
                capacity,
                available,
            } => {
                let mut status = json!({ "conditions": [available] });
                if let Some(g) = generation {
                    status["observedGeneration"] = json!(g);
                }
                if let Ok(c) = capacity {
                    status["totalBytes"] = json!(c.total_bytes);
                    status["freeBytes"] = json!(c.free_bytes);
                    status["chunkBytes"] = json!(c.chunk_bytes);
                }
                json!({
                    "apiVersion": api_version,
                    "kind": "Pool",
                    "metadata": { "name": name },
                    "status": status,
                })
            }
            Self::Volume { name, entry } => json!({
                "apiVersion": api_version,
                "kind": "Volume",
                "metadata": { "name": name },
                "status": { "replicas": [entry] },
            }),
            Self::VolumeGone { name } => json!({
                "apiVersion": api_version,
                "kind": "Volume",
                "metadata": { "name": name },
                "status": { "replicas": [] },
            }),
            Self::Snapshot { name, ack } => json!({
                "apiVersion": api_version,
                "kind": "Snapshot",
                "metadata": { "name": name },
                "status": { "acks": [ack] },
            }),
        }
    }

    pub async fn apply(&self, client: &Client, node: &str) -> Result<(), kube::Error> {
        let params = PatchParams::apply(&model::keys::agent_field_manager(node)).force();
        let patch = Patch::Apply(self.body());
        match self {
            Self::Pool { name, .. } => {
                Api::<Pool>::all(client.clone())
                    .patch_status(name, &params, &patch)
                    .await?;
            }
            Self::Volume { name, .. } | Self::VolumeGone { name } => {
                Api::<Volume>::all(client.clone())
                    .patch_status(name, &params, &patch)
                    .await?;
            }
            Self::Snapshot { name, .. } => {
                Api::<Snapshot>::all(client.clone())
                    .patch_status(name, &params, &patch)
                    .await?;
            }
        }
        Ok(())
    }
}

/// lvs and vgs reads of one pass, per VG; a mutation drops the VG's entry.
#[derive(Debug, Default)]
struct Reads {
    lvs: HashMap<String, Result<Vec<LvRow>, String>>,
    vgs: HashMap<String, Result<Vec<VgRow>, String>>,
    /// Tags per LV, read only when a decision needs them.
    tags: HashMap<String, Result<HashMap<String, Vec<String>>, String>>,
    /// `vg/lv` of the LVs this pass created; never snapshotted in the same pass.
    created: HashSet<String>,
}

impl Reads {
    async fn lvs(&mut self, lvm: &Lvm, vg: &str) -> Result<Vec<LvRow>, String> {
        if let Some(r) = self.lvs.get(vg) {
            return r.clone();
        }
        let r = lvm.lvs(vg).await.map_err(|e| e.to_string());
        self.lvs.insert(vg.to_owned(), r.clone());
        r
    }

    async fn vgs(&mut self, lvm: &Lvm, vg: &str) -> Result<Vec<VgRow>, String> {
        if let Some(r) = self.vgs.get(vg) {
            return r.clone();
        }
        let r = lvm.vgs(vg).await.map_err(|e| e.to_string());
        self.vgs.insert(vg.to_owned(), r.clone());
        r
    }

    /// The LV `name` of the Pool's thin pool, from a (possibly fresh) read.
    async fn find(
        &mut self,
        lvm: &Lvm,
        vg: &str,
        name: &str,
        thin: &str,
    ) -> Result<Option<LvRow>, String> {
        let rows = self.lvs(lvm, vg).await?;
        find_in_pool(&rows, name, thin).map(Option::<&LvRow>::cloned)
    }

    /// Tags of one LV (empty when it has none).
    async fn tags(&mut self, lvm: &Lvm, vg: &str, lv: &str) -> Result<Vec<String>, String> {
        if !self.tags.contains_key(vg) {
            let r = lvm.tags(vg).await.map_err(|e| e.to_string());
            self.tags.insert(vg.to_owned(), r);
        }
        match &self.tags[vg] {
            Ok(m) => Ok(m.get(lv).cloned().unwrap_or_default()),
            Err(e) => Err(e.clone()),
        }
    }

    fn invalidate(&mut self, vg: &str) {
        self.lvs.remove(vg);
        self.vgs.remove(vg);
        self.tags.remove(vg);
    }
}

/// What a snapshot copies.
enum Origin {
    /// A replicas 1 LV: no DRBD, no tags.
    Local,
    /// A DRBD backing LV, with why it must not be copied now, if anything.
    Drbd(Option<String>),
}

/// Where an LV of this node lives.
#[derive(Debug, Clone)]
struct Place {
    pool: String,
    vg: String,
    thin: String,
}

#[derive(Debug)]
pub struct Reconciler {
    node: String,
    lvm: Lvm,
    orphans_logged: HashSet<String>,
    drbd: DrbdEnv,
    /// DRBD address of every node, from its InternalIP.
    addresses: BTreeMap<String, IpAddr>,
    /// (volume, survivor, generation) of the split-brain resolutions done.
    resolved: HashSet<(String, String, i64)>,
    /// Last pass facts of each replicated volume, for event refreshes.
    facts: HashMap<String, Facts>,
    /// When each resource was last adjusted, for the rate limit.
    adjusted: HashMap<String, tokio::time::Instant>,
    /// This node's Pools whose vg or thinPool name failed the check this pass.
    invalid_pools: HashSet<String>,
}

impl Reconciler {
    /// Without [`Self::with_drbd`] replicated volumes report that DRBD is missing.
    pub fn new(node: impl Into<String>, exec: Arc<dyn Exec>) -> Self {
        Self {
            node: node.into(),
            lvm: Lvm::new(exec),
            orphans_logged: HashSet::new(),
            drbd: DrbdEnv::new(
                None,
                model::keys::RES_DIR,
                Arc::new(Mutex::new(Tracker::default())),
            ),
            addresses: BTreeMap::new(),
            resolved: HashSet::new(),
            facts: HashMap::new(),
            adjusted: HashMap::new(),
            invalid_pools: HashSet::new(),
        }
    }

    #[must_use]
    pub fn with_drbd(mut self, drbd: DrbdEnv) -> Self {
        self.drbd = drbd;
        self
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn tracker(&self) -> Arc<Mutex<Tracker>> {
        self.drbd.tracker.clone()
    }

    pub fn set_addresses(&mut self, addresses: BTreeMap<String, IpAddr>) {
        self.addresses = addresses;
    }

    /// One pass over the current objects. Returns only writes that change
    /// something, so an idle node writes nothing.
    pub async fn pass(
        &mut self,
        pools: &[Arc<Pool>],
        volumes: &[Arc<Volume>],
        snapshots: &[Arc<Snapshot>],
    ) -> Vec<StatusWrite> {
        let mut reads = Reads::default();
        let mut writes = Vec::new();

        let mut pools: Vec<&Pool> = pools
            .iter()
            .map(AsRef::as_ref)
            .filter(|p| p.spec.node == self.node)
            .collect();
        pools.sort_by_key(|p| p.name_any());
        for pool in &pools {
            if let Some(w) = self.pool(&mut reads, pool).await {
                writes.push(w);
            }
        }
        // A bad name never reaches an lvm argv, not even through lvs.
        self.invalid_pools = pools
            .iter()
            .filter(|p| invalid_name(p).is_some())
            .map(|p| p.name_any())
            .collect();
        pools.retain(|p| invalid_name(p).is_none());

        let mut vols: Vec<&Volume> = volumes.iter().map(AsRef::as_ref).collect();
        vols.sort_by_key(|v| v.name_any());
        for vol in &vols {
            if let Some(w) = self.volume(&mut reads, &pools, snapshots, vol).await {
                writes.push(w);
            }
        }

        let mut snaps: Vec<&Snapshot> = snapshots.iter().map(AsRef::as_ref).collect();
        snaps.sort_by_key(|s| s.name_any());
        for snap in &snaps {
            if let Some(w) = self.snapshot(&mut reads, &pools, &vols, snap).await {
                writes.push(w);
            }
        }

        self.log_orphans(&mut reads, &pools, &vols, &snaps).await;
        writes
    }

    async fn pool(&self, reads: &mut Reads, pool: &Pool) -> Option<StatusWrite> {
        let (vg, thin) = (&pool.spec.vg, &pool.spec.thin_pool);
        let probed = match invalid_name(pool) {
            Some(e) => Err((REASON_INVALID_NAME, e)),
            None => self.probe_pool(reads, vg, thin).await,
        };
        let generation = pool.metadata.generation;
        let (status, reason, message) = match &probed {
            Ok(_) => (condition::TRUE, REASON_READY, String::new()),
            Err((reason, e)) => (condition::FALSE, *reason, e.clone()),
        };
        let capacity = probed.map_err(|(_, e)| e);
        if let Err(e) = &capacity {
            tracing::warn!(pool = pool.name_any(), error = %e, "pool unavailable");
        }
        let current = pool.status.clone().unwrap_or_default();
        let mut conditions = current.conditions.clone();
        condition::set(
            &mut conditions,
            condition::new(
                condition::types::AVAILABLE,
                status,
                reason,
                message,
                generation,
                Time(k8s_openapi::jiff::Timestamp::now()),
            ),
        );
        let available = condition::get(&conditions, condition::types::AVAILABLE)?.clone();
        let cap = capacity.as_ref().ok();
        let unchanged = current.total_bytes == cap.map(|c| c.total_bytes)
            && current.free_bytes == cap.map(|c| c.free_bytes)
            && current.chunk_bytes == cap.map(|c| c.chunk_bytes)
            && current.observed_generation == generation
            && condition::get(&current.conditions, condition::types::AVAILABLE) == Some(&available);
        (!unchanged).then(|| StatusWrite::Pool {
            name: pool.name_any(),
            generation,
            capacity,
            available,
        })
    }

    /// Capacity of a usable thin pool. Pool zeroing is switched on first:
    /// without it a new thin LV can expose blocks of deleted ones.
    async fn probe_pool(
        &self,
        reads: &mut Reads,
        vg: &str,
        thin: &str,
    ) -> Result<PoolCapacity, (&'static str, String)> {
        let unavailable = |e: String| (REASON_UNAVAILABLE, e);
        let mut lvs = reads.lvs(&self.lvm, vg).await.map_err(unavailable)?;
        if lvs.iter().any(|r| r.lv_name == thin && !r.is_zeroing()) {
            tracing::info!(vg, thin, "enabling thin pool zeroing");
            let res = self.lvm.thinpool_zero(vg, thin).await;
            reads.invalidate(vg);
            res.map_err(|e| (REASON_ZEROING_DISABLED, e.to_string()))?;
            lvs = reads.lvs(&self.lvm, vg).await.map_err(unavailable)?;
        }
        let vgs = reads.vgs(&self.lvm, vg).await.map_err(unavailable)?;
        storage::pool_capacity(&lvs, &vgs, thin).map_err(unavailable)
    }

    fn my_replica<'a>(&self, vol: &'a Volume) -> Option<&'a AssignedReplica> {
        vol.spec
            .assignment
            .as_ref()?
            .replicas
            .iter()
            .find(|r| r.node == self.node)
    }

    /// This node's diskful replica of the volume.
    fn local_replica<'a>(&self, vol: &'a Volume) -> Option<&'a AssignedReplica> {
        self.my_replica(vol).filter(|r| r.role == Role::Diskful)
    }

    fn place(&self, pools: &[&Pool], pool_name: Option<&str>) -> Result<Place, String> {
        let name = pool_name.ok_or_else(|| "assignment has no pool".to_owned())?;
        if self.invalid_pools.contains(name) {
            return Err(format!("pool {name} has an invalid vg or thinPool name"));
        }
        let pool = pools
            .iter()
            .find(|p| p.name_any() == name)
            .ok_or_else(|| format!("pool {name} not found on node {}", self.node))?;
        Ok(Place {
            pool: name.to_owned(),
            vg: pool.spec.vg.clone(),
            thin: pool.spec.thin_pool.clone(),
        })
    }

    async fn volume(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snapshots: &[Arc<Snapshot>],
        vol: &Volume,
    ) -> Option<StatusWrite> {
        if vol.spec.replicas >= 2 {
            return self.drbd_volume(reads, pools, snapshots, vol).await;
        }
        let mine = self.local_replica(vol)?;
        let name = vol.name_any();
        let prev = vol
            .status
            .as_ref()
            .and_then(|s| s.replicas.iter().find(|r| r.node == self.node))
            .cloned();
        let entry = match self.place(pools, mine.pool.as_deref()) {
            Ok(place) => {
                self.volume_on(reads, snapshots, vol, &place, prev.as_ref())
                    .await
            }
            Err(e) => self.error_entry(prev.as_ref(), e),
        };
        if let Some(e) = &entry.last_error {
            tracing::warn!(volume = name, error = %e, "volume reconcile failed");
        }
        (prev.as_ref() != Some(&entry)).then_some(StatusWrite::Volume { name, entry })
    }

    fn error_entry(&self, prev: Option<&ReplicaStatus>, error: String) -> ReplicaStatus {
        let mut e = prev.cloned().unwrap_or_else(|| ReplicaStatus {
            node: self.node.clone(),
            ..ReplicaStatus::default()
        });
        e.last_error = Some(keep_lost(prev, error));
        e
    }

    async fn volume_on(
        &self,
        reads: &mut Reads,
        snapshots: &[Arc<Snapshot>],
        vol: &Volume,
        place: &Place,
        prev: Option<&ReplicaStatus>,
    ) -> ReplicaStatus {
        let name = vol.name_any();
        let lv = lvm::names::volume_lv(&name);
        let vg = place.vg.as_str();
        let rows = match reads.lvs(&self.lvm, vg).await {
            Ok(r) => r,
            Err(e) => return self.error_entry(prev, e),
        };
        // A same-named LV elsewhere says nothing about ours: keep the report.
        let mut row = match find_in_pool(&rows, &lv, &place.thin) {
            Ok(r) => r.cloned(),
            Err(e) => return self.error_entry(prev, e),
        };
        let mut error = None;
        let mut changed = false;

        if vol.metadata.deletion_timestamp.is_some() {
            if let Some(r) = &row {
                error = self.remove_volume(vg, r).await.err();
                changed = true;
            }
        } else {
            let desired = match reads.vgs(&self.lvm, vg).await.and_then(|v| {
                v.first()
                    .map(|r| storage::desired_kib(vol.spec.size_bytes, r.vg_extent_size))
                    .ok_or_else(|| format!("volume group {vg} not found"))
            }) {
                Ok(d) => d,
                Err(e) => return self.error_entry(prev, e),
            };
            if row.is_none() {
                let res = if lost_after_report(prev) {
                    Err(LV_LOST.to_owned())
                } else {
                    self.create_lv(&rows, snapshots, place, vol, &lv, desired)
                        .await
                };
                match res {
                    Ok(()) => {
                        reads.invalidate(vg);
                        row = match reads.find(&self.lvm, vg, &lv, &place.thin).await {
                            Ok(r) => r,
                            Err(e) => return self.error_entry(prev, e),
                        };
                    }
                    Err(e) => {
                        error = Some(e);
                        changed = true;
                    }
                }
            }
            if let (None, Some(r)) = (&error, &row) {
                if !r.is_active() {
                    changed = true;
                    if let Err(e) = self.lvm.activate(vg, &lv).await {
                        error = Some(e.to_string());
                    }
                }
                if error.is_none() && r.lv_size < desired {
                    tracing::info!(
                        volume = name,
                        from = r.lv_size,
                        to = desired,
                        "growing volume LV"
                    );
                    changed = true;
                    if let Err(e) = self.lvm.resize(vg, &lv, desired).await {
                        error = Some(e.to_string());
                    }
                }
            }
        }

        let row = if changed {
            reads.invalidate(vg);
            match reads.find(&self.lvm, vg, &lv, &place.thin).await {
                Ok(r) => r,
                Err(e) => return self.error_entry(prev, error.unwrap_or(e)),
            }
        } else {
            row
        };
        let observed_generation = if error.is_none() {
            vol.metadata.generation
        } else {
            prev.and_then(|p| p.observed_generation)
        };
        replica_entry(&self.node, row.as_ref(), observed_generation, error)
    }

    /// Create or restore a missing LV. A DRBD backing LV carries the new tag
    /// from lvcreate on, so it never exists untagged.
    async fn create_lv(
        &self,
        rows: &[LvRow],
        snapshots: &[Arc<Snapshot>],
        place: &Place,
        vol: &Volume,
        lv: &str,
        desired: u64,
    ) -> Result<(), String> {
        let tag = (vol.spec.replicas >= 2).then_some(storage::NEW_TAG);
        if let Some(src) = &vol.spec.source {
            return self
                .restore(rows, snapshots, place, &src.snapshot, lv, tag)
                .await;
        }
        tracing::info!(lv, vg = place.vg, kib = desired, "creating volume LV");
        self.lvm
            .create_tagged(&place.vg, &place.thin, lv, desired, tag)
            .await
            .map_err(|e| e.to_string())
    }

    async fn remove_volume(&self, vg: &str, row: &LvRow) -> Result<(), String> {
        tracing::info!(lv = row.lv_name, vg, "removing volume LV");
        if row.is_active() {
            self.lvm
                .deactivate(vg, &row.lv_name)
                .await
                .map_err(|e| e.to_string())?;
        }
        self.lvm
            .remove(vg, &row.lv_name)
            .await
            .map_err(|e| e.to_string())
    }

    /// Restore source: `<Snapshot.spec.volume>_<snapshot>` in the same VG.
    /// A missing source is an error, never an empty LV: the caller asked for data.
    async fn restore(
        &self,
        rows: &[LvRow],
        snapshots: &[Arc<Snapshot>],
        place: &Place,
        snapshot: &str,
        lv: &str,
        tag: Option<&str>,
    ) -> Result<(), String> {
        let snap = snapshots
            .iter()
            .find(|s| s.name_any() == snapshot)
            .ok_or_else(|| format!("source snapshot {snapshot} not found"))?;
        if let Some(pool) = snap
            .status
            .as_ref()
            .and_then(|s| s.acks.iter().find(|a| a.node == self.node))
            .and_then(|a| a.pool.as_deref())
            && pool != place.pool
        {
            return Err(format!(
                "source snapshot {snapshot} lives in pool {pool}, volume assigned to {}",
                place.pool
            ));
        }
        let source = lvm::names::snapshot_lv(&snap.spec.volume, snapshot);
        if find_in_pool(rows, &source, &place.thin)?.is_none() {
            return Err(format!(
                "snapshot LV {}/{source} not found on this node",
                place.vg
            ));
        }
        tracing::info!(
            lv,
            source,
            vg = place.vg,
            "restoring volume LV from snapshot"
        );
        self.lvm
            .restore_tagged(&place.vg, &source, lv, tag)
            .await
            .map_err(|e| e.to_string())
    }

    async fn snapshot(
        &self,
        reads: &mut Reads,
        pools: &[&Pool],
        vols: &[&Volume],
        snap: &Snapshot,
    ) -> Option<StatusWrite> {
        let name = snap.name_any();
        let status = snap.status.clone().unwrap_or_default();
        let prev = status.acks.iter().find(|a| a.node == self.node).cloned();
        let vol = vols.iter().find(|v| v.name_any() == snap.spec.volume);
        let replica = vol.and_then(|v| self.local_replica(v));
        let replicated = vol.filter(|v| v.spec.replicas >= 2);
        if replicated.is_some() && !self.drbd_available() {
            return None;
        }
        let deleting =
            snap.metadata.deletion_timestamp.is_some() || status.phase == SnapshotPhase::Deleting;
        let pool_name = replica
            .and_then(|r| r.pool.clone())
            .or_else(|| prev.as_ref().and_then(|a| a.pool.clone()));
        let lv = lvm::names::snapshot_lv(&snap.spec.volume, &name);

        let ack = if deleting {
            pool_name.as_ref()?;
            let res = match self.place(pools, pool_name.as_deref()) {
                Ok(place) => self.remove_snapshot(reads, &place, &lv).await,
                Err(e) => Err(e),
            };
            ack(&self.node, SnapshotPhase::Deleting, pool_name, res)
        } else {
            match status.phase {
                SnapshotPhase::Taking => {
                    replica?;
                    let origin = lvm::names::volume_lv(&snap.spec.volume);
                    let res = match self.place(pools, pool_name.as_deref()) {
                        Ok(place) => {
                            let origin_kind = match replicated {
                                Some(_) => {
                                    Origin::Drbd(self.snapshot_blocker(reads, &place, &origin))
                                }
                                None => Origin::Local,
                            };
                            self.take_snapshot(reads, &place, &origin, &lv, origin_kind)
                                .await
                        }
                        Err(e) => Err(e),
                    };
                    ack(&self.node, SnapshotPhase::Taking, pool_name, res)
                }
                p @ (SnapshotPhase::Suspending | SnapshotPhase::Resuming) => {
                    if let Some(v) = replicated {
                        self.my_replica(v)?;
                        let res = self.suspend_resume(&snap.spec.volume, p).await;
                        ack(&self.node, p, pool_name, res)
                    } else {
                        replica?;
                        ack(&self.node, p, pool_name, Err(NO_DRBD_LOCAL.to_owned()))
                    }
                }
                _ => return None,
            }
        };
        if let Some(e) = &ack.error {
            tracing::warn!(snapshot = name, error = %e, "snapshot step failed");
        }
        (prev.as_ref() != Some(&ack)).then_some(StatusWrite::Snapshot { name, ack })
    }

    async fn take_snapshot(
        &self,
        reads: &mut Reads,
        place: &Place,
        origin: &str,
        lv: &str,
        kind: Origin,
    ) -> Result<(), String> {
        let rows = reads.lvs(&self.lvm, &place.vg).await?;
        if find_in_pool(&rows, lv, &place.thin)?.is_some() {
            return Ok(());
        }
        if find_in_pool(&rows, origin, &place.thin)?.is_none() {
            return Err(format!(
                "volume LV {}/{origin} not found on this node",
                place.vg
            ));
        }
        let Origin::Drbd(not_ready) = kind else {
            tracing::info!(lv, origin, vg = place.vg, "taking snapshot");
            let res = self.lvm.snapshot(&place.vg, origin, lv).await;
            reads.invalidate(&place.vg);
            return res.map_err(|e| e.to_string());
        };
        if let Some(e) = not_ready {
            return Err(e);
        }
        // An origin's tags are copied: a snapshot must not inherit the
        // proof of the origin's metadata.
        let inherited: Vec<String> = reads
            .tags(&self.lvm, &place.vg, origin)
            .await?
            .into_iter()
            .filter(|t| storage::is_md_tag(t) || t == storage::NEW_TAG)
            .collect();
        tracing::info!(lv, origin, vg = place.vg, "taking snapshot");
        let res = self.lvm.snapshot(&place.vg, origin, lv).await;
        reads.invalidate(&place.vg);
        res.map_err(|e| e.to_string())?;
        if !inherited.is_empty() {
            let del: Vec<&str> = inherited.iter().map(String::as_str).collect();
            self.lvm
                .change_tags(&place.vg, lv, &[], &del)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Why a replicated origin must not be snapshotted now: a copy of an LV
    /// made this pass, or of a disk that is not UpToDate, is no data at all.
    fn snapshot_blocker(&self, reads: &Reads, place: &Place, origin: &str) -> Option<String> {
        if reads.created.contains(&format!("{}/{origin}", place.vg)) {
            return Some(format!(
                "volume LV {}/{origin} was created this pass: nothing to snapshot yet",
                place.vg
            ));
        }
        let disk = self
            .tracked(origin)
            .and_then(|r| r.devices.get(&crate::drbd::VOLUME).cloned())
            .and_then(|d| d.disk);
        (disk.as_deref() != Some(DISK_UP_TO_DATE)).then(|| {
            format!(
                "local DRBD disk is {}, not UpToDate",
                disk.as_deref().unwrap_or("not attached")
            )
        })
    }

    async fn remove_snapshot(
        &self,
        reads: &mut Reads,
        place: &Place,
        lv: &str,
    ) -> Result<(), String> {
        let rows = reads.lvs(&self.lvm, &place.vg).await?;
        if find_in_pool(&rows, lv, &place.thin)?.is_none() {
            return Ok(());
        }
        tracing::info!(lv, vg = place.vg, "removing snapshot LV");
        let res = self.lvm.remove(&place.vg, lv).await;
        reads.invalidate(&place.vg);
        res.map_err(|e| e.to_string())
    }

    /// Thin LVs nobody claims are left alone, logged once per name.
    async fn log_orphans(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        vols: &[&Volume],
        snaps: &[&Snapshot],
    ) {
        let known: HashSet<String> = vols
            .iter()
            .map(|v| lvm::names::volume_lv(&v.name_any()))
            .chain(
                snaps
                    .iter()
                    .map(|s| lvm::names::snapshot_lv(&s.spec.volume, &s.name_any())),
            )
            .collect();
        for pool in pools {
            let Ok(rows) = reads.lvs(&self.lvm, &pool.spec.vg).await else {
                continue;
            };
            for r in storage::thin_lvs(&rows, &pool.spec.thin_pool) {
                let key = format!("{}/{}", r.vg_name, r.lv_name);
                if !known.contains(&r.lv_name) && self.orphans_logged.insert(key.clone()) {
                    tracing::info!(lv = key, "LV without a Volume or Snapshot: left alone");
                }
            }
        }
    }
}

/// The model's CEL rule already rejects these; an object written before the
/// rule, or past it, must still never put such a name into an lvm argv.
fn invalid_name(pool: &Pool) -> Option<String> {
    [("vg", &pool.spec.vg), ("thinPool", &pool.spec.thin_pool)]
        .into_iter()
        .find(|(_, v)| !lvm::names::valid_lvm_name(v))
        .map(|(field, v)| format!("{field} {v:?} is not a valid LVM name"))
}

/// This node already reported the LV, so a missing one is lost data: a new
/// empty LV would pass for it, block volumes and restores included. Sticky
/// through our own refusal entry; a never-reported LV (a failed restore
/// among them) stays creatable.
fn lost_after_report(prev: Option<&ReplicaStatus>) -> bool {
    prev.is_some_and(|p| p.present || p.last_error.as_deref().is_some_and(is_lost))
}

fn is_lost(error: &str) -> bool {
    error.starts_with(LV_LOST)
}

const LOST_SEP: &str = "; ";
const REBUILD: &str = "rebuild: ";

pub(crate) fn rebuild_error(e: &str) -> String {
    if is_lost(e) {
        e.to_owned()
    } else {
        format!("{LV_LOST}{LOST_SEP}{REBUILD}{e}")
    }
}

/// A later error of a lost entry keeps LV_LOST in front: without it the
/// next pass would see no loss and create an empty LV in its place.
fn keep_lost(prev: Option<&ReplicaStatus>, error: String) -> String {
    let lost = prev.is_some_and(|p| p.last_error.as_deref().is_some_and(is_lost));
    if lost && !is_lost(&error) {
        format!("{LV_LOST}{LOST_SEP}{error}")
    } else {
        error
    }
}

pub(crate) fn unwrap_rebuild(e: &str) -> &str {
    e.strip_prefix(LV_LOST)
        .and_then(|r| r.strip_prefix(LOST_SEP))
        .map_or(e, |r| r.strip_prefix(REBUILD).unwrap_or(r))
}

/// The LV of that name in the Pool's thin pool. A same-named LV elsewhere
/// (thick, another thin pool) is not ours and is never touched.
fn find_in_pool<'a>(
    rows: &'a [LvRow],
    name: &str,
    thin: &str,
) -> Result<Option<&'a LvRow>, String> {
    match rows.iter().find(|r| r.lv_name == name) {
        Some(r) if r.pool_lv.as_deref() != Some(thin) => {
            Err(format!("LV exists outside thin pool {thin}"))
        }
        r => Ok(r),
    }
}

fn ack(
    node: &str,
    phase: SnapshotPhase,
    pool: Option<String>,
    res: Result<(), String>,
) -> SnapshotAck {
    SnapshotAck {
        node: node.to_owned(),
        phase,
        pool,
        ok: res.is_ok(),
        error: res.err(),
    }
}

/// This node's Volume status entry from the LV row after the pass.
pub fn replica_entry(
    node: &str,
    row: Option<&LvRow>,
    observed_generation: Option<i64>,
    last_error: Option<String>,
) -> ReplicaStatus {
    ReplicaStatus {
        node: node.to_owned(),
        present: row.is_some(),
        disk_state: row.map(|_| DISK_UP_TO_DATE.to_owned()),
        in_use: row.is_some_and(storage::is_open),
        device_path: row.filter(|r| r.is_active()).map(|r| r.lv_path.clone()),
        allocated_bytes: row.map(|r| r.lv_size * 1024),
        size_bytes: row.map(|r| r.lv_size * 1024),
        observed_generation,
        last_error,
        ..ReplicaStatus::default()
    }
}

#[cfg(test)]
mod tests;
