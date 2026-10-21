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
use lvm::report::{Lv, Tagged, Vg};
use model::condition::{self, Condition};
use model::{
    AssignedReplica, DeviceState, DeviceStatus, Pool, ReplicaStatus, Role, Snapshot, SnapshotAck,
    SnapshotPhase, Volume,
};
use serde_json::json;

use crate::Exec;
use crate::devices::{HostDisks, SysDisks};
use crate::digest::{self, Digests};
use crate::drbd::Facts;
use crate::events2::Tracker;
use crate::storage::{self, Lvm, PoolCapacity};

mod devices;
mod live;
mod replicated;
pub use live::{KubeLiveVolumes, LiveVolumes, NoLiveVolumes};
pub use replicated::DrbdEnv;

const DISK_UP_TO_DATE: &str = "UpToDate";
const REASON_READY: &str = "ThinPoolReady";
pub(crate) const REASON_UNAVAILABLE: &str = "ThinPoolUnavailable";
const REASON_ZEROING_DISABLED: &str = "ZeroingDisabled";
const REASON_INVALID_NAME: &str = "InvalidName";
const REASON_LVM_UNSUPPORTED: &str = "LvmUnsupported";
// Pools built from devices.
pub(crate) const REASON_DEVICE_NOT_EMPTY: &str = "DeviceNotEmpty";
pub(crate) const REASON_DEVICE_BUSY: &str = "DeviceBusy";
pub(crate) const REASON_DEVICE_MISSING: &str = "DeviceMissing";
pub(crate) const REASON_NOT_A_DISK: &str = "NotADisk";
pub(crate) const REASON_VG_NOT_OWNED: &str = "VolumeGroupNotOwned";
pub(crate) const REASON_BUILDING: &str = "Building";
// Released, reclaimPolicy Delete.
const REASON_REMOVED: &str = "Removed";
const REASON_REMOVE_BLOCKED: &str = "RemoveBlocked";
const REASON_REMOVE_FAILED: &str = "RemoveFailed";
const NO_DRBD_LOCAL: &str = "replicas 1 volume: no DRBD to suspend or resume";
pub(crate) const LV_LOST: &str = model::keys::LV_LOST;

/// One server-side apply on a status subresource, this node's part only.
#[derive(Debug, Clone, PartialEq)]
pub enum StatusWrite {
    Pool {
        name: String,
        generation: Option<i64>,
        capacity: Result<PoolCapacity, String>,
        /// From the probe, else the last one written: the chunk is fixed at
        /// creation, and a value that vanished while the probe fails would
        /// re-render every replicated volume of the Pool.
        chunk_bytes: Option<u64>,
        available: Condition,
        /// Pools built from devices only.
        devices: Option<Vec<DeviceStatus>>,
        released: Option<Condition>,
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
    /// An empty list drops the ack this node owns.
    SnapshotGone {
        name: String,
    },
    /// A Warning Event on a Pool or a Snapshot.
    Event {
        kind: &'static str,
        name: String,
        uid: Option<String>,
        reason: String,
        message: String,
    },
}

impl StatusWrite {
    pub fn name(&self) -> &str {
        match self {
            Self::Pool { name, .. }
            | Self::Volume { name, .. }
            | Self::VolumeGone { name }
            | Self::Snapshot { name, .. }
            | Self::SnapshotGone { name }
            | Self::Event { name, .. } => name,
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
                chunk_bytes,
                available,
                devices,
                released,
            } => {
                let mut conditions = vec![available];
                conditions.extend(released);
                let mut status = json!({ "conditions": conditions });
                if let Some(d) = devices {
                    status["devices"] = json!(d);
                }
                if let Some(g) = generation {
                    status["observedGeneration"] = json!(g);
                }
                if let Ok(c) = capacity {
                    status["totalBytes"] = json!(c.total_bytes);
                    status["freeBytes"] = json!(c.free_bytes);
                }
                if let Some(c) = chunk_bytes {
                    status["chunkBytes"] = json!(c);
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
            Self::SnapshotGone { name } => json!({
                "apiVersion": api_version,
                "kind": "Snapshot",
                "metadata": { "name": name },
                "status": { "acks": [] },
            }),
            Self::Event { .. } => json!(self.event()),
        }
    }

    /// The core Event of an [`Self::Event`], in "default" where kubectl
    /// describe looks for cluster-scoped objects.
    fn event(&self) -> k8s_openapi::api::core::v1::Event {
        use k8s_openapi::api::core::v1::{Event, EventSource, ObjectReference};
        use kube::api::ObjectMeta;
        let Self::Event {
            kind,
            name,
            uid,
            reason,
            message,
        } = self
        else {
            return Event::default();
        };
        let now = model::time::now();
        Event {
            metadata: ObjectMeta {
                generate_name: Some(format!("{name}.")),
                namespace: Some("default".into()),
                ..ObjectMeta::default()
            },
            involved_object: ObjectReference {
                api_version: Some(format!("{}/{}", model::GROUP, model::VERSION)),
                kind: Some((*kind).into()),
                name: Some(name.clone()),
                uid: uid.clone(),
                ..ObjectReference::default()
            },
            reason: Some(reason.clone()),
            message: Some(message.clone()),
            type_: Some("Warning".into()),
            count: Some(1),
            first_timestamp: Some(now.clone()),
            last_timestamp: Some(now),
            source: Some(EventSource {
                component: Some(crate::node::AGENT_COMPONENT.into()),
                host: None,
            }),
            reporting_component: Some(crate::node::AGENT_COMPONENT.into()),
            ..Event::default()
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
            Self::Snapshot { name, .. } | Self::SnapshotGone { name } => {
                Api::<Snapshot>::all(client.clone())
                    .patch_status(name, &params, &patch)
                    .await?;
            }
            Self::Event { .. } => {
                let api: Api<k8s_openapi::api::core::v1::Event> =
                    Api::namespaced(client.clone(), "default");
                let mut ev = self.event();
                ev.reporting_instance = Some(node.to_owned());
                if let Some(s) = ev.source.as_mut() {
                    s.host = Some(node.to_owned());
                }
                api.create(&kube::api::PostParams::default(), &ev).await?;
            }
        }
        Ok(())
    }
}

/// lvs and vgs reads of one pass, per VG; a mutation drops the VG's entry.
#[derive(Debug, Default)]
struct Reads {
    lvs: HashMap<String, Result<Vec<Lv>, String>>,
    vgs: HashMap<String, Result<Vg, String>>,
    /// Tags and creation time per LV, read only when a decision needs them.
    tags: HashMap<String, Result<HashMap<String, Tagged>, String>>,
    /// `vg/lv` of the LVs this pass created; never snapshotted in the same pass.
    created: HashSet<String>,
}

impl Reads {
    async fn lvs(&mut self, lvm: &Lvm, vg: &str) -> Result<Vec<Lv>, String> {
        if let Some(r) = self.lvs.get(vg) {
            return r.clone();
        }
        let r = lvm.lvs(vg).await.map_err(|e| e.to_string());
        self.lvs.insert(vg.to_owned(), r.clone());
        r
    }

    async fn vg(&mut self, lvm: &Lvm, vg: &str) -> Result<Vg, String> {
        if let Some(r) = self.vgs.get(vg) {
            return r.clone();
        }
        let r = lvm.vg(vg).await.map_err(|e| e.to_string());
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
    ) -> Result<Option<Lv>, String> {
        let rows = self.lvs(lvm, vg).await?;
        find_in_pool(&rows, name, thin).map(Option::<&Lv>::cloned)
    }

    /// Tags of one LV (empty when it has none).
    async fn tags(&mut self, lvm: &Lvm, vg: &str, lv: &str) -> Result<Vec<String>, String> {
        Ok(self.tagged(lvm, vg, lv).await?.tags)
    }

    /// Tags and creation time of one LV (default when it is not there).
    async fn tagged(&mut self, lvm: &Lvm, vg: &str, lv: &str) -> Result<Tagged, String> {
        if !self.tags.contains_key(vg) {
            let r = lvm.tagged(vg).await.map_err(|e| e.to_string());
            self.tags.insert(vg.to_owned(), r);
        }
        match &self.tags[vg] {
            Ok(m) => Ok(m.get(lv).cloned().unwrap_or_default()),
            Err(e) => Err(e.clone()),
        }
    }

    /// The thin pool's chunk from this pass's lvs, when already read.
    fn chunk_bytes(&self, place: &Place) -> Option<u64> {
        let rows = self.lvs.get(&place.vg)?.as_ref().ok()?;
        rows.iter()
            .find(|r| r.name == place.thin && r.thin_pool)
            .map(|r| r.chunk_bytes)
            .filter(|&c| c > 0)
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

/// A user IO suspend of one volume, as this agent run knows it.
#[derive(Debug, Clone)]
struct Freeze {
    /// On this agent's monotonic clock, never a wall clock: the deadlines
    /// must hold whatever the other nodes' clocks say.
    since: tokio::time::Instant,
    /// The uid of the Snapshot whose round this run suspended it for: no
    /// other Snapshot holds it, so a status lost long ago never stretches
    /// a later round's freeze. None: a suspend found after a restart,
    /// started at an unknown time; any Snapshot of the volume holds it, it
    /// is resumed in time and never taken from.
    round: Option<String>,
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
    /// Volumes whose first data this node makes current, with when it
    /// started waiting for its peers.
    init_started: HashMap<String, Option<tokio::time::Instant>>,
    /// A refresh saw a change a pass must act on.
    pass_wanted: bool,
    /// Volumes whose replica here was rebuilt empty before initializedAt.
    /// Memory only. After an agent restart the timeout start is still held
    /// by the lost-LV error in this node's status entry and by any entry
    /// that showed data. A status wipe on top removes both (and
    /// initializedAt); then only DRBD guards: the start needs quorum, and a
    /// peer holding data that connects later finds unrelated data and goes
    /// StandAlone, visible in the replica's peers, never a silent sync.
    rebuilt_seen: HashSet<String>,
    /// Dry-run plans that came back after a successful adjust.
    repeats: HashMap<String, replicated::Repeat>,
    /// Spec generation of each volume's last clean bitmap slot sweep.
    slots_swept: HashMap<String, i64>,
    /// Volumes whose left-out settings were logged.
    dropped_logged: HashSet<String>,
    /// This node's Pools whose vg or thinPool name failed the check this pass.
    invalid_pools: HashSet<String>,
    /// This node's Pools built from devices that are not ready this pass,
    /// with why: no LV is created in a VG that is not built, or not ours.
    unready_pools: HashMap<String, String>,
    /// Why the host's LVM cannot serve Pools, from the startup check.
    lvm_unsupported: Option<String>,
    /// Connections out of Connected, per volume and peer.
    peers: crate::peer::Watch,
    /// Agent Leases by node; None until set, which tests without Leases
    /// read as every peer alive.
    peer_leases: Option<BTreeMap<String, crate::peer::PeerLease>>,
    disks: Arc<dyn HostDisks>,
    /// Per Pool, the paths last announced by a DeviceNotUsable Event.
    device_events: HashMap<String, BTreeMap<String, DeviceState>>,
    /// Per Pool, the note last announced by a PoolNotGrown Event.
    pool_notes: HashMap<String, String>,
    /// A Pool step ran or a Pool's device rows changed.
    disks_changed: bool,
    /// Per Pool, the device rows of the last pass.
    device_rows: HashMap<String, Vec<DeviceStatus>>,
    /// Shared with the NodeDisks inventory.
    disk_lock: Arc<tokio::sync::Mutex<()>>,
    /// The loop's deadline, kept while waiting for the disk lock.
    progress: crate::node::Progress,
    /// Live reads for the restore decisions the watch cache cannot prove.
    live: Arc<dyn LiveVolumes>,
    /// Applies writes during the pass; None returns them all from it.
    publish: Option<Arc<dyn Publish>>,
    /// Per volume, the IO suspend this agent keeps. Memory only: a restart
    /// forgets it, which only forbids takes until the next round.
    freezes: HashMap<String, Freeze>,
    digests: Digests,
    /// The snapshot LVs this pass found still to hash.
    digest_jobs: Vec<digest::Job>,
    /// Snapshot and orphan Events of this pass, returned with its writes.
    events: Vec<StatusWrite>,
    /// `pool/lv` of the snapshot LVs of no Snapshot in the last pass.
    orphan_suspects: HashSet<String>,
    /// `pool/lv` of those reported, once each.
    orphans_reported: HashSet<String>,
    /// The uid of every Pool of this node, usable or not, sorted: what a
    /// discovery answer names as searched.
    pool_uids: Vec<String>,
    /// Per Snapshot uid, the step and error last logged at warn.
    snapshot_warnings: HashMap<String, (&'static str, String)>,
}

/// Where a pass sends its writes as each step ends, so statuses trail the
/// host by one step, not by a whole pass.
#[async_trait::async_trait]
pub trait Publish: std::fmt::Debug + Send + Sync {
    /// Applies `writes`; returns the Events that did not get through.
    async fn publish(&self, writes: Vec<StatusWrite>) -> Vec<StatusWrite>;
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
            init_started: HashMap::new(),
            pass_wanted: false,
            rebuilt_seen: HashSet::new(),
            repeats: HashMap::new(),
            slots_swept: HashMap::new(),
            dropped_logged: HashSet::new(),
            invalid_pools: HashSet::new(),
            unready_pools: HashMap::new(),
            lvm_unsupported: None,
            peers: crate::peer::Watch::default(),
            peer_leases: None,
            disks: Arc::new(SysDisks::default()),
            device_events: HashMap::new(),
            pool_notes: HashMap::new(),
            disks_changed: false,
            device_rows: HashMap::new(),
            disk_lock: Arc::default(),
            progress: crate::node::Progress::default(),
            live: Arc::new(NoLiveVolumes),
            publish: None,
            freezes: HashMap::new(),
            digests: Digests::default(),
            digest_jobs: Vec::new(),
            events: Vec::new(),
            orphan_suspects: HashSet::new(),
            orphans_reported: HashSet::new(),
            pool_uids: Vec::new(),
            snapshot_warnings: HashMap::new(),
        }
    }

    /// Without it this node hashes no snapshot LV.
    #[must_use]
    pub fn with_digests(mut self, digests: Digests) -> Self {
        self.digests = digests;
        self
    }

    #[must_use]
    pub fn with_publish(mut self, publish: Arc<dyn Publish>) -> Self {
        self.publish = Some(publish);
        self
    }

    /// Sends the writes so far when a publisher is set.
    async fn flush(&mut self, writes: &mut Vec<StatusWrite>) {
        let Some(p) = self.publish.clone() else {
            return;
        };
        if writes.is_empty() {
            return;
        }
        let unsent = p.publish(std::mem::take(writes)).await;
        self.events_unsent(unsent);
    }

    #[must_use]
    pub fn with_live(mut self, live: Arc<dyn LiveVolumes>) -> Self {
        self.live = live;
        self
    }

    #[must_use]
    pub fn with_progress(mut self, progress: crate::node::Progress) -> Self {
        self.progress = progress;
        self
    }

    /// The disk lock, the wait declared to the heartbeat: the inventory
    /// holds it for one bounded step at a time.
    async fn lock_disks(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.progress.within(crate::inventory::scan::LOCK_HOLD_MAX);
        self.disk_lock.clone().lock_owned().await
    }

    #[must_use]
    pub fn with_disk_lock(mut self, lock: Arc<tokio::sync::Mutex<()>>) -> Self {
        self.disk_lock = lock;
        self
    }

    #[must_use]
    pub fn with_disks(mut self, disks: Arc<dyn HostDisks>) -> Self {
        self.disks = disks;
        self
    }

    /// Every Pool reports Available False with this reason's text.
    pub fn set_lvm_unsupported(&mut self, why: Option<String>) {
        self.lvm_unsupported = why;
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

    pub fn set_peer_leases(&mut self, leases: BTreeMap<String, crate::peer::PeerLease>) {
        self.peer_leases = Some(leases);
    }

    pub fn set_addresses(&mut self, addresses: BTreeMap<String, IpAddr>) {
        self.addresses = addresses;
    }

    /// One pass over the current objects. Returns only writes that change
    /// something, so an idle node writes nothing; with a publisher, only
    /// those it has not sent after a step.
    pub async fn pass(
        &mut self,
        pools: &[Arc<Pool>],
        volumes: &[Arc<Volume>],
        snapshots: &[Arc<Snapshot>],
    ) -> Vec<StatusWrite> {
        let mut reads = Reads::default();
        let mut writes = Vec::new();
        // Unsent Snapshot Events of a Snapshot since gone say nothing more.
        self.events.retain(|e| match e {
            StatusWrite::Event {
                kind: "Snapshot",
                uid,
                ..
            } => snapshots.iter().any(|s| s.metadata.uid == *uid),
            _ => true,
        });

        let mut pools: Vec<&Pool> = pools
            .iter()
            .map(AsRef::as_ref)
            .filter(|p| p.spec.node == self.node)
            .collect();
        pools.sort_by_key(|p| p.name_any());
        self.pool_uids = pools.iter().filter_map(|p| p.uid()).collect();
        self.pool_uids.sort();
        self.unready_pools.clear();
        for pool in &pools {
            writes.extend(self.pool(&mut reads, pool).await);
        }
        self.flush(&mut writes).await;
        // A bad name never reaches an lvm argv, not even through lvs.
        self.invalid_pools = pools
            .iter()
            .filter(|p| invalid_name(p).is_some())
            .map(|p| p.name_any())
            .collect();
        // Not searched for snapshot LVs either: not finding one there
        // proves nothing.
        if let Some(e) = &self.lvm_unsupported {
            for p in &pools {
                self.unready_pools.insert(p.name_any(), e.clone());
            }
        }
        pools.retain(|p| {
            invalid_name(p).is_none()
                && self.lvm_unsupported.is_none()
                && !self.unready_pools.contains_key(&p.name_any())
        });

        let mut vols: Vec<&Volume> = volumes.iter().map(AsRef::as_ref).collect();
        vols.sort_by_key(|v| v.name_any());
        let mut snaps: Vec<&Snapshot> = snapshots.iter().map(AsRef::as_ref).collect();
        snaps.sort_by_key(|s| s.name_any());
        let round_of = |vol: &Volume, s: &Snapshot| s.is_of(vol) && replicated::in_round(s);
        for vol in &vols {
            writes.extend(self.freeze(vol, &snaps).await);
            if let Some(w) = self.volume(&mut reads, &pools, snapshots, vol).await {
                writes.push(w);
            }
            self.flush(&mut writes).await;
            // Right after its volume: a frozen volume never waits for the
            // others, and the take window never runs out on their account.
            for snap in snaps.iter().filter(|s| round_of(vol, s)) {
                if let Some(w) = self.snapshot(&mut reads, &pools, &vols, snap).await {
                    writes.push(w);
                    self.flush(&mut writes).await;
                }
            }
        }
        self.freezes
            .retain(|name, _| vols.iter().any(|v| v.name_any() == *name));
        self.probe_peers(&vols, &mut writes).await;

        for snap in &snaps {
            if vols.iter().any(|v| round_of(v, snap)) {
                continue;
            }
            if let Some(w) = self.snapshot(&mut reads, &pools, &vols, snap).await {
                writes.push(w);
            }
        }

        self.snapshot_warnings
            .retain(|uid, _| snaps.iter().any(|s| s.metadata.uid.as_ref() == Some(uid)));
        self.digests.schedule(std::mem::take(&mut self.digest_jobs));
        self.log_orphans(&mut reads, &pools, &vols, &snaps).await;
        writes.append(&mut self.events);
        writes
    }

    async fn pool(&mut self, reads: &mut Reads, pool: &Pool) -> Vec<StatusWrite> {
        let (vg, thin) = (&pool.spec.vg, &pool.spec.thin_pool);
        let mut built = None;
        let probed = match (invalid_name(pool), &self.lvm_unsupported) {
            (Some(e), _) => Err((REASON_INVALID_NAME, e)),
            (None, Some(e)) => Err((REASON_LVM_UNSUPPORTED, e.clone())),
            (None, None) if pool.spec.devices.is_some() => {
                let out = self.devices_pool(reads, pool).await;
                let gate = out.gate.clone();
                if out.unready {
                    let why = match &gate {
                        Err((_, why)) => why.clone(),
                        Ok(()) => "its thin pool is not built".into(),
                    };
                    self.unready_pools.insert(pool.name_any(), why);
                }
                built = Some(out);
                match gate {
                    Ok(()) => self.probe_pool(reads, vg, thin).await,
                    Err(e) => Err(e),
                }
            }
            (None, None) => self.probe_pool(reads, vg, thin).await,
        };
        let generation = pool.metadata.generation;
        let (status, reason, message) = match &probed {
            // Capacity and chunk stay in the write: existing volumes go on.
            Ok((_, Some(sick))) => (condition::FALSE, REASON_UNAVAILABLE, sick.clone()),
            Ok((_, None)) => (
                condition::TRUE,
                REASON_READY,
                built
                    .as_ref()
                    .and_then(|o| o.note.clone())
                    .unwrap_or_default(),
            ),
            Err((reason, e)) => (condition::FALSE, *reason, e.clone()),
        };
        let capacity = probed.map(|(c, _)| c).map_err(|(_, e)| e);
        if status == condition::FALSE {
            tracing::warn!(pool = pool.name_any(), error = %message, "pool unavailable");
        }
        let current = pool.status.clone().unwrap_or_default();
        let now = model::time::now();
        let mut conditions = current.conditions.clone();
        condition::set(
            &mut conditions,
            condition::new(
                condition::types::AVAILABLE,
                status,
                reason,
                message,
                generation,
                now.clone(),
            ),
        );
        let mut writes = Vec::new();
        let (devices, released) = match built {
            Some(out) => {
                writes.extend(out.events);
                if let Some((st, why, msg)) = out.released {
                    condition::set(
                        &mut conditions,
                        condition::new(condition::types::RELEASED, st, why, msg, generation, now),
                    );
                }
                (
                    Some(out.rows.unwrap_or_else(|| current.devices.clone())),
                    condition::get(&conditions, condition::types::RELEASED)
                        .cloned()
                        .filter(|_| pool.metadata.deletion_timestamp.is_some()),
                )
            }
            None => (None, None),
        };
        let Some(available) = condition::get(&conditions, condition::types::AVAILABLE).cloned()
        else {
            return writes;
        };
        let cap = capacity.as_ref().ok();
        let chunk_bytes = cap.map(|c| c.chunk_bytes).or(current.chunk_bytes);
        let unchanged = current.total_bytes == cap.map(|c| c.total_bytes)
            && current.free_bytes == cap.map(|c| c.free_bytes)
            && current.chunk_bytes == chunk_bytes
            && current.observed_generation == generation
            && condition::get(&current.conditions, condition::types::AVAILABLE) == Some(&available)
            && devices.as_ref().is_none_or(|d| *d == current.devices)
            && condition::get(&current.conditions, condition::types::RELEASED) == released.as_ref();
        if !unchanged {
            writes.push(StatusWrite::Pool {
                name: pool.name_any(),
                generation,
                capacity,
                chunk_bytes,
                available,
                devices,
                released,
            });
        }
        writes
    }

    /// Capacity of a usable thin pool, with why it should take no new
    /// volumes if it is unhealthy. Pool zeroing is switched on first:
    /// without it a new thin LV can expose blocks of deleted ones.
    /// The VG read here serves every volume of the pass.
    async fn probe_pool(
        &self,
        reads: &mut Reads,
        vg: &str,
        thin: &str,
    ) -> Result<(PoolCapacity, Option<String>), (&'static str, String)> {
        let unavailable = |e: String| (REASON_UNAVAILABLE, e);
        let mut lvs = reads.lvs(&self.lvm, vg).await.map_err(unavailable)?;
        if lvs
            .iter()
            .any(|r| r.name == thin && r.thin_pool && !r.zeroing)
        {
            tracing::info!(vg, thin, "enabling thin pool zeroing");
            let res = self.lvm.enable_zeroing(vg, thin).await;
            reads.invalidate(vg);
            res.map_err(|e| (REASON_ZEROING_DISABLED, e.to_string()))?;
            lvs = reads.lvs(&self.lvm, vg).await.map_err(unavailable)?;
        }
        reads.vg(&self.lvm, vg).await.map_err(unavailable)?;
        let capacity = storage::pool_capacity(&lvs, thin).map_err(unavailable)?;
        Ok((capacity, storage::pool_health(&lvs, thin)))
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

    /// `vol` restores from `snapshot` on this node and has not made its first
    /// data yet: until then a lost LV is restored again from this copy.
    fn restores_here_from(&self, vol: &Volume, snapshot: &Snapshot) -> bool {
        vol.restoring().is_some_and(|src| src.is(snapshot))
            && vol.metadata.deletion_timestamp.is_none()
            && self.local_replica(vol).is_some()
    }

    fn place(&self, pools: &[&Pool], pool_name: Option<&str>) -> Result<Place, String> {
        let name = pool_name.ok_or_else(|| "assignment has no pool".to_owned())?;
        if let Some(e) = &self.lvm_unsupported {
            return Err(e.clone());
        }
        if self.invalid_pools.contains(name) {
            return Err(format!("pool {name} has an invalid vg or thinPool name"));
        }
        if let Some(why) = self.unready_pools.get(name) {
            return Err(format!("pool {name} is not ready: {why}"));
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
            let desired = match reads.vg(&self.lvm, vg).await {
                Ok(v) => storage::desired_bytes(vol.spec.size_bytes, v.extent_bytes),
                Err(e) => return self.error_entry(prev, e),
            };
            if row.is_none() {
                let res = if lost_after_report(prev) {
                    Err(LV_LOST.to_owned())
                } else {
                    self.create_lv(
                        &rows,
                        source_snapshot(vol, snapshots),
                        place,
                        vol,
                        &lv,
                        desired,
                    )
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
                if !r.active {
                    changed = true;
                    if let Err(e) = self.lvm.activate(vg, &lv).await {
                        error = Some(e.to_string());
                    }
                }
                if error.is_none() && r.size_bytes < desired {
                    tracing::info!(
                        volume = name,
                        from = r.size_bytes,
                        to = desired,
                        "growing volume LV"
                    );
                    changed = true;
                    if let Err(e) = self.lvm.extend(vg, &lv, desired).await {
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
        rows: &[Lv],
        source: Option<&Snapshot>,
        place: &Place,
        vol: &Volume,
        lv: &str,
        desired: u64,
    ) -> Result<(), String> {
        let drbd = vol.spec.replicas >= 2;
        let tags: &[&str] = if drbd { &[storage::NEW_TAG] } else { &[] };
        if let Some(src) = &vol.spec.source {
            let snapshot = &src.snapshot;
            let snap = source.ok_or_else(|| format!("source snapshot {snapshot} not found"))?;
            // In the same lvcreate, so a restored DRBD backing LV never
            // exists without its proof.
            let from = if drbd {
                let uid = snap
                    .metadata
                    .uid
                    .as_deref()
                    .ok_or_else(|| format!("source snapshot {snapshot} has no uid"))?;
                let vol_uid = vol.metadata.uid.as_deref().ok_or("Volume has no uid")?;
                Some(storage::from_tag(vol_uid, uid))
            } else {
                None
            };
            let tags: Vec<&str> = tags.iter().copied().chain(from.as_deref()).collect();
            return self.restore(rows, place, snap, lv, &tags).await;
        }
        tracing::info!(lv, vg = place.vg, bytes = desired, "creating volume LV");
        self.lvm
            .create(&place.vg, &place.thin, lv, desired, tags)
            .await
            .map_err(|e| e.to_string())
    }

    async fn remove_volume(&self, vg: &str, row: &Lv) -> Result<(), String> {
        tracing::info!(lv = row.name, vg, "removing volume LV");
        self.lvm
            .remove(vg, &row.name)
            .await
            .map_err(|e| e.to_string())
    }

    /// Restore source: the snapshot LV in the same VG.
    /// A missing source is an error, never an empty LV: the caller asked for data.
    async fn restore(
        &self,
        rows: &[Lv],
        place: &Place,
        snap: &Snapshot,
        lv: &str,
        tags: &[&str],
    ) -> Result<(), String> {
        let snapshot = snap.name_any();
        match snap
            .status
            .as_ref()
            .and_then(|s| s.acks.iter().find(|a| a.node == self.node))
            .and_then(|a| a.taken_pool.as_deref())
        {
            Some(pool) if pool == place.pool => {}
            Some(pool) => {
                return Err(format!(
                    "source snapshot {snapshot} lives in pool {pool}, volume assigned to {}",
                    place.pool
                ));
            }
            None => {
                return Err(format!(
                    "source snapshot {snapshot} has no copy acked on this node"
                ));
            }
        }
        let source = lvm::names::snapshot_lv(&snap.spec.volume, &snapshot);
        if find_in_pool(rows, &source, &place.thin)?.is_none() {
            return Err(format!(
                "snapshot LV {}/{source} not found on this node",
                place.vg
            ));
        }
        let vg = place.vg.as_str();
        let tagged = self.lvm.tags(vg).await.map_err(|e| e.to_string())?;
        let source_tags = tagged.get(&source).map_or(&[][..], Vec::as_slice);
        let uid = snap.metadata.uid.as_deref().unwrap_or_default();
        if !matches!(snap_tags(source_tags, uid), SnapTags::Own(_)) {
            return Err(format!(
                "snapshot LV {vg}/{source} has no tag of this snapshot: not its copy"
            ));
        }
        tracing::info!(lv, source, vg, "restoring volume LV from snapshot");
        self.lvm
            .restore(vg, &source, lv, tags)
            .await
            .map_err(|e| e.to_string())?;
        // LVM copies no tags to a thin snapshot; a copy must never pass
        // for the snapshot itself if one ever does.
        let tagged = self.lvm.tags(vg).await.map_err(|e| e.to_string())?;
        let inherited: Vec<&str> = tagged
            .get(lv)
            .into_iter()
            .flatten()
            .filter(|t| storage::is_snap_tag(t))
            .map(String::as_str)
            .collect();
        if inherited.is_empty() {
            return Ok(());
        }
        self.lvm
            .change_tags(vg, lv, &[], &inherited)
            .await
            .map_err(|e| e.to_string())
    }

    async fn snapshot(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        vols: &[&Volume],
        snap: &Snapshot,
    ) -> Option<StatusWrite> {
        let name = snap.name_any();
        let status = snap.status.clone().unwrap_or_default();
        let prev = status.acks.iter().find(|a| a.node == self.node).cloned();
        let lv = lvm::names::snapshot_lv(&snap.spec.volume, &name);
        if status.phase == Some(SnapshotPhase::Discovering) {
            let ack = self
                .discovery_answer(reads, pools, snap, &lv, prev.as_ref())
                .await?;
            return Some(StatusWrite::Snapshot { name, ack });
        }
        // Found answers are holder acks from then on; the others go.
        if prev
            .as_ref()
            .is_some_and(|a| a.phase == SnapshotPhase::Discovering && a.taken_pool.is_none())
        {
            return Some(StatusWrite::SnapshotGone { name });
        }
        let taken = Taken::of(prev.as_ref());
        let vol = vols.iter().find(|v| snap.is_of(v));
        let replica = vol.and_then(|v| self.local_replica(v));
        let replicated = vol.filter(|v| v.spec.replicas >= 2);
        if replicated.is_some() && !self.drbd_available() {
            return None;
        }
        // Without a phase the status was lost: its copies are found before
        // any is removed.
        let deleting = status.phase == Some(SnapshotPhase::Deleting)
            || (snap.metadata.deletion_timestamp.is_some() && status.phase.is_some());
        // Before its take the round works in the replica's Pool.
        let replica_pool = replica.and_then(|r| r.pool.clone());

        let ack = if deleting {
            let related = prev.is_some() || replica.is_some();
            self.delete_snapshot(reads, pools, vols, snap, &lv, taken, related)
                .await?
        } else {
            match status.phase {
                Some(SnapshotPhase::Taking) => match (replica, replicated) {
                    // Taken: the LV and its ack stay as they are.
                    (Some(_), _) if taken.pool.is_some() => return None,
                    (Some(_), _) => {
                        let vol = vol?;
                        let pool = replica_pool.as_deref();
                        self.take_step(reads, pools, snap, vol, pool, &lv, taken, prev.as_ref())
                            .await
                    }
                    // Diskless or tiebreaker: no copy; its ack says its IO
                    // is still suspended, and Ready waits for it.
                    (None, Some(v)) => {
                        self.my_replica(v)?;
                        let res = if self.suspended(&snap.spec.volume) {
                            Ok(())
                        } else {
                            Err("IO is not suspended here: resumed, or restarted, mid round"
                                .to_owned())
                        };
                        ack(&self.node, SnapshotPhase::Taking, taken, res)
                    }
                    (None, None) => return None,
                },
                Some(SnapshotPhase::Suspending) => {
                    if let Some(v) = replicated {
                        self.my_replica(v)?;
                        // Backstop to the controller's admission check: an
                        // LV name LVM refuses here would fail in Taking,
                        // with IO already suspended on every replica.
                        let refused = replica
                            .is_some()
                            .then(|| self.place(pools, replica_pool.as_deref()))
                            .and_then(|place| {
                                let place = place.ok()?;
                                lvm::names::check_new_lv(&place.vg, &lv).err()
                            });
                        let res = self.suspend_step(v, snap, prev.as_ref(), refused).await?;
                        ack(&self.node, SnapshotPhase::Suspending, taken, res)
                    } else {
                        replica?;
                        ack(
                            &self.node,
                            SnapshotPhase::Suspending,
                            taken,
                            Err(NO_DRBD_LOCAL.to_owned()),
                        )
                    }
                }
                Some(SnapshotPhase::Ready) if prev.is_none() && holder_missing(&status) => {
                    self.rediscover(reads, pools, snap, &lv).await?
                }
                Some(SnapshotPhase::Ready)
                    if taken.pool.is_some() && taken.digest.is_none() && wants_digest(&status) =>
                {
                    let prev = prev.clone()?;
                    self.digest_step(reads, pools, vols, snap, &lv, prev)
                        .await?
                }
                Some(SnapshotPhase::Failed) => {
                    self.remove_failed_copy(reads, pools, snap, &lv).await;
                    return None;
                }
                _ => return None,
            }
        };
        if let Some(e) = &ack.error {
            tracing::warn!(snapshot = name, error = %e, "snapshot step failed");
        }
        (prev.as_ref() != Some(&ack)).then_some(StatusWrite::Snapshot { name, ack })
    }

    /// The Deleting ack, None when this node has nothing to say: it never
    /// took part and holds no LV of that name.
    #[allow(clippy::too_many_arguments)]
    async fn delete_snapshot(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        vols: &[&Volume],
        snap: &Snapshot,
        lv: &str,
        taken: Taken,
        related: bool,
    ) -> Option<SnapshotAck> {
        let phase = SnapshotPhase::Deleting;
        if let Some(v) = vols.iter().find(|v| self.restores_here_from(v, snap)) {
            let e = format!(
                "kept: volume {} restores from it here and is not initialized yet",
                v.name_any()
            );
            return Some(ack(&self.node, phase, taken, Err(e)));
        }
        let uid = snap.metadata.uid.as_deref().unwrap_or_default();
        if taken.pool.is_some() {
            let res = match self.place(pools, taken.pool.as_deref()) {
                Ok(place) => self.remove_snapshot(reads, &place, lv, uid).await,
                Err(e) => Err(e),
            };
            return Some(ack(&self.node, phase, taken, res));
        }
        // Without takenPool the ack was lost, or never written: the LV, if
        // any, is wherever the search finds it.
        match self.find_own_lv(reads, pools, lv, uid).await {
            Ok(Some(own)) => {
                let res = self.remove_snapshot(reads, &own.place, lv, uid).await;
                Some(ack(&self.node, phase, own.taken(), res))
            }
            Ok(None) => related.then(|| ack(&self.node, phase, taken, Ok(()))),
            Err(e) if related => Some(ack(&self.node, phase, taken, Err(e))),
            Err(e) => {
                self.warn_once(snap, "snapshot LV search failed", e);
                None
            }
        }
    }

    /// Logs a failed snapshot step at warn once per change of its error,
    /// then at debug: a search that keeps failing says the same each pass.
    fn warn_once(&mut self, snap: &Snapshot, what: &'static str, error: String) {
        let name = snap.name_any();
        let uid = snap.metadata.uid.clone().unwrap_or_default();
        let now = (what, error);
        if self.snapshot_warnings.get(&uid) == Some(&now) {
            tracing::debug!(snapshot = name, error = now.1, "{what}");
        } else {
            tracing::warn!(snapshot = name, error = now.1, "{what}");
            self.snapshot_warnings.insert(uid, now);
        }
    }

    /// The step went through: its next failure is news again.
    fn warn_cleared(&mut self, snap: &Snapshot) {
        if let Some(uid) = &snap.metadata.uid {
            self.snapshot_warnings.remove(uid);
        }
    }

    /// The ack with this node's digest once its job finished; until then
    /// the job goes to this pass's schedule.
    async fn digest_step(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        vols: &[&Volume],
        snap: &Snapshot,
        lv: &str,
        prev: SnapshotAck,
    ) -> Option<SnapshotAck> {
        let uid = snap.metadata.uid.clone()?;
        if let Some(d) = self.digests.result(&uid) {
            return Some(SnapshotAck {
                digest: Some(d.hex),
                digest_bytes: Some(d.bytes),
                ..prev
            });
        }
        let name = snap.name_any();
        let place = match self.place(pools, prev.taken_pool.as_deref()) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(snapshot = name, error = %e, "no digest: pool not usable");
                return None;
            }
        };
        match reads.find(&self.lvm, &place.vg, lv, &place.thin).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::debug!(snapshot = name, lv, "no digest: snapshot LV not found");
                return None;
            }
            Err(e) => {
                tracing::warn!(snapshot = name, error = %e, "no digest: LV read failed");
                return None;
            }
        }
        let first = vols.iter().any(|v| {
            v.metadata.deletion_timestamp.is_none() && v.restoring().is_some_and(|s| s.is(snap))
        });
        self.digest_jobs.push(digest::Job {
            uid,
            snapshot: name,
            vg: place.vg,
            lv: lv.to_owned(),
            first,
        });
        None
    }

    /// A node without an ack of a Ready Snapshot short of holders, left
    /// out of a discovery say, may hold a copy: one found by its uid tag is
    /// acked again, so restores and deletes find it through takenPool.
    async fn rediscover(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snap: &Snapshot,
        lv: &str,
    ) -> Option<SnapshotAck> {
        let name = snap.name_any();
        let uid = snap.metadata.uid.as_deref()?;
        match self.find_own_lv(reads, pools, lv, uid).await {
            Ok(Some(own)) => {
                self.warn_cleared(snap);
                tracing::info!(
                    snapshot = name,
                    lv,
                    pool = own.place.pool,
                    "snapshot LV rediscovered without its ack: acked again"
                );
                Some(ack(&self.node, SnapshotPhase::Taking, own.taken(), Ok(())))
            }
            Ok(None) => {
                self.warn_cleared(snap);
                None
            }
            Err(e) => {
                self.warn_once(snap, "snapshot LV search failed", e);
                None
            }
        }
    }

    /// This node's answer to a discovery, None when the one it gave
    /// stands: it searched the thin pools of all its Pools for the
    /// Snapshot's LV by its uid tag. An answer holds only for the Pools it
    /// names and the discovery whose phaseSince it echoes, so a changed
    /// Pool set or a new discovery searches again. A failed search is no
    /// answer: its ack says why and is not ok. A node without a Pool is not
    /// asked.
    async fn discovery_answer(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snap: &Snapshot,
        lv: &str,
        prev: Option<&SnapshotAck>,
    ) -> Option<SnapshotAck> {
        let searched = &self.pool_uids;
        let since = snap.status.as_ref().and_then(|st| st.phase_since.clone());
        let stands = |a: &SnapshotAck| {
            a.phase == SnapshotPhase::Discovering
                && a.ok
                && a.searched_pools == *searched
                && a.phase_since == since
        };
        if searched.is_empty() || prev.is_some_and(stands) {
            return None;
        }
        let uid = snap.metadata.uid.as_deref()?;
        let asked = SnapshotAck {
            searched_pools: searched.clone(),
            phase_since: since,
            ..SnapshotAck::new(&self.node, SnapshotPhase::Discovering)
        };
        let found = self.find_own_lv(reads, pools, lv, uid).await;
        if found.is_ok() {
            self.warn_cleared(snap);
        }
        let ack = match found {
            Ok(Some(own)) => {
                tracing::info!(
                    snapshot = snap.name_any(),
                    lv,
                    pool = own.place.pool,
                    "copy found by its tag"
                );
                SnapshotAck {
                    found: Some(true),
                    searched_pools: asked.searched_pools,
                    phase_since: asked.phase_since,
                    ..ack(&self.node, SnapshotPhase::Discovering, own.taken(), Ok(()))
                }
            }
            Ok(None) => SnapshotAck {
                found: Some(false),
                ok: true,
                ..asked
            },
            Err(e) => {
                self.warn_once(snap, "discovery search failed", e.clone());
                SnapshotAck {
                    error: Some(e),
                    ..asked
                }
            }
        };
        (prev != Some(&ack)).then_some(ack)
    }

    /// A Failed Snapshot is never restored, and a partial copy left behind
    /// would pass for an image after a status loss: each node removes an
    /// LV with its uid tag, on every pass, whether or not it took part.
    async fn remove_failed_copy(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snap: &Snapshot,
        lv: &str,
    ) {
        let Some(uid) = snap.metadata.uid.as_deref() else {
            return;
        };
        let res = match self.find_own_lv(reads, pools, lv, uid).await {
            Ok(Some(own)) => self.remove_snapshot(reads, &own.place, lv, uid).await,
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        };
        match res {
            Ok(()) => self.warn_cleared(snap),
            Err(e) => self.warn_once(snap, "copy of a failed snapshot not removed", e),
        }
    }

    /// This Snapshot's LV on this node: the exact name with its uid's snap
    /// tag, in the thin pool of one of this node's Pools. The tag decides
    /// first: an LV of the name without it belongs to no search. One with
    /// it outside those thin pools is an error, never removed; so is a
    /// Pool that cannot be read, since the LV may be there.
    async fn find_own_lv(
        &self,
        reads: &mut Reads,
        pools: &[&Pool],
        lv: &str,
        uid: &str,
    ) -> Result<Option<OwnLv>, String> {
        if let Some(p) = self
            .invalid_pools
            .iter()
            .chain(self.unready_pools.keys())
            .next()
        {
            return Err(format!("pool {p} cannot be searched: not ready"));
        }
        let mut hits: Vec<OwnLv> = Vec::new();
        let mut vgs = HashSet::new();
        for p in pools {
            let vg = p.spec.vg.as_str();
            if !vgs.insert(vg) {
                continue;
            }
            let rows = reads.lvs(&self.lvm, vg).await?;
            let Some(row) = rows.iter().find(|r| r.name == lv).cloned() else {
                continue;
            };
            let tagged = reads.tagged(&self.lvm, vg, lv).await?;
            let SnapTags::Own(tag) = snap_tags(&tagged.tags, uid) else {
                continue;
            };
            let Some(pool) = pools.iter().find(|q| {
                q.spec.vg == vg && row.pool.as_deref() == Some(q.spec.thin_pool.as_str())
            }) else {
                return Err(format!(
                    "LV {vg}/{lv} of this snapshot lies outside the thin pools of this node's Pools"
                ));
            };
            hits.push(OwnLv {
                place: self.place(pools, Some(&pool.name_any()))?,
                row,
                tag,
                created: tagged.created,
            });
        }
        if hits.len() > 1 {
            let at: Vec<&str> = hits.iter().map(|h| h.place.pool.as_str()).collect();
            return Err(format!(
                "LV {lv} found in more than one Pool: {}",
                at.join(", ")
            ));
        }
        Ok(hits.pop())
    }

    /// Suspending on a replicated volume; None leaves the ack as it is.
    async fn suspend_step(
        &mut self,
        vol: &Volume,
        snap: &Snapshot,
        prev: Option<&SnapshotAck>,
        refused: Option<String>,
    ) -> Option<Result<(), String>> {
        let volume = &vol.name_any();
        let Some(p) = prev.filter(|a| a.phase == SnapshotPhase::Suspending) else {
            return Some(match (refused, snap.metadata.uid.as_deref()) {
                (Some(e), _) => Err(e),
                (None, None) => Err("snapshot has no uid".to_owned()),
                (None, Some(uid)) => match vol.spec.assignment.as_ref().and_then(|a| a.minor) {
                    Some(minor) => self.suspend(volume, minor, uid).await,
                    None => Err("assignment has no minor".to_owned()),
                },
            });
        };
        // Never a second suspend in one round: its take window would start
        // after the ack the controller already counted.
        if !p.ok {
            return None;
        }
        Some(if self.suspended(volume) {
            Ok(())
        } else {
            Err("IO is no longer suspended here".to_owned())
        })
    }

    /// The Taking ack of a diskful replica. An LV of the name that is not
    /// this Snapshot's fails the round untouched, with one Event.
    #[allow(clippy::too_many_arguments)]
    async fn take_step(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snap: &Snapshot,
        vol: &Volume,
        pool: Option<&str>,
        lv: &str,
        taken: Taken,
        prev: Option<&SnapshotAck>,
    ) -> SnapshotAck {
        let origin = lvm::names::volume_lv(&snap.spec.volume);
        let mut conflict = false;
        let res = match (self.place(pools, pool), snap_tag_of(snap)) {
            (Ok(place), Ok(tag)) => match self.foreign_lv(reads, &place, lv, &tag.uid).await {
                Ok(Some(why)) => {
                    conflict = true;
                    Err(why)
                }
                Ok(None) => {
                    let kind = if vol.spec.replicas >= 2 {
                        Origin::Drbd(self.snapshot_blocker(reads, &place, &origin, vol).await)
                    } else {
                        Origin::Local
                    };
                    self.take_snapshot(reads, &place, &origin, lv, kind, &tag)
                        .await
                }
                Err(e) => Err(e),
            },
            (Err(e), _) | (_, Err(e)) => Err(e),
        };
        let a = match res {
            Ok(t) => ack(&self.node, SnapshotPhase::Taking, t, Ok(())),
            Err(e) => ack(&self.node, SnapshotPhase::Taking, taken, Err(e)),
        };
        if conflict && prev != Some(&a) {
            self.events.push(StatusWrite::Event {
                kind: "Snapshot",
                name: snap.name_any(),
                uid: snap.metadata.uid.clone(),
                reason: SNAPSHOT_LV_CONFLICT.into(),
                message: a.error.clone().unwrap_or_default(),
            });
        }
        a
    }

    /// Why an LV of the snapshot LV's name is not this Snapshot's copy:
    /// it has no snap tag, or a foreign one. Such an LV is never adopted.
    async fn foreign_lv(
        &self,
        reads: &mut Reads,
        place: &Place,
        lv: &str,
        uid: &str,
    ) -> Result<Option<String>, String> {
        let rows = reads.lvs(&self.lvm, &place.vg).await?;
        if find_in_pool(&rows, lv, &place.thin)?.is_none() {
            return Ok(None);
        }
        let tags = reads.tags(&self.lvm, &place.vg, lv).await?;
        let at = format!("LV {}/{lv}", place.vg);
        Ok(match snap_tags(&tags, uid) {
            SnapTags::Own(_) => None,
            SnapTags::Foreign(t) => Some(format!(
                "{at} carries tag {t}, not this snapshot's; left untouched"
            )),
            SnapTags::None => Some(format!("{at} has no snapshot tag; left untouched")),
        })
    }

    /// Takes the snapshot LV, or finds it taken; returns what its ack
    /// records.
    async fn take_snapshot(
        &self,
        reads: &mut Reads,
        place: &Place,
        origin: &str,
        lv: &str,
        kind: Origin,
        tag: &storage::SnapTag,
    ) -> Result<Taken, String> {
        let tagged = storage::snap_tag(&tag.uid, tag.size_bytes, tag.source_replicas);
        self.take_lv(reads, place, origin, lv, kind, &tagged)
            .await?;
        let rows = reads.lvs(&self.lvm, &place.vg).await?;
        let row = find_in_pool(&rows, lv, &place.thin)?
            .ok_or_else(|| format!("snapshot LV {}/{lv} missing after its take", place.vg))?;
        let created = reads.tagged(&self.lvm, &place.vg, lv).await?.created;
        Ok(Taken {
            pool: Some(place.pool.clone()),
            lv_bytes: Some(row.size_bytes),
            // The snapshot is never active, so lvs gives it no allocation;
            // at the take it maps exactly what its origin maps.
            allocated_bytes: find_in_pool(&rows, origin, &place.thin)?
                .and_then(lvm::report::thin_used_bytes),
            lv_created_at: created.and_then(unix_time),
            tag_size_bytes: Some(tag.size_bytes),
            tag_source_replicas: Some(tag.source_replicas),
            ..Taken::default()
        })
    }

    async fn take_lv(
        &self,
        reads: &mut Reads,
        place: &Place,
        origin: &str,
        lv: &str,
        kind: Origin,
        tag: &str,
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
            let res = self.lvm.snapshot(&place.vg, origin, lv, &[tag]).await;
            reads.invalidate(&place.vg);
            return res.map_err(|e| e.to_string());
        };
        if let Some(e) = not_ready {
            return Err(e);
        }
        // Thin snapshots copy no tags (seen on LVM 2.03); kept as a guard,
        // since a snapshot must never carry the origin's proofs of metadata
        // or provenance.
        let inherited: Vec<String> = reads
            .tags(&self.lvm, &place.vg, origin)
            .await?
            .into_iter()
            .filter(|t| {
                crate::drbd::is_md_tag(t) || t == storage::NEW_TAG || storage::is_from_tag(t)
            })
            .collect();
        tracing::info!(lv, origin, vg = place.vg, "taking snapshot");
        let res = self.lvm.snapshot(&place.vg, origin, lv, &[tag]).await;
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
    /// made this pass, of a disk that is not UpToDate, or that may differ
    /// from the other copies of the round, is no data at all.
    async fn snapshot_blocker(
        &self,
        reads: &Reads,
        place: &Place,
        origin: &str,
        vol: &Volume,
    ) -> Option<String> {
        if reads.created.contains(&format!("{}/{origin}", place.vg)) {
            return Some(format!(
                "volume LV {}/{origin} was created this pass: nothing to snapshot yet",
                place.vg
            ));
        }
        let res = self.tracked(origin);
        let disk = res
            .as_ref()
            .and_then(|r| r.devices.get(&crate::drbd::VOLUME).cloned())
            .and_then(|d| d.disk);
        if disk.as_deref() != Some(DISK_UP_TO_DATE) {
            return Some(format!(
                "local DRBD disk is {}, not UpToDate",
                disk.as_deref().unwrap_or("not attached")
            ));
        }
        let (Some(res), Some(a)) = (res, vol.spec.assignment.as_ref()) else {
            return Some("not assigned or not up".to_owned());
        };
        // A Secondary cut from the Primary may still say UpToDate.
        for p in a
            .replicas
            .iter()
            .filter(|p| p.role == Role::Diskful && p.node != self.node)
        {
            let conn = res.connections.get(&p.node).map(|c| c.state.as_str());
            let peer_disk = res
                .peer_devices
                .get(&(p.node.clone(), crate::drbd::VOLUME))
                .and_then(|d| d.peer_disk.as_deref());
            if conn != Some("Connected") || peer_disk != Some(DISK_UP_TO_DATE) {
                return Some(format!(
                    "peer {} is {}, disk {}",
                    p.node,
                    conn.unwrap_or("unknown"),
                    peer_disk.unwrap_or("unknown")
                ));
            }
        }
        // Between lvextend and the DRBD resize, the metadata sits mid LV.
        if let Some(minor) = a.minor {
            let bytes = match self.sectors(minor).await {
                Ok(n) => n * 512,
                Err(e) => return Some(format!("size of the DRBD device: {e}")),
            };
            if bytes < vol.spec.size_bytes {
                return Some(format!(
                    "DRBD device has {bytes} bytes, below the volume's {}: a resize is in flight",
                    vol.spec.size_bytes
                ));
            }
        }
        let window = model::snapshot::TAKE_WINDOW;
        let fresh = self
            .freezes
            .get(origin)
            .is_some_and(|f| f.round.is_some() && f.since.elapsed() < window);
        (!fresh || !res.suspended_by_user()).then(|| {
            format!(
                "IO not suspended by this agent within the last {} s",
                window.as_secs()
            )
        })
    }

    /// Removes the snapshot LV in `place` unless a foreign snap tag marks
    /// it: then it is not this one's, and none was found. One with no snap
    /// tag was taken before tags existed.
    async fn remove_snapshot(
        &self,
        reads: &mut Reads,
        place: &Place,
        lv: &str,
        uid: &str,
    ) -> Result<(), String> {
        let rows = reads.lvs(&self.lvm, &place.vg).await?;
        if find_in_pool(&rows, lv, &place.thin)?.is_none() {
            return Ok(());
        }
        let tags = reads.tags(&self.lvm, &place.vg, lv).await?;
        if let SnapTags::Foreign(tag) = snap_tags(&tags, uid) {
            tracing::info!(
                lv,
                vg = place.vg,
                tag,
                "snapshot LV not this snapshot's: left"
            );
            return Ok(());
        }
        tracing::info!(lv, vg = place.vg, "removing snapshot LV");
        let res = self.lvm.remove(&place.vg, lv).await;
        reads.invalidate(&place.vg);
        res.map_err(|e| e.to_string())
    }

    /// Thin LVs nobody claims are left alone, logged once per name. A
    /// snapshot LV of no Snapshot also gets one ImageOrphanLv Event on its
    /// Pool, once it was one two passes in a row: a cache that lags behind
    /// a new Snapshot never makes one.
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
        let mut suspects = HashSet::new();
        for pool in pools {
            let Ok(rows) = reads.lvs(&self.lvm, &pool.spec.vg).await else {
                continue;
            };
            let thin: Vec<Lv> = storage::thin_lvs(&rows, &pool.spec.thin_pool)
                .cloned()
                .collect();
            for r in &thin {
                let key = format!("{}/{}", pool.spec.vg, r.name);
                if !known.contains(&r.name) && self.orphans_logged.insert(key.clone()) {
                    tracing::info!(lv = key, "LV without a Volume or Snapshot: left alone");
                }
            }
            // A holder ack names an LV whose tag its take or search checked.
            let pool_name = pool.name_any();
            let acked = |lv: &str| {
                snaps.iter().any(|s| {
                    lvm::names::snapshot_lv(&s.spec.volume, &s.name_any()) == lv
                        && s.status.iter().flat_map(|st| &st.acks).any(|a| {
                            a.node == self.node
                                && a.taken_pool.as_deref() == Some(pool_name.as_str())
                        })
                })
            };
            for r in thin
                .iter()
                .filter(|r| r.name.contains(lvm::names::SNAPSHOT_SEP) && !acked(&r.name))
            {
                let Ok(tagged) = reads.tagged(&self.lvm, &pool.spec.vg, &r.name).await else {
                    break;
                };
                let Some(why) = orphan_snapshot_lv(snaps, &tagged.tags) else {
                    continue;
                };
                let key = format!("{}/{}", pool.name_any(), r.name);
                if self.orphan_suspects.contains(&key) && self.orphans_reported.insert(key.clone())
                {
                    let message = format!("LV {}/{}: {why}; left alone", pool.spec.vg, r.name);
                    tracing::warn!(pool = pool.name_any(), message, "orphan snapshot LV");
                    self.events.push(StatusWrite::Event {
                        kind: "Pool",
                        name: pool.name_any(),
                        uid: pool.metadata.uid.clone(),
                        reason: IMAGE_ORPHAN_LV.into(),
                        message,
                    });
                }
                suspects.insert(key);
            }
        }
        self.orphan_suspects = suspects;
    }
}

/// Why a snapshot LV no holder ack names belongs to no Snapshot: a snap
/// tag names a uid no Snapshot has (objects restored from a backup get new
/// uids) or does not parse, or it has no snap tag.
fn orphan_snapshot_lv(snaps: &[&Snapshot], tags: &[String]) -> Option<String> {
    let mut snap_tags = tags.iter().filter(|t| storage::is_snap_tag(t)).peekable();
    if snap_tags.peek().is_none() {
        return Some("no snapshot tag, and no Snapshot's ack names it".to_owned());
    }
    snap_tags.find_map(|tag| match storage::parse_snap_tag(tag) {
        None => Some(format!("tag {tag} does not parse")),
        Some(t) => (!snaps
            .iter()
            .any(|s| s.metadata.uid.as_deref() == Some(t.uid.as_str())))
        .then(|| format!("tag {tag} names a Snapshot uid no Snapshot has")),
    })
}

/// Warning Event on a Pool: a snapshot LV in it belongs to no Snapshot.
pub const IMAGE_ORPHAN_LV: &str = "ImageOrphanLv";

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

/// The LV of that name in the Pool's thin pool. A same-named LV elsewhere
/// (thick, another thin pool) is not ours and is never touched.
fn find_in_pool<'a>(rows: &'a [Lv], name: &str, thin: &str) -> Result<Option<&'a Lv>, String> {
    match rows.iter().find(|r| r.name == name) {
        Some(r) if r.pool.as_deref() != Some(thin) => {
            Err(format!("LV exists outside thin pool {thin}"))
        }
        r => Ok(r),
    }
}

/// A Snapshot's LV found by its uid tag.
#[derive(Debug, Clone)]
struct OwnLv {
    place: Place,
    row: Lv,
    tag: storage::SnapTag,
    /// lv_time, unix seconds.
    created: Option<i64>,
}

impl OwnLv {
    /// What an ack records of it. An inactive snapshot reports no
    /// allocation, and its origin has moved on since the take.
    fn taken(&self) -> Taken {
        Taken {
            pool: Some(self.place.pool.clone()),
            lv_bytes: Some(self.row.size_bytes),
            lv_created_at: self.created.and_then(unix_time),
            tag_size_bytes: Some(self.tag.size_bytes),
            tag_source_replicas: Some(self.tag.source_replicas),
            ..Taken::default()
        }
    }
}

/// Where this node's snapshot LV lives and what its tag records, as its
/// successful Taking ack recorded them, and its digest once hashed; every
/// later ack carries them unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Taken {
    pool: Option<String>,
    lv_bytes: Option<u64>,
    allocated_bytes: Option<u64>,
    lv_created_at: Option<Time>,
    tag_size_bytes: Option<u64>,
    tag_source_replicas: Option<u32>,
    digest: Option<String>,
    digest_bytes: Option<u64>,
}

impl Taken {
    fn of(prev: Option<&SnapshotAck>) -> Self {
        prev.map_or_else(Self::default, |a| Self {
            pool: a.taken_pool.clone(),
            lv_bytes: a.lv_bytes,
            allocated_bytes: a.allocated_bytes,
            lv_created_at: a.lv_created_at.clone(),
            tag_size_bytes: a.tag_size_bytes,
            tag_source_replicas: a.tag_source_replicas,
            digest: a.digest.clone(),
            digest_bytes: a.digest_bytes,
        })
    }
}

fn ack(node: &str, phase: SnapshotPhase, taken: Taken, res: Result<(), String>) -> SnapshotAck {
    SnapshotAck {
        taken_pool: taken.pool,
        lv_bytes: taken.lv_bytes,
        allocated_bytes: taken.allocated_bytes,
        lv_created_at: taken.lv_created_at,
        tag_size_bytes: taken.tag_size_bytes,
        tag_source_replicas: taken.tag_source_replicas,
        digest: taken.digest,
        digest_bytes: taken.digest_bytes,
        ok: res.is_ok(),
        error: res.err(),
        ..SnapshotAck::new(node, phase)
    }
}

/// Warning Event: an LV of a Snapshot's LV name that is not its copy.
pub const SNAPSHOT_LV_CONFLICT: &str = "SnapshotLvConflict";

/// What this Snapshot's LV tag records: the controller writes sizeBytes
/// and sourceReplicas on the way into Taking.
fn snap_tag_of(snap: &Snapshot) -> Result<storage::SnapTag, String> {
    let st = snap.status.as_ref();
    match (
        snap.metadata.uid.as_deref(),
        st.and_then(|s| s.size_bytes),
        st.and_then(|s| s.source_replicas),
    ) {
        (Some(uid), Some(size_bytes), Some(source_replicas)) => Ok(storage::SnapTag {
            uid: uid.to_owned(),
            size_bytes,
            source_replicas,
        }),
        _ => Err("no uid, sizeBytes or sourceReplicas to tag the snapshot LV with".to_owned()),
    }
}

/// What an LV's snap tags say about Snapshot `uid`.
enum SnapTags {
    /// Taken before tags existed.
    None,
    Own(storage::SnapTag),
    /// A snap tag that names another uid or does not parse: the LV is
    /// never this Snapshot's, never adopted or removed by it.
    Foreign(String),
}

fn snap_tags(tags: &[String], uid: &str) -> SnapTags {
    let mut own = None;
    for t in tags.iter().filter(|t| storage::is_snap_tag(t)) {
        match storage::parse_snap_tag(t) {
            Some(p) if p.uid == uid => {
                own.get_or_insert(p);
            }
            _ => return SnapTags::Foreign(t.clone()),
        }
    }
    own.map_or(SnapTags::None, SnapTags::Own)
}

fn unix_time(secs: i64) -> Option<Time> {
    k8s_openapi::jiff::Timestamp::from_second(secs)
        .ok()
        .map(Time)
}

/// Fewer acks with takenPool than the source had replicas: a holder may
/// have lost its ack. Bounds the search to such Snapshots, so a complete
/// set is never searched for again.
fn holder_missing(status: &model::SnapshotStatus) -> bool {
    let holders = status
        .acks
        .iter()
        .filter(|a| a.taken_pool.is_some())
        .count();
    status
        .source_replicas
        .is_some_and(|n| holders < usize::try_from(n).unwrap_or(usize::MAX))
}

/// A single-replica source has no DRBD superblock to bound the digest, and
/// a single copy nothing to match.
fn wants_digest(status: &model::SnapshotStatus) -> bool {
    status.source_replicas.is_some_and(|n| n != 1)
}

/// The Snapshot a volume restores from, when it exists with the uid the source names.
fn source_snapshot<'a>(vol: &Volume, snapshots: &'a [Arc<Snapshot>]) -> Option<&'a Snapshot> {
    let src = vol.spec.source.as_ref()?;
    snapshots.iter().find(|x| src.is(x)).map(AsRef::as_ref)
}

/// This node's Volume status entry from the LV row after the pass.
pub fn replica_entry(
    node: &str,
    row: Option<&Lv>,
    observed_generation: Option<i64>,
    last_error: Option<String>,
) -> ReplicaStatus {
    ReplicaStatus {
        node: node.to_owned(),
        present: row.is_some(),
        disk_state: row.map(|_| DISK_UP_TO_DATE.to_owned()),
        in_use: row.is_some_and(|r| r.open),
        device_path: row.filter(|r| r.active).map(|r| r.path.clone()),
        allocated_bytes: row.map(|r| r.size_bytes),
        size_bytes: row.map(|r| r.size_bytes),
        observed_generation,
        last_error,
        ..ReplicaStatus::default()
    }
}

#[cfg(test)]
mod tests;
