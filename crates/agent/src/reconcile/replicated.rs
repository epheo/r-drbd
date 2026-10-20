//! Replicated volumes (spec.replicas >= 2) on this node: backing LV of
//! gross size, resource file, metadata owned through LVM tags, adjust when
//! drbdadm has something to do, the first data made current, peers
//! leaving, resize, teardown, snapshot suspend and resume, split-brain
//! resolution, status from events2.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drbd_config::Features;
use kube::ResourceExt;
use lvm::report::Lv as LvRow;
use model::{
    AssignedReplica, Assignment, Pool, ReplicaStatus, Resolve, Role, Snapshot, SnapshotPhase,
    Volume,
};

use super::{
    Place, Reads, Reconciler, StatusWrite, find_in_pool, is_lost, lost_after_report, rebuild_error,
};
use crate::drbd::{self, DRBD_TIMEOUT, Facts, FileInputs, Host, RealHost, ResFiles, VOLUME, argv};
use crate::events2::Resource;
use crate::events2::Tracker;
use crate::exec::{failure, run_any, run_ok};
use crate::{ExecMode, storage};

const NO_DRBD: &str = "drbd-utils or the DRBD module not found on this node";
/// The wording of drbdsetup's refusal, matched to retry.
const RESYNC_BUSY: &str = "Resize not allowed during resync";
const POLL: Duration = Duration::from_millis(100);
const NOT_OURS: &str = "LV exists without an r-drbd ownership tag; not used. Remove it, or tag it r-drbd.new to let r-drbd recreate its DRBD metadata";

/// Everything DRBD work needs besides the executor.
#[derive(Debug, Clone)]
pub struct DrbdEnv {
    /// None when drbd-utils or the module are missing on this node.
    pub features: Option<Features>,
    pub files: ResFiles,
    pub host: Arc<dyn Host>,
    pub tracker: Arc<Mutex<Tracker>>,
    /// Bound on the events2 destroy after `drbdsetup down`.
    pub down_wait: Duration,
    /// Sleeps between adjust retries while a resync blocks a resize in it.
    pub adjust_retry: Vec<Duration>,
    /// How long the initial node waits for its diskful peers before it
    /// starts the volume without them.
    pub init_wait: Duration,
    /// First wait before a plan that came back is adjusted again.
    pub repeat_backoff: Duration,
    /// This host's kernel node name, for its own `on` section: drbdadm
    /// finds its host by uname, which may differ from the Kubernetes
    /// node name. None uses the node name.
    pub host_name: Option<String>,
}

impl DrbdEnv {
    pub fn new(
        features: Option<Features>,
        res_dir: impl Into<PathBuf>,
        tracker: Arc<Mutex<Tracker>>,
    ) -> Self {
        Self {
            features,
            files: ResFiles::new(res_dir),
            host: Arc::new(RealHost),
            tracker,
            // Down is a local state change; events2 reports it within
            // milliseconds, and drbdsetup status decides after anyway.
            down_wait: Duration::from_secs(5),
            // A resync finishing on a fresh small volume takes seconds;
            // a longer one leaves the adjust to the next pass.
            adjust_retry: vec![Duration::from_secs(1), Duration::from_secs(3)],
            // A peer agent is stale after 40 s without a heartbeat. A peer
            // still away after 60 s is down, and waiting longer only keeps
            // the volume unusable; it costs that peer one full sync.
            init_wait: Duration::from_secs(60),
            // One pass interval: a plan back on the next pass waits a pass.
            repeat_backoff: Duration::from_secs(30),
            host_name: None,
        }
    }
}

/// Gross backing size for the volume's net size.
fn gross_bytes(vol: &Volume) -> u64 {
    drbd_config::gross_bytes(vol.spec.size_bytes, model::keys::PEER_SLOTS)
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

/// Disk states that hold data a just-created replica must not override.
fn holds_data(disk: Option<&str>) -> bool {
    matches!(disk, Some("UpToDate" | "Consistent" | "Outdated"))
}

/// The backing LV after [`Reconciler::ensure_drbd_lv`].
struct Backing {
    row: LvRow,
    /// False while a needed growth waits for the attach.
    grown: bool,
    /// This pass created the LV.
    created: bool,
    /// Created in place of an LV this node had reported.
    rebuilt: bool,
    /// Created this pass from the source snapshot.
    restored: bool,
    /// Created empty this pass because this peer could not restore its copy.
    fallback: bool,
}

/// This replica's LV as a copy of a restore's source snapshot.
struct Copy {
    /// vg/lv, for messages.
    lv: String,
    /// Carries the from tag of this Volume and that Snapshot.
    tagged: bool,
    /// Has the size of the snapshot LV in the same lvs report: its metadata
    /// sits where the source's did, so its data area holds nothing but the
    /// snapshot's.
    at_size: bool,
}

fn snapshot_lv_name(snap: &Snapshot) -> String {
    lvm::names::snapshot_lv(&snap.spec.volume, &snap.name_any())
}

/// A dry-run plan that came back after its adjust succeeded.
#[derive(Debug, Clone)]
pub(super) struct Repeat {
    pub(super) plan: String,
    pub(super) times: u32,
    next: tokio::time::Instant,
}

/// The cap on the wait between adjusts of a plan that keeps coming back:
/// a StandAlone after an auth mismatch or a disk failing on attach gains
/// nothing from tries more often than this, and a fixed cause is picked up
/// within it.
const REPEAT_MAX: Duration = Duration::from_secs(600);

enum Resize {
    NotNeeded,
    Done,
    /// This node resizes once every diskful peer has grown its LV.
    Waiting,
}

/// Why the initial node must not start without its missing peers, if
/// anything.
fn timeout_blocker(s: &Step<'_>, r: &Resource, lost: bool) -> Option<&'static str> {
    if s.restoring() {
        // Only the initial node ever makes a restore's data, and first_data
        // gets here only with its proven copy of the snapshot, restored
        // again after a loss too: whatever a peer was shown before
        // initializedAt is that snapshot, and quorum protects nothing.
        return None;
    }
    // Statuses only report a disk state DRBD reached; UpToDate, Consistent
    // or Outdated anywhere means data was made already.
    let seen_data = s
        .vol
        .status
        .iter()
        .flat_map(|st| &st.replicas)
        .any(|e| holds_data(e.disk_state.as_deref()));
    if lost || seen_data {
        return Some("data may exist on a peer not connected");
    }
    if r.devices.get(&VOLUME).and_then(|d| d.quorum) != Some(true) {
        // UpToDate without quorum serves nothing and costs the missing
        // peers a full sync.
        return Some("no quorum yet");
    }
    None
}

/// Diskful peers the first data still waits for: not connected, not
/// Inconsistent, or, for a restore from `source`, without a present entry
/// reporting that snapshot in restoredFrom.
fn unready_peers<'a>(
    s: &'a Step<'_>,
    r: &Resource,
    node: &str,
    source: Option<&str>,
    entries: &[ReplicaStatus],
) -> Vec<&'a str> {
    // present and devicePath: the entry was written after the LV existed,
    // so a report from before a lost LV never counts.
    let restored = |peer: &str| {
        source.is_none_or(|snap| {
            entries.iter().any(|e| {
                e.node == peer
                    && e.present
                    && e.device_path.is_some()
                    && e.restored_from.as_deref() == Some(snap)
            })
        })
    };
    s.a.replicas
        .iter()
        .filter(|p| p.node != node && drbd::is_diskful(p))
        .filter(|p| {
            let connected = r
                .connections
                .get(&p.node)
                .is_some_and(|c| c.state == "Connected");
            let inconsistent = r
                .peer_devices
                .get(&(p.node.clone(), VOLUME))
                .is_some_and(|d| d.peer_disk.as_deref() == Some("Inconsistent"));
            !(connected && inconsistent && restored(&p.node))
        })
        .map(|p| p.node.as_str())
        .collect()
}

/// Why this pass must not adjust, if anything.
fn adjust_blocker(s: &Step<'_>, snapshots: &[Arc<Snapshot>], resolved: bool) -> Option<String> {
    let r = s.res?;
    // The controller's latched condition outlives an agent restart, which
    // loses the split-brain helper call.
    let latched = s.vol.status.as_ref().is_some_and(|st| {
        model::condition::is_true(&st.conditions, model::condition::types::SPLIT_BRAIN)
    });
    let diskful_peer = |p: &str| {
        s.a.replicas
            .iter()
            .any(|x| x.node == p && x.role == Role::Diskful)
    };
    if let Some((peer, _)) = r
        .connections
        .iter()
        .find(|(p, c)| c.state == "StandAlone" && (c.split_brain || (latched && diskful_peer(p))))
    {
        // A reconnect would only split again; spec.resolve decides.
        return Some(format!("split brain with {peer} waits for spec.resolve"));
    }
    if r.suspended_by_user() && snapshot_round_active(snapshots, &s.vol.name_any()) {
        // An attach or a connect can block on the frozen IO and push the
        // round past its bound; the round resumes IO soon.
        return Some("IO suspended for a snapshot round".to_owned());
    }
    if resolved {
        // The discard handshake is in flight; adjust would race it.
        return Some("split-brain resolution in progress".to_owned());
    }
    None
}

impl Reconciler {
    pub(super) fn drbd_available(&self) -> bool {
        self.drbd.features.is_some() && self.drbd.tracker.lock().is_ok_and(|t| t.is_available())
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

    async fn drbd_cmd(&self, argv: &[String], mode: ExecMode) -> Result<String, String> {
        run_ok(self.lvm.exec(), argv, mode)
            .await
            .map(|o| o.stdout)
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
        let Some(features) = self.drbd.features else {
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
                let step = Step {
                    vol,
                    a: assignment,
                    mine: &m,
                    features,
                    minor: 0,
                    res: res.as_ref(),
                    prev: prev.as_ref(),
                };
                self.drbd_up(reads, pools, snapshots, step).await
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
        self.facts.remove(&name);
        self.init_started.remove(&name);
        self.rebuilt_seen.remove(&name);
        self.repeats.remove(&name);
        self.slots_swept.remove(&name);
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
                    || self.drbd.files.candidate_path(&name).exists();
                if local {
                    self.teardown(reads, &name, res, found.as_ref()).await
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
    /// dropped, the LV left alone as an orphan without its ownership tag.
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
            // Placed here again, the copy is stale and must not pass as ours.
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
            .filter(|t| drbd::is_md_tag(t) || storage::is_from_tag(t))
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
            self.drbd_cmd(&argv::resume_io(name), DRBD_TIMEOUT).await?;
        }
        tracing::info!(volume = name, "taking DRBD resource down");
        self.drbd_cmd(&argv::down(name), DRBD_TIMEOUT).await?;
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

    /// Down (resumed first if a snapshot round left it suspended), remove
    /// the LV, remove the files. The LV goes with its metadata: a thin
    /// pool hands out zeroed blocks, so nothing of it reaches a later LV.
    async fn teardown(
        &self,
        reads: &mut Reads,
        name: &str,
        res: Option<&Resource>,
        found: Option<&(Place, LvRow)>,
    ) -> Result<(), String> {
        match res {
            Some(r) => self.down(name, r).await?,
            // Not tracked is not proof of down: lvremove follows.
            None => self.confirm_down(name).await?,
        }
        if let Some((place, row)) = found {
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

    async fn confirm_down(&self, name: &str) -> Result<(), String> {
        let out = run_any(self.lvm.exec(), &argv::status(name), DRBD_TIMEOUT)
            .await
            .map_err(|e| e.to_string())?;
        if out.exit_code == Some(0) {
            Err(format!("DRBD resource {name} still up after down"))
        } else {
            Ok(())
        }
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
        let mut entry = drbd::entry(&self.node, now.as_ref(), facts, sectors, fallback);
        entry.unreachable_peers = self.peers.reported(name, tokio::time::Instant::now());
        entry
    }

    /// Feeds the connection states of this node's replica to the stuck
    /// connection watch.
    fn observe_peers(&mut self, vol: &Volume, a: &Assignment, res: Option<&Resource>) {
        let name = vol.name_any();
        let entries = vol.status.as_ref().map(|s| s.replicas.as_slice());
        let up = |node: &str| {
            entries
                .unwrap_or_default()
                .iter()
                .any(|e| e.node == node && e.present)
        };
        let lease = |node: &str| match &self.peer_leases {
            None => (true, None),
            Some(m) => m
                .get(node)
                .map_or((false, None), |l| (l.fresh(), l.acquired)),
        };
        let prior = entries
            .unwrap_or_default()
            .iter()
            .find(|e| e.node == self.node)
            .map(|e| e.unreachable_peers.as_slice())
            .unwrap_or_default();
        let seen = res
            .into_iter()
            .flat_map(|r| &r.connections)
            .filter_map(|(peer, c)| {
                let r = a.replicas.iter().find(|r| &r.node == peer)?;
                let target = drbd::socket(peer, r.port, &self.addresses).ok()?;
                let (fresh, epoch) = lease(peer);
                Some((
                    peer.clone(),
                    crate::peer::Seen {
                        state: c.state.clone(),
                        target,
                        peer_up: up(peer) && fresh,
                        epoch,
                        prior: prior.iter().find(|p| &p.node == peer).cloned(),
                    },
                ))
            })
            .collect();
        self.peers
            .observe(&name, &seen, tokio::time::Instant::now());
    }

    /// Probes the connections stuck past the window, all at once so a
    /// firewall that drops costs one timeout per pass, not one per volume;
    /// then refreshes the writes of the volumes they changed.
    pub(super) async fn probe_peers(&mut self, vols: &[&Volume], writes: &mut Vec<StatusWrite>) {
        if let Ok(t) = self.drbd.tracker.lock() {
            self.peers.retain(|v| t.resource(v).is_some());
        }
        let due = self.peers.due(tokio::time::Instant::now());
        if due.is_empty() {
            return;
        }
        // From the address DRBD uses here, as source-keyed rules see it.
        let from = self.addresses.get(&self.node).copied();
        // Boxed: a stream of borrowing closures trips the Send check of
        // the spawned reconcile loop.
        let probes: Vec<futures::future::BoxFuture<'static, std::io::Result<()>>> = due
            .iter()
            .map(|(_, _, target)| {
                let (host, target) = (self.drbd.host.clone(), *target);
                Box::pin(
                    async move { host.connect(from, target, crate::peer::PROBE_TIMEOUT).await },
                ) as futures::future::BoxFuture<'static, _>
            })
            .collect();
        let results = futures::future::join_all(probes).await;
        let mut changed = std::collections::BTreeSet::new();
        let now = tokio::time::Instant::now();
        for ((volume, peer, target), r) in due.into_iter().zip(results) {
            let Some(p) = self.peers.record(&volume, &peer, target, &r, now) else {
                continue;
            };
            tracing::warn!(
                volume,
                peer,
                %target,
                result = ?p.result,
                error = p.error.as_deref().unwrap_or_default(),
                "DRBD connection stuck; probed the peer's port"
            );
            changed.insert(volume);
        }
        for name in changed {
            let Some(vol) = vols.iter().find(|v| v.name_any() == name) else {
                continue;
            };
            let Some(facts) = self.facts.get(&name).cloned() else {
                continue;
            };
            writes.retain(|w| !matches!(w, StatusWrite::Volume { name: n, .. } if *n == name));
            let entry = self.current_entry(&name, &facts).await;
            if self.prev_entry(vol).as_ref() != Some(&entry) {
                writes.push(StatusWrite::Volume { name, entry });
            }
        }
    }

    async fn drbd_up(
        &mut self,
        reads: &mut Reads,
        pools: &[&Pool],
        snapshots: &[Arc<Snapshot>],
        mut step: Step<'_>,
    ) -> Option<StatusWrite> {
        let name = step.vol.name_any();
        let prev = step.prev;
        let Some(minor) = step.a.minor else {
            let e = self.error_entry(prev, "assignment has no minor".to_owned());
            return (prev != Some(&e)).then_some(StatusWrite::Volume { name, entry: e });
        };
        step.minor = minor;
        let mut facts = Facts {
            minor,
            diskful: step.mine.role == Role::Diskful,
            allocated_bytes: self
                .facts
                .get(&name)
                .and_then(|f| f.allocated_bytes)
                .or_else(|| prev.and_then(|p| p.allocated_bytes)),
            observed_generation: prev.and_then(|p| p.observed_generation),
            last_error: None,
            restored_from: None,
        };
        match self
            .drbd_apply(reads, pools, snapshots, &step, &mut facts)
            .await
        {
            Ok(true) => facts.observed_generation = step.vol.metadata.generation,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(volume = name, error = %e, "replicated volume reconcile failed");
                facts.last_error = Some(e);
            }
        }
        let res = self.tracked(&name);
        self.observe_peers(step.vol, step.a, res.as_ref());
        let entry = self.current_entry(&name, &facts).await;
        self.facts.insert(name.clone(), facts);
        (prev != Some(&entry)).then_some(StatusWrite::Volume { name, entry })
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
        // loss on record, so the next pass rebuilds again (never restores).
        let prev_lost = s.prev_lost();
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
        let source = super::source_snapshot(s.vol, snapshots);
        let backing = if diskful {
            let place = self.place(pools, s.mine.pool.as_deref())?;
            let b = self.ensure_drbd_lv(reads, source, s, &place).await?;
            *rebuilt = b.rebuilt;
            if b.restored {
                self.rebuilt_seen.remove(&name);
            } else if b.rebuilt {
                self.rebuilt_seen.insert(name.clone());
            }
            complete &= b.grown;
            facts.allocated_bytes = Some(b.row.size_bytes);
            if b.fallback {
                // Its status must land first: the entry, without
                // restoredFrom, reaches the initial node before this
                // replica's DRBD disk can show up Inconsistent beside it.
                return Ok(false);
            }
            let chunk = reads.chunk_bytes(&place).or_else(|| {
                pools
                    .iter()
                    .find(|p| p.name_any() == place.pool)
                    .and_then(|p| p.status.as_ref()?.chunk_bytes)
            });
            Some((b, chunk, place))
        } else {
            None
        };

        self.write_file(
            s,
            &name,
            backing.as_ref().map(|(b, c, _)| (b.row.path.as_str(), *c)),
        )
        .await?;

        self.remove_departed(s, &name, diskful).await?;
        if let Some((b, ..)) = &backing {
            self.sweep_slots(s, &name, &b.row.path).await;
        }

        let copy = match &backing {
            Some((b, _, place)) => self.restore_copy(reads, source, s, place, &b.row).await?,
            None => None,
        };
        if self.awaiting_own_entry(s, copy.as_ref()).await {
            return Ok(false);
        }
        if let Some((b, _, place)) = &backing
            && let Err(e) = self.ensure_metadata(reads, s, place, b).await
        {
            if b.created {
                // An LV made this pass holds no data; without it the next
                // pass starts over from a clean create.
                tracing::warn!(volume = name, "removing the LV created this pass");
                let removed = self.remove_volume(&place.vg, &b.row).await;
                reads.invalidate(&place.vg);
                if let Err(re) = removed {
                    tracing::warn!(volume = name, error = %re, "LV removal failed");
                }
            }
            return Err(e);
        }
        facts.restored_from = copy
            .as_ref()
            .filter(|c| c.tagged && c.at_size)
            .and(s.vol.spec.source.as_ref())
            .map(|src| src.snapshot.clone());

        if let Some(r) = s.res
            && r.suspended_by_user()
            && !snapshot_round_active(snapshots, &name)
        {
            tracing::info!(
                volume = name,
                "IO suspended without a snapshot round: resuming"
            );
            self.drbd_cmd(&argv::resume_io(&name), DRBD_TIMEOUT).await?;
        }

        let mut resolved = false;
        if diskful && let (Some(rv), Some(r)) = (&s.vol.spec.resolve, s.res) {
            resolved = self.resolve(s, rv, r).await?;
        }

        if diskful && let Some(r) = s.res {
            match self.resize_step(s, r).await? {
                Resize::NotNeeded | Resize::Done => {}
                Resize::Waiting => complete = false,
            }
        }

        match adjust_blocker(s, snapshots, resolved) {
            Some(why) => tracing::debug!(volume = name, why, "adjust held"),
            None => {
                if let Some(held) = self.adjust_if_needed(s, &name).await? {
                    // Only the adjust waits; the rest of the pass runs.
                    tracing::warn!(volume = name, held, "adjust backed off");
                    facts.last_error = Some(held);
                    complete = false;
                }
            }
        }

        if diskful {
            let lost = *rebuilt || s.prev_lost() || self.rebuilt_seen.contains(&name);
            self.first_data(s, &name, lost, copy.as_ref()).await?;
        }
        Ok(complete)
    }

    /// Renders, and installs only a changed file that drbdadm accepts.
    async fn write_file(
        &mut self,
        s: &Step<'_>,
        name: &str,
        backing: Option<(&str, Option<u64>)>,
    ) -> Result<(), String> {
        let inputs = FileInputs {
            node: &self.node,
            host_name: self.drbd.host_name.as_deref().unwrap_or(&self.node),
            volume: name,
            assignment: s.a,
            backing: backing.map(|(p, _)| p),
            addresses: &self.addresses,
            chunk_bytes: backing.and_then(|(_, c)| c),
            access_mode: s.vol.spec.access_mode,
        };
        let resource = drbd::resource(&inputs)?;
        let rendered = drbd_config::render(&resource, s.features).map_err(|e| e.to_string())?;
        if !rendered.dropped.is_empty() && self.dropped_logged.insert(name.to_owned()) {
            tracing::warn!(
                volume = name,
                settings = rendered.dropped.join(", "),
                "drbd-utils on this node lack these settings: left out"
            );
        }
        let files = &self.drbd.files;
        if files.installed(name, &rendered.text)? {
            return Ok(());
        }
        let candidate = files.write_candidate(name, &rendered.text)?;
        let installed = files.path(name);
        let check = argv::validate(
            &candidate.to_string_lossy(),
            &installed.to_string_lossy(),
            name,
        );
        if let Err(e) = self.drbd_cmd(&check, DRBD_TIMEOUT).await {
            let _ = std::fs::remove_file(&candidate);
            return Err(format!("drbdadm rejects the new resource file: {e}"));
        }
        tracing::info!(volume = name, "installing resource file");
        self.drbd.files.install(name)
    }

    /// Create, restore, activate or grow the backing LV to the DRBD gross
    /// size rounded to the extent, when [`Step::may_grow`] allows.
    /// A lost LV is rebuilt empty once the volume is initialized: its peers
    /// hold the data, and a restore would bring back older data. Before
    /// that, every diskful replica of a restore restores from its own copy
    /// of the snapshot, a rebuild too: the copies are identical, so the
    /// first data needs no sync, and an empty copy on the initial node
    /// would become the source.
    async fn ensure_drbd_lv(
        &self,
        reads: &mut Reads,
        source: Option<&Snapshot>,
        s: &Step<'_>,
        place: &Place,
    ) -> Result<Backing, String> {
        let (vol, prev) = (s.vol, s.prev);
        let lv = lvm::names::volume_lv(&vol.name_any());
        let vg = place.vg.as_str();
        let rows = reads.lvs(&self.lvm, vg).await?;
        let mut row = find_in_pool(&rows, &lv, &place.thin)?.cloned();
        let extent = reads.vg(&self.lvm, vg).await?.extent_bytes;
        let desired = storage::desired_bytes(gross_bytes(vol), extent);
        let rebuilt = row.is_none() && lost_after_report(prev);
        let restore = row.is_none() && s.restoring();
        let mut fallback = false;
        if row.is_none() {
            let created = if restore {
                if rebuilt {
                    tracing::warn!(
                        lv,
                        vg,
                        "restoring the lost backing LV again: not initialized yet"
                    );
                }
                let (r, fell_back) = self
                    .restore_or_fall_back(&rows, source, s, place, &lv, desired)
                    .await;
                fallback = fell_back;
                r
            } else if rebuilt {
                tracing::warn!(lv, vg, bytes = desired, "rebuilding lost backing LV");
                self.lvm
                    .create(vg, &place.thin, &lv, desired, &[storage::NEW_TAG])
                    .await
                    .map_err(|e| e.to_string())
            } else {
                tracing::info!(lv, vg, bytes = desired, "creating DRBD backing LV");
                self.lvm
                    .create(vg, &place.thin, &lv, desired, &[storage::NEW_TAG])
                    .await
                    .map_err(|e| e.to_string())
            };
            reads.invalidate(vg);
            // The loss stays on record until a pass completes.
            created.map_err(|e| if rebuilt { rebuild_error(&e) } else { e })?;
            row = reads.find(&self.lvm, vg, &lv, &place.thin).await?;
        }
        let created = row.is_some() && rows.iter().all(|r| r.name != lv);
        if created {
            reads.created.insert(format!("{vg}/{lv}"));
        }
        let restoring = s.restoring();
        let r = row.ok_or_else(|| format!("LV {vg}/{lv} missing after create"))?;
        let may_grow = s.may_grow(created) || s.regrows(rebuilt && created, r.size_bytes);
        let mut changed = false;
        if !r.active {
            changed = true;
            self.lvm
                .activate(vg, &lv)
                .await
                .map_err(|e| e.to_string())?;
        }
        let mut grown = true;
        if r.size_bytes < desired && !may_grow {
            if restoring {
                tracing::info!(lv, "restored LV grows once its disk holds data");
            } else {
                tracing::info!(lv, "backing LV grows once the DRBD disk is attached");
            }
            grown = false;
        } else if r.size_bytes < desired {
            tracing::info!(
                lv,
                from = r.size_bytes,
                to = desired,
                "growing DRBD backing LV"
            );
            changed = true;
            self.lvm
                .extend(vg, &lv, desired)
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
            restored: restore && !fallback,
            fallback,
        })
    }

    /// Restores this replica's LV from its copy. A peer that cannot starts
    /// empty and syncs in full from the initial node (true); the initial
    /// node has nothing to sync from, so its error stands.
    async fn restore_or_fall_back(
        &self,
        rows: &[LvRow],
        source: Option<&Snapshot>,
        s: &Step<'_>,
        place: &Place,
        lv: &str,
        desired: u64,
    ) -> (Result<(), String>, bool) {
        let vg = place.vg.as_str();
        match self.create_lv(rows, source, place, s.vol, lv, desired).await {
            Err(e) if !s.initial_node() => {
                tracing::warn!(
                    lv,
                    vg,
                    error = %e,
                    "cannot restore this node's copy: empty LV, full sync from the initial node"
                );
                let r = self
                    .lvm
                    .create(vg, &place.thin, lv, desired, &[storage::NEW_TAG])
                    .await
                    .map_err(|e2| format!("restore failed: {e}; the empty LV failed too: {e2}"));
                (r, true)
            }
            r => (
                r.map_err(|e| format!("restore before the volume was initialized: {e}; the volume cannot start without its source")),
                false,
            ),
        }
    }

    /// The peers still unproven on a live read of the Volume, None when the
    /// read failed: the cache may lag behind a peer's committed entry while
    /// its connection already shows, so the clear-bitmap rests on this read,
    /// and a failed one waits.
    async fn live_unready<'a>(
        &self,
        s: &'a Step<'_>,
        r: &Resource,
        proving: Option<&str>,
    ) -> Option<Vec<&'a str>> {
        let name = s.vol.name_any();
        let live = match self.live.get(&name).await {
            Ok(v) if v.metadata.uid == s.vol.metadata.uid => v,
            Ok(_) => {
                tracing::debug!(volume = name, "live read found another Volume: waiting");
                return None;
            }
            Err(e) => {
                tracing::debug!(volume = name, error = %e, "live read failed: waiting");
                return None;
            }
        };
        let entries = live.status.as_ref().map_or(&[][..], |st| &st.replicas);
        Some(unready_peers(s, r, &self.node, proving, entries))
    }

    /// A peer of a restore whose LV is not a proven copy brings its disk up
    /// only once a live read shows its own entry without restoredFrom: an
    /// entry from before the LV was lost must never sit in the API server
    /// while this disk can connect. Until then the step ends, without an
    /// error, and the pass's write carries the entry.
    async fn awaiting_own_entry(&self, s: &Step<'_>, copy: Option<&Copy>) -> bool {
        let Some(c) = copy else { return false };
        if s.initial_node() || (c.tagged && c.at_size) {
            return false;
        }
        let attached = s
            .res
            .and_then(|r| r.devices.get(&VOLUME))
            .is_some_and(|d| drbd::disk_has_metadata(d.disk.as_deref()));
        if attached {
            return false;
        }
        let name = s.vol.name_any();
        match self.live.get(&name).await {
            Ok(v) if v.metadata.uid == s.vol.metadata.uid => {
                let stale = v
                    .status
                    .iter()
                    .flat_map(|st| &st.replicas)
                    .any(|e| e.node == self.node && e.restored_from.is_some());
                if stale {
                    tracing::debug!(volume = name, "awaiting the entry without restoredFrom");
                }
                stale
            }
            Ok(_) => {
                tracing::debug!(volume = name, "live read found another Volume: waiting");
                true
            }
            Err(e) => {
                tracing::debug!(volume = name, error = %e, "live read failed: awaiting the entry");
                true
            }
        }
    }

    /// What this replica's LV is to a restore not yet initialized, None for
    /// any other volume. Read from LVM each pass, never remembered; a read
    /// error fails the pass, so no first-data decision rests on it.
    async fn restore_copy(
        &self,
        reads: &mut Reads,
        source: Option<&Snapshot>,
        s: &Step<'_>,
        place: &Place,
        row: &LvRow,
    ) -> Result<Option<Copy>, String> {
        if !s.restoring() {
            return Ok(None);
        }
        let mut copy = Copy {
            lv: format!("{}/{}", place.vg, row.name),
            tagged: false,
            at_size: false,
        };
        let Some(snap) = source else {
            return Ok(Some(copy));
        };
        let (Some(vol_uid), Some(snap_uid)) =
            (s.vol.metadata.uid.as_deref(), snap.metadata.uid.as_deref())
        else {
            return Ok(Some(copy));
        };
        let tag = storage::from_tag(vol_uid, snap_uid);
        copy.tagged = reads
            .tags(&self.lvm, &place.vg, &row.name)
            .await?
            .contains(&tag);
        let rows = reads.lvs(&self.lvm, &place.vg).await?;
        let size =
            |name: &str| find_in_pool(&rows, name, &place.thin).map(|r| r.map(|r| r.size_bytes));
        let (mine, theirs) = (size(&row.name)?, size(&snapshot_lv_name(snap))?);
        copy.at_size = mine.is_some() && mine == theirs;
        Ok(Some(copy))
    }

    /// Ownership comes from the LV's tags only:
    /// - `r-drbd.md.<volume uid>.<node id>` of this replica: ours, kept;
    /// - `r-drbd.new` (set by lvcreate) or created this pass: never held
    ///   data, metadata is (re)created;
    /// - another r-drbd metadata tag: a former placement of this node, or a
    ///   deleted Volume that had the same name; stale, made new first;
    /// - no r-drbd tag: what departure leaves behind, so stale when this node
    ///   has no entry on an initialized volume (its peers hold the data);
    ///   otherwise not provably ours and refused.
    ///
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
        let name = s.vol.name_any();
        let uid = s.vol.metadata.uid.as_deref().ok_or("Volume has no uid")?;
        let own = drbd::md_tag(uid, s.mine.node_id);
        let mut tags = reads.tags(&self.lvm, &place.vg, &row.name).await?;
        let has = |tags: &[String], t: &str| tags.iter().any(|x| x == t);
        if has(&tags, &own) {
            return Ok(false);
        }
        let fresh = b.created || has(&tags, storage::NEW_TAG);
        if !fresh {
            let tagged = tags.iter().any(|t| drbd::is_md_tag(t));
            let departed = s.initialized() && s.prev.is_none();
            if !tagged && !departed {
                return Err(NOT_OURS.to_owned());
            }
            tracing::warn!(volume = name, ?tags, "stale DRBD metadata: recreating");
            // The new tag goes on before the metadata changes, so a crash
            // in between leaves an LV that is plainly fresh.
            let del: Vec<&str> = tags
                .iter()
                .map(String::as_str)
                .filter(|t| drbd::is_md_tag(t) || storage::is_from_tag(t))
                .collect();
            let r = self
                .lvm
                .change_tags(&place.vg, &row.name, &[storage::NEW_TAG], &del)
                .await;
            reads.tags.remove(&place.vg);
            r.map_err(|e| e.to_string())?;
            tags = vec![storage::NEW_TAG.to_owned()];
        }
        tracing::info!(volume = name, dev = row.path, "creating DRBD metadata");
        self.drbd_cmd(
            &argv::create_md(&name, model::keys::PEER_SLOTS),
            drbd::CREATE_MD_TIMEOUT,
        )
        .await
        .map_err(|e| format!("create-md {name}/{VOLUME}: {e}"))?;
        // Own tag in, the new tag and any other metadata tag out, in one
        // lvchange.
        let del: Vec<&str> = tags
            .iter()
            .map(String::as_str)
            .filter(|t| *t != own && (*t == storage::NEW_TAG || drbd::is_md_tag(t)))
            .collect();
        let r = self
            .lvm
            .change_tags(&place.vg, &row.name, &[&own], &del)
            .await;
        reads.tags.remove(&place.vg);
        r.map_err(|e| e.to_string())?;
        Ok(true)
    }

    /// Makes the first data current, on the initial node of a volume not
    /// yet initialized, while no replica holds data. Once every diskful
    /// peer is connected and Inconsistent the bitmap is cleared and nobody
    /// syncs; after `init_wait` the volume starts with whoever is there,
    /// when DRBD reports quorum, and late peers full-sync.
    ///
    /// A restore counts a peer as ready only when its entry also reports
    /// the source snapshot in restoredFrom: clearing the bitmap beside an
    /// empty disk would declare it in sync. Without that on every peer it
    /// starts after `init_wait` as the sync source. So it does when this
    /// node's LV is not `at_snapshot_size`: metadata of another size leaves
    /// old metadata in the data area. A node whose LV is not this restore's
    /// copy never starts it.
    ///
    /// `lost`: this replica was rebuilt. A rebuilt replica may still take
    /// the all-connected path (nobody holds data then), never the timeout
    /// path: a second clear-bitmap beside data made by the first would
    /// leave two unrelated data sets.
    async fn first_data(
        &mut self,
        s: &Step<'_>,
        name: &str,
        lost: bool,
        copy: Option<&Copy>,
    ) -> Result<(), String> {
        if s.initialized() || !s.initial_node() {
            self.init_started.remove(name);
            self.rebuilt_seen.remove(name);
            return Ok(());
        }
        let Some(r) = s.res else {
            // Not up yet: its events must bring the next pass.
            self.init_started.entry(name.to_owned()).or_insert(None);
            return Ok(());
        };
        let local = r.devices.get(&VOLUME).and_then(|d| d.disk.as_deref());
        let peer_data = r.peer_devices.iter().any(|((_, v), p)| {
            *v == VOLUME
                && (holds_data(p.peer_disk.as_deref())
                    || matches!(
                        p.replication.as_deref(),
                        Some("SyncSource" | "SyncTarget" | "PausedSyncS" | "PausedSyncT")
                    ))
        });
        if holds_data(local) || peer_data {
            self.init_started.remove(name);
            return Ok(());
        }
        if local != Some("Inconsistent") {
            // Still attaching: its events must bring the next pass.
            self.init_started.entry(name.to_owned()).or_insert(None);
            return Ok(());
        }
        let source = s.vol.spec.source.as_ref().map(|x| x.snapshot.as_str());
        if let Some(snap) = source
            && !copy.is_some_and(|c| c.tagged)
        {
            // Not this restore's data: starting would publish it as the
            // restore.
            let lv = copy.map_or("this node's LV", |c| c.lv.as_str());
            return Err(format!(
                "LV {lv} is not this restore's copy of snapshot {snap}; delete the claim and restore again"
            ));
        }
        let at_snapshot_size = copy.is_some_and(|c| c.at_size);
        let proving = source.filter(|_| at_snapshot_size);
        let cached = s.vol.status.as_ref().map_or(&[][..], |st| &st.replicas);
        let mut waiting = unready_peers(s, r, &self.node, proving, cached);
        if proving.is_some() && waiting.is_empty() {
            let Some(live) = self.live_unready(s, r, proving).await else {
                return Ok(());
            };
            waiting = live;
        }
        let since = *self
            .init_started
            .entry(name.to_owned())
            .or_insert(None)
            .get_or_insert_with(tokio::time::Instant::now);
        if !waiting.is_empty() && since.elapsed() < self.drbd.init_wait {
            tracing::info!(
                volume = name,
                ?waiting,
                "waiting for diskful peers before the first data"
            );
            return Ok(());
        }
        if !waiting.is_empty()
            && let Some(why) = timeout_blocker(s, r, lost)
        {
            tracing::warn!(
                volume = name,
                ?waiting,
                why,
                "not starting without these peers"
            );
            return Ok(());
        }
        if source.is_some() && (!at_snapshot_size || !waiting.is_empty()) {
            tracing::warn!(
                volume = name,
                ?waiting,
                at_snapshot_size,
                "restored copies not all identical: this one is the source, the others full-sync"
            );
            self.drbd_cmd(&argv::start_as_source(name), DRBD_TIMEOUT)
                .await?;
            self.init_started.remove(name);
            return Ok(());
        }
        if !waiting.is_empty() {
            tracing::warn!(
                volume = name,
                ?waiting,
                "starting without these peers: they will full-sync"
            );
        }
        if source.is_some() {
            tracing::info!(volume = name, "restored copies current without a sync");
        } else {
            tracing::info!(volume = name, "first data current without a sync");
        }
        self.drbd_cmd(&argv::start_without_sync(name), DRBD_TIMEOUT)
            .await?;
        self.init_started.remove(name);
        Ok(())
    }

    /// Connections to peers no longer in the assignment.
    async fn remove_departed(&self, s: &Step<'_>, name: &str, diskful: bool) -> Result<(), String> {
        let Some(r) = s.res else { return Ok(()) };
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
        }
        Ok(())
    }

    /// Frees bitmap slots of node ids no longer in the assignment, read from
    /// the superblock: a forget-peer that failed, or a peer that left while
    /// this node was down, leaves one behind. Once per spec generation when
    /// clean; a failure is retried on the next pass.
    async fn sweep_slots(&mut self, s: &Step<'_>, name: &str, dev: &str) {
        let attached = s
            .res
            .and_then(|r| r.devices.get(&VOLUME))
            .is_some_and(|d| drbd::disk_has_metadata(d.disk.as_deref()));
        let generation = s.vol.metadata.generation.unwrap_or_default();
        if !attached || self.slots_swept.get(name) == Some(&generation) {
            return;
        }
        let argv = argv::dump_superblock(s.minor, dev);
        let holders = match self.drbd_cmd(&argv, DRBD_TIMEOUT).await {
            Ok(out) => drbd::slot_holders(&out),
            Err(e) => Err(e),
        };
        let stale: Vec<u8> = match holders {
            Ok(h) => h
                .into_iter()
                .filter(|id| *id != s.mine.node_id)
                .filter(|id| !s.a.replicas.iter().any(|x| x.node_id == *id))
                .collect(),
            Err(e) => {
                tracing::warn!(volume = name, error = %e, "reading bitmap slots failed, retried next pass");
                return;
            }
        };
        let mut clean = true;
        for id in stale {
            tracing::info!(
                volume = name,
                id,
                "freeing the bitmap slot of a departed node id"
            );
            if let Err(e) = self
                .drbd_cmd(&argv::forget_peer(name, id), DRBD_TIMEOUT)
                .await
            {
                tracing::warn!(volume = name, id, error = %e, "forget-peer failed, retried next pass");
                clean = false;
            }
        }
        if clean {
            self.slots_swept.insert(name.to_owned(), generation);
        }
    }

    /// del-peer, then forget-peer on a diskful replica to free the slot.
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
            .drbd_cmd(&argv::del_peer(name, id), DRBD_TIMEOUT)
            .await
            .err();
        if !local_diskful {
            return del.map_or(Ok(()), Err);
        }
        match self
            .drbd_cmd(&argv::forget_peer(name, id), DRBD_TIMEOUT)
            .await
        {
            Ok(_) => Ok(()),
            // A diskless peer never had a slot to free.
            Err(_) if peer_diskless => Ok(()),
            Err(fe) => match del {
                // The connection is gone; the slot sweep frees it later.
                None => {
                    tracing::warn!(volume = name, peer, error = %fe, "forget-peer failed after del-peer");
                    Ok(())
                }
                Some(de) => Err(de),
            },
        }
    }

    /// Only the diskful replica with the lowest node id resizes, once every
    /// diskful peer reports an LV of the new gross size and is connected:
    /// DRBD grows only to what every connected peer can hold, and a peer
    /// away during the resize would have to catch up on the size later.
    async fn resize_step(&self, s: &Step<'_>, r: &Resource) -> Result<Resize, String> {
        if !r.devices.contains_key(&VOLUME) {
            return Ok(Resize::NotNeeded);
        }
        let current = self
            .sectors(s.minor)
            .await
            .map_err(|e| format!("size of {}: {e}", drbd::device_path(s.minor)))?
            * 512;
        if current >= s.vol.spec.size_bytes {
            return Ok(Resize::NotNeeded);
        }
        let diskful = || s.a.replicas.iter().filter(|x| x.role == Role::Diskful);
        if diskful().map(|x| x.node_id).min() != Some(s.mine.node_id) {
            return Ok(Resize::NotNeeded);
        }
        let gross = gross_bytes(s.vol);
        let entries = s
            .vol
            .status
            .as_ref()
            .map(|st| st.replicas.as_slice())
            .unwrap_or_default();
        let peers_ready = diskful().filter(|x| x.node != self.node).all(|x| {
            let grown = entries
                .iter()
                .any(|e| e.node == x.node && e.allocated_bytes.is_some_and(|b| b >= gross));
            let connected = r
                .connections
                .get(&x.node)
                .is_some_and(|c| c.state == "Connected");
            grown && connected
        });
        let up_to_date = r
            .devices
            .get(&VOLUME)
            .is_some_and(|d| d.disk.as_deref() == Some("UpToDate"));
        if !peers_ready || !up_to_date {
            return Ok(Resize::Waiting);
        }
        let name = s.vol.name_any();
        // A restore grows from its snapshot's size: the new area holds each
        // node's old metadata, which differs, so it syncs.
        let synced = s.restoring();
        tracing::info!(volume = name, from = current, synced, "growing DRBD device");
        let argv = if synced {
            argv::resize_synced(&name)
        } else {
            argv::resize(&name)
        };
        self.drbd_cmd(&argv, DRBD_TIMEOUT).await?;
        Ok(Resize::Done)
    }

    /// Spec.resolve: the non-survivor discards its data once per epoch; the
    /// survivor reconnects its StandAlone diskful connections. Returns true
    /// when this pass changed a connection.
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
                    self.drbd_cmd(&argv::connect_peer(&name, peer), DRBD_TIMEOUT)
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
        self.drbd_cmd(&argv::disconnect(&name), DRBD_TIMEOUT)
            .await?;
        self.drbd_cmd(&argv::connect_discard(&name), DRBD_TIMEOUT)
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

    /// Adjusts only when drbdadm's dry run lists commands: the dry run
    /// compares the file with the kernel's state, so no list of states
    /// is kept here.
    ///
    /// A plan that comes back after a successful adjust does not converge
    /// (a StandAlone after an auth mismatch, a disk failing on attach): it
    /// is retried on a doubling wait from `repeat_backoff` up to
    /// [`REPEAT_MAX`] and reported in lastError meanwhile. A new or empty
    /// plan starts over.
    /// Ok(Some) carries the backoff message of a held adjust.
    async fn adjust_if_needed(
        &mut self,
        s: &Step<'_>,
        name: &str,
    ) -> Result<Option<String>, String> {
        let planned = self
            .drbd_cmd(&argv::adjust_dry_run(name), DRBD_TIMEOUT)
            .await
            .map_err(|e| format!("adjust {name} (dry run): {e}"))?;
        let planned = planned.trim().to_owned();
        if planned.is_empty() {
            self.repeats.remove(name);
            return Ok(None);
        }
        let now = tokio::time::Instant::now();
        let repeat = self
            .repeats
            .get(name)
            .filter(|r| r.plan == planned)
            .cloned();
        if let Some(r) = &repeat
            && now < r.next
        {
            return Ok(Some(format!(
                "adjust {name} does not converge: drbdadm plans the same commands after each adjust ({}); tried {} times, next in {} s",
                planned.lines().next().unwrap_or_default(),
                r.times,
                (r.next - now).as_secs()
            )));
        }
        tracing::info!(volume = name, planned, "adjusting");
        if s.res.is_none() {
            self.probe_port(s.mine).await?;
        }
        let argv = argv::adjust(name);
        let mut retries = self.drbd.adjust_retry.iter();
        loop {
            match run_any(self.lvm.exec(), &argv, DRBD_TIMEOUT).await {
                Ok(o) if o.exit_code == Some(0) => {
                    let times = repeat.map_or(0, |r| r.times + 1);
                    let wait = self
                        .drbd
                        .repeat_backoff
                        .saturating_mul(1 << times.min(10))
                        .min(REPEAT_MAX);
                    self.repeats.insert(
                        name.to_owned(),
                        Repeat {
                            plan: planned,
                            times,
                            next: tokio::time::Instant::now() + wait,
                        },
                    );
                    return Ok(None);
                }
                Ok(o) if o.stderr.contains(RESYNC_BUSY) => match retries.next() {
                    Some(d) => tokio::time::sleep(*d).await,
                    None => return Err(format!("adjust {name}: {}", failure(&argv, &o))),
                },
                Ok(o) => return Err(format!("adjust {name}: {}", failure(&argv, &o))),
                Err(e) => return Err(format!("adjust {name}: {e}")),
            }
        }
    }

    /// `done` only comes with events2 statistics; ask the status command
    /// while syncing without it.
    async fn sync_fallback(&self, name: &str, res: Option<&Resource>) -> Option<u32> {
        if !drbd::derive(res).sync_unknown {
            return None;
        }
        let out = run_ok(self.lvm.exec(), &argv::status_json(name), DRBD_TIMEOUT)
            .await
            .ok()?;
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
                self.drbd_cmd(&argv::suspend_io(volume), DRBD_TIMEOUT)
                    .await
                    .map(|_| ())
            }
            SnapshotPhase::Resuming if r.suspended_by_user() => {
                tracing::info!(volume, "resuming IO after a snapshot");
                self.drbd_cmd(&argv::resume_io(volume), DRBD_TIMEOUT)
                    .await
                    .map(|_| ())
            }
            _ => Ok(()),
        }
    }

    /// Status of the replicated volumes events2 touched since the last call,
    /// from the last pass facts; no host work besides the sysfs size. A
    /// volume whose first data waits on its peers asks for a pass.
    pub async fn refresh(&mut self, volumes: &[Arc<Volume>]) -> Vec<StatusWrite> {
        let dirty = match self.drbd.tracker.lock() {
            Ok(mut t) if t.is_available() => t.take_dirty(),
            _ => return Vec::new(),
        };
        let mut writes = Vec::new();
        for name in dirty {
            if self.init_started.contains_key(&name) {
                self.pass_wanted = true;
            }
            let Some(vol) = volumes.iter().find(|v| v.name_any() == name) else {
                continue;
            };
            let Some(facts) = self.facts.get(&name).cloned() else {
                continue;
            };
            if vol.metadata.deletion_timestamp.is_some() {
                continue;
            }
            if let Some(a) = &vol.spec.assignment {
                let res = self.tracked(&name);
                self.observe_peers(vol, a, res.as_ref());
            }
            let prev = self.prev_entry(vol);
            let entry = self.current_entry(&name, &facts).await;
            if prev.as_ref() != Some(&entry) {
                writes.push(StatusWrite::Volume { name, entry });
            }
        }
        writes
    }

    /// Whether a refresh saw a change a pass must act on; clears the flag.
    pub fn take_pass_wanted(&mut self) -> bool {
        std::mem::take(&mut self.pass_wanted)
    }
}

/// The inputs of one replica's pass.
struct Step<'a> {
    vol: &'a Volume,
    a: &'a Assignment,
    mine: &'a AssignedReplica,
    features: Features,
    minor: u32,
    res: Option<&'a Resource>,
    prev: Option<&'a ReplicaStatus>,
}

impl Step<'_> {
    /// The previous pass left this replica's loss on record.
    fn prev_lost(&self) -> bool {
        self.prev
            .and_then(|p| p.last_error.as_deref())
            .is_some_and(is_lost)
    }

    /// The initial node of a restore, restored again this pass after a
    /// loss, beside a diskful peer whose device is larger than this LV
    /// gives: at the snapshot's size the attach would be refused and
    /// nothing grows first. Grown before its metadata, it starts as the
    /// sync source (no longer at the snapshot's size, so never clear-bitmap).
    fn regrows(&self, restored_again: bool, lv_bytes: u64) -> bool {
        if !(restored_again && self.restoring() && self.initial_node()) {
            return false;
        }
        let usable = drbd_config::usable_bytes(lv_bytes, model::keys::PEER_SLOTS);
        let larger = self.vol.status.iter().flat_map(|st| &st.replicas).any(|e| {
            e.node != self.mine.node
                && self
                    .a
                    .replicas
                    .iter()
                    .any(|p| p.node == e.node && drbd::is_diskful(p))
                && e.size_bytes.is_some_and(|b| b > usable)
        });
        if larger {
            tracing::warn!(
                volume = self.vol.name_any(),
                "restored again beside a grown peer: growing before attach"
            );
        }
        larger
    }

    /// A restore that has not made its first data yet.
    fn restoring(&self) -> bool {
        self.vol.restoring().is_some()
    }

    fn initialized(&self) -> bool {
        self.vol
            .status
            .as_ref()
            .is_some_and(|st| st.initialized_at.is_some())
    }

    /// Growth moves the end of the device where internal metadata lives, so
    /// an existing LV grows only under an attached disk (DRBD relocates its
    /// metadata online) or right after this pass created it. A restore keeps
    /// its snapshot's size until its disk holds data: fresh metadata then
    /// lands where the source's was, the copies stay identical, and the
    /// grown part syncs through the resize.
    fn may_grow(&self, created: bool) -> bool {
        let disk = self
            .res
            .and_then(|r| r.devices.get(&VOLUME))
            .and_then(|d| d.disk.as_deref());
        if self.restoring() {
            return holds_data(disk);
        }
        created || holds_data(disk) || disk == Some("Inconsistent")
    }

    fn initial_node(&self) -> bool {
        self.a.initial_up_to_date_node.as_deref() == Some(self.mine.node.as_str())
    }
}

#[cfg(test)]
mod tests;
