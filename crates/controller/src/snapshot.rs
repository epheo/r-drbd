//! Snapshot reconciler: drives the rounds through status.phase and the
//! agents' acks.
//!
//! A replicated restore starts without a sync, so the copies one round
//! takes must be identical: a round starts only on a volume in sync
//! everywhere, every replica suspends IO before any take, and every agent
//! resumes on its own (model::snapshot holds the bounds).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::jiff::Timestamp;
use kube::api::ListParams;
use kube::{Api, ResourceExt};
use model::condition::{self, Condition, FALSE, TRUE, types};
use model::keys::FINALIZER_SNAPSHOT;
use model::{Assignment, Role, Snapshot, SnapshotAck, SnapshotPhase, Volume, VolumePhase};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::store::Stores;
use crate::volume::{Cond, build_conditions};
use crate::{Ctx, Memory, apply_status_at, has_finalizer, set_finalizer, valid_name};

/// How long a Pending Snapshot waits for a Volume missing from the store.
pub const VOLUME_GRACE: Duration = crate::RESYNC;

/// Inputs of one state machine step.
#[derive(Debug)]
pub struct Round<'a> {
    pub phase: SnapshotPhase,
    /// DRBD volume: IO is suspended around the take.
    pub replicated: bool,
    pub volume: &'a Volume,
    /// Every replica, diskless and tiebreaker included.
    pub assignment: &'a Assignment,
    pub acks: &'a [SnapshotAck],
    /// Listed nodes whose agent is not fresh.
    pub stale: &'a [String],
    /// Another Snapshot of the volume whose round runs.
    pub other_round: Option<&'a str>,
    /// Time spent in the current phase, from status.phaseSince.
    pub phase_for: Option<Duration>,
    pub take_timeout: Duration,
    pub suspend_timeout: Duration,
}

impl Round<'_> {
    fn nodes(&self, role: Option<Role>) -> Vec<&str> {
        self.assignment
            .replicas
            .iter()
            .filter(|r| role.is_none_or(|x| r.role == x))
            .map(|r| r.node.as_str())
            .collect()
    }

    fn ack(&self, node: &str) -> Option<&SnapshotAck> {
        self.acks.iter().find(|a| a.node == node)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Stay,
    /// Stay in Pending, saying why.
    Hold(String),
    Go {
        phase: SnapshotPhase,
        error: Option<String>,
    },
}

fn go(phase: SnapshotPhase, error: Option<String>) -> Step {
    Step::Go { phase, error }
}

fn failed(error: String) -> Step {
    go(SnapshotPhase::Failed, Some(error))
}

/// The phase machine, pure.
pub fn next(r: &Round<'_>) -> Step {
    use SnapshotPhase as P;
    match r.phase {
        P::Pending => match start_blocker(r) {
            Some(why) => Step::Hold(why),
            None if r.replicated => go(P::Suspending, None),
            None => go(P::Taking, None),
        },
        P::Suspending => suspending(r),
        P::Taking => taking(r),
        _ => Step::Stay,
    }
}

/// Why a round must not start yet. Each copy must hold the same data: a
/// replica out of sync or cut from a diskful peer would take other bytes,
/// a replica whose agent cannot be reached would never suspend, and a
/// resize moves DRBD metadata under the take.
pub fn start_blocker(r: &Round<'_>) -> Option<String> {
    let v = r.volume;
    let st = v.status.as_ref();
    if st.is_none_or(|s| s.phase != VolumePhase::Ready) {
        return Some(format!("volume {} is not Ready", v.name_any()));
    }
    if let Some(other) = r.other_round {
        return Some(format!(
            "snapshot {other} of the same volume is in its round"
        ));
    }
    if !r.replicated {
        return None;
    }
    if !r.stale.is_empty() {
        return Some(format!("agent not reachable on {}", r.stale.join(", ")));
    }
    let entries = st.map(|s| s.replicas.as_slice()).unwrap_or_default();
    let entry = |n: &str| entries.iter().find(|e| e.node == n);
    // Each Secondary suspends after the Primary drained; with two
    // Primaries (a live migration) neither order holds for both.
    let primaries: Vec<&str> = r
        .nodes(None)
        .into_iter()
        .filter(|n| entry(n).is_some_and(|e| e.in_use))
        .collect();
    if primaries.len() > 1 {
        return Some(format!(
            "more than one Primary ({}): no round while two nodes write",
            primaries.join(", ")
        ));
    }
    let diskful = r.nodes(Some(Role::Diskful));
    let mut problems = Vec::new();
    for n in &diskful {
        let e = entry(n);
        let disk = e.and_then(|e| e.disk_state.as_deref());
        if disk != Some("UpToDate") {
            problems.push(format!("{n} is {}", disk.unwrap_or("unknown")));
        }
        if e.and_then(|e| e.observed_generation) != v.metadata.generation {
            problems.push(format!("{n} has not applied the current spec"));
        }
    }
    // Writers: diskful and attached replicas. A tiebreaker writes nothing.
    let writers = r
        .assignment
        .replicas
        .iter()
        .filter(|x| x.role != Role::Tiebreaker);
    for w in writers {
        for p in diskful.iter().filter(|p| **p != w.node) {
            let state = entry(&w.node).and_then(|e| e.peers.get(*p));
            if state.map(String::as_str) != Some("Connected") {
                problems.push(format!(
                    "{} to {p} is {}",
                    w.node,
                    state.map_or("unknown", String::as_str)
                ));
            }
        }
    }
    if st
        .and_then(|s| s.usable_bytes)
        .is_none_or(|u| u < v.spec.size_bytes)
    {
        problems.push("a resize is in flight".to_owned());
    }
    (!problems.is_empty()).then(|| format!("waiting: {}", problems.join("; ")))
}

/// Every listed replica suspends, within the bound. The bound wins over
/// late acks: suspends spread wider than it would outrun the agents'
/// resume deadlines.
fn suspending(r: &Round<'_>) -> Step {
    use SnapshotPhase as P;
    let listed = r.nodes(None);
    let errors: Vec<String> = listed
        .iter()
        .filter_map(|n| r.ack(n).filter(|a| a.phase == P::Suspending && !a.ok))
        .map(|a| format!("{}: {}", a.node, a.error.as_deref().unwrap_or("failed")))
        .collect();
    if !errors.is_empty() {
        return failed(format!("Suspending failed on {}", errors.join("; ")));
    }
    if r.phase_for.is_some_and(|d| d >= r.suspend_timeout) {
        let missing: Vec<&str> = listed
            .iter()
            .filter(|n| !r.ack(n).is_some_and(|a| a.phase == P::Suspending && a.ok))
            .copied()
            .collect();
        return failed(format!(
            "suspend not acknowledged within {} s by {}",
            r.suspend_timeout.as_secs(),
            missing.join(", ")
        ));
    }
    if !listed
        .iter()
        .all(|n| r.ack(n).is_some_and(|a| a.phase == P::Suspending && a.ok))
    {
        return Step::Stay;
    }
    let entries = r
        .volume
        .status
        .as_ref()
        .map(|s| s.replicas.as_slice())
        .unwrap_or_default();
    let stale: Vec<&str> = r
        .nodes(Some(Role::Diskful))
        .into_iter()
        .filter(|n| {
            // No entry is not UpToDate: a replica just placed by
            // spec.evict has none yet, and its LV is blank.
            entries
                .iter()
                .find(|e| e.node == *n)
                .and_then(|e| e.disk_state.as_deref())
                .is_none_or(|d| !d.eq_ignore_ascii_case("UpToDate"))
        })
        .collect();
    if stale.is_empty() {
        go(P::Taking, None)
    } else {
        failed(format!("not UpToDate on {}", stale.join(", ")))
    }
}

/// Every listed replica acks Taking: a diskful one with its copy, the
/// others with IO still suspended at ack time. A node that acked Taking
/// resumes only once the phase leaves Taking or at RESUME_AFTER, past
/// every take window, so no write lands between two copies.
fn taking(r: &Round<'_>) -> Step {
    use SnapshotPhase as P;
    let asked: Vec<&model::AssignedReplica> = r
        .assignment
        .replicas
        .iter()
        .filter(|x| r.replicated || x.role == Role::Diskful)
        .collect();
    let errors: Vec<String> = asked
        .iter()
        .filter_map(|x| r.ack(&x.node))
        .filter(|a| matches!(a.phase, P::Suspending | P::Taking) && !a.ok)
        .map(|a| format!("{}: {}", a.node, a.error.as_deref().unwrap_or("failed")))
        .collect();
    if !errors.is_empty() {
        return failed(format!("Taking failed on {}", errors.join("; ")));
    }
    let taken = asked.iter().all(|x| {
        r.ack(&x.node).is_some_and(|a| {
            a.phase == P::Taking && a.ok && (x.role != Role::Diskful || a.taken_pool.is_some())
        })
    });
    if taken {
        return go(P::Ready, None);
    }
    if r.phase_for.is_some_and(|d| d >= r.take_timeout) {
        return failed(format!(
            "take not acknowledged by every node within {} s",
            r.take_timeout.as_secs()
        ));
    }
    Step::Stay
}

/// Nodes that may hold a snapshot LV: the agent acked its takenPool, or
/// it already works on the delete. Once the round reached Taking, also
/// every diskful node, since a node may have created the LV without
/// getting to write its ack; and while no ack names a takenPool (all
/// lost, or written before acks named it), every node that acked: each
/// finds its LV, if any, by search.
pub fn holders(acks: &[SnapshotAck], reached_taking: bool, diskful: &[String]) -> BTreeSet<String> {
    let none_taken = acks.iter().all(|a| a.taken_pool.is_none());
    let diskful = if reached_taking { diskful } else { &[] };
    acks.iter()
        .filter(|a| {
            a.taken_pool.is_some()
                || a.phase == SnapshotPhase::Deleting
                || (reached_taking && none_taken)
        })
        .map(|a| a.node.clone())
        .chain(diskful.iter().cloned())
        .collect()
}

/// The earliest creation time of a holder's snapshot LV: createdAt, so a
/// status rebuilt from the LVs equals the lost one.
pub fn earliest_holder(acks: &[SnapshotAck]) -> Option<Time> {
    acks.iter()
        .filter(|a| a.taken_pool.is_some())
        .filter_map(|a| a.lv_created_at.clone())
        .min_by_key(|t| t.0)
}

/// Every holder acked Deleting ok.
pub fn deleted_everywhere(acks: &[SnapshotAck], holders: &BTreeSet<String>) -> bool {
    holders.iter().all(|n| {
        acks.iter()
            .any(|a| a.node == *n && a.phase == SnapshotPhase::Deleting && a.ok)
    })
}

#[derive(Debug, Clone, PartialEq)]
struct Owned {
    phase: Option<SnapshotPhase>,
    conditions: Vec<Condition>,
    size_bytes: Option<u64>,
    created_at: Option<Time>,
    phase_since: Option<Time>,
    file_system: Option<String>,
    source_replicas: Option<u32>,
    copies_identical: Option<bool>,
    last_error: Option<String>,
    observed_generation: Option<i64>,
}

impl Owned {
    fn current(s: &Snapshot) -> Self {
        let st = s.status.clone().unwrap_or_default();
        Self {
            phase: st.phase,
            conditions: condition::get(&st.conditions, types::READY)
                .cloned()
                .into_iter()
                .collect(),
            size_bytes: st.size_bytes,
            created_at: st.created_at,
            phase_since: st.phase_since,
            file_system: st.file_system,
            source_replicas: st.source_replicas,
            copies_identical: st.copies_identical,
            last_error: st.last_error,
            observed_generation: st.observed_generation,
        }
    }

    fn json(&self) -> serde_json::Value {
        let mut s = json!({ "conditions": self.conditions });
        if let Some(m) = s.as_object_mut() {
            if let Some(v) = self.phase {
                m.insert("phase".into(), json!(v));
            }
            if let Some(v) = self.size_bytes {
                m.insert("sizeBytes".into(), json!(v));
            }
            if let Some(v) = &self.created_at {
                m.insert("createdAt".into(), json!(v));
            }
            if let Some(v) = &self.file_system {
                m.insert("fileSystem".into(), json!(v));
            }
            if let Some(v) = self.source_replicas {
                m.insert("sourceReplicas".into(), json!(v));
            }
            if let Some(v) = self.copies_identical {
                m.insert("copiesIdentical".into(), json!(v));
            }
            if let Some(v) = &self.phase_since {
                m.insert("phaseSince".into(), json!(v));
            }
            if let Some(v) = &self.last_error {
                m.insert("lastError".into(), json!(v));
            }
            if let Some(v) = self.observed_generation {
                m.insert("observedGeneration".into(), json!(v));
            }
        }
        s
    }
}

fn ready_cond(phase: SnapshotPhase, error: Option<&str>) -> Cond {
    match phase {
        SnapshotPhase::Ready => Cond {
            status: TRUE,
            reason: "Ready",
            message: String::new(),
        },
        SnapshotPhase::Failed => Cond {
            status: FALSE,
            reason: "Failed",
            message: error.unwrap_or_default().to_owned(),
        },
        p => Cond {
            status: FALSE,
            reason: phase_reason(p),
            message: error.unwrap_or_default().to_owned(),
        },
    }
}

fn phase_reason(p: SnapshotPhase) -> &'static str {
    match p {
        SnapshotPhase::Pending => "Pending",
        SnapshotPhase::Suspending => "Suspending",
        SnapshotPhase::Taking => "Taking",
        SnapshotPhase::Ready => "Ready",
        SnapshotPhase::Failed => "Failed",
        SnapshotPhase::Deleting => "Deleting",
        SnapshotPhase::Discovering => "Discovering",
        SnapshotPhase::Lost => "Lost",
        SnapshotPhase::Unknown => "Unknown",
    }
}

/// Returns whether a patch went through.
async fn write(api: &Api<Snapshot>, s: &Snapshot, mut wanted: Owned) -> bool {
    let Some(phase) = wanted.phase else {
        return false;
    };
    let existing = s
        .status
        .as_ref()
        .map(|st| st.conditions.as_slice())
        .unwrap_or_default();
    let cond = ready_cond(phase, wanted.last_error.as_deref());
    wanted.conditions = build_conditions(existing, &[(types::READY, &cond)], s.metadata.generation);
    wanted.observed_generation = s.metadata.generation;
    if Owned::current(s) == wanted {
        return false;
    }
    // Computed from `s`: a store copy that lags must never move the phase
    // backwards, e.g. a started round back to Pending.
    let rv = s.metadata.resource_version.as_deref();
    apply_status_at(api, &s.name_any(), rv, wanted.json()).await
}

/// Returns the time left until the earliest bound, if any.
pub(crate) async fn reconcile_all(ctx: &Ctx<'_>, memory: &mut Memory) -> Option<Duration> {
    let api: Api<Snapshot> = Api::all(ctx.client.clone());
    let snapshots = Stores::sorted(&ctx.stores.snapshots);
    let restores = restoring_from(&Stores::sorted(&ctx.stores.volumes));
    let mut wake: Option<Duration> = None;
    for s in snapshots {
        if let Some(d) = reconcile(ctx, memory, &api, &s, &restores).await {
            wake = Some(wake.map_or(d, |w| w.min(d)));
        }
    }
    wake
}

/// What a Snapshot whose Volume is not usable does: Failed once the volume
/// is going away or stayed absent past the grace (the store may lag behind
/// a Volume created just before), else Pending with when to look again.
pub fn without_volume(
    phase: SnapshotPhase,
    name: &str,
    volume: Option<&Volume>,
    age: Duration,
) -> (SnapshotPhase, String, Option<Duration>) {
    let pending = phase == SnapshotPhase::Pending;
    match volume {
        Some(v) if v.metadata.deletion_timestamp.is_some() && pending => (
            SnapshotPhase::Failed,
            format!("volume {name} is being deleted"),
            None,
        ),
        Some(_) if pending => (phase, format!("volume {name} is not placed yet"), None),
        None if pending && age < VOLUME_GRACE => (
            phase,
            format!("volume {name} not found"),
            Some(VOLUME_GRACE.saturating_sub(age)),
        ),
        Some(_) => (
            SnapshotPhase::Failed,
            format!("volume {name} is not placed"),
            None,
        ),
        None => (
            SnapshotPhase::Failed,
            format!("volume {name} not found"),
            None,
        ),
    }
}

async fn reconcile(
    ctx: &Ctx<'_>,
    memory: &mut Memory,
    api: &Api<Snapshot>,
    s: &Snapshot,
    restores: &BTreeMap<String, String>,
) -> Option<Duration> {
    let name = s.name_any();
    let current = Owned::current(s);
    let volume = ctx.stores.volume(&s.spec.volume).filter(|v| s.is_of(v));
    let assignment = volume
        .as_deref()
        .and_then(|v| memory.assignment(v).cloned());
    let diskful: Vec<String> = assignment
        .iter()
        .flat_map(|a| &a.replicas)
        .filter(|r| r.role == Role::Diskful)
        .map(|r| r.node.clone())
        .collect();
    if s.metadata.deletion_timestamp.is_some() {
        let restoring = s.uid().and_then(|u| restores.get(&u)).map(String::as_str);
        let exists = |n: &str| ctx.stores.node(n).is_some();
        delete(api, s, current, &diskful, restoring, &exists).await;
        return None;
    }
    if !has_finalizer(s, FINALIZER_SNAPSHOT) {
        // The first status comes before the finalizer, so no version of
        // the object has the finalizer without a phase.
        if current.phase.is_none() {
            let wanted = Owned {
                phase: Some(SnapshotPhase::Pending),
                ..current
            };
            write(api, s, wanted).await;
        } else {
            set_finalizer(api, s, FINALIZER_SNAPSHOT, true).await;
        }
        return None;
    }
    let phase = current.phase?;
    if phase == SnapshotPhase::Ready {
        record_proof(ctx, api, s, current).await;
        return None;
    }
    // Unknown: a phase another version wrote. Left as it is, like Failed
    // and Lost: rewriting it could fail a newer version's good snapshot.
    if matches!(
        phase,
        SnapshotPhase::Failed
            | SnapshotPhase::Lost
            | SnapshotPhase::Discovering
            | SnapshotPhase::Unknown
    ) {
        return None;
    }
    if !valid_name(&name) {
        let error = format!("name must match {}", model::keys::NAME_PATTERN);
        fail(api, s, current, error).await;
        return None;
    }
    let usable = volume
        .as_deref()
        .filter(|v| v.metadata.deletion_timestamp.is_none() || phase != SnapshotPhase::Pending);
    let (Some(v), Some(a)) = (usable, assignment) else {
        let age = s
            .metadata
            .creation_timestamp
            .as_ref()
            .map_or(Duration::ZERO, |t| {
                Duration::try_from(Timestamp::now().duration_since(t.0)).unwrap_or(Duration::ZERO)
            });
        let (phase, error, wake) = without_volume(phase, &s.spec.volume, volume.as_deref(), age);
        write(
            api,
            s,
            Owned {
                phase: Some(phase),
                last_error: Some(error),
                ..current
            },
        )
        .await;
        return wake;
    };
    // Before any suspend: a snapshot LV name LVM would refuse in some
    // replica's VG fails the snapshot here, not in Taking with IO suspended.
    if phase == SnapshotPhase::Pending
        && let Some(error) = lv_name_problem(ctx, &a, &s.spec.volume, &name)
    {
        fail(api, s, current, error).await;
        return None;
    }
    // A bounded phase without a start (written by hand or by an older
    // controller): the bound starts now, persisted so it does not slide.
    if bound(ctx, phase).is_some() && current.phase_since.is_none() {
        info!(
            snapshot = name,
            ?phase,
            "phase without phaseSince; starting the bound now"
        );
        let wanted = Owned {
            phase_since: Some(crate::now()),
            ..current
        };
        write(api, s, wanted).await;
        return bound(ctx, phase);
    }
    step(ctx, api, s, v, &a, phase, current).await
}

async fn fail(api: &Api<Snapshot>, s: &Snapshot, current: Owned, error: String) {
    let wanted = Owned {
        phase: Some(SnapshotPhase::Failed),
        last_error: Some(error),
        ..current
    };
    write(api, s, wanted).await;
}

/// What the holders' digests prove, once every holder acked one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proof {
    pub identical: bool,
    /// Each holder with its digest, for the Event.
    pub digests: String,
}

/// The proof to write, None while a digest is pending or once written.
/// A holder is a node whose ack names takenPool. With fewer holders than
/// the source had replicas, a lost ack may still come back with another
/// digest, so nothing is written.
pub fn copies_proof(st: &model::SnapshotStatus) -> Option<Proof> {
    if st.copies_identical.is_some() {
        return None;
    }
    let digests = st
        .acks
        .iter()
        .filter(|a| a.taken_pool.is_some())
        .map(|a| Some((a.node.as_str(), a.digest.as_deref()?, a.digest_bytes?)))
        .collect::<Option<Vec<_>>>()?;
    let n = u32::try_from(digests.len()).ok()?;
    if n < st.source_replicas?.max(1) {
        return None;
    }
    let identical = digests
        .windows(2)
        .all(|w| (w[0].1, w[0].2) == (w[1].1, w[1].2));
    let digests = digests
        .iter()
        .map(|(n, d, b)| format!("{n} {d} over {b} bytes"))
        .collect::<Vec<_>>()
        .join(", ");
    Some(Proof { identical, digests })
}

/// Writes copiesIdentical once; copies that differ get one Warning Event.
async fn record_proof(ctx: &Ctx<'_>, api: &Api<Snapshot>, s: &Snapshot, current: Owned) {
    let Some(p) = s.status.as_ref().and_then(copies_proof) else {
        return;
    };
    let name = s.name_any();
    let wanted = Owned {
        copies_identical: Some(p.identical),
        ..current
    };
    if !write(api, s, wanted).await {
        return;
    }
    if p.identical {
        info!(snapshot = name, "holder copies identical");
    } else {
        warn!(snapshot = name, digests = p.digests, "holder copies differ");
        let message = format!(
            "holder copies differ, restores sync from one copy: {}",
            p.digests
        );
        crate::warning_event(ctx.client, s, IMAGE_COPIES_DIFFER, &message).await;
    }
}

/// Warning Event: the holders' digests of a Snapshot differ.
pub const IMAGE_COPIES_DIFFER: &str = "ImageCopiesDiffer";

/// Why the snapshot LV cannot be made in the VG of some diskful replica.
/// A Pool not in the store yet is skipped; the agents check again before
/// they suspend.
fn lv_name_problem(
    ctx: &Ctx<'_>,
    a: &model::Assignment,
    volume: &str,
    snapshot: &str,
) -> Option<String> {
    let lv = lvm::names::snapshot_lv(volume, snapshot);
    a.replicas
        .iter()
        .filter(|r| r.role == Role::Diskful)
        .filter_map(|r| ctx.stores.pool(r.pool.as_deref()?))
        .find_map(|p| {
            lvm::names::check_new_lv(&p.spec.vg, &lv)
                .err()
                .map(|e| format!("pool {}: {e}", p.name_any()))
        })
}

/// One state machine step for a snapshot whose volume is placed.
async fn step(
    ctx: &Ctx<'_>,
    api: &Api<Snapshot>,
    s: &Snapshot,
    v: &Volume,
    a: &Assignment,
    phase: SnapshotPhase,
    current: Owned,
) -> Option<Duration> {
    // Measured from the persisted start, so a controller restart keeps the bound.
    let phase_for = current.phase_since.as_ref().map(|t| {
        Duration::try_from(Timestamp::now().duration_since(t.0)).unwrap_or(Duration::ZERO)
    });
    let acks = s
        .status
        .as_ref()
        .map(|st| st.acks.as_slice())
        .unwrap_or_default();
    let stale: Vec<String> = a
        .replicas
        .iter()
        .filter(|r| !ctx.stores.agent_fresh(&r.node))
        .map(|r| r.node.clone())
        .collect();
    let other = crate::volume::snapshot_in_progress(ctx.stores, &s.spec.volume);
    let round = Round {
        phase,
        replicated: v.spec.replicas >= 2,
        volume: v,
        assignment: a,
        acks,
        stale: &stale,
        other_round: other.as_deref(),
        phase_for,
        take_timeout: ctx.cfg.take_timeout,
        suspend_timeout: ctx.cfg.suspend_timeout,
    };
    let mut step = next(&round);
    if phase == SnapshotPhase::Pending && matches!(step, Step::Go { .. }) {
        match live_round(api, s).await {
            Ok(None) => {}
            Ok(Some(other)) => {
                step = Step::Hold(format!(
                    "snapshot {other} of the same volume is in its round"
                ));
            }
            Err(e) => step = Step::Hold(format!("reading snapshots: {e}")),
        }
    }
    apply_step(ctx, api, s, v, phase, current, phase_for, step).await
}

/// Another Snapshot of the volume in its round, as the API server has it.
/// The store may not show a round this controller started a moment ago,
/// and two rounds at once would let one resume IO under the other's take.
async fn live_round(api: &Api<Snapshot>, s: &Snapshot) -> Result<Option<String>, kube::Error> {
    let all = api.list(&ListParams::default()).await?;
    Ok(all
        .items
        .iter()
        .filter(|x| x.metadata.uid != s.metadata.uid && x.spec.volume == s.spec.volume)
        .find(|x| crate::volume::round_running(x))
        .map(ResourceExt::name_any))
}

/// The bound of a phase, if it has one.
fn bound(ctx: &Ctx<'_>, phase: SnapshotPhase) -> Option<Duration> {
    match phase {
        SnapshotPhase::Suspending => Some(ctx.cfg.suspend_timeout),
        SnapshotPhase::Taking => Some(ctx.cfg.take_timeout),
        _ => None,
    }
}

/// What restores can rely on: the requested size, or less while a replica
/// has not grown to it yet. The DRBD device is larger than the request (the LV
/// rounds up to an extent), so reporting it would refuse a same-size restore.
pub fn restore_size(v: &Volume) -> u64 {
    let usable = v.status.as_ref().and_then(|st| st.usable_bytes);
    usable.map_or(v.spec.size_bytes, |u| u.min(v.spec.size_bytes))
}

/// Writes the outcome of one step; returns when to look again.
#[allow(clippy::too_many_arguments)]
async fn apply_step(
    ctx: &Ctx<'_>,
    api: &Api<Snapshot>,
    s: &Snapshot,
    v: &Volume,
    from: SnapshotPhase,
    current: Owned,
    phase_for: Option<Duration>,
    step: Step,
) -> Option<Duration> {
    let name = s.name_any();
    match step {
        Step::Stay => {
            let left = bound(ctx, from)
                .zip(phase_for)
                .map(|(b, t)| b.saturating_sub(t));
            write(api, s, current).await;
            left
        }
        Step::Hold(why) => {
            if current.last_error.as_deref() != Some(why.as_str()) {
                debug!(snapshot = name, why, "snapshot round held");
            }
            let wanted = Owned {
                last_error: Some(why),
                ..current
            };
            write(api, s, wanted).await;
            None
        }
        Step::Go { phase, error } => {
            let mut wanted = current;
            wanted.phase_since = Some(crate::now());
            if from == SnapshotPhase::Pending {
                wanted.last_error = None;
            }
            if phase == SnapshotPhase::Taking {
                wanted.size_bytes = Some(restore_size(v));
                // A restore starts with the source's filesystem marker.
                wanted.file_system = v.status.as_ref().and_then(|st| st.file_system.clone());
                wanted.source_replicas = Some(v.spec.replicas);
            }
            if phase == SnapshotPhase::Ready && wanted.created_at.is_none() {
                let acks = s.status.as_ref().map(|st| st.acks.as_slice());
                wanted.created_at =
                    Some(earliest_holder(acks.unwrap_or_default()).unwrap_or_else(crate::now));
            }
            if error.is_some() {
                wanted.last_error = error;
            }
            info!(snapshot = name, ?from, to = ?phase, error = ?wanted.last_error, "snapshot round");
            wanted.phase = Some(phase);
            write(api, s, wanted).await;
            bound(ctx, phase)
        }
    }
}

/// Snapshot uid to one placed restore from it without its first data yet.
/// Its holders still need their copy: a lost LV is restored again until
/// initializedAt, and a replica without a copy turns the restore into a
/// full sync. Built once per pass.
pub fn restoring_from(volumes: &[std::sync::Arc<Volume>]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for v in volumes {
        let Some(uid) = v.restoring().and_then(|s| s.snapshot_uid.as_ref()) else {
            continue;
        };
        if v.spec.assignment.is_some() && v.metadata.deletion_timestamp.is_none() {
            out.entry(uid.clone()).or_insert_with(|| v.name_any());
        }
    }
    out
}

/// Releases the finalizer once every node that may hold the LV removed it,
/// and no uninitialized restore reads from it. A holder whose Node object
/// is gone took its LV with it; one whose agent is only stale may return.
async fn delete(
    api: &Api<Snapshot>,
    s: &Snapshot,
    current: Owned,
    diskful: &[String],
    restoring: Option<&str>,
    exists: &(dyn Fn(&str) -> bool + Sync),
) {
    if !has_finalizer(s, FINALIZER_SNAPSHOT) {
        return;
    }
    let acks = s
        .status
        .as_ref()
        .map(|st| st.acks.as_slice())
        .unwrap_or_default();
    // sizeBytes is written on the way into Taking.
    let reached_taking = current.size_bytes.is_some()
        || matches!(
            current.phase,
            Some(SnapshotPhase::Taking | SnapshotPhase::Ready)
        );
    let mut held = holders(acks, reached_taking, diskful);
    held.retain(|n| exists(n));
    if let Some(v) = restoring {
        debug!(
            snapshot = s.name_any(),
            volume = v,
            "kept for a restore not initialized yet"
        );
    }
    if restoring.is_none() && deleted_everywhere(acks, &held) {
        info!(
            snapshot = s.name_any(),
            "snapshot removed on every node; releasing"
        );
        set_finalizer(api, s, FINALIZER_SNAPSHOT, false).await;
    } else {
        let wanted = Owned {
            phase: Some(SnapshotPhase::Deleting),
            ..current
        };
        write(api, s, wanted).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SnapshotPhase as P;
    use model::{AssignedReplica, ReplicaStatus, VolumeStatus};

    const GI: u64 = 1 << 30;

    fn ack(node: &str, phase: P, ok: bool) -> SnapshotAck {
        SnapshotAck {
            node: node.into(),
            phase,
            taken_pool: (phase == P::Taking).then(|| format!("p-{node}")),
            lv_bytes: None,
            allocated_bytes: None,
            lv_created_at: None,
            digest: None,
            digest_bytes: None,
            ok,
            error: (!ok).then(|| "boom".into()),
        }
    }

    fn replica(node: &str, role: Role) -> AssignedReplica {
        AssignedReplica {
            node: node.into(),
            pool: (role == Role::Diskful).then(|| format!("p-{node}")),
            node_id: 0,
            port: None,
            role,
        }
    }

    /// n1, n2 diskful, n3 tiebreaker; `extra` appended.
    fn assignment(extra: &[(&str, Role)]) -> Assignment {
        let mut replicas = vec![
            replica("n1", Role::Diskful),
            replica("n2", Role::Diskful),
            replica("n3", Role::Tiebreaker),
        ];
        replicas.extend(extra.iter().map(|(n, r)| replica(n, *r)));
        Assignment {
            replicas,
            minor: Some(1000),
            secret: None,
            initial_up_to_date_node: Some("n1".into()),
        }
    }

    /// The entry an agent writes for a healthy replica at generation 3.
    fn entry(node: &str, disk: &str, peers: &[&str]) -> ReplicaStatus {
        ReplicaStatus {
            node: node.into(),
            present: true,
            disk_state: Some(disk.into()),
            peers: peers
                .iter()
                .map(|p| ((*p).to_owned(), "Connected".to_owned()))
                .collect(),
            observed_generation: Some(3),
            ..ReplicaStatus::default()
        }
    }

    /// A Ready replicated volume of 1 GiB, in sync everywhere.
    fn volume() -> Volume {
        let mut v = Volume::new(
            "pvc-1",
            model::VolumeSpec {
                size_bytes: GI,
                replicas: 2,
                tier: "test".into(),
                pool: None,
                node: None,
                access_mode: model::AccessMode::default(),
                attachments: vec![],
                evict: vec![],
                source: None,
                resolve: None,
                assignment: None,
            },
        );
        v.metadata.generation = Some(3);
        v.status = Some(VolumeStatus {
            phase: VolumePhase::Ready,
            usable_bytes: Some(GI + 4096),
            replicas: vec![
                entry("n1", "UpToDate", &["n2"]),
                entry("n2", "UpToDate", &["n1"]),
                entry("n3", "Diskless", &["n1", "n2"]),
            ],
            ..VolumeStatus::default()
        });
        v
    }

    struct Case {
        phase: P,
        replicated: bool,
        volume: Volume,
        assignment: Assignment,
        acks: Vec<SnapshotAck>,
        stale: Vec<String>,
        other: Option<&'static str>,
        phase_for: Duration,
    }

    impl Case {
        fn new(phase: P, acks: &[SnapshotAck]) -> Self {
            Self {
                phase,
                replicated: true,
                volume: volume(),
                assignment: assignment(&[]),
                acks: acks.to_vec(),
                stale: vec![],
                other: None,
                phase_for: Duration::ZERO,
            }
        }

        fn local(phase: P, acks: &[SnapshotAck]) -> Self {
            let mut c = Self::new(phase, acks);
            c.replicated = false;
            c.assignment.replicas.truncate(1);
            c
        }

        fn next(&self) -> Step {
            next(&Round {
                phase: self.phase,
                replicated: self.replicated,
                volume: &self.volume,
                assignment: &self.assignment,
                acks: &self.acks,
                stale: &self.stale,
                other_round: self.other,
                phase_for: Some(self.phase_for),
                take_timeout: model::snapshot::TAKE_BOUND,
                suspend_timeout: model::snapshot::SUSPEND_BOUND,
            })
        }

        fn status(&mut self) -> &mut VolumeStatus {
            self.volume.status.as_mut().unwrap()
        }
    }

    fn held(step: &Step, needle: &str) {
        let Step::Hold(why) = step else {
            panic!("expected Hold with {needle}, got {step:?}");
        };
        assert!(why.contains(needle), "{why}");
    }

    fn failed_with(step: &Step, needle: &str) {
        let Step::Go {
            phase: P::Failed,
            error: Some(e),
        } = step
        else {
            panic!("expected Failed with {needle}, got {step:?}");
        };
        assert!(e.contains(needle), "{e}");
    }

    #[test]
    fn a_healthy_volume_starts() {
        assert_eq!(Case::new(P::Pending, &[]).next(), go(P::Suspending, None));
        assert_eq!(Case::local(P::Pending, &[]).next(), go(P::Taking, None));
    }

    #[test]
    fn every_start_condition_holds_pending() {
        type Edit = fn(&mut Case);
        let cases: &[(&str, Edit, &str)] = &[
            (
                "not ready",
                |c| c.status().phase = VolumePhase::Degraded,
                "not Ready",
            ),
            ("other round", |c| c.other = Some("s0"), "snapshot s0"),
            (
                "stale agent",
                |c| c.stale = vec!["n3".into()],
                "agent not reachable on n3",
            ),
            (
                "not up to date",
                |c| c.status().replicas[1].disk_state = Some("Outdated".into()),
                "n2 is Outdated",
            ),
            (
                "no entry",
                |c| {
                    c.status().replicas.remove(1);
                },
                "n2 is unknown",
            ),
            (
                "diskful peer cut",
                |c| {
                    c.status().replicas[1]
                        .peers
                        .insert("n1".into(), "Connecting".into());
                },
                "n2 to n1 is Connecting",
            ),
            (
                "attached writer cut",
                |c| {
                    c.assignment = assignment(&[("n4", Role::Diskless)]);
                    c.status().replicas.push(entry("n4", "Diskless", &["n1"]));
                },
                "n4 to n2 is unknown",
            ),
            (
                "spec not applied",
                |c| c.volume.metadata.generation = Some(4),
                "has not applied",
            ),
            (
                "resize in flight",
                |c| c.volume.spec.size_bytes = 2 * GI,
                "resize",
            ),
        ];
        let two_primaries: Edit = |c| {
            c.status().replicas[0].in_use = true;
            c.status().replicas[1].in_use = true;
        };
        let cases = &[
            cases,
            &[(
                "two primaries",
                two_primaries,
                "more than one Primary (n1, n2)",
            )],
        ]
        .concat();
        for (what, edit, needle) in cases {
            let mut c = Case::new(P::Pending, &[]);
            edit(&mut c);
            let step = c.next();
            assert!(matches!(step, Step::Hold(_)), "{what}: {step:?}");
            held(&step, needle);
        }
        // One Primary is the common case.
        let mut c = Case::new(P::Pending, &[]);
        c.status().replicas[0].in_use = true;
        assert_eq!(c.next(), go(P::Suspending, None));
        // A cut tiebreaker writes nothing: no hold.
        let mut c = Case::new(P::Pending, &[]);
        c.status().replicas[2].peers.clear();
        assert_eq!(c.next(), go(P::Suspending, None));
        // A local volume needs no agent or peer condition, only no other round.
        let mut c = Case::local(P::Pending, &[]);
        c.stale = vec!["n1".into()];
        c.volume.metadata.generation = Some(4);
        assert_eq!(c.next(), go(P::Taking, None));
        c.other = Some("s0");
        held(&c.next(), "snapshot s0");
    }

    #[test]
    fn every_listed_replica_suspends() {
        let two = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, true),
        ];
        assert_eq!(
            Case::new(P::Suspending, &two).next(),
            Step::Stay,
            "n3 missing"
        );
        let all = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, true),
            ack("n3", P::Suspending, true),
        ];
        assert_eq!(Case::new(P::Suspending, &all).next(), go(P::Taking, None));
        // An attachment added during Suspending is waited for too.
        let mut c = Case::new(P::Suspending, &all);
        c.assignment = assignment(&[("n4", Role::Diskless)]);
        assert_eq!(c.next(), Step::Stay);
        let bad = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, false),
        ];
        failed_with(&Case::new(P::Suspending, &bad).next(), "n2: boom");
        // UpToDate gate on the way to Taking.
        let mut c = Case::new(P::Suspending, &all);
        c.status().replicas[1].disk_state = Some("Inconsistent".into());
        failed_with(&c.next(), "not UpToDate on n2");
    }

    #[test]
    fn the_suspend_bound_wins_over_late_acks() {
        let all = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, true),
            ack("n3", P::Suspending, true),
        ];
        let mut c = Case::new(P::Suspending, &all);
        c.phase_for = model::snapshot::SUSPEND_BOUND.saturating_sub(Duration::from_secs(1));
        assert_eq!(c.next(), go(P::Taking, None));
        c.phase_for = model::snapshot::SUSPEND_BOUND;
        failed_with(&c.next(), "within 30 s");
        let mut c = Case::new(P::Suspending, &all[..2]);
        c.phase_for = model::snapshot::SUSPEND_BOUND;
        let step = c.next();
        failed_with(&step, "by n3");
        let Step::Go { error: Some(e), .. } = step else {
            unreachable!()
        };
        assert!(!e.contains("n1"), "{e}");
    }

    /// A diskless or tiebreaker Taking ack: IO still suspended there.
    fn frozen(node: &str) -> SnapshotAck {
        let mut a = ack(node, P::Taking, true);
        a.taken_pool = None;
        a
    }

    #[test]
    fn taking_needs_every_listed_replica() {
        let sus = |n: &str| ack(n, P::Suspending, true);
        let c = Case::new(P::Taking, &[sus("n1"), sus("n2"), sus("n3")]);
        assert_eq!(c.next(), Step::Stay);
        let mut no_pool = ack("n2", P::Taking, true);
        no_pool.taken_pool = None;
        let c = Case::new(
            P::Taking,
            &[ack("n1", P::Taking, true), no_pool, frozen("n3")],
        );
        assert_eq!(c.next(), Step::Stay, "a diskful ack without its copy");
        let copies = [
            ack("n1", P::Taking, true),
            ack("n2", P::Taking, true),
            sus("n3"),
        ];
        assert_eq!(
            Case::new(P::Taking, &copies).next(),
            Step::Stay,
            "the tiebreaker has not confirmed its freeze"
        );
        let done = [
            ack("n1", P::Taking, true),
            ack("n2", P::Taking, true),
            frozen("n3"),
        ];
        assert_eq!(Case::new(P::Taking, &done).next(), go(P::Ready, None));
        // All acked after the bound: each agent acked within its own window.
        let mut c = Case::new(P::Taking, &done);
        c.phase_for = model::snapshot::TAKE_BOUND * 2;
        assert_eq!(c.next(), go(P::Ready, None));
        // An attachment added mid round must confirm a freeze it never had.
        let mut c = Case::new(P::Taking, &done);
        c.assignment = assignment(&[("n4", Role::Diskless)]);
        assert_eq!(c.next(), Step::Stay);
        c.phase_for = model::snapshot::TAKE_BOUND;
        failed_with(&c.next(), "within 120 s");
        let bad = [
            ack("n1", P::Taking, true),
            ack("n2", P::Taking, false),
            frozen("n3"),
        ];
        failed_with(&Case::new(P::Taking, &bad).next(), "n2: boom");
    }

    #[test]
    fn taking_fails_on_a_replica_not_suspended() {
        // Resumed on its own deadline, or found IO live again.
        for late in [ack("n3", P::Taking, false), ack("n3", P::Suspending, false)] {
            let acks = [ack("n1", P::Taking, true), ack("n2", P::Taking, true), late];
            failed_with(&Case::new(P::Taking, &acks).next(), "n3: boom");
        }
    }

    #[test]
    fn unknown_phases_run_no_round_and_never_count() {
        // A Snapshot in a phase another version wrote: no step at all.
        let old = |n: &str| ack(n, P::Unknown, true);
        let c = Case::new(P::Unknown, &[old("n1"), old("n2"), old("n3")]);
        assert_eq!(c.next(), Step::Stay);
        // An ack in an unknown phase counts toward nothing, even ok.
        let c = Case::new(P::Suspending, &[old("n1"), old("n2"), old("n3")]);
        assert_eq!(c.next(), Step::Stay);
        let mut c = Case::new(
            P::Taking,
            &[
                ack("n1", P::Taking, true),
                ack("n2", P::Taking, true),
                old("n3"),
            ],
        );
        assert_eq!(c.next(), Step::Stay);
        c.phase_for = model::snapshot::TAKE_BOUND;
        failed_with(&c.next(), "within");
    }

    #[test]
    fn local_rounds() {
        assert_eq!(Case::local(P::Taking, &[]).next(), Step::Stay);
        let acks = [ack("n1", P::Taking, true)];
        assert_eq!(Case::local(P::Taking, &acks).next(), go(P::Ready, None));
        let acks = [ack("n1", P::Taking, false)];
        failed_with(&Case::local(P::Taking, &acks).next(), "n1: boom");
        let mut c = Case::local(P::Taking, &[]);
        c.phase_for = model::snapshot::TAKE_BOUND;
        failed_with(&c.next(), "within");
    }

    #[test]
    fn a_snapshot_without_its_volume() {
        let mut v = volume();
        let s = Duration::from_secs;
        assert_eq!(
            without_volume(P::Pending, "v", None, s(1)),
            (
                P::Pending,
                "volume v not found".into(),
                Some(VOLUME_GRACE.saturating_sub(s(1)))
            )
        );
        assert_eq!(
            without_volume(P::Pending, "v", None, VOLUME_GRACE).0,
            P::Failed
        );
        assert_eq!(without_volume(P::Taking, "v", None, s(1)).0, P::Failed);
        assert_eq!(
            without_volume(P::Pending, "v", Some(&v), s(1)).0,
            P::Pending
        );
        assert_eq!(without_volume(P::Taking, "v", Some(&v), s(1)).0, P::Failed);
        v.metadata.deletion_timestamp = Some(Time(Timestamp::now()));
        let (phase, why, _) = without_volume(P::Pending, "v", Some(&v), s(1));
        assert_eq!(
            (phase, why.as_str()),
            (P::Failed, "volume v is being deleted")
        );
    }

    fn sized(size: u64, usable: Option<u64>) -> Volume {
        let mut v = volume();
        v.spec.size_bytes = size;
        v.status.as_mut().unwrap().usable_bytes = usable;
        v
    }

    #[test]
    fn restore_size_is_the_request_not_the_drbd_device() {
        // The DRBD device of a 1Gi replicated volume.
        assert_eq!(restore_size(&sized(GI, Some(1_077_698_560))), GI);
        assert_eq!(restore_size(&sized(GI, None)), GI);
        // Mid-resize: a replica still at the old size bounds what a restore gets.
        assert_eq!(
            restore_size(&sized(2 * GI, Some(1_077_698_560))),
            1_077_698_560
        );
    }

    #[test]
    fn holders_for_delete() {
        let mut a = ack("n1", P::Taking, true);
        a.taken_pool = Some("p1".into());
        let b = ack("n2", P::Suspending, true);
        let c = ack("n3", P::Deleting, false);
        let acks = [a, b, c];
        let names = |h: BTreeSet<String>| h.into_iter().collect::<Vec<_>>();
        assert_eq!(
            names(holders(&acks, false, &["n4".to_owned()])),
            ["n1", "n3"]
        );
        assert!(!deleted_everywhere(&acks, &holders(&acks, false, &[])));
        // After Taking, a diskful node without any ack still holds.
        let h = holders(&acks, true, &["n4".to_owned()]);
        assert_eq!(names(h), ["n1", "n3", "n4"]);
        // No ack names a takenPool: any node that acked may hold one.
        let lost = [ack("n2", P::Suspending, true), ack("n3", P::Deleting, true)];
        assert_eq!(names(holders(&lost, true, &[])), ["n2", "n3"]);
        assert_eq!(names(holders(&lost, false, &[])), ["n3"]);
        let done = [ack("n1", P::Deleting, true), ack("n3", P::Deleting, true)];
        assert!(deleted_everywhere(&done, &holders(&done, false, &[])));
        assert!(!deleted_everywhere(
            &done,
            &holders(&done, true, &["n4".to_owned()])
        ));
    }

    #[test]
    fn created_at_is_the_earliest_holders_lv_time() {
        let at = |s: i64| Some(Time(Timestamp::from_second(s).unwrap()));
        let taken = |node: &str, time: i64| SnapshotAck {
            lv_created_at: at(time),
            ..ack(node, P::Taking, true)
        };
        // A diskless ack is no holder, whatever it says.
        let mut tiebreaker = taken("n3", 1);
        tiebreaker.taken_pool = None;
        let acks = [taken("n1", 20), taken("n2", 10), tiebreaker];
        assert_eq!(earliest_holder(&acks), at(10));
        assert_eq!(earliest_holder(&[ack("n1", P::Taking, true)]), None);
    }

    #[test]
    fn an_uninitialized_placed_restore_holds_its_source() {
        use std::sync::Arc;
        let restore = |name: &str, from: &str| {
            let spec: model::VolumeSpec = serde_json::from_value(json!({
                "sizeBytes": 1, "tier": "t", "replicas": 2,
                "source": { "snapshot": "snap", "snapshotUid": from },
                "assignment": { "replicas": [] },
            }))
            .unwrap();
            Volume::new(name, spec)
        };
        let v = restore("v", "u1");
        let held = restoring_from(&[Arc::new(v.clone())]);
        assert_eq!(held.get("u1").map(String::as_str), Some("v"));
        assert_eq!(held.get("snap"), None);
        assert_eq!(held.get("u2"), None);
        // Without a uid it binds no Snapshot, so it holds none.
        let mut legacy = v.clone();
        legacy.spec.source.as_mut().unwrap().snapshot_uid = None;
        let mut init = v.clone();
        init.status = Some(VolumeStatus {
            initialized_at: Some(Time(Timestamp::now())),
            ..VolumeStatus::default()
        });
        let mut unplaced = v.clone();
        unplaced.spec.assignment = None;
        let mut deleting = v;
        deleting.metadata.deletion_timestamp = Some(Time(Timestamp::now()));
        for x in [legacy, init, unplaced, deleting] {
            assert!(restoring_from(&[Arc::new(x)]).is_empty());
        }
    }

    fn holder(node: &str, digest: Option<&str>, bytes: u64) -> SnapshotAck {
        SnapshotAck {
            digest: digest.map(str::to_owned),
            digest_bytes: digest.map(|_| bytes),
            ..ack(node, P::Taking, true)
        }
    }

    fn status(acks: Vec<SnapshotAck>, source_replicas: Option<u32>) -> model::SnapshotStatus {
        model::SnapshotStatus {
            phase: Some(P::Ready),
            acks,
            source_replicas,
            ..model::SnapshotStatus::default()
        }
    }

    #[test]
    fn copies_proof_once_every_holder_digested() {
        let same = status(
            vec![holder("n1", Some("d"), 9), holder("n2", Some("d"), 9)],
            Some(2),
        );
        let p = copies_proof(&same).unwrap();
        assert!(p.identical);
        assert_eq!(p.digests, "n1 d over 9 bytes, n2 d over 9 bytes");

        let differ = status(
            vec![holder("n1", Some("d"), 9), holder("n2", Some("e"), 9)],
            Some(2),
        );
        assert!(!copies_proof(&differ).unwrap().identical);
        // Another range is another content.
        let range = status(
            vec![holder("n1", Some("d"), 9), holder("n2", Some("d"), 8)],
            Some(2),
        );
        assert!(!copies_proof(&range).unwrap().identical);
        // A diskless ack is no holder.
        let mut tiebreaker = same.clone();
        tiebreaker.acks.push(ack("n3", P::Suspending, true));
        assert!(copies_proof(&tiebreaker).unwrap().identical);
    }

    #[test]
    fn copies_proof_pending() {
        let one_missing = status(
            vec![holder("n1", Some("d"), 9), holder("n2", None, 9)],
            Some(2),
        );
        assert_eq!(copies_proof(&one_missing), None);
        // A lost ack may come back with another digest.
        let short = status(
            vec![holder("n1", Some("d"), 9), holder("n2", Some("d"), 9)],
            Some(3),
        );
        assert_eq!(copies_proof(&short), None);
        assert_eq!(copies_proof(&status(vec![], Some(2))), None);
        let mut written = status(
            vec![holder("n1", Some("d"), 9), holder("n2", Some("e"), 9)],
            Some(2),
        );
        written.copies_identical = Some(true);
        assert_eq!(copies_proof(&written), None, "written once");
    }

    #[test]
    fn copies_proof_needs_source_replicas() {
        let two = status(
            vec![holder("n1", Some("d"), 9), holder("n2", Some("d"), 9)],
            None,
        );
        assert_eq!(copies_proof(&two), None);
    }
}
