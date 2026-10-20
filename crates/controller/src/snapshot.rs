//! Snapshot reconciler: drives the rounds through status.phase and the
//! agents' acks.
//!
//! Suspended and Taken are markers the controller passes through on its
//! own: the next round starts on the following pass without new acks.

use std::collections::BTreeSet;
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::jiff::Timestamp;
use kube::{Api, ResourceExt};
use model::condition::{self, Condition, FALSE, TRUE, types};
use model::keys::FINALIZER_SNAPSHOT;
use model::{Role, Snapshot, SnapshotAck, SnapshotPhase, Volume, VolumePhase, VolumeStatus};
use serde_json::json;
use tracing::{debug, info};

use crate::store::Stores;
use crate::volume::{Cond, build_conditions};
use crate::{Ctx, Memory, apply_status, has_finalizer, set_finalizer, valid_name};

/// Inputs of one state machine step.
#[derive(Debug)]
pub struct Round<'a> {
    pub phase: SnapshotPhase,
    /// DRBD volume: IO is suspended around the take.
    pub replicated: bool,
    pub diskful: &'a [String],
    /// Replicas that must ack the current IO round, diskless and
    /// tiebreaker included (`suspend_members`, `resume_members`).
    pub members: &'a [String],
    pub acks: &'a [SnapshotAck],
    pub volume_ready: bool,
    pub volume_status: Option<&'a VolumeStatus>,
    /// Set once a round failed while IO was suspended: Resuming then ends in Failed.
    pub last_error: Option<&'a str>,
    /// Time spent in the current phase, from status.phaseSince.
    pub phase_for: Option<Duration>,
    pub take_timeout: Duration,
    pub suspend_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Stay,
    Go {
        phase: SnapshotPhase,
        error: Option<String>,
    },
}

fn go(phase: SnapshotPhase, error: Option<String>) -> Step {
    Step::Go { phase, error }
}

/// The phase machine, pure.
pub fn next(r: &Round<'_>) -> Step {
    use SnapshotPhase as P;
    let current: Vec<&SnapshotAck> = r.acks.iter().filter(|a| a.phase == r.phase).collect();
    // Taking needs a pool from every diskful replica; the IO rounds an ack
    // from every member.
    let need = match r.phase {
        P::Suspending | P::Resuming => r.members,
        _ => r.diskful,
    };
    let failed: Vec<String> = current
        .iter()
        .filter(|a| !a.ok && need.contains(&a.node))
        .map(|a| format!("{}: {}", a.node, a.error.as_deref().unwrap_or("failed")))
        .collect();
    let all_ok = need.iter().all(|n| {
        current
            .iter()
            .any(|a| a.node == *n && a.ok && (r.phase != P::Taking || a.pool.is_some()))
    });
    let failure =
        (!failed.is_empty()).then(|| format!("{:?} failed on {}", r.phase, failed.join("; ")));
    // While IO is suspended a failure first resumes, then fails.
    let abort = |error: String| {
        if r.replicated {
            go(P::Resuming, Some(error))
        } else {
            go(P::Failed, Some(error))
        }
    };
    match r.phase {
        P::Pending if r.volume_ready => go(
            if r.replicated {
                P::Suspending
            } else {
                P::Taking
            },
            None,
        ),
        P::Suspending | P::Taking if failure.is_some() => abort(failure.unwrap_or_default()),
        P::Suspending if all_ok => go(P::Suspended, None),
        // A dead member must not keep IO frozen on the others.
        P::Suspending if r.phase_for.is_some_and(|d| d >= r.suspend_timeout) => {
            let missing: Vec<&str> = need
                .iter()
                .filter(|n| !current.iter().any(|a| a.node == **n && a.ok))
                .map(String::as_str)
                .collect();
            abort(format!(
                "suspend not acknowledged within {} s by {}",
                r.suspend_timeout.as_secs(),
                missing.join(", ")
            ))
        }
        P::Suspended => {
            let stale: Vec<&str> = r
                .diskful
                .iter()
                .filter(|n| {
                    // No entry is not UpToDate: a replica just placed by
                    // spec.evict has none yet, and its LV is blank.
                    r.volume_status
                        .and_then(|s| s.replicas.iter().find(|e| &e.node == *n))
                        .and_then(|e| e.disk_state.as_deref())
                        .is_none_or(|d| !d.eq_ignore_ascii_case("UpToDate"))
                })
                .map(String::as_str)
                .collect();
            if stale.is_empty() {
                go(P::Taking, None)
            } else {
                abort(format!("not UpToDate on {}", stale.join(", ")))
            }
        }
        P::Taking if all_ok => go(if r.replicated { P::Taken } else { P::Ready }, None),
        P::Taking if r.phase_for.is_some_and(|d| d >= r.take_timeout) => abort(format!(
            "take not acknowledged by every node within {} s",
            r.take_timeout.as_secs()
        )),
        P::Taken => go(P::Resuming, None),
        P::Resuming if failure.is_some() => {
            let mut e = r.last_error.map(str::to_owned).unwrap_or_default();
            if !e.is_empty() {
                e.push_str("; ");
            }
            e.push_str(&failure.unwrap_or_default());
            go(P::Failed, Some(e))
        }
        P::Resuming if all_ok => match r.last_error {
            Some(e) => go(P::Failed, Some(e.to_owned())),
            None => go(P::Ready, None),
        },
        _ => Step::Stay,
    }
}

/// Nodes that may hold a snapshot LV: the agent set a pool when it took
/// it, or it already works on the delete, plus every diskful node once
/// the round reached Taking (`diskful` is empty before), since a node may
/// have created the LV without getting to write its ack.
pub fn holders(acks: &[SnapshotAck], diskful: &[String]) -> BTreeSet<String> {
    acks.iter()
        .filter(|a| a.pool.is_some() || a.phase == SnapshotPhase::Deleting)
        .map(|a| a.node.clone())
        .chain(diskful.iter().cloned())
        .collect()
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
    phase: SnapshotPhase,
    conditions: Vec<Condition>,
    size_bytes: Option<u64>,
    created_at: Option<Time>,
    phase_since: Option<Time>,
    file_system: Option<String>,
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
            last_error: st.last_error,
            observed_generation: st.observed_generation,
        }
    }

    fn json(&self) -> serde_json::Value {
        let mut s = json!({ "phase": self.phase, "conditions": self.conditions });
        if let Some(m) = s.as_object_mut() {
            if let Some(v) = self.size_bytes {
                m.insert("sizeBytes".into(), json!(v));
            }
            if let Some(v) = &self.created_at {
                m.insert("createdAt".into(), json!(v));
            }
            if let Some(v) = &self.file_system {
                m.insert("fileSystem".into(), json!(v));
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
        SnapshotPhase::Suspended => "Suspended",
        SnapshotPhase::Taking => "Taking",
        SnapshotPhase::Taken => "Taken",
        SnapshotPhase::Resuming => "Resuming",
        SnapshotPhase::Ready => "Ready",
        SnapshotPhase::Failed => "Failed",
        SnapshotPhase::Deleting => "Deleting",
    }
}

async fn write(api: &Api<Snapshot>, s: &Snapshot, mut wanted: Owned) {
    let existing = s
        .status
        .as_ref()
        .map(|st| st.conditions.as_slice())
        .unwrap_or_default();
    let cond = ready_cond(wanted.phase, wanted.last_error.as_deref());
    wanted.conditions = build_conditions(existing, &[(types::READY, &cond)], s.metadata.generation);
    wanted.observed_generation = s.metadata.generation;
    if Owned::current(s) == wanted {
        return;
    }
    apply_status(api, &s.name_any(), wanted.json()).await;
}

/// Returns the time left until the earliest take timeout, if any.
pub(crate) async fn reconcile_all(ctx: &Ctx<'_>, memory: &mut Memory) -> Option<Duration> {
    let api: Api<Snapshot> = Api::all(ctx.client.clone());
    let snapshots = Stores::sorted(&ctx.stores.snapshots);
    let mut wake: Option<Duration> = None;
    for s in snapshots {
        if let Some(d) = reconcile(ctx, memory, &api, &s).await {
            wake = Some(wake.map_or(d, |w| w.min(d)));
        }
    }
    wake
}

async fn reconcile(
    ctx: &Ctx<'_>,
    memory: &mut Memory,
    api: &Api<Snapshot>,
    s: &Snapshot,
) -> Option<Duration> {
    let name = s.name_any();
    let current = Owned::current(s);
    let volume = ctx.stores.volume(&s.spec.volume);
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
        let volumes = Stores::sorted(&ctx.stores.volumes);
        let restoring = restoring_from(&volumes, &name);
        delete(api, s, current, &diskful, restoring.as_deref()).await;
        return None;
    }
    if !has_finalizer(s, FINALIZER_SNAPSHOT) {
        set_finalizer(api, s, FINALIZER_SNAPSHOT, true).await;
        return None;
    }
    if matches!(current.phase, SnapshotPhase::Ready | SnapshotPhase::Failed) {
        return None;
    }
    if !valid_name(&name) {
        let error = format!("name must match {}", model::keys::NAME_PATTERN);
        write(
            api,
            s,
            Owned {
                phase: SnapshotPhase::Failed,
                last_error: Some(error),
                ..current
            },
        )
        .await;
        return None;
    }
    let (Some(v), Some(a)) = (volume.as_deref(), assignment) else {
        let error = format!("volume {} not found or not placed yet", s.spec.volume);
        let phase = if current.phase == SnapshotPhase::Pending {
            SnapshotPhase::Pending
        } else {
            SnapshotPhase::Failed
        };
        write(
            api,
            s,
            Owned {
                phase,
                last_error: Some(error),
                ..current
            },
        )
        .await;
        return None;
    };
    // Before any suspend: a snapshot LV name LVM would refuse in some
    // replica's VG fails the snapshot here, not in Taking with IO suspended.
    if current.phase == SnapshotPhase::Pending
        && let Some(error) = lv_name_problem(ctx, &a, &s.spec.volume, &name)
    {
        write(
            api,
            s,
            Owned {
                phase: SnapshotPhase::Failed,
                last_error: Some(error),
                ..current
            },
        )
        .await;
        return None;
    }
    // A bounded phase without a start (written by hand or by an older
    // controller): the bound starts now, persisted so it does not slide.
    if bound(ctx, current.phase).is_some() && current.phase_since.is_none() {
        info!(
            snapshot = name,
            phase = ?current.phase,
            "phase without phaseSince; starting the bound now"
        );
        let wanted = Owned {
            phase_since: Some(crate::now()),
            ..current
        };
        write(api, s, wanted).await;
        return bound(ctx, current.phase);
    }
    step(ctx, memory, api, s, v, &diskful, current).await
}

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
    memory: &mut Memory,
    api: &Api<Snapshot>,
    s: &Snapshot,
    v: &Volume,
    diskful: &[String],
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
    let replicated = v.spec.replicas >= 2;
    if current.phase == SnapshotPhase::Pending
        && replicated
        && let Some(e) = primary_unreachable(ctx, v)
    {
        write(
            api,
            s,
            Owned {
                last_error: Some(e),
                ..current
            },
        )
        .await;
        return None;
    }
    let members = members(ctx, memory, s, v, current.phase, acks);
    let round = Round {
        phase: current.phase,
        replicated,
        diskful,
        members: &members,
        acks,
        volume_ready: volume_ready(v),
        volume_status: v.status.as_ref(),
        last_error: current.last_error.as_deref(),
        phase_for,
        take_timeout: ctx.cfg.take_timeout,
        suspend_timeout: ctx.cfg.suspend_timeout,
    };
    let step = next(&round);
    apply_step(ctx, api, s, v, current, phase_for, step).await
}

/// Suspending around an unreachable Primary could never be undone there.
/// Read from the raw entries, as status.primary leaves unreachable nodes
/// out. A reachable entry in use means a failover happened and the stale
/// one is the old Primary; a node whose Node object is gone is ignored.
fn primary_unreachable(ctx: &Ctx<'_>, v: &Volume) -> Option<String> {
    let entries = &v.status.as_ref()?.replicas;
    let reachable = |n: &str| ctx.stores.agent_fresh(n);
    let exists = |n: &str| ctx.stores.node(n).is_some();
    held_by_unreachable_primary(entries, &reachable, &exists)
}

/// The lastError holding a round, or None to let it start.
pub fn held_by_unreachable_primary(
    entries: &[model::ReplicaStatus],
    reachable: &dyn Fn(&str) -> bool,
    exists: &dyn Fn(&str) -> bool,
) -> Option<String> {
    if entries.iter().any(|e| e.in_use && reachable(&e.node)) {
        return None;
    }
    let stale: Vec<&str> = entries
        .iter()
        .filter(|e| e.in_use && !reachable(&e.node) && exists(&e.node))
        .map(|e| e.node.as_str())
        .collect();
    (!stale.is_empty()).then(|| format!("primary {} is unreachable (last known)", stale.join(", ")))
}

fn members(
    ctx: &Ctx<'_>,
    memory: &mut Memory,
    s: &Snapshot,
    v: &Volume,
    phase: SnapshotPhase,
    acks: &[SnapshotAck],
) -> Vec<String> {
    match phase {
        SnapshotPhase::Suspending => suspend_members(ctx, memory, s, v),
        SnapshotPhase::Resuming => resume_members(&listed(memory, v), acks),
        _ => Vec::new(),
    }
}

/// The bound of a phase, if it has one.
fn bound(ctx: &Ctx<'_>, phase: SnapshotPhase) -> Option<Duration> {
    match phase {
        SnapshotPhase::Suspending => Some(ctx.cfg.suspend_timeout),
        SnapshotPhase::Taking => Some(ctx.cfg.take_timeout),
        _ => None,
    }
}

fn listed(memory: &Memory, v: &Volume) -> Vec<String> {
    memory
        .assignment(v)
        .iter()
        .flat_map(|a| &a.replicas)
        .map(|r| r.node.clone())
        .collect()
}

/// Suspending: the replicas listed with a reachable agent when the round
/// started (first seen here, so after a restart: now) that are still
/// listed. One added mid round never suspended; one removed or found dead
/// at the start does not block.
fn suspend_members(ctx: &Ctx<'_>, memory: &mut Memory, s: &Snapshot, v: &Volume) -> Vec<String> {
    let listed = listed(memory, v);
    let fresh = || -> Vec<String> {
        listed
            .iter()
            .filter(|n| ctx.stores.agent_fresh(n))
            .cloned()
            .collect()
    };
    let Some(uid) = s.metadata.uid.clone() else {
        return fresh();
    };
    memory
        .rounds
        .entry(uid)
        .or_insert_with(fresh)
        .iter()
        .filter(|n| listed.contains(n))
        .cloned()
        .collect()
}

/// Resuming: every replica still listed that suspended, read from the acks
/// so it survives a restart: an ok Suspending ack, or any later ack.
pub fn resume_members(listed: &[String], acks: &[SnapshotAck]) -> Vec<String> {
    acks.iter()
        .filter(|a| {
            (a.phase == SnapshotPhase::Suspending && a.ok)
                || matches!(a.phase, SnapshotPhase::Taking | SnapshotPhase::Resuming)
        })
        .filter(|a| listed.contains(&a.node))
        .map(|a| a.node.clone())
        .collect()
}

/// What restores can rely on: the requested size, or less while a replica
/// has not grown to it yet. The DRBD device is larger than the request (the LV
/// rounds up to an extent), so reporting it would refuse a same-size restore.
pub fn restore_size(v: &Volume) -> u64 {
    let usable = v.status.as_ref().and_then(|st| st.usable_bytes);
    usable.map_or(v.spec.size_bytes, |u| u.min(v.spec.size_bytes))
}

/// Writes the outcome of one step; returns when to look again.
async fn apply_step(
    ctx: &Ctx<'_>,
    api: &Api<Snapshot>,
    s: &Snapshot,
    v: &Volume,
    current: Owned,
    phase_for: Option<Duration>,
    step: Step,
) -> Option<Duration> {
    let name = s.name_any();
    match step {
        Step::Stay => {
            let mut wanted = current;
            if wanted.phase == SnapshotPhase::Pending {
                // The volume exists now; the earlier "not found" is stale.
                wanted.last_error = None;
            }
            let left = bound(ctx, wanted.phase)
                .zip(phase_for)
                .map(|(b, t)| b.saturating_sub(t));
            write(api, s, wanted).await;
            left
        }
        Step::Go { phase, error } => {
            let from = current.phase;
            let mut wanted = current;
            wanted.phase_since = Some(crate::now());
            if phase == SnapshotPhase::Taking {
                wanted.size_bytes = Some(restore_size(v));
                // A restore starts with the source's filesystem marker.
                wanted.file_system = v.status.as_ref().and_then(|st| st.file_system.clone());
            }
            let taken = phase == SnapshotPhase::Taken
                || (phase == SnapshotPhase::Ready && from == SnapshotPhase::Taking);
            if taken && wanted.created_at.is_none() {
                wanted.created_at = Some(crate::now());
            }
            if error.is_some() {
                wanted.last_error = error;
            }
            info!(snapshot = name, ?from, to = ?phase, error = ?wanted.last_error, "snapshot round");
            wanted.phase = phase;
            write(api, s, wanted).await;
            bound(ctx, phase)
        }
    }
}

/// A placed restore from `snapshot` without its first data yet. Its holders
/// still need their copy: a lost LV is restored again until initializedAt,
/// and a replica without a copy turns the restore into a full sync.
pub fn restoring_from(volumes: &[std::sync::Arc<Volume>], snapshot: &str) -> Option<String> {
    volumes
        .iter()
        .find(|v| {
            v.spec
                .source
                .as_ref()
                .is_some_and(|src| src.snapshot == snapshot)
                && v.spec.assignment.is_some()
                && v.metadata.deletion_timestamp.is_none()
                && v.status
                    .as_ref()
                    .is_none_or(|st| st.initialized_at.is_none())
        })
        .map(|v| v.name_any())
}

/// Releases the finalizer once every node that may hold the LV removed it,
/// and no uninitialized restore reads from it.
async fn delete(
    api: &Api<Snapshot>,
    s: &Snapshot,
    current: Owned,
    diskful: &[String],
    restoring: Option<&str>,
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
            SnapshotPhase::Taking
                | SnapshotPhase::Taken
                | SnapshotPhase::Resuming
                | SnapshotPhase::Ready
        );
    let held = holders(acks, if reached_taking { diskful } else { &[] });
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
            phase: SnapshotPhase::Deleting,
            ..current
        };
        write(api, s, wanted).await;
    }
}

fn volume_ready(v: &Volume) -> bool {
    v.status
        .as_ref()
        .is_some_and(|s| s.phase == VolumePhase::Ready)
}

#[cfg(test)]
mod tests {
    use super::*;
    use SnapshotPhase as P;

    fn ack(node: &str, phase: P, ok: bool) -> SnapshotAck {
        SnapshotAck {
            node: node.into(),
            phase,
            pool: (phase == P::Taking).then(|| format!("p-{node}")),
            ok,
            error: (!ok).then(|| "boom".into()),
        }
    }

    fn round<'a>(
        phase: P,
        replicated: bool,
        diskful: &'a [String],
        acks: &'a [SnapshotAck],
    ) -> Round<'a> {
        Round {
            phase,
            replicated,
            diskful,
            members: diskful,
            acks,
            volume_ready: true,
            volume_status: None,
            last_error: None,
            phase_for: Some(Duration::ZERO),
            take_timeout: Duration::from_secs(120),
            suspend_timeout: Duration::from_secs(30),
        }
    }

    fn volume(size: u64, usable: Option<u64>) -> Volume {
        let mut v = Volume::new(
            "pvc-1",
            model::VolumeSpec {
                size_bytes: size,
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
        v.status = Some(VolumeStatus {
            usable_bytes: usable,
            ..VolumeStatus::default()
        });
        v
    }

    #[test]
    fn restore_size_is_the_request_not_the_drbd_device() {
        const GI: u64 = 1 << 30;
        // The DRBD device of a 1Gi replicated volume.
        assert_eq!(restore_size(&volume(GI, Some(1_077_698_560))), GI);
        assert_eq!(restore_size(&volume(GI, None)), GI);
        // Mid-resize: a replica still at the old size bounds what a restore gets.
        assert_eq!(
            restore_size(&volume(2 * GI, Some(1_077_698_560))),
            1_077_698_560
        );
    }

    #[test]
    fn local_rounds() {
        let d = vec!["n1".to_owned()];
        assert_eq!(
            next(&round(P::Pending, false, &d, &[])),
            go(P::Taking, None)
        );
        let mut r = round(P::Pending, false, &d, &[]);
        r.volume_ready = false;
        assert_eq!(next(&r), Step::Stay);
        assert_eq!(next(&round(P::Taking, false, &d, &[])), Step::Stay);
        let acks = [ack("n1", P::Taking, true)];
        assert_eq!(
            next(&round(P::Taking, false, &d, &acks)),
            go(P::Ready, None)
        );
        let acks = [ack("n1", P::Taking, false)];
        assert!(matches!(
            next(&round(P::Taking, false, &d, &acks)),
            Step::Go {
                phase: P::Failed,
                error: Some(_)
            }
        ));
        let mut r = round(P::Taking, false, &d, &[]);
        r.phase_for = Some(Duration::from_secs(120));
        assert!(matches!(
            next(&r),
            Step::Go {
                phase: P::Failed,
                error: Some(_)
            }
        ));
    }

    #[test]
    fn replicated_rounds() {
        let d = vec!["n1".to_owned(), "n2".to_owned()];
        assert_eq!(
            next(&round(P::Pending, true, &d, &[])),
            go(P::Suspending, None)
        );
        let half = [ack("n1", P::Suspending, true)];
        assert_eq!(next(&round(P::Suspending, true, &d, &half)), Step::Stay);
        let both = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, true),
        ];
        assert_eq!(
            next(&round(P::Suspending, true, &d, &both)),
            go(P::Suspended, None)
        );
        let up = |n: &str| model::ReplicaStatus {
            node: n.into(),
            present: true,
            disk_state: Some("UpToDate".into()),
            ..model::ReplicaStatus::default()
        };
        let st = VolumeStatus {
            replicas: vec![up("n1"), up("n2")],
            ..VolumeStatus::default()
        };
        let mut r = round(P::Suspended, true, &d, &both);
        r.volume_status = Some(&st);
        assert_eq!(next(&r), go(P::Taking, None));
        // A diskful node without an entry (just placed by spec.evict, its
        // LV blank) is not UpToDate: no take.
        let partial = VolumeStatus {
            replicas: vec![up("n1")],
            ..VolumeStatus::default()
        };
        r.volume_status = Some(&partial);
        assert_eq!(next(&r), go(P::Resuming, Some("not UpToDate on n2".into())));
        r.volume_status = None;
        assert_eq!(
            next(&r),
            go(P::Resuming, Some("not UpToDate on n1, n2".into()))
        );
        // Acks of the previous round do not count for this one.
        assert_eq!(next(&round(P::Taking, true, &d, &both)), Step::Stay);
        let taken = [ack("n1", P::Taking, true), ack("n2", P::Taking, true)];
        assert_eq!(
            next(&round(P::Taking, true, &d, &taken)),
            go(P::Taken, None)
        );
        assert_eq!(
            next(&round(P::Taken, true, &d, &taken)),
            go(P::Resuming, None)
        );
        let resumed = [ack("n1", P::Resuming, true), ack("n2", P::Resuming, true)];
        assert_eq!(
            next(&round(P::Resuming, true, &d, &resumed)),
            go(P::Ready, None)
        );
    }

    #[test]
    fn every_member_acks_the_io_rounds() {
        let d = vec!["n1".to_owned(), "n2".to_owned()];
        let m = vec!["n1".to_owned(), "n2".to_owned(), "n3".to_owned()];
        let with = |phase: P, acks: &[SnapshotAck]| -> Step {
            let mut r = round(phase, true, &d, acks);
            r.members = &m;
            next(&r)
        };
        let both = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, true),
        ];
        assert_eq!(with(P::Suspending, &both), Step::Stay, "tiebreaker missing");
        let all = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, true),
            ack("n3", P::Suspending, true),
        ];
        assert_eq!(with(P::Suspending, &all), go(P::Suspended, None));
        // Taking: diskful only, and only acks naming a pool.
        let mut no_pool = ack("n2", P::Taking, true);
        no_pool.pool = None;
        let taking = [ack("n1", P::Taking, true), no_pool];
        assert_eq!(with(P::Taking, &taking), Step::Stay);
        let taking = [ack("n1", P::Taking, true), ack("n2", P::Taking, true)];
        assert_eq!(with(P::Taking, &taking), go(P::Taken, None));
        // A diskless member failing to resume fails the snapshot.
        let resumed = [
            ack("n1", P::Resuming, true),
            ack("n2", P::Resuming, true),
            ack("n3", P::Resuming, false),
        ];
        assert!(matches!(
            with(P::Resuming, &resumed),
            Step::Go {
                phase: P::Failed,
                error: Some(_)
            }
        ));
    }

    #[test]
    fn replicated_failures_resume_first() {
        let d = vec!["n1".to_owned(), "n2".to_owned()];
        let acks = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, false),
        ];
        let Step::Go {
            phase: P::Resuming,
            error: Some(e),
        } = next(&round(P::Suspending, true, &d, &acks))
        else {
            panic!("expected Resuming");
        };
        assert!(e.contains("n2: boom"), "{e}");
        let resumed = [ack("n1", P::Resuming, true), ack("n2", P::Resuming, true)];
        let mut r = round(P::Resuming, true, &d, &resumed);
        r.last_error = Some(&e);
        assert_eq!(next(&r), go(P::Failed, Some(e.clone())));

        let mut r = round(P::Taking, true, &d, &[]);
        r.phase_for = Some(Duration::from_secs(121));
        assert!(matches!(
            next(&r),
            Step::Go {
                phase: P::Resuming,
                error: Some(_)
            }
        ));

        // UpToDate gate between suspend and take.
        let st = VolumeStatus {
            replicas: vec![model::ReplicaStatus {
                node: "n2".into(),
                disk_state: Some("Inconsistent".into()),
                ..model::ReplicaStatus::default()
            }],
            ..VolumeStatus::default()
        };
        let mut r = round(P::Suspended, true, &d, &[]);
        r.volume_status = Some(&st);
        let Step::Go {
            phase: P::Resuming,
            error: Some(e),
        } = next(&r)
        else {
            panic!("expected Resuming");
        };
        assert!(e.contains("n2"));

        let bad = [ack("n1", P::Resuming, false)];
        assert!(matches!(
            next(&round(P::Resuming, true, &d, &bad)),
            Step::Go {
                phase: P::Failed,
                ..
            }
        ));
    }

    #[test]
    fn holders_for_delete() {
        let mut a = ack("n1", P::Taking, true);
        a.pool = Some("p1".into());
        let b = ack("n2", P::Suspending, true);
        let c = ack("n3", P::Deleting, false);
        let acks = [a, b, c];
        let h = holders(&acks, &[]);
        assert_eq!(
            h.iter().map(String::as_str).collect::<Vec<_>>(),
            ["n1", "n3"]
        );
        assert!(!deleted_everywhere(&acks, &h));
        // After Taking, a diskful node without any ack still holds.
        let h = holders(&acks, &["n4".to_owned()]);
        assert!(h.contains("n4"));
        let done = [ack("n1", P::Deleting, true), ack("n3", P::Deleting, true)];
        assert!(deleted_everywhere(&done, &holders(&done, &[])));
        assert!(!deleted_everywhere(
            &done,
            &holders(&done, &["n4".to_owned()])
        ));
    }

    #[test]
    fn an_uninitialized_placed_restore_holds_its_source() {
        use std::sync::Arc;
        let restore = |name: &str, from: &str| {
            let spec: model::VolumeSpec = serde_json::from_value(json!({
                "sizeBytes": 1, "tier": "t", "replicas": 2,
                "source": { "snapshot": from },
                "assignment": { "replicas": [] },
            }))
            .unwrap();
            Volume::new(name, spec)
        };
        let v = restore("v", "snap");
        assert_eq!(
            restoring_from(&[Arc::new(v.clone())], "snap").as_deref(),
            Some("v")
        );
        assert_eq!(restoring_from(&[Arc::new(v.clone())], "other"), None);
        let mut init = v.clone();
        init.status = Some(VolumeStatus {
            initialized_at: Some(Time(Timestamp::now())),
            ..VolumeStatus::default()
        });
        let mut unplaced = v.clone();
        unplaced.spec.assignment = None;
        let mut deleting = v;
        deleting.metadata.deletion_timestamp = Some(Time(Timestamp::now()));
        for x in [init, unplaced, deleting] {
            assert_eq!(restoring_from(&[Arc::new(x)], "snap"), None);
        }
    }

    #[test]
    fn suspend_bound_names_the_silent_members() {
        let d = vec!["n1".to_owned(), "n2".to_owned()];
        let m = vec!["n1".to_owned(), "n2".to_owned(), "n3".to_owned()];
        let acks = [
            ack("n1", P::Suspending, true),
            ack("n2", P::Suspending, true),
        ];
        let mut r = round(P::Suspending, true, &d, &acks);
        r.members = &m;
        r.phase_for = Some(Duration::from_secs(29));
        assert_eq!(next(&r), Step::Stay);
        r.phase_for = Some(Duration::from_secs(30));
        let Step::Go {
            phase: P::Resuming,
            error: Some(e),
        } = next(&r)
        else {
            panic!("expected Resuming");
        };
        assert!(e.contains("n3") && !e.contains("n1"), "{e}");
    }

    #[test]
    fn resume_members_from_acks() {
        let listed: Vec<String> = ["n1", "n2", "n3", "n4", "n5"]
            .iter()
            .map(|n| (*n).to_owned())
            .collect();
        let acks = [
            ack("n1", P::Taking, true),
            ack("n2", P::Resuming, true),
            ack("n3", P::Suspending, true),
            // Failed to suspend: nothing to resume.
            ack("n4", P::Suspending, false),
            // Removed from the assignment mid round.
            ack("gone", P::Suspending, true),
        ];
        // n5 was attached mid round and never acked.
        assert_eq!(resume_members(&listed, &acks), ["n1", "n2", "n3"]);
    }

    #[test]
    fn unreachable_primary_holds_the_round() {
        let e = |node: &str, in_use: bool| model::ReplicaStatus {
            node: node.into(),
            present: true,
            in_use,
            ..model::ReplicaStatus::default()
        };
        let all = |_: &str| true;
        let not_n2 = |n: &str| n != "n2";
        // Halted Primary n2, nobody promoted yet: held.
        let r = [e("n1", false), e("n2", true)];
        let held = held_by_unreachable_primary(&r, &not_n2, &all).unwrap();
        assert!(held.contains("n2"), "{held}");
        // n1 took over: the stale entry no longer holds the round.
        let r = [e("n1", true), e("n2", true)];
        assert_eq!(held_by_unreachable_primary(&r, &not_n2, &all), None);
        // n2's Node object is gone: ignored.
        let r = [e("n1", false), e("n2", true)];
        assert_eq!(held_by_unreachable_primary(&r, &not_n2, &not_n2), None);
        // Everyone reachable: never held.
        assert_eq!(held_by_unreachable_primary(&r, &all, &all), None);
    }
}
