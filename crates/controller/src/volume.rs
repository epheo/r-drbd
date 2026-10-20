//! Volume reconciler: placement and allocation, status aggregation from
//! the agents' replica entries, deletion.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::{Patch, PatchParams};
use kube::{Api, ResourceExt};
use model::condition::{self, Condition, FALSE, TRUE, types};
use model::keys::{CONTROLLER_FIELD_MANAGER, FINALIZER_VOLUME};
use model::{
    AssignedReplica, Assignment, AttachmentError, AttachmentErrorReason, Pool, Role, Snapshot,
    SnapshotPhase, Volume, VolumePhase, VolumeStatus,
};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::alloc::{self, Entropy, Used};
use crate::pool::Usage;
use crate::store::Stores;
use crate::{Ctx, Memory, apply_status, has_finalizer, set_finalizer, valid_volume_name};

pub mod reason {
    pub const INVALID_NAME: &str = "InvalidName";
    pub const INSUFFICIENT: &str = "Insufficient";
    pub const UNSUPPORTED_RESTORE: &str = "UnsupportedRestore";
    pub const RESTORE_NODE_CONFLICT: &str = "RestoreNodeConflict";
    pub const SNAPSHOT_NOT_READY: &str = "SnapshotNotReady";
    pub const SNAPSHOT_FAILED: &str = "SnapshotFailed";
    pub const ALLOCATION_FAILED: &str = "AllocationFailed";
    /// AttachmentFailed True, and its Warning Event.
    pub const ATTACHMENT_FAILED: &str = "AttachmentFailed";
    /// AttachmentFailed False.
    pub const ATTACHED: &str = "Attached";
    pub const PLACING: &str = "Placing";
    pub const WAITING: &str = "Waiting";
    pub const ALL_UP_TO_DATE: &str = "AllReplicasUpToDate";
    pub const REPLICA_MISSING: &str = "ReplicaMissing";
    pub const REPLICA_NOT_UP_TO_DATE: &str = "ReplicaNotUpToDate";
    pub const REPLICA_DISCONNECTED: &str = "ReplicaDisconnected";
    pub const NODE_UNREACHABLE: &str = "NodeUnreachable";
    pub const RESIZING: &str = "Resizing";
    pub const PROVISIONING: &str = "Provisioning";
    pub const DELETING: &str = "Deleting";
    pub const NO_QUORUM: &str = "NoQuorum";
    pub const QUORATE: &str = "Quorate";
    pub const SPLIT_BRAIN: &str = "SplitBrain";
    pub const NO_SPLIT_BRAIN: &str = "NoSplitBrain";
    pub const INVALID_SURVIVOR: &str = "InvalidSurvivor";
    pub const RESOLVE_WITHOUT_SPLIT_BRAIN: &str = "ResolveWithoutSplitBrain";
    /// Event reason once spec.resolve is cleared.
    pub const SPLIT_BRAIN_RESOLVED: &str = "SplitBrainResolved";
    /// Event reasons of spec.evict.
    pub const EVICTED: &str = "Evicted";
    pub const EVICTION_BLOCKED: &str = "EvictionBlocked";
    /// PeerUnreachable reasons, from the stuck side's probe of the peer.
    pub const PEERS_REACHABLE: &str = "PeersReachable";
    pub const FIREWALL: &str = "Firewall";
    pub const NOT_LISTENING: &str = "NotListening";
    pub const TIMEOUT: &str = "Timeout";
    pub const UNKNOWN: &str = "Unknown";
    /// Event reasons of PeerUnreachable transitions.
    pub const PEER_UNREACHABLE: &str = "PeerUnreachable";
}

/// Condition types the controller owns on a Volume.
const OWNED_TYPES: [&str; 6] = [
    types::READY,
    types::ATTACHMENT_FAILED,
    types::DEGRADED,
    types::QUORUM_LOST,
    types::SPLIT_BRAIN,
    types::PEER_UNREACHABLE,
];

/// One controller-owned condition before it gets a timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cond {
    pub status: &'static str,
    pub reason: &'static str,
    pub message: String,
}

impl Cond {
    fn new(status: &'static str, reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            reason,
            message: message.into(),
        }
    }
}

/// What the controller derives from an assigned volume's replica entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub phase: VolumePhase,
    pub ready: Cond,
    pub degraded: Cond,
    pub quorum_lost: Cond,
    pub split_brain: Cond,
    pub peer_unreachable: Cond,
    /// Pairs to keep latched for the next pass.
    pub split_pairs: BTreeSet<Pair>,
    /// The phase is Ready: every diskful replica present and UpToDate,
    /// connected to the other diskful ones, all from fresh agents. No
    /// consumer can have written before (CreateVolume returns on Ready), so
    /// status.initializedAt is due; a replica created after it starts from
    /// just-created metadata and a full sync.
    pub initialized: bool,
    /// Net size every reachable diskful replica provides.
    pub usable_bytes: Option<u64>,
    pub nodes: Option<String>,
    /// The reachable node whose entry is in use.
    pub primary: Option<String>,
}

impl Summary {
    /// Ready False for a reason outside the replica states (an invalid
    /// survivor); a volume that was Ready turns Degraded.
    fn not_ready(&mut self, ready: Cond) {
        if self.phase == VolumePhase::Ready {
            self.phase = VolumePhase::Degraded;
        }
        self.ready = ready;
    }

    fn conds(&self) -> [(&'static str, &Cond); 5] {
        [
            (types::READY, &self.ready),
            (types::DEGRADED, &self.degraded),
            (types::QUORUM_LOST, &self.quorum_lost),
            (types::SPLIT_BRAIN, &self.split_brain),
            (types::PEER_UNREACHABLE, &self.peer_unreachable),
        ]
    }
}

/// What the pass knows about nodes, beside the replica entries.
pub struct Nodes<'a> {
    /// Node Ready condition is True.
    pub ready: &'a dyn Fn(&str) -> bool,
    /// The agent Lease is fresh; false means its entries are unknown.
    pub reachable: &'a dyn Fn(&str) -> bool,
    /// The volume's spec.evict: those nodes' connections are expected to break.
    pub evicting: &'a [String],
    /// (prober, record): the peer agent run the prober saw still holds the
    /// Lease and renewed after this controller first saw the record.
    pub alive_across: &'a dyn Fn(&str, &model::PeerProbe) -> bool,
}

impl std::fmt::Debug for Nodes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Nodes").finish_non_exhaustive()
    }
}

/// The gaps in words. An unreachable node's entries are "unknown": its
/// agent cannot update them, so they may still read UpToDate or in use.
fn describe(g: &Gaps<'_>) -> Vec<String> {
    let mut parts = Vec::new();
    if !g.unreachable.is_empty() {
        parts.push(format!(
            "unknown (agent unreachable): {}",
            g.unreachable.join(", ")
        ));
    }
    if !g.unready.is_empty() {
        parts.push(format!("node not ready: {}", g.unready.join(", ")));
    }
    if !g.missing.is_empty() {
        parts.push(format!("missing on {}", g.missing.join(", ")));
    }
    if !g.stale.is_empty() {
        parts.push(format!("not UpToDate on {}", g.stale.join(", ")));
    }
    if !g.disconnected.is_empty() {
        parts.push(format!("not connected: {}", g.disconnected.join(", ")));
    }
    parts
}

#[derive(Default)]
struct Gaps<'a> {
    unreachable: Vec<&'a str>,
    missing: Vec<&'a str>,
    unready: Vec<&'a str>,
    stale: Vec<&'a str>,
    disconnected: Vec<String>,
}

fn eq(a: Option<&String>, b: &str) -> bool {
    a.is_some_and(|x| x.eq_ignore_ascii_case(b))
}

/// Diskless attachment replicas are left out: they come and go with
/// publishes and the publish call waits for them itself.
fn gaps<'a>(
    assignment: &'a Assignment,
    diskful: &[&'a str],
    entries: &HashMap<&str, &model::ReplicaStatus>,
    nodes: &Nodes<'_>,
) -> Gaps<'a> {
    let drbd = diskful.len() >= 2;
    let mut g = Gaps::default();
    for r in assignment
        .replicas
        .iter()
        .filter(|r| r.role != Role::Diskless)
    {
        let node = r.node.as_str();
        if !(nodes.reachable)(node) {
            g.unreachable.push(node);
            continue;
        }
        if !(nodes.ready)(node) {
            g.unready.push(node);
            continue;
        }
        let Some(e) = entries.get(node).filter(|e| e.present) else {
            g.missing.push(node);
            continue;
        };
        if r.role != Role::Diskful {
            continue;
        }
        if !eq(e.disk_state.as_ref(), "UpToDate") {
            g.stale.push(node);
        }
        if drbd {
            for peer in diskful.iter().filter(|p| **p != node) {
                if !eq(e.peers.get(*peer), "Connected") {
                    g.disconnected.push(format!("{node} to {peer}"));
                }
            }
        }
    }
    g
}

/// A diskful node pair, names in order.
pub type Pair = (String, String);

fn pair(a: &str, b: &str) -> Pair {
    if a <= b {
        (a.to_owned(), b.to_owned())
    } else {
        (b.to_owned(), a.to_owned())
    }
}

const SPLIT_PREFIX: &str = "split brain between ";

/// The pairs a SplitBrain message names; seeds the latch after a restart.
pub fn pairs_from_message(message: &str) -> BTreeSet<Pair> {
    let Some(rest) = message.strip_prefix(SPLIT_PREFIX) else {
        return BTreeSet::new();
    };
    rest.split(';')
        .next()
        .unwrap_or_default()
        .split(", ")
        .filter_map(|p| p.split_once(" and "))
        .map(|(a, b)| pair(a.trim(), b.trim()))
        .collect()
}

/// QuorumLost and SplitBrain from the reachable diskful replicas, plus the
/// split pairs to latch. Split brain needs both sides StandAlone toward
/// each other (one side alone is a plain disconnect); once seen it holds
/// until both sides report Connected to each other, because the agent
/// re-adjusts a StandAlone connection and DRBD cycles through Connecting
/// before dropping to StandAlone again.
fn drbd_conditions(
    diskful: &[&str],
    entries: &HashMap<&str, &model::ReplicaStatus>,
    nodes: &Nodes<'_>,
    latched: &BTreeSet<Pair>,
) -> (Cond, Cond, BTreeSet<Pair>) {
    let known = |n: &str| entries.get(n).copied().filter(|_| (nodes.reachable)(n));
    let no_quorum: Vec<&str> = diskful
        .iter()
        .copied()
        .filter(|n| known(n).is_some_and(|e| e.present && e.quorum == Some(false)))
        .collect();
    let quorum = if no_quorum.is_empty() {
        Cond::new(FALSE, reason::QUORATE, "")
    } else {
        Cond::new(
            TRUE,
            reason::NO_QUORUM,
            format!("no quorum on {}", no_quorum.join(", ")),
        )
    };
    let mut pairs = BTreeSet::new();
    for (i, a) in diskful.iter().enumerate() {
        for b in &diskful[i + 1..] {
            let p = pair(a, b);
            let split = match (known(a), known(b)) {
                (Some(ea), Some(eb)) => {
                    let both =
                        |state: &str| eq(ea.peers.get(*b), state) && eq(eb.peers.get(*a), state);
                    both("StandAlone") || (latched.contains(&p) && !both("Connected"))
                }
                _ => latched.contains(&p),
            };
            if split {
                pairs.insert(p);
            }
        }
    }
    let split = if pairs.is_empty() {
        Cond::new(FALSE, reason::NO_SPLIT_BRAIN, "")
    } else {
        let names: Vec<String> = pairs.iter().map(|(a, b)| format!("{a} and {b}")).collect();
        Cond::new(
            TRUE,
            reason::SPLIT_BRAIN,
            format!(
                "{SPLIT_PREFIX}{}; set spec.resolve.survivor",
                names.join(", ")
            ),
        )
    };
    (quorum, split, pairs)
}

/// The stuck connections an agent probed, worst cause first. A record
/// counts only when both ends have a fresh agent, report their resource
/// up and are not in spec.evict, the peer's agent ran across the window
/// and renewed after the probe, and the pair is not split: a down or
/// rebooting node is NodeUnreachable's news, an evicted node is expected
/// to drop, and a StandAlone connection is SplitBrain's.
fn peer_unreachable(
    assignment: &Assignment,
    entries: &HashMap<&str, &model::ReplicaStatus>,
    nodes: &Nodes<'_>,
    split_pairs: &BTreeSet<Pair>,
) -> Cond {
    let assigned = |n: &str| assignment.replicas.iter().any(|r| r.node == n);
    let up = |n: &str| {
        assigned(n)
            && (nodes.reachable)(n)
            && !nodes.evicting.iter().any(|e| e == n)
            && entries.get(n).is_some_and(|e| e.present)
    };
    let stand_alone = |a: &str, b: &str| {
        entries
            .get(a)
            .is_some_and(|e| eq(e.peers.get(b), "StandAlone"))
    };
    let mut found = Vec::new();
    for e in entries.values().filter(|e| up(&e.node)) {
        for p in &e.unreachable_peers {
            let (a, b) = (e.node.as_str(), p.node.as_str());
            if !up(b)
                || !(nodes.alive_across)(a, p)
                || split_pairs.contains(&pair(a, b))
                || stand_alone(a, b)
                || stand_alone(b, a)
            {
                continue;
            }
            found.push((a, p));
        }
    }
    peer_message(&found)
}

/// Long messages crowd kubectl output; per pair detail stays in
/// replicas[].unreachablePeers.
const PEER_MESSAGE_MAX: usize = 300;

/// Nodes, ports and whether the port range hint applies, of one result.
type Group<'a> = (BTreeSet<&'a str>, BTreeSet<u16>, bool);

/// One clause per probe result, naming the nodes and ports involved.
fn peer_message(found: &[(&str, &model::PeerProbe)]) -> Cond {
    use model::ProbeResult as R;
    // (rank, reason, words, firewall hint), most actionable first.
    let cause = |p: &model::PeerProbe| match p.result {
        R::Filtered => (
            0,
            reason::FIREWALL,
            "rejected by a firewall".to_owned(),
            true,
        ),
        R::TimedOut => (
            1,
            reason::TIMEOUT,
            "no answer, packets dropped".to_owned(),
            true,
        ),
        // A firewall that answers with a TCP reset looks the same.
        R::Refused => (
            2,
            reason::NOT_LISTENING,
            "connection refused (nothing listens, or a firewall resets)".to_owned(),
            true,
        ),
        R::Accepted => (
            3,
            reason::UNKNOWN,
            "port answers but DRBD does not connect (see the kernel log on both nodes)".to_owned(),
            false,
        ),
        R::Failed => (
            3,
            reason::UNKNOWN,
            format!(
                "probe failed ({})",
                p.error.as_deref().unwrap_or("no error text")
            ),
            false,
        ),
    };
    let mut groups: std::collections::BTreeMap<_, Group<'_>> = std::collections::BTreeMap::new();
    for (a, p) in found {
        let (rank, why, words, hint) = cause(p);
        let g = groups.entry((rank, why, words)).or_default();
        g.0.insert(a);
        g.0.insert(&p.node);
        g.1.insert(p.port);
        g.2 |= hint;
    }
    let Some(((_, why, _), _)) = groups.first_key_value() else {
        return Cond::new(FALSE, reason::PEERS_REACHABLE, "");
    };
    let why = *why;
    let range = model::keys::PORT_RANGE;
    let hint = groups
        .values()
        .any(|g| g.2)
        .then(|| format!("open TCP {}-{} between nodes", range.start(), range.end()));
    let clauses: Vec<String> = groups
        .iter()
        .map(|((_, _, words), (nodes, ports, _))| {
            let nodes: Vec<&str> = nodes.iter().copied().collect();
            let ports: Vec<String> = ports.iter().map(u16::to_string).collect();
            format!(
                "{words} between {} (TCP {})",
                nodes.join(", "),
                ports.join(", ")
            )
        })
        .collect();
    let room = PEER_MESSAGE_MAX.saturating_sub(hint.as_ref().map_or(0, |h| h.len() + 2));
    let mut message = String::new();
    for (i, c) in clauses.iter().enumerate() {
        let more = format!("; +{} more", clauses.len() - i);
        let next = if message.is_empty() {
            c.clone()
        } else {
            format!("{message}; {c}")
        };
        let last = i + 1 == clauses.len();
        if i > 0 && next.len() + if last { 0 } else { more.len() } > room {
            message.push_str(&more);
            break;
        }
        message = next;
    }
    if let Some(h) = hint {
        message = format!("{message}; {h}");
    }
    Cond::new(TRUE, why, message)
}

/// Status aggregation, pure. Provisioning until every assigned replica is
/// present on a Ready node, every diskful one UpToDate and, for DRBD,
/// connected to every other diskful one; afterwards a gap is Degraded,
/// because the volume was usable once and still serves from the rest.
pub fn aggregate(
    size_bytes: u64,
    assignment: &Assignment,
    status: Option<&VolumeStatus>,
    node_ready: &dyn Fn(&str) -> bool,
) -> Summary {
    aggregate_nodes(
        size_bytes,
        assignment,
        status,
        &Nodes {
            ready: node_ready,
            reachable: &|_| true,
            evicting: &[],
            alive_across: &|_, _| true,
        },
        &BTreeSet::new(),
        None,
    )
}

/// usableBytes and primary from reachable nodes only: an unreachable
/// node's entry is stale (a halted Primary still reads in use).
fn known_values(
    assignment: &Assignment,
    diskful: &[&str],
    entries: &HashMap<&str, &model::ReplicaStatus>,
    status: Option<&VolumeStatus>,
    nodes: &Nodes<'_>,
    generation: Option<i64>,
) -> (Option<u64>, Option<String>) {
    let usable_bytes = diskful
        .iter()
        .filter(|n| (nodes.reachable)(n))
        .filter_map(|n| entries.get(n).and_then(|e| e.size_bytes))
        .min();
    // Several entries in use: ReadWriteMany, or an old Primary whose Lease
    // came back before its agent rewrote the entry. Those written for this
    // generation win, then the smallest node name, so list order (which
    // agent wrote first) never decides.
    let mut in_use: Vec<&model::ReplicaStatus> = status
        .iter()
        .flat_map(|s| &s.replicas)
        .filter(|e| e.in_use && (nodes.reachable)(&e.node))
        .filter(|e| assignment.replicas.iter().any(|r| r.node == e.node))
        .collect();
    in_use.sort_by(|a, b| a.node.cmp(&b.node));
    let primary = in_use
        .iter()
        .find(|e| generation.is_some() && e.observed_generation == generation)
        .or(in_use.first())
        .map(|e| e.node.clone());
    (usable_bytes, primary)
}

/// Every agent-reported error, "node: text", sorted.
fn replica_errors(status: Option<&VolumeStatus>) -> Vec<String> {
    let mut errors: Vec<String> = status
        .map(|s| {
            s.replicas
                .iter()
                .filter_map(|e| e.last_error.as_ref().map(|x| format!("{}: {x}", e.node)))
                .collect()
        })
        .unwrap_or_default();
    errors.sort();
    errors
}

/// `aggregate` with agent reachability (an unreachable node's entries
/// count as unknown) and the split pairs latched so far.
pub fn aggregate_nodes(
    size_bytes: u64,
    assignment: &Assignment,
    status: Option<&VolumeStatus>,
    nodes: &Nodes<'_>,
    latched: &BTreeSet<Pair>,
    generation: Option<i64>,
) -> Summary {
    let entries: HashMap<&str, &model::ReplicaStatus> = status
        .map(|s| s.replicas.iter().map(|e| (e.node.as_str(), e)).collect())
        .unwrap_or_default();
    let diskful: Vec<&str> = assignment
        .replicas
        .iter()
        .filter(|r| r.role == Role::Diskful)
        .map(|r| r.node.as_str())
        .collect();
    let g = gaps(assignment, &diskful, &entries, nodes);
    let (quorum_lost, split_brain, split_pairs) =
        drbd_conditions(&diskful, &entries, nodes, latched);
    let peer_unreachable = peer_unreachable(assignment, &entries, nodes, &split_pairs);
    let (usable_bytes, primary) =
        known_values(assignment, &diskful, &entries, status, nodes, generation);
    let node_list = (!diskful.is_empty()).then(|| diskful.join(","));
    let was_ready =
        status.is_some_and(|s| matches!(s.phase, VolumePhase::Ready | VolumePhase::Degraded));
    let errors = replica_errors(status);

    let parts = describe(&g);
    let gap = parts.join("; ");
    let with_errors = |m: String| {
        if errors.is_empty() {
            m
        } else if m.is_empty() {
            format!("errors: {}", errors.join("; "))
        } else {
            format!("{m}; errors: {}", errors.join("; "))
        }
    };
    let (phase, ready, degraded) = if parts.is_empty() {
        let ready = match usable_bytes {
            Some(u) if u >= size_bytes => {
                Cond::new(TRUE, reason::ALL_UP_TO_DATE, with_errors(String::new()))
            }
            u => Cond::new(
                TRUE,
                reason::RESIZING,
                with_errors(format!(
                    "growing to {size_bytes} bytes, usable {}",
                    u.unwrap_or(0)
                )),
            ),
        };
        (
            VolumePhase::Ready,
            ready,
            Cond::new(FALSE, reason::ALL_UP_TO_DATE, ""),
        )
    } else {
        let why = if !g.unreachable.is_empty() {
            reason::NODE_UNREACHABLE
        } else if !g.missing.is_empty() || !g.unready.is_empty() {
            reason::REPLICA_MISSING
        } else if !g.stale.is_empty() {
            reason::REPLICA_NOT_UP_TO_DATE
        } else {
            reason::REPLICA_DISCONNECTED
        };
        if was_ready {
            (
                VolumePhase::Degraded,
                Cond::new(FALSE, why, with_errors(gap.clone())),
                Cond::new(TRUE, why, gap),
            )
        } else {
            // An unreachable agent is news even before the first Ready.
            let degraded = if g.unreachable.is_empty() {
                Cond::new(FALSE, reason::PROVISIONING, "")
            } else {
                Cond::new(TRUE, why, gap.clone())
            };
            (
                VolumePhase::Provisioning,
                Cond::new(
                    FALSE,
                    reason::WAITING,
                    with_errors(format!("waiting for replicas: {gap}")),
                ),
                degraded,
            )
        }
    };
    Summary {
        phase,
        ready,
        degraded,
        quorum_lost,
        split_brain,
        peer_unreachable,
        split_pairs,
        initialized: phase == VolumePhase::Ready,
        usable_bytes,
        nodes: node_list,
        primary,
    }
}

/// spec.resolve is done: no split brain left, every diskful replica saw
/// the resolve generation, the survivor is UpToDate and every other one is
/// Connected to it and UpToDate or resyncing.
pub fn resolve_done(
    assignment: &Assignment,
    status: Option<&VolumeStatus>,
    survivor: &str,
    generation: i64,
    summary: &Summary,
    reachable: &dyn Fn(&str) -> bool,
) -> bool {
    if summary.split_brain.status != FALSE {
        return false;
    }
    let entries: HashMap<&str, &model::ReplicaStatus> = status
        .map(|s| s.replicas.iter().map(|e| (e.node.as_str(), e)).collect())
        .unwrap_or_default();
    assignment
        .replicas
        .iter()
        .filter(|r| r.role == Role::Diskful)
        .all(|r| {
            let Some(e) = entries.get(r.node.as_str()) else {
                return false;
            };
            if !reachable(&r.node) || !e.present || e.observed_generation.unwrap_or(0) < generation
            {
                return false;
            }
            let up = eq(e.disk_state.as_ref(), "UpToDate");
            if r.node == survivor {
                return up;
            }
            let syncing = eq(e.disk_state.as_ref(), "Inconsistent") && e.sync_percent.is_some();
            eq(e.peers.get(survivor), "Connected") && (up || syncing)
        })
}

/// What to do with a valid spec.resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveAction {
    /// Split brain or resync in progress: the agents work, keep the field.
    Wait,
    /// Resynced: clear spec.resolve.
    Clear,
    /// No split brain seen under this resolve: the agents refuse to act
    /// and the user must remove it.
    WithoutSplitBrain,
}

/// `seen_split` is whether SplitBrain was True while this resolve was set.
pub fn resolve_action(split_brain: bool, seen_split: bool, done: bool) -> ResolveAction {
    match (split_brain, seen_split, done) {
        (true, _, _) | (false, true, false) => ResolveAction::Wait,
        (false, true, true) => ResolveAction::Clear,
        (false, false, _) => ResolveAction::WithoutSplitBrain,
    }
}

/// Diskless entries converged to the attachments: a diskless entry whose
/// node is no longer attached goes first, so its node id is free again;
/// then an attached node without a replica gets one (lowest free node id
/// in the volume, lowest free port on that node). A node in spec.evict
/// never gets a new one, but keeps the one it has while attached: that is
/// a running workload. Diskful and tiebreaker entries never change. A node
/// that cannot get a replica is reported and never holds back another.
/// The assignment is None when nothing changes.
pub fn converge_attachments(
    a: &Assignment,
    attachments: &[String],
    evict: &[String],
    used: &Used,
) -> (Option<Assignment>, Vec<AttachmentError>) {
    let mut out = a.clone();
    out.replicas
        .retain(|r| r.role != Role::Diskless || attachments.contains(&r.node));
    let mut changed = out.replicas.len() != a.replicas.len();
    let mut failed = Vec::new();
    for node in attachments {
        if evict.contains(node) || out.replicas.iter().any(|r| r.node == *node) {
            continue;
        }
        let node_id = (0..=model::keys::NODE_ID_MAX)
            .find(|id| !out.replicas.iter().any(|r| r.node_id == *id));
        let port = used.lowest_free_port(node);
        let (node_id, port) = match (node_id, port) {
            (Some(id), Some(port)) => (id, port),
            (None, _) => {
                failed.push(attach_error(
                    node,
                    AttachmentErrorReason::TooManyPeers,
                    &alloc::AllocError::TooManyPeers,
                ));
                continue;
            }
            (_, None) => {
                failed.push(attach_error(
                    node,
                    AttachmentErrorReason::NoPort,
                    &alloc::AllocError::NoPort(node.clone()),
                ));
                continue;
            }
        };
        out.replicas.push(AssignedReplica {
            node: node.clone(),
            pool: None,
            node_id,
            port: Some(port),
            role: Role::Diskless,
        });
        changed = true;
    }
    (changed.then_some(out), failed)
}

fn attach_error(
    node: &str,
    reason: AttachmentErrorReason,
    cause: &alloc::AllocError,
) -> AttachmentError {
    AttachmentError {
        node: node.to_owned(),
        reason,
        message: format!("no diskless replica on {node}: {cause}"),
    }
}

/// Controller-owned part of a Volume status, as compared and applied.
#[derive(Debug, Clone, PartialEq)]
struct Owned {
    phase: VolumePhase,
    conditions: Vec<Condition>,
    usable_bytes: Option<u64>,
    primary: Option<String>,
    nodes: Option<String>,
    initialized_at: Option<Time>,
    attachment_errors: Vec<AttachmentError>,
    observed_generation: Option<i64>,
}

impl Owned {
    /// Every controller-owned condition type counts, so dropping one is a
    /// change too.
    fn current(v: &Volume) -> Option<Self> {
        let s = v.status.as_ref()?;
        let mut conditions: Vec<Condition> = OWNED_TYPES
            .iter()
            .filter_map(|t| condition::get(&s.conditions, t).cloned())
            .collect();
        conditions.sort_by(|a, b| a.type_.cmp(&b.type_));
        Some(Self {
            phase: s.phase,
            conditions,
            usable_bytes: s.usable_bytes,
            primary: s.primary.clone(),
            nodes: s.nodes.clone(),
            initialized_at: s.initialized_at.clone(),
            attachment_errors: s.attachment_errors.clone(),
            observed_generation: s.observed_generation,
        })
    }

    fn wanted(
        v: &Volume,
        phase: VolumePhase,
        conds: &[(&str, &Cond)],
        summary: Option<&Summary>,
    ) -> Self {
        let existing = v
            .status
            .as_ref()
            .map(|s| s.conditions.as_slice())
            .unwrap_or_default();
        let generation = v.metadata.generation;
        let mut conditions = build_conditions(existing, conds, generation);
        conditions.sort_by(|a, b| a.type_.cmp(&b.type_));
        // Only the candidate; `write` carries an earlier value first.
        let initialized_at = summary
            .filter(|s| s.initialized)
            .map(|_| model::time::now());
        Self {
            phase,
            conditions,
            usable_bytes: summary.and_then(|s| s.usable_bytes),
            primary: summary.and_then(|s| s.primary.clone()),
            nodes: summary.and_then(|s| s.nodes.clone()),
            initialized_at,
            attachment_errors: Vec::new(),
            observed_generation: generation,
        }
    }

    fn json(&self) -> serde_json::Value {
        let mut s = json!({
            "phase": self.phase,
            "conditions": self.conditions,
        });
        if let Some(m) = s.as_object_mut() {
            if let Some(u) = self.usable_bytes {
                m.insert("usableBytes".into(), json!(u));
            }
            if let Some(p) = &self.primary {
                m.insert("primary".into(), json!(p));
            }
            if let Some(n) = &self.nodes {
                m.insert("nodes".into(), json!(n));
            }
            if let Some(g) = self.observed_generation {
                m.insert("observedGeneration".into(), json!(g));
            }
            if let Some(t) = &self.initialized_at {
                m.insert("initializedAt".into(), json!(t));
            }
            if !self.attachment_errors.is_empty() {
                m.insert("attachmentErrors".into(), json!(self.attachment_errors));
            }
        }
        s
    }
}

/// Conditions with their transition time kept while the status holds.
pub(crate) fn build_conditions(
    existing: &[Condition],
    wanted: &[(&str, &Cond)],
    generation: Option<i64>,
) -> Vec<Condition> {
    let mut out = existing.to_vec();
    out.retain(|c| wanted.iter().any(|(t, _)| *t == c.type_));
    for (t, c) in wanted {
        condition::set(
            &mut out,
            condition::new(
                t,
                c.status,
                c.reason,
                c.message.clone(),
                generation,
                model::time::now(),
            ),
        );
    }
    out
}

/// initializedAt is set once and carried by every apply (dropping it from
/// a server-side apply removes it, and agents rely on it never going
/// away): the value this process wrote, else the stored one, else `new`.
pub fn carry_initialized(
    remembered: Option<&Time>,
    stored: Option<&Time>,
    new: Option<Time>,
) -> Option<Time> {
    remembered.or(stored).cloned().or(new)
}

async fn write(memory: &mut Memory, api: &Api<Volume>, v: &Volume, wanted: Owned) {
    let mut wanted = wanted;
    let uid = v.metadata.uid.clone();
    wanted.initialized_at = carry_initialized(
        uid.as_deref().and_then(|u| memory.initialized.get(u)),
        v.status.as_ref().and_then(|s| s.initialized_at.as_ref()),
        wanted.initialized_at,
    );
    if let (Some(uid), Some(t)) = (uid, &wanted.initialized_at) {
        memory.initialized.insert(uid, t.clone());
    }
    if Owned::current(v).as_ref() == Some(&wanted) {
        return;
    }
    apply_status(api, &v.name_any(), wanted.json()).await;
}

/// Placement inputs built once per pass and updated after every
/// assignment, so later volumes of the same pass see the reservations.
struct Placing {
    views: Vec<placer::PoolView>,
    used: Used,
    ready_nodes: Vec<String>,
}

impl Placing {
    fn new(stores: &Stores, usage: &Usage, memory: &Memory, volumes: &[Arc<Volume>]) -> Self {
        let pools = Stores::sorted(&stores.pools);
        let ready_nodes = stores.ready_nodes();
        Self {
            views: pool_views(&pools, usage, &ready_nodes),
            used: Used::from_assignments(volumes.iter().filter_map(|v| memory.assignment(v))),
            ready_nodes,
        }
    }

    /// One more replica of `size` in (node, pool), as a later volume of the
    /// pass must see it.
    fn reserve(&mut self, node: &str, pool: &str, size: u64) {
        if let Some(view) = self
            .views
            .iter_mut()
            .find(|p| p.node == node && p.name == pool)
        {
            view.committed_bytes = view.committed_bytes.saturating_add(size);
            view.volume_count = view.volume_count.saturating_add(1);
        }
    }

    fn record(&mut self, a: &Assignment, size: u64) {
        self.used.add(a);
        for r in a.replicas.iter().filter(|r| r.role == Role::Diskful) {
            if let Some(pool) = &r.pool {
                self.reserve(&r.node, pool, size);
            }
        }
    }
}

fn pool_views(pools: &[Arc<Pool>], usage: &Usage, ready_nodes: &[String]) -> Vec<placer::PoolView> {
    pools
        .iter()
        .map(|p| crate::pool::view(p, usage, ready_nodes))
        .collect()
}

/// A peer agent's Lease: its run (acquireTime) and when this process last
/// saw it renewed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentLease {
    pub run: Option<k8s_openapi::jiff::Timestamp>,
    pub renewed_seen: Option<std::time::Instant>,
}

/// A probe record counts when the peer run the prober saw still holds the
/// Lease and renewed after this process first saw the record. Run ids are
/// compared for equality and renewals on this process's clock, so node
/// clocks never meet: a peer that rebooted or crashed has a new run or no
/// renewal since.
pub fn probe_counts(
    lease: Option<&AgentLease>,
    peer_run: Option<&Time>,
    first_seen: Option<std::time::Instant>,
) -> bool {
    lease.is_some_and(|l| {
        l.run.is_some()
            && l.run == peer_run.map(|t| t.0)
            && matches!((l.renewed_seen, first_seen), (Some(r), Some(f)) if r > f)
    })
}

/// A record that could count but waits for a renewal this process sees;
/// only those need an early pass. One from another run, or without a run,
/// never counts and waits for nothing.
pub fn awaiting_renewal(
    lease: Option<&AgentLease>,
    peer_run: Option<&Time>,
    first_seen: Option<std::time::Instant>,
) -> bool {
    lease.is_some_and(|l| l.run.is_some() && l.run == peer_run.map(|t| t.0))
        && !probe_counts(lease, peer_run, first_seen)
}

/// When this process first saw each probe record, by (prober, peer,
/// peerRun). Not by probedAt: a re-probe of the same run keeps counting
/// instead of waiting for another renewal.
pub type ProbesSeen = HashMap<(String, String, String), std::time::Instant>;

fn probe_key(prober: &str, p: &model::PeerProbe) -> (String, String, String) {
    (
        prober.to_owned(),
        p.node.clone(),
        p.peer_run
            .as_ref()
            .map(|t| t.0.to_string())
            .unwrap_or_default(),
    )
}

/// Notes the first sight of the Volume's probe records, forgetting gone ones.
pub fn note_probes(seen: &mut ProbesSeen, v: &Volume, now: std::time::Instant) {
    let current: Vec<_> = v
        .status
        .iter()
        .flat_map(|s| &s.replicas)
        .flat_map(|e| e.unreachable_peers.iter().map(|p| probe_key(&e.node, p)))
        .collect();
    seen.retain(|k, _| current.contains(k));
    for k in current {
        seen.entry(k).or_insert(now);
    }
}

/// Node facts of one pass.
pub(crate) struct Health {
    ready: HashSet<String>,
    /// Nodes holding a replica or a status entry whose agent Lease is stale
    /// or missing; an entry outside the assignment (an evicted node) must
    /// not feed status.primary either.
    unreachable: HashSet<String>,
    /// Nodes with a core Node object.
    exists: HashSet<String>,
    leases: HashMap<String, AgentLease>,
}

impl Health {
    fn new(
        stores: &Stores,
        ready_nodes: &[String],
        volumes: &[Arc<Volume>],
        memory: &Memory,
    ) -> Self {
        let mut holding: HashSet<&str> = volumes
            .iter()
            .filter_map(|v| memory.assignment(v))
            .flat_map(|a| a.replicas.iter().map(|r| r.node.as_str()))
            .collect();
        holding.extend(
            volumes
                .iter()
                .filter_map(|v| v.status.as_ref())
                .flat_map(|s| s.replicas.iter().map(|e| e.node.as_str())),
        );
        let unreachable = holding
            .into_iter()
            .filter(|n| !stores.agent_fresh(n))
            .map(str::to_owned)
            .collect();
        Self {
            ready: ready_nodes.iter().cloned().collect(),
            unreachable,
            exists: stores.nodes.state().iter().map(|n| n.name_any()).collect(),
            leases: stores
                .leases
                .state()
                .iter()
                .filter_map(|l| {
                    let node = l
                        .name_any()
                        .strip_prefix(&model::keys::agent_lease_name(""))?
                        .to_owned();
                    let run = l.spec.as_ref()?.acquire_time.as_ref().map(|t| t.0);
                    let renewed_seen = stores.agent_renewed_at(&node);
                    Some((node, AgentLease { run, renewed_seen }))
                })
                .collect(),
        }
    }

    /// Any probe record of `v` that awaits a renewal.
    fn awaiting(&self, v: &Volume, seen: &ProbesSeen) -> bool {
        v.status
            .iter()
            .flat_map(|s| &s.replicas)
            .flat_map(|e| e.unreachable_peers.iter().map(move |p| (e, p)))
            .any(|(e, p)| {
                awaiting_renewal(
                    self.leases.get(&p.node),
                    p.peer_run.as_ref(),
                    seen.get(&probe_key(&e.node, p)).copied(),
                )
            })
    }

    fn summarize(
        &self,
        v: &Volume,
        a: &Assignment,
        latched: &BTreeSet<Pair>,
        seen: &ProbesSeen,
    ) -> Summary {
        aggregate_nodes(
            v.spec.size_bytes,
            a,
            v.status.as_ref(),
            &Nodes {
                ready: &|n| self.ready.contains(n),
                reachable: &|n| !self.unreachable.contains(n),
                evicting: &v.spec.evict,
                alive_across: &|prober, p| {
                    probe_counts(
                        self.leases.get(&p.node),
                        p.peer_run.as_ref(),
                        seen.get(&probe_key(prober, p)).copied(),
                    )
                },
            },
            latched,
            v.metadata.generation,
        )
    }
}

pub(crate) async fn reconcile_all(ctx: &Ctx<'_>, memory: &mut Memory) {
    let api: Api<Volume> = Api::all(ctx.client.clone());
    let volumes = Stores::sorted(&ctx.stores.volumes);
    let usage = crate::pool::usage(&volumes, memory);
    let mut placing = Placing::new(ctx.stores, &usage, memory, &volumes);
    let health = Health::new(ctx.stores, &placing.ready_nodes, &volumes, memory);
    for v in &volumes {
        reconcile(ctx, memory, &mut placing, &health, &api, v).await;
    }
}

async fn reconcile(
    ctx: &Ctx<'_>,
    memory: &mut Memory,
    placing: &mut Placing,
    health: &Health,
    api: &Api<Volume>,
    v: &Volume,
) {
    let name = v.name_any();
    if v.metadata.deletion_timestamp.is_some() {
        delete(memory, api, v, health).await;
        return;
    }
    if !has_finalizer(v, FINALIZER_VOLUME) {
        set_finalizer(api, v, FINALIZER_VOLUME, true).await;
        return;
    }
    if !valid_volume_name(&name) {
        let ready = Cond::new(
            FALSE,
            reason::INVALID_NAME,
            format!(
                "name must match {} and not start with snapshot or pvmove (LVM reserves them)",
                model::keys::NAME_PATTERN
            ),
        );
        write(
            memory,
            api,
            v,
            Owned::wanted(v, VolumePhase::Failed, &[(types::READY, &ready)], None),
        )
        .await;
        return;
    }
    if let Some(a) = memory.assignment(v).cloned() {
        let (a, attach_errors) = converge(memory, placing, api, v, a).await;
        let seen = memory.probes_seen(v);
        memory.set_probe_wait(v, health.awaiting(v, &seen));
        let mut summary = health.summarize(v, &a, &memory.latched(v), &seen);
        memory.latch(v, summary.split_pairs.clone());
        evict_step(ctx, memory, placing, api, v, &a, &mut summary).await;
        remove_orphans(ctx, health, api, v, &a).await;
        if v.spec.resolve.is_some() {
            resolve_step(ctx, memory, api, v, &a, &mut summary, health).await;
        } else if let Some(uid) = &v.metadata.uid {
            memory.resolving.remove(uid);
        }
        peer_event(ctx, memory, v, &summary.peer_unreachable).await;
        let attach = attach_report(ctx, memory, v, &attach_errors).await;
        let mut conds = summary.conds().to_vec();
        conds.push((types::ATTACHMENT_FAILED, &attach));
        let mut wanted = Owned::wanted(v, summary.phase, &conds, Some(&summary));
        wanted.attachment_errors = attach_errors;
        write(memory, api, v, wanted).await;
        return;
    }
    match place(ctx, placing, v) {
        Placement::Assign(a) => {
            if write_assignment(api, v, &a).await {
                placing.record(&a, v.spec.size_bytes);
                memory.remember(v, a);
            }
        }
        Placement::Wait(c) => {
            write(
                memory,
                api,
                v,
                Owned::wanted(v, VolumePhase::Pending, &[(types::READY, &c)], None),
            )
            .await;
        }
        Placement::Fail(c) => {
            write(
                memory,
                api,
                v,
                Owned::wanted(v, VolumePhase::Failed, &[(types::READY, &c)], None),
            )
            .await;
        }
    }
}

/// Converges the diskless entries of a DRBD volume to spec.attachments;
/// returns the assignment the rest of the pass works with and the
/// attached nodes left without a replica, sorted by node.
async fn converge(
    memory: &mut Memory,
    placing: &mut Placing,
    api: &Api<Volume>,
    v: &Volume,
    a: Assignment,
) -> (Assignment, Vec<AttachmentError>) {
    if v.spec.replicas < 2 || a.minor.is_none() {
        return (a, Vec::new());
    }
    let (next, mut failed) =
        converge_attachments(&a, &v.spec.attachments, &v.spec.evict, &placing.used);
    failed.sort_by(|x, y| x.node.cmp(&y.node));
    let Some(next) = next else {
        return (a, failed);
    };
    if write_assignment(api, v, &next).await {
        placing.used.add(&next);
        memory.remember(v, next.clone());
        (next, failed)
    } else {
        (a, failed)
    }
}

/// The AttachmentFailed condition. A change in the set of failed nodes
/// logs at warn and each new message is one Warning Event, so a stuck
/// attachment shows without a log line or Event per pass.
async fn attach_report(
    ctx: &Ctx<'_>,
    memory: &mut Memory,
    v: &Volume,
    failed: &[AttachmentError],
) -> Cond {
    let name = v.name_any();
    let uid = v.metadata.uid.clone().unwrap_or_default();
    let nodes: Vec<&str> = failed.iter().map(|e| e.node.as_str()).collect();
    let message = failed
        .iter()
        .map(|e| e.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    let before = memory.attach_failed.get(&uid);
    let nodes_before: Vec<&str> = before
        .map(|(n, _)| n.iter().map(String::as_str).collect())
        .unwrap_or_default();
    if nodes != nodes_before {
        if failed.is_empty() {
            info!(volume = name, "every attached node has a replica");
        } else {
            warn!(
                volume = name,
                ?nodes,
                message,
                "attached nodes without a replica"
            );
        }
    } else if !failed.is_empty() {
        debug!(
            volume = name,
            ?nodes,
            "attached nodes still without a replica"
        );
    }
    if failed.is_empty() {
        memory.attach_failed.remove(&uid);
        return Cond::new(FALSE, reason::ATTACHED, "");
    }
    if before.is_none_or(|(_, m)| *m != message) {
        crate::warning_event(ctx.client, v, reason::ATTACHMENT_FAILED, &message).await;
    }
    let nodes = nodes.iter().map(|n| (*n).to_owned()).collect();
    memory.attach_failed.insert(uid, (nodes, message.clone()));
    Cond::new(TRUE, reason::ATTACHMENT_FAILED, message)
}

/// A snapshot round runs from Suspending to Resuming. Pending is not one:
/// it waits for a Ready volume, which may need the very move it would block.
pub fn round_running(s: &Snapshot) -> bool {
    matches!(
        s.status.as_ref().map(|st| st.phase),
        Some(
            SnapshotPhase::Suspending
                | SnapshotPhase::Suspended
                | SnapshotPhase::Taking
                | SnapshotPhase::Taken
                | SnapshotPhase::Resuming
        )
    )
}

/// The first Snapshot of `volume` by name whose round runs.
fn snapshot_in_progress(stores: &Stores, volume: &str) -> Option<String> {
    Stores::sorted(&stores.snapshots)
        .iter()
        .filter(|s| s.spec.volume == volume)
        .find(|s| round_running(s))
        .map(|s| s.name_any())
}

/// A 422 with no details: the JSON patch did not apply. The apiserver
/// (1.33) answers a failed test op and a vanished index alike, both mean the
/// list moved since the store copy. A schema rejection names its kind and
/// causes and is a real error.
pub fn patch_not_applied(s: &kube::core::Status) -> bool {
    s.code == 422
        && s.details
            .as_ref()
            .is_none_or(|d| d.kind.is_empty() && d.causes.is_empty())
}

/// One spec.evict step (crate::evict). A block shows in the Degraded
/// message while Degraded is True and as a Warning Event once per reason.
async fn evict_step(
    ctx: &Ctx<'_>,
    memory: &mut Memory,
    placing: &mut Placing,
    api: &Api<Volume>,
    v: &Volume,
    a: &Assignment,
    summary: &mut Summary,
) {
    let uid = v.metadata.uid.clone().unwrap_or_default();
    let name = v.name_any();
    let entries = v
        .status
        .as_ref()
        .map(|s| s.replicas.as_slice())
        .unwrap_or_default();
    let initialized = v
        .status
        .as_ref()
        .is_some_and(|s| s.initialized_at.is_some())
        || memory.initialized.contains_key(&uid);
    let fresh = |n: &str| ctx.stores.agent_fresh(n);
    let snapshot = (!v.spec.evict.is_empty())
        .then(|| snapshot_in_progress(ctx.stores, &name))
        .flatten();
    let step = crate::evict::decide(&crate::evict::Input {
        name: &name,
        spec: &v.spec,
        assignment: a,
        entries,
        initialized,
        split_brain: summary.split_brain.status == TRUE,
        snapshot_in_progress: snapshot.as_deref(),
        fresh: &fresh,
        node_exists: &|n| ctx.stores.node(n).is_some(),
        views: &placing.views,
        ready_nodes: &placing.ready_nodes,
        used: &placing.used,
    });
    if !matches!(step, crate::evict::Step::Blocked { .. }) {
        memory.evict_blocked.remove(&uid);
    }
    match step {
        crate::evict::Step::Idle => {}
        crate::evict::Step::Wait { node } => {
            debug!(
                volume = name,
                node, "eviction waits for the diskless entry or attachment to go"
            );
        }
        crate::evict::Step::Clear { node, rest } => {
            let patch = json!({
                "metadata": { "resourceVersion": v.metadata.resource_version },
                "spec": { "evict": rest },
            });
            match api
                .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => info!(
                    volume = name,
                    node, "evicted node holds nothing; removed from spec.evict"
                ),
                Err(kube::Error::Api(s)) if s.is_conflict() || s.is_not_found() => {
                    debug!(
                        volume = name,
                        "clearing spec.evict raced; retrying next pass"
                    );
                }
                Err(e) => warn!(volume = name, error = %e, "clearing spec.evict failed"),
            }
        }
        crate::evict::Step::Blocked { node, message } => {
            if summary.degraded.status == TRUE {
                summary.degraded.message = if summary.degraded.message.is_empty() {
                    message.clone()
                } else {
                    format!("{}; {message}", summary.degraded.message)
                };
            }
            if memory.evict_blocked.get(&uid) != Some(&message) {
                warn!(volume = name, node, message, "eviction blocked");
                crate::warning_event(ctx.client, v, reason::EVICTION_BLOCKED, &message).await;
                memory.evict_blocked.insert(uid, message);
            }
        }
        crate::evict::Step::Move {
            node,
            next,
            diskful,
            event,
        } => {
            if write_assignment(api, v, &next).await {
                placing.used.add(&next);
                if let Some((n, p)) = &diskful {
                    placing.reserve(n, p, v.spec.size_bytes);
                }
                memory.remember(v, next);
                info!(volume = name, node, event, "evicted");
                crate::event(ctx.client, v, reason::EVICTED, &event).await;
            }
        }
    }
}

/// False must hold this long before the Normal Event: a link that flaps
/// with long Connected spells (20 s window, 10 s steady, 30 s pass)
/// would otherwise announce every cycle.
pub const PEER_CLEAR_GRACE: std::time::Duration = std::time::Duration::from_secs(300);

/// What was last announced for a Volume's PeerUnreachable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Announced {
    /// The reason of the last Warning; empty once cleared.
    pub reason: String,
    /// When False was first seen after that Warning.
    pub false_since: Option<std::time::Instant>,
}

/// The next announcement state and the Event due, if any: a Warning when
/// it turns True or its reason changes, a Normal one once False held
/// `grace`.
pub fn peer_transition(
    last: &Announced,
    now: &Cond,
    at: std::time::Instant,
    grace: std::time::Duration,
) -> (Announced, Option<(&'static str, String)>) {
    if now.status == TRUE {
        let next = Announced {
            reason: now.reason.to_owned(),
            false_since: None,
        };
        let event = (last.reason != now.reason).then(|| ("Warning", now.message.clone()));
        return (next, event);
    }
    if last.reason.is_empty() {
        return (Announced::default(), None);
    }
    let since = last.false_since.unwrap_or(at);
    if at.duration_since(since) >= grace {
        return (
            Announced::default(),
            Some(("Normal", "no stuck DRBD connection to report".to_owned())),
        );
    }
    (
        Announced {
            reason: last.reason.clone(),
            false_since: Some(since),
        },
        None,
    )
}

/// One Event per PeerUnreachable transition. Memory, seeded from the
/// stored condition after a restart, keeps a lagging store copy from
/// repeating it.
async fn peer_event(ctx: &Ctx<'_>, memory: &mut Memory, v: &Volume, now: &Cond) {
    let Some(uid) = v.metadata.uid.clone() else {
        return;
    };
    let last = memory
        .peer_unreachable
        .get(&uid)
        .cloned()
        .unwrap_or_else(|| Announced {
            reason: v
                .status
                .as_ref()
                .and_then(|s| condition::get(&s.conditions, types::PEER_UNREACHABLE))
                .filter(|c| c.status == TRUE)
                .map(|c| c.reason.clone())
                .unwrap_or_default(),
            false_since: None,
        });
    let (next, event) = peer_transition(
        &last,
        now,
        std::time::Instant::now(),
        ctx.cfg.peer_clear_grace,
    );
    match event {
        Some(("Warning", message)) => {
            warn!(volume = v.name_any(), message, "peer unreachable");
            crate::warning_event(ctx.client, v, reason::PEER_UNREACHABLE, &message).await;
        }
        Some((_, message)) => {
            info!(volume = v.name_any(), "peers reachable again");
            crate::event(ctx.client, v, reason::PEERS_REACHABLE, &message).await;
        }
        None => {}
    }
    memory.peer_unreachable.insert(uid, next);
}

/// Status entries no agent will remove (crate::evict::orphan_entries).
async fn remove_orphans(
    ctx: &Ctx<'_>,
    health: &Health,
    api: &Api<Volume>,
    v: &Volume,
    a: &Assignment,
) {
    let Some(status) = v.status.as_ref() else {
        return;
    };
    let fresh = |n: &str| ctx.stores.agent_fresh(n);
    let exists = |n: &str| health.exists.contains(n);
    let orphans = crate::evict::orphan_entries(a, &status.replicas, &fresh, &exists);
    if orphans.is_empty() {
        return;
    }
    let name = v.name_any();
    let patch = match serde_json::from_value(crate::evict::orphan_patch(&orphans)) {
        Ok(p) => Patch::Json::<()>(p),
        Err(e) => {
            warn!(volume = name, error = %e, "building the status entry removal failed");
            return;
        }
    };
    let nodes: Vec<&str> = orphans.iter().map(|(_, n)| n.as_str()).collect();
    match api
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
    {
        Ok(_) => {
            info!(volume = name, nodes = ?nodes, "removed status entries of nodes outside the assignment");
        }
        Err(kube::Error::Api(s)) if patch_not_applied(&s) || s.is_not_found() => {
            debug!(volume = name, error = %s, "status entry removal raced; retrying next pass");
        }
        Err(e) => {
            warn!(volume = name, error = %e, "removing status entries failed; retrying next pass");
        }
    }
}

/// spec.resolve: an unknown survivor only turns Ready False; otherwise the
/// agents do the work and the field is cleared once resynced.
async fn resolve_step(
    ctx: &Ctx<'_>,
    memory: &mut Memory,
    api: &Api<Volume>,
    v: &Volume,
    a: &Assignment,
    summary: &mut Summary,
    health: &Health,
) {
    let Some(survivor) = v.spec.resolve.as_ref().map(|r| r.survivor.as_str()) else {
        return;
    };
    let diskful: Vec<&str> = a
        .replicas
        .iter()
        .filter(|r| r.role == Role::Diskful)
        .map(|r| r.node.as_str())
        .collect();
    if !diskful.contains(&survivor) {
        summary.not_ready(Cond::new(
            FALSE,
            reason::INVALID_SURVIVOR,
            format!(
                "survivor {survivor} is not a diskful replica ({})",
                diskful.join(", ")
            ),
        ));
        return;
    }
    let generation = v.metadata.generation.unwrap_or(0);
    let reachable = |n: &str| !health.unreachable.contains(n);
    let split = summary.split_brain.status == TRUE;
    let uid = v.metadata.uid.clone().unwrap_or_default();
    if split {
        memory.resolving.insert(uid.clone());
    }
    let done = resolve_done(
        a,
        v.status.as_ref(),
        survivor,
        generation,
        summary,
        &reachable,
    );
    match resolve_action(split, memory.resolving.contains(&uid), done) {
        ResolveAction::Wait => return,
        ResolveAction::WithoutSplitBrain => {
            // Ready keeps its status: the volume itself is fine.
            summary.ready = Cond::new(
                summary.ready.status,
                reason::RESOLVE_WITHOUT_SPLIT_BRAIN,
                "spec.resolve is set but there is no split brain; agents ignore it; remove spec.resolve",
            );
            return;
        }
        ResolveAction::Clear => {
            memory.resolving.remove(&uid);
        }
    }
    let name = v.name_any();
    let patch = json!({
        "metadata": { "resourceVersion": v.metadata.resource_version },
        "spec": { "resolve": null },
    });
    match api
        .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
    {
        Ok(_) => {
            info!(volume = name, survivor, "split brain resolved");
            crate::event(
                ctx.client,
                v,
                reason::SPLIT_BRAIN_RESOLVED,
                &format!("resynced from {survivor}"),
            )
            .await;
        }
        Err(kube::Error::Api(s)) if s.is_conflict() || s.is_not_found() => {
            debug!(volume = name, "clearing resolve raced; retrying next pass");
        }
        Err(e) => warn!(volume = name, error = %e, "clearing spec.resolve failed"),
    }
}

/// A replica on a node whose Node object is gone counts as removed; one on
/// an unreachable node that still exists keeps blocking.
async fn delete(memory: &mut Memory, api: &Api<Volume>, v: &Volume, health: &Health) {
    if !has_finalizer(v, FINALIZER_VOLUME) {
        return;
    }
    let Some(a) = memory.assignment(v).cloned() else {
        set_finalizer(api, v, FINALIZER_VOLUME, false).await;
        return;
    };
    let generation = v.metadata.generation.unwrap_or(0);
    let gone: HashSet<&str> = v
        .status
        .iter()
        .flat_map(|s| &s.replicas)
        .filter(|e| !e.present && e.observed_generation.unwrap_or(0) >= generation)
        .map(|e| e.node.as_str())
        .collect();
    let pending: Vec<&str> = a
        .replicas
        .iter()
        .map(|r| r.node.as_str())
        .filter(|n| !gone.contains(n) && health.exists.contains(*n))
        .collect();
    if pending.is_empty() {
        info!(volume = v.name_any(), "every replica removed; releasing");
        set_finalizer(api, v, FINALIZER_VOLUME, false).await;
        return;
    }
    let mut summary = health.summarize(v, &a, &memory.latched(v), &memory.probes_seen(v));
    summary.ready = Cond::new(
        FALSE,
        reason::DELETING,
        format!("waiting for {} to remove the replica", pending.join(", ")),
    );
    let wanted = Owned::wanted(v, VolumePhase::Deleting, &summary.conds(), Some(&summary));
    write(memory, api, v, wanted).await;
}

/// Writes spec.assignment with server-side apply, guarded by the
/// resourceVersion `v` was read at. False when the object moved
/// or is gone: the next pass places it again from fresh state.
pub async fn write_assignment(api: &Api<Volume>, v: &Volume, a: &Assignment) -> bool {
    let name = v.name_any();
    let patch = json!({
        "apiVersion": "r-drbd.io/v1alpha1",
        "kind": "Volume",
        "metadata": { "name": name, "resourceVersion": v.metadata.resource_version },
        "spec": { "assignment": a },
    });
    let pp = PatchParams::apply(CONTROLLER_FIELD_MANAGER).force();
    match api.patch(&name, &pp, &Patch::Apply(&patch)).await {
        Ok(_) => {
            info!(volume = name, nodes = ?a.replicas.iter().map(|r| r.node.as_str()).collect::<Vec<_>>(), minor = ?a.minor, "assigned");
            true
        }
        Err(kube::Error::Api(s)) if s.is_conflict() || s.is_not_found() => {
            debug!(volume = name, "assignment raced; retrying next pass");
            false
        }
        Err(e) => {
            warn!(volume = name, error = %e, "writing the assignment failed");
            false
        }
    }
}

enum Placement {
    Assign(Assignment),
    Wait(Cond),
    Fail(Cond),
}

/// The (node, pool) holding a snapshot's LV: the first acked holder by
/// node name.
pub fn snapshot_holder(snap: &Snapshot) -> Option<(String, String)> {
    snapshot_holders(snap).into_iter().next()
}

/// Every (node, pool) holding the snapshot LV, by node name: acks that
/// succeeded and name a pool (set at Taking and carried by later acks).
pub fn snapshot_holders(snap: &Snapshot) -> Vec<(String, String)> {
    let mut holders: Vec<(String, String)> = snap
        .status
        .iter()
        .flat_map(|s| &s.acks)
        .filter(|a| a.ok)
        .filter_map(|a| a.pool.clone().map(|p| (a.node.clone(), p)))
        .collect();
    holders.sort();
    holders.dedup_by(|a, b| a.0 == b.0);
    holders
}

/// The restore's diskful (node, pool) pairs: the snapshot's holders, one
/// for a local volume, all of them for a replicated one.
fn restore_holders(
    v: &Volume,
    snap: Option<&Snapshot>,
    wanted: &str,
) -> Result<Vec<(String, String)>, Placement> {
    let Some(snap) = snap else {
        return Err(Placement::Wait(Cond::new(
            FALSE,
            reason::SNAPSHOT_NOT_READY,
            format!("snapshot {wanted} not found"),
        )));
    };
    let status = snap.status.clone().unwrap_or_default();
    match status.phase {
        SnapshotPhase::Ready => {}
        SnapshotPhase::Failed => {
            return Err(Placement::Fail(Cond::new(
                FALSE,
                reason::SNAPSHOT_FAILED,
                format!(
                    "snapshot {wanted} failed: {}",
                    status.last_error.unwrap_or_default()
                ),
            )));
        }
        p => {
            return Err(Placement::Wait(Cond::new(
                FALSE,
                reason::SNAPSHOT_NOT_READY,
                format!("snapshot {wanted} is {p:?}"),
            )));
        }
    }
    let mut holders = snapshot_holders(snap);
    if holders.is_empty() {
        return Err(Placement::Fail(Cond::new(
            FALSE,
            reason::SNAPSHOT_FAILED,
            format!("snapshot {wanted} has no node holding it"),
        )));
    }
    if v.spec.replicas == 1 {
        holders.truncate(1);
    } else if holders.len() != usize::try_from(v.spec.replicas).unwrap_or(usize::MAX) {
        return Err(Placement::Fail(Cond::new(
            FALSE,
            reason::UNSUPPORTED_RESTORE,
            format!(
                "snapshot holders {}, replicas {}",
                holders.len(),
                v.spec.replicas
            ),
        )));
    }
    let node_conflict = v
        .spec
        .node
        .as_ref()
        .is_some_and(|n| !holders.iter().any(|(h, _)| h == n));
    // A pool pin holds every replica; a node pin only one of them.
    let pool_conflict = v
        .spec
        .pool
        .as_ref()
        .is_some_and(|p| !holders.iter().all(|(_, h)| h == p));
    if node_conflict || pool_conflict {
        let at: Vec<String> = holders.iter().map(|(n, p)| format!("{n}/{p}")).collect();
        return Err(Placement::Fail(Cond::new(
            FALSE,
            reason::RESTORE_NODE_CONFLICT,
            format!(
                "snapshot {wanted} lives on {}, pinned to {}/{}",
                at.join(", "),
                v.spec.node.as_deref().unwrap_or("any"),
                v.spec.pool.as_deref().unwrap_or("any")
            ),
        )));
    }
    Ok(holders)
}

/// Assignment for chosen diskful replicas, with a tiebreaker when their
/// count is even; recomputed with the Node list because a node without
/// pools (such as a control plane node) is the preferred tiebreaker.
fn assign(placing: &Placing, v: &Volume, diskful: &[placer::Chosen]) -> Placement {
    let nodes: Vec<String> = diskful.iter().map(|c| c.node.clone()).collect();
    let tiebreaker = if nodes.len() >= 2 && nodes.len().is_multiple_of(2) {
        let t = placer::tiebreaker(&nodes, &placing.ready_nodes, &placing.views, &v.spec.tier);
        if t.is_none() {
            warn!(volume = v.name_any(), "no node for the quorum tiebreaker");
        }
        t
    } else {
        None
    };
    let entropy = match Entropy::random() {
        Ok(e) => e,
        Err(e) => {
            return Placement::Wait(Cond::new(
                FALSE,
                reason::ALLOCATION_FAILED,
                format!("random source: {e}"),
            ));
        }
    };
    match alloc::allocate(diskful, tiebreaker.as_deref(), &placing.used, entropy) {
        Ok(a) => Placement::Assign(a),
        Err(e) => Placement::Fail(Cond::new(FALSE, reason::ALLOCATION_FAILED, e.to_string())),
    }
}

/// The volume's LV name in each chosen Pool's VG, as LVM would take it
/// (device-mapper length with the VG): a name that does not fit fails
/// here, not on every agent pass.
fn lv_name_fits(ctx: &Ctx<'_>, v: &Volume, diskful: &[placer::Chosen]) -> Option<Placement> {
    let lv = lvm::names::volume_lv(&v.name_any());
    diskful.iter().find_map(|c| {
        let pool = ctx.stores.pool(&c.pool)?;
        let e = lvm::names::check_new_lv(&pool.spec.vg, &lv).err()?;
        Some(Placement::Fail(Cond::new(
            FALSE,
            reason::INVALID_NAME,
            format!("pool {}: {e}", c.pool),
        )))
    })
}

fn place(ctx: &Ctx<'_>, placing: &Placing, v: &Volume) -> Placement {
    let (mut node, mut pool) = (v.spec.node.clone(), v.spec.pool.clone());
    let mut holds_data = false;
    if let Some(src) = &v.spec.source {
        let holders = match restore_holders(
            v,
            ctx.stores.snapshot(&src.snapshot).as_deref(),
            &src.snapshot,
        ) {
            Ok(h) => h,
            Err(p) => return p,
        };
        // Holders are never gated by capacity: each one already holds the
        // restore's blocks and has nowhere else to go.
        if v.spec.replicas >= 2 {
            // Every replica restores from its own copy of the snapshot.
            let diskful: Vec<placer::Chosen> = holders
                .into_iter()
                .map(|(node, pool)| placer::Chosen { node, pool })
                .collect();
            return lv_name_fits(ctx, v, &diskful).unwrap_or_else(|| assign(placing, v, &diskful));
        }
        if let Some((n, p)) = holders.into_iter().next() {
            node = Some(n);
            pool = Some(p);
            holds_data = true;
        }
    }
    let req = placer::Request {
        size_bytes: v.spec.size_bytes,
        replicas: v.spec.replicas,
        tier: v.spec.tier.clone(),
        pool,
        node,
        existing: Vec::new(),
        holds_data,
    };
    match placer::place(&req, &placing.views) {
        placer::Outcome::Placed { diskful, .. } => {
            lv_name_fits(ctx, v, &diskful).unwrap_or_else(|| assign(placing, v, &diskful))
        }
        placer::Outcome::AlreadyPlaced => Placement::Wait(Cond::new(FALSE, reason::PLACING, "")),
        // Pending, not Failed: placement is retried whenever Pools or Nodes change.
        placer::Outcome::Insufficient { missing, rejected } => {
            let list = if rejected.is_empty() {
                "none (no pools)".to_owned()
            } else {
                rejected
                    .iter()
                    .map(|r| format!("{}/{} {:?}", r.node, r.pool, r.reason))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            Placement::Wait(Cond::new(
                FALSE,
                reason::INSUFFICIENT,
                format!(
                    "{missing} more replica(s) of tier {} and {} bytes needed; rejected: {list}",
                    v.spec.tier, v.spec.size_bytes
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{AssignedReplica, ReplicaStatus};

    fn assignment(nodes: &[(&str, Role)]) -> Assignment {
        Assignment {
            replicas: nodes
                .iter()
                .enumerate()
                .map(|(i, (n, role))| AssignedReplica {
                    node: (*n).into(),
                    pool: (*role == Role::Diskful).then(|| format!("p-{n}")),
                    node_id: u8::try_from(i).unwrap(),
                    port: None,
                    role: *role,
                })
                .collect(),
            minor: None,
            secret: None,
            initial_up_to_date_node: None,
        }
    }

    fn entry(node: &str, present: bool, disk: &str, size: u64, in_use: bool) -> ReplicaStatus {
        ReplicaStatus {
            node: node.into(),
            present,
            disk_state: Some(disk.into()),
            in_use,
            size_bytes: Some(size),
            device_path: Some("/dev/x".into()),
            peers: ["n1", "n2"]
                .iter()
                .filter(|p| **p != node)
                .map(|p| ((*p).to_owned(), "Connected".to_owned()))
                .collect(),
            ..ReplicaStatus::default()
        }
    }

    fn status(phase: VolumePhase, replicas: Vec<ReplicaStatus>) -> VolumeStatus {
        VolumeStatus {
            phase,
            replicas,
            ..VolumeStatus::default()
        }
    }

    const G: u64 = 1 << 30;

    #[test]
    #[allow(clippy::too_many_lines, clippy::type_complexity)]
    fn aggregation_table() {
        let two = assignment(&[
            ("n1", Role::Diskful),
            ("n2", Role::Diskful),
            ("n3", Role::Tiebreaker),
        ]);
        let one = assignment(&[("n1", Role::Diskful)]);
        let ok1 = entry("n1", true, "UpToDate", G, true);
        let ok2 = entry("n2", true, "UpToDate", G, false);
        let tb = entry("n3", true, "Diskless", 0, false);
        // (case, assignment, size, status, phase, ready reason, degraded status)
        let cases: Vec<(
            &str,
            &Assignment,
            u64,
            Option<VolumeStatus>,
            VolumePhase,
            &str,
            &str,
        )> = vec![
            (
                "no status",
                &one,
                G,
                None,
                VolumePhase::Provisioning,
                reason::WAITING,
                FALSE,
            ),
            (
                "local ready",
                &one,
                G,
                Some(status(VolumePhase::Provisioning, vec![ok1.clone()])),
                VolumePhase::Ready,
                reason::ALL_UP_TO_DATE,
                FALSE,
            ),
            (
                "disk state case-insensitive",
                &one,
                G,
                Some(status(
                    VolumePhase::Provisioning,
                    vec![entry("n1", true, "uptodate", G, false)],
                )),
                VolumePhase::Ready,
                reason::ALL_UP_TO_DATE,
                FALSE,
            ),
            (
                "tiebreaker missing while provisioning",
                &two,
                G,
                Some(status(
                    VolumePhase::Provisioning,
                    vec![ok1.clone(), ok2.clone()],
                )),
                VolumePhase::Provisioning,
                reason::WAITING,
                FALSE,
            ),
            (
                "replicated ready, tiebreaker diskless",
                &two,
                G,
                Some(status(
                    VolumePhase::Provisioning,
                    vec![ok1.clone(), ok2.clone(), tb.clone()],
                )),
                VolumePhase::Ready,
                reason::ALL_UP_TO_DATE,
                FALSE,
            ),
            (
                "inconsistent while provisioning",
                &two,
                G,
                Some(status(
                    VolumePhase::Provisioning,
                    vec![
                        ok1.clone(),
                        entry("n2", true, "Inconsistent", G, false),
                        tb.clone(),
                    ],
                )),
                VolumePhase::Provisioning,
                reason::WAITING,
                FALSE,
            ),
            (
                "replica gone after ready",
                &two,
                G,
                Some(status(VolumePhase::Ready, vec![ok1.clone(), tb.clone()])),
                VolumePhase::Degraded,
                reason::REPLICA_MISSING,
                TRUE,
            ),
            (
                "present false after ready",
                &one,
                G,
                Some(status(
                    VolumePhase::Ready,
                    vec![entry("n1", false, "UpToDate", G, false)],
                )),
                VolumePhase::Degraded,
                reason::REPLICA_MISSING,
                TRUE,
            ),
            (
                "outdated after ready",
                &two,
                G,
                Some(status(
                    VolumePhase::Degraded,
                    vec![
                        ok1.clone(),
                        entry("n2", true, "Outdated", G, false),
                        tb.clone(),
                    ],
                )),
                VolumePhase::Degraded,
                reason::REPLICA_NOT_UP_TO_DATE,
                TRUE,
            ),
            (
                "back from degraded",
                &one,
                G,
                Some(status(VolumePhase::Degraded, vec![ok1.clone()])),
                VolumePhase::Ready,
                reason::ALL_UP_TO_DATE,
                FALSE,
            ),
            (
                "resizing",
                &one,
                2 * G,
                Some(status(VolumePhase::Ready, vec![ok1.clone()])),
                VolumePhase::Ready,
                reason::RESIZING,
                FALSE,
            ),
            (
                "resize half done",
                &two,
                2 * G,
                Some(status(
                    VolumePhase::Ready,
                    vec![
                        entry("n1", true, "UpToDate", 2 * G, true),
                        ok2.clone(),
                        tb.clone(),
                    ],
                )),
                VolumePhase::Ready,
                reason::RESIZING,
                FALSE,
            ),
        ];
        for (case, a, size, st, phase, ready, degraded) in cases {
            let s = aggregate(size, a, st.as_ref(), &|_| true);
            assert_eq!(s.phase, phase, "{case}");
            assert_eq!(s.ready.reason, ready, "{case}");
            assert_eq!(s.degraded.status, degraded, "{case}");
        }
    }

    #[test]
    fn summary_fields() {
        let two = assignment(&[
            ("n1", Role::Diskful),
            ("n2", Role::Diskful),
            ("n3", Role::Tiebreaker),
        ]);
        let st = status(
            VolumePhase::Ready,
            vec![
                entry("n1", true, "UpToDate", 2 * G, false),
                entry("n2", true, "UpToDate", G, true),
                entry("n3", true, "Diskless", 0, false),
            ],
        );
        let s = aggregate(G, &two, Some(&st), &|_| true);
        assert_eq!(s.usable_bytes, Some(G));
        assert_eq!(s.nodes.as_deref(), Some("n1,n2"));
        assert_eq!(s.primary.as_deref(), Some("n2"));
        assert_eq!(s.ready.status, TRUE);
        let s = aggregate(
            G,
            &two,
            Some(&status(
                VolumePhase::Ready,
                vec![entry("n1", true, "UpToDate", G, false)],
            )),
            &|_| true,
        );
        assert_eq!(s.ready.message, "missing on n2, n3");
        assert_eq!(s.primary, None);
    }

    #[test]
    fn replicated_needs_connections() {
        let two = assignment(&[
            ("n1", Role::Diskful),
            ("n2", Role::Diskful),
            ("n3", Role::Tiebreaker),
        ]);
        let mut n2 = entry("n2", true, "UpToDate", G, false);
        n2.peers.insert("n1".into(), "Connecting".into());
        let st = status(
            VolumePhase::Ready,
            vec![
                entry("n1", true, "UpToDate", G, false),
                n2,
                entry("n3", true, "Diskless", 0, false),
            ],
        );
        let s = aggregate(G, &two, Some(&st), &|_| true);
        assert_eq!(s.phase, VolumePhase::Degraded);
        assert_eq!(s.ready.reason, reason::REPLICA_DISCONNECTED);
        assert_eq!(s.ready.message, "not connected: n2 to n1");
        // A local volume has no peers to check.
        let one = assignment(&[("n1", Role::Diskful)]);
        let mut lone = entry("n1", true, "UpToDate", G, false);
        lone.peers.clear();
        let s = aggregate(
            G,
            &one,
            Some(&status(VolumePhase::Provisioning, vec![lone])),
            &|_| true,
        );
        assert_eq!(s.phase, VolumePhase::Ready);
    }

    #[test]
    fn node_not_ready_and_errors() {
        let one = assignment(&[("n1", Role::Diskful)]);
        let mut e = entry("n1", true, "UpToDate", G, false);
        e.last_error = Some("lvcreate failed".into());
        let st = status(VolumePhase::Ready, vec![e]);
        let s = aggregate(G, &one, Some(&st), &|_| true);
        assert_eq!(s.phase, VolumePhase::Ready);
        assert_eq!(s.ready.message, "errors: n1: lvcreate failed");
        let s = aggregate(G, &one, Some(&st), &|n| n != "n1");
        assert_eq!(s.phase, VolumePhase::Degraded);
        assert_eq!(s.ready.reason, reason::REPLICA_MISSING);
        assert_eq!(
            s.ready.message,
            "node not ready: n1; errors: n1: lvcreate failed"
        );
        assert_eq!(s.degraded.message, "node not ready: n1");
    }

    fn two_with_tb() -> Assignment {
        assignment(&[
            ("n1", Role::Diskful),
            ("n2", Role::Diskful),
            ("n3", Role::Tiebreaker),
        ])
    }

    fn healthy() -> Vec<ReplicaStatus> {
        vec![
            entry("n1", true, "UpToDate", G, true),
            entry("n2", true, "UpToDate", G, false),
            entry("n3", true, "Diskless", 0, false),
        ]
    }

    fn probe(peer: &str, result: model::ProbeResult) -> model::PeerProbe {
        let t = Time(k8s_openapi::jiff::Timestamp::UNIX_EPOCH);
        model::PeerProbe {
            node: peer.into(),
            address: format!("10.0.0.{}", &peer[1..]),
            port: 11003,
            since: t.clone(),
            result,
            probed_at: t,
            error: None,
            peer_run: None,
        }
    }

    fn peer_cond(r: Vec<ReplicaStatus>, nodes: &Nodes<'_>, latched: &BTreeSet<Pair>) -> Cond {
        aggregate_nodes(
            G,
            &two_with_tb(),
            Some(&status(VolumePhase::Provisioning, r)),
            nodes,
            latched,
            None,
        )
        .peer_unreachable
    }

    #[test]
    fn peer_unreachable_rule() {
        use model::ProbeResult as R;
        let all = Nodes {
            ready: &|_| true,
            reachable: &|_| true,
            evicting: &[],
            alive_across: &|_, _| true,
        };
        let none = BTreeSet::new();
        let c = peer_cond(healthy(), &all, &none);
        assert_eq!((c.status, c.reason), (FALSE, reason::PEERS_REACHABLE));

        let mut r = healthy();
        r[0].peers.insert("n2".into(), "Connecting".into());
        r[0].unreachable_peers = vec![probe("n2", R::Filtered)];
        let c = peer_cond(r.clone(), &all, &none);
        assert_eq!((c.status, c.reason), (TRUE, reason::FIREWALL));
        assert_eq!(
            c.message,
            "rejected by a firewall between n1, n2 (TCP 11003); open TCP 11000-11999 between nodes"
        );

        // Both directions and the tiebreaker: one clause per result, the
        // worst names the reason.
        r[1].unreachable_peers = vec![probe("n1", R::Filtered)];
        r[2].unreachable_peers = vec![probe("n1", R::Refused)];
        let c = peer_cond(r.clone(), &all, &none);
        assert_eq!(c.reason, reason::FIREWALL);
        assert_eq!(
            c.message,
            "rejected by a firewall between n1, n2 (TCP 11003); connection refused (nothing listens, or a firewall resets) between n1, n3 (TCP 11003); open TCP 11000-11999 between nodes"
        );
        r[0].unreachable_peers = vec![probe("n2", R::TimedOut)];
        r[1].unreachable_peers.clear();
        assert_eq!(peer_cond(r.clone(), &all, &none).reason, reason::TIMEOUT);
        r[0].unreachable_peers.clear();
        let c = peer_cond(r.clone(), &all, &none);
        assert_eq!(c.reason, reason::NOT_LISTENING);
        assert!(c.message.ends_with("open TCP 11000-11999 between nodes"));
        r[2].unreachable_peers = vec![probe("n1", R::Accepted)];
        let c = peer_cond(r.clone(), &all, &none);
        assert_eq!(c.reason, reason::UNKNOWN);
        assert!(!c.message.contains("open TCP"), "{}", c.message);

        let mut r = healthy();
        r[0].peers.insert("n2".into(), "Connecting".into());
        r[0].unreachable_peers = vec![probe("n2", R::TimedOut)];
        assert_eq!(peer_cond(r.clone(), &all, &none).status, TRUE);
        // A stale Lease on either end: NodeUnreachable's business.
        for down in ["n1", "n2"] {
            let nodes = Nodes {
                ready: &|_| true,
                reachable: &|n| n != down,
                evicting: &[],
                alive_across: &|_, _| true,
            };
            assert_eq!(peer_cond(r.clone(), &nodes, &none).status, FALSE, "{down}");
        }
        // An end in spec.evict.
        for gone in ["n1", "n2"] {
            let evicting = [gone.to_owned()];
            let nodes = Nodes {
                ready: &|_| true,
                reachable: &|_| true,
                evicting: &evicting,
                alive_across: &|_, _| true,
            };
            assert_eq!(peer_cond(r.clone(), &nodes, &none).status, FALSE, "{gone}");
        }
        // The peer's resource not up yet.
        let mut q = r.clone();
        q[1].present = false;
        assert_eq!(peer_cond(q, &all, &none).status, FALSE);
        // A split pair, latched or StandAlone on either side.
        let split = BTreeSet::from([pair("n1", "n2")]);
        assert_eq!(peer_cond(r.clone(), &all, &split).status, FALSE);
        for side in [0, 1] {
            let mut q = r.clone();
            let peer = if side == 0 { "n2" } else { "n1" };
            q[side].peers.insert(peer.into(), "StandAlone".into());
            assert_eq!(peer_cond(q, &all, &none).status, FALSE, "{side}");
        }
        // A peer agent not alive across the window and the probe.
        let nodes = Nodes {
            ready: &|_| true,
            reachable: &|_| true,
            evicting: &[],
            alive_across: &|_, p| p.node != "n2",
        };
        assert_eq!(peer_cond(r, &nodes, &none).status, FALSE);
    }

    #[test]
    fn peer_message_is_bounded() {
        let probes: Vec<(String, model::PeerProbe)> = (0..20)
            .map(|i| {
                let mut p = probe("n2", model::ProbeResult::Failed);
                p.error = Some(format!("error number {i} with a long text"));
                (format!("node-{i:02}"), p)
            })
            .collect();
        let found: Vec<(&str, &model::PeerProbe)> =
            probes.iter().map(|(a, p)| (a.as_str(), p)).collect();
        let c = peer_message(&found);
        assert!(c.message.len() <= PEER_MESSAGE_MAX, "{}", c.message.len());
        assert!(c.message.contains(" more"), "{}", c.message);
    }

    fn ts(s: i64) -> k8s_openapi::jiff::Timestamp {
        k8s_openapi::jiff::Timestamp::from_second(s).unwrap()
    }

    #[test]
    fn probe_counts_on_run_identity_and_local_renewals() {
        let t0 = std::time::Instant::now();
        let later = t0 + std::time::Duration::from_secs(10);
        let run = Time(ts(1000));
        let lease = |r: i64, renewed| AgentLease {
            run: Some(ts(r)),
            renewed_seen: Some(renewed),
        };
        // Same run, renewed after the record was first seen.
        assert!(probe_counts(
            Some(&lease(1000, later)),
            Some(&run),
            Some(t0)
        ));
        // No renewal since: rebooted or crashed.
        assert!(!probe_counts(
            Some(&lease(1000, t0)),
            Some(&run),
            Some(later)
        ));
        // Back with a new run.
        assert!(!probe_counts(
            Some(&lease(1090, later)),
            Some(&run),
            Some(t0)
        ));
        // Peer clock far ahead or behind the prober's: only identity counts.
        let ahead = Time(ts(4_000_000_000));
        let l = AgentLease {
            run: Some(ahead.0),
            renewed_seen: Some(later),
        };
        assert!(probe_counts(Some(&l), Some(&ahead), Some(t0)));
        let behind = Time(ts(1));
        let l = AgentLease {
            run: Some(behind.0),
            renewed_seen: Some(later),
        };
        assert!(probe_counts(Some(&l), Some(&behind), Some(t0)));
        // No run recorded, no Lease, never seen.
        assert!(!probe_counts(Some(&lease(1000, later)), None, Some(t0)));
        assert!(!probe_counts(None, Some(&run), Some(t0)));
        assert!(!probe_counts(Some(&lease(1000, later)), Some(&run), None));
    }

    #[test]
    fn a_reprobe_of_the_same_run_keeps_counting() {
        let mut v = Volume::new(
            "v",
            model::VolumeSpec {
                size_bytes: G,
                replicas: 2,
                tier: "t".into(),
                pool: None,
                node: None,
                access_mode: model::AccessMode::default(),
                attachments: vec![],
                evict: vec![],
                source: None,
                resolve: None,
                assignment: Some(two_with_tb()),
            },
        );
        let run = Time(ts(1000));
        let mut p = probe("n2", model::ProbeResult::Filtered);
        p.peer_run = Some(run.clone());
        let mut e = ReplicaStatus {
            node: "n1".into(),
            ..ReplicaStatus::default()
        };
        e.unreachable_peers = vec![p];
        v.status = Some(status(VolumePhase::Provisioning, vec![e]));
        let t0 = std::time::Instant::now();
        let mut seen = ProbesSeen::new();
        note_probes(&mut seen, &v, t0);
        let lease = AgentLease {
            run: Some(run.0),
            renewed_seen: Some(t0 + std::time::Duration::from_secs(5)),
        };
        let counts = |seen: &ProbesSeen, v: &Volume| {
            let p = &v.status.as_ref().unwrap().replicas[0].unreachable_peers[0];
            probe_counts(
                Some(&lease),
                p.peer_run.as_ref(),
                seen.get(&probe_key("n1", p)).copied(),
            )
        };
        assert!(counts(&seen, &v));
        // Five minutes later the agent probes again: a new probedAt.
        v.status.as_mut().unwrap().replicas[0].unreachable_peers[0].probed_at = Time(ts(1300));
        note_probes(&mut seen, &v, t0 + std::time::Duration::from_secs(300));
        assert!(counts(&seen, &v), "no False gap after a re-probe");
    }

    #[test]
    fn the_peer_timer_disarms() {
        let t0 = std::time::Instant::now();
        let run = Time(ts(1000));
        let waiting = AgentLease {
            run: Some(run.0),
            renewed_seen: Some(t0),
        };
        let renewed = AgentLease {
            run: Some(run.0),
            renewed_seen: Some(t0 + std::time::Duration::from_secs(10)),
        };
        assert!(awaiting_renewal(Some(&waiting), Some(&run), Some(t0)));
        assert!(
            !awaiting_renewal(Some(&renewed), Some(&run), Some(t0)),
            "counted"
        );
        assert!(!awaiting_renewal(Some(&waiting), None, Some(t0)), "no run");
        let other = Time(ts(2000));
        assert!(
            !awaiting_renewal(Some(&waiting), Some(&other), Some(t0)),
            "old run"
        );

        let mut v = Volume::new(
            "v",
            model::VolumeSpec {
                size_bytes: G,
                replicas: 2,
                tier: "t".into(),
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
        v.metadata.uid = Some("u".into());
        let mut m = crate::Memory::default();
        assert_eq!(m.peer_timer(), None);
        m.set_probe_wait(&v, true);
        assert!(m.peer_timer().is_some());
        m.set_probe_wait(&v, false);
        assert_eq!(m.peer_timer(), None);
        m.peer_unreachable.insert(
            "u".into(),
            Announced {
                reason: reason::FIREWALL.into(),
                false_since: Some(t0),
            },
        );
        assert!(m.peer_timer().is_some(), "grace pending");
        m.peer_unreachable.insert("u".into(), Announced::default());
        assert_eq!(m.peer_timer(), None);
    }

    #[test]
    fn first_sight_of_probe_records() {
        let mut v = Volume::new(
            "v",
            model::VolumeSpec {
                size_bytes: G,
                replicas: 2,
                tier: "t".into(),
                pool: None,
                node: None,
                access_mode: model::AccessMode::default(),
                attachments: vec![],
                evict: vec![],
                source: None,
                resolve: None,
                assignment: Some(two_with_tb()),
            },
        );
        let mut e = ReplicaStatus {
            node: "n1".into(),
            ..ReplicaStatus::default()
        };
        e.unreachable_peers = vec![probe("n2", model::ProbeResult::Filtered)];
        v.status = Some(status(VolumePhase::Provisioning, vec![e]));
        let mut seen = ProbesSeen::new();
        let t0 = std::time::Instant::now();
        note_probes(&mut seen, &v, t0);
        note_probes(&mut seen, &v, t0 + std::time::Duration::from_secs(5));
        assert_eq!(seen.values().copied().collect::<Vec<_>>(), vec![t0]);
        v.status.as_mut().unwrap().replicas[0]
            .unreachable_peers
            .clear();
        note_probes(&mut seen, &v, t0);
        assert!(seen.is_empty());
    }

    #[test]
    fn peer_events_once_per_transition() {
        let t0 = std::time::Instant::now();
        let t = Cond::new(TRUE, reason::FIREWALL, "m");
        let f = Cond::new(FALSE, reason::PEERS_REACHABLE, "");
        let quiet = Announced::default();
        assert_eq!(
            peer_transition(&quiet, &f, t0, PEER_CLEAR_GRACE),
            (quiet.clone(), None)
        );
        let (fw, ev) = peer_transition(&quiet, &t, t0, PEER_CLEAR_GRACE);
        assert_eq!(ev, Some(("Warning", "m".to_owned())));
        assert_eq!(peer_transition(&fw, &t, t0, PEER_CLEAR_GRACE).1, None);
        let timeout = Cond::new(TRUE, reason::TIMEOUT, "n");
        assert_eq!(
            peer_transition(&fw, &timeout, t0, PEER_CLEAR_GRACE).1,
            Some(("Warning", "n".to_owned()))
        );
        // False starts the grace; True again inside it says nothing.
        let (waiting, ev) = peer_transition(&fw, &f, t0, PEER_CLEAR_GRACE);
        assert_eq!(ev, None);
        assert_eq!(waiting.false_since, Some(t0));
        let later = t0 + PEER_CLEAR_GRACE / 2;
        let (again, ev) = peer_transition(&waiting, &t, later, PEER_CLEAR_GRACE);
        assert_eq!((again.clone(), ev), (fw.clone(), None));
        // False held through the grace: one Normal Event.
        let (waiting, _) = peer_transition(&again, &f, later, PEER_CLEAR_GRACE);
        assert_eq!(
            peer_transition(&waiting, &f, later + PEER_CLEAR_GRACE / 2, PEER_CLEAR_GRACE).1,
            None
        );
        let (done, ev) = peer_transition(&waiting, &f, later + PEER_CLEAR_GRACE, PEER_CLEAR_GRACE);
        assert_eq!(ev.map(|(k, _)| k), Some("Normal"));
        assert_eq!(done, quiet);
    }

    #[test]
    fn quorum_and_split_brain() {
        let two = two_with_tb();
        let s = aggregate(
            G,
            &two,
            Some(&status(VolumePhase::Ready, healthy())),
            &|_| true,
        );
        assert_eq!(s.quorum_lost.status, FALSE);
        assert_eq!(s.split_brain.status, FALSE);

        let mut r = healthy();
        r[1].quorum = Some(false);
        r[0].quorum = Some(true);
        // A tiebreaker without quorum is not a diskful replica.
        r[2].quorum = Some(false);
        let s = aggregate(G, &two, Some(&status(VolumePhase::Ready, r)), &|_| true);
        assert_eq!(s.quorum_lost.status, TRUE);
        assert_eq!(s.quorum_lost.reason, reason::NO_QUORUM);
        assert_eq!(s.quorum_lost.message, "no quorum on n2");

        // One side StandAlone is a disconnect, not a split brain.
        let mut r = healthy();
        r[0].peers.insert("n2".into(), "StandAlone".into());
        let s = aggregate(
            G,
            &two,
            Some(&status(VolumePhase::Ready, r.clone())),
            &|_| true,
        );
        assert_eq!(s.split_brain.status, FALSE);
        assert_eq!(s.phase, VolumePhase::Degraded);
        r[1].peers.insert("n1".into(), "standalone".into());
        let s = aggregate(
            G,
            &two,
            Some(&status(VolumePhase::Ready, r.clone())),
            &|_| true,
        );
        assert_eq!(s.split_brain.status, TRUE);
        assert_eq!(s.split_brain.reason, reason::SPLIT_BRAIN);
        assert!(
            s.split_brain.message.contains("n1 and n2"),
            "{}",
            s.split_brain.message
        );
        assert_eq!(s.ready.status, FALSE);
        // An unreachable side reports nothing current.
        let s = aggregate_nodes(
            G,
            &two,
            Some(&status(VolumePhase::Ready, r)),
            &Nodes {
                ready: &|_| true,
                reachable: &|n| n != "n2",
                evicting: &[],
                alive_across: &|_, _| true,
            },
            &BTreeSet::new(),
            None,
        );
        assert_eq!(s.split_brain.status, FALSE);
    }

    #[test]
    fn unreachable_nodes() {
        let two = two_with_tb();
        let nodes = Nodes {
            ready: &|_| true,
            reachable: &|n| n != "n2",
            evicting: &[],
            alive_across: &|_, _| true,
        };
        let s = aggregate_nodes(
            G,
            &two,
            Some(&status(VolumePhase::Ready, healthy())),
            &nodes,
            &BTreeSet::new(),
            None,
        );
        assert_eq!(s.phase, VolumePhase::Degraded);
        assert_eq!(s.ready.status, FALSE);
        assert_eq!(s.ready.reason, reason::NODE_UNREACHABLE);
        assert_eq!(s.degraded.status, TRUE);
        assert_eq!(s.degraded.reason, reason::NODE_UNREACHABLE);
        assert_eq!(s.degraded.message, "unknown (agent unreachable): n2");
        // n2's stale entry no longer feeds primary or usableBytes.
        let mut r = healthy();
        r[0].in_use = false;
        r[1].in_use = true;
        r[1].size_bytes = Some(G / 2);
        let s = aggregate_nodes(
            G,
            &two,
            Some(&status(VolumePhase::Ready, r.clone())),
            &nodes,
            &BTreeSet::new(),
            None,
        );
        assert_eq!(s.primary, None);
        assert_eq!(s.usable_bytes, Some(G));
        r[0].in_use = true;
        let s = aggregate_nodes(
            G,
            &two,
            Some(&status(VolumePhase::Ready, r)),
            &nodes,
            &BTreeSet::new(),
            None,
        );
        assert_eq!(s.primary.as_deref(), Some("n1"));
        // Never Ready yet: Provisioning, but Degraded already says why.
        let s = aggregate_nodes(
            G,
            &two,
            Some(&status(VolumePhase::Provisioning, healthy())),
            &nodes,
            &BTreeSet::new(),
            None,
        );
        assert_eq!(s.phase, VolumePhase::Provisioning);
        assert_eq!(s.degraded.status, TRUE);
        // The only diskful replica unreachable.
        let one = assignment(&[("n2", Role::Diskful)]);
        let s = aggregate_nodes(
            G,
            &one,
            Some(&status(
                VolumePhase::Ready,
                vec![entry("n2", true, "UpToDate", G, true)],
            )),
            &nodes,
            &BTreeSet::new(),
            None,
        );
        assert_eq!(s.ready.status, FALSE);
        assert_eq!(s.ready.reason, reason::NODE_UNREACHABLE);
    }

    #[test]
    fn diskless_attachments_do_not_gate_ready() {
        let mut two = two_with_tb();
        two.replicas.push(AssignedReplica {
            node: "n4".into(),
            pool: None,
            node_id: 3,
            port: None,
            role: Role::Diskless,
        });
        let s = aggregate(
            G,
            &two,
            Some(&status(VolumePhase::Ready, healthy())),
            &|n| n != "n4",
        );
        assert_eq!(s.phase, VolumePhase::Ready);
    }

    #[test]
    fn resolve_lifecycle() {
        let two = two_with_tb();
        let reach = |_: &str| true;
        let split = |r: &mut Vec<ReplicaStatus>| {
            r[0].peers.insert("n2".into(), "StandAlone".into());
            r[1].peers.insert("n1".into(), "StandAlone".into());
        };
        let check = |r: Vec<ReplicaStatus>, generation: i64| {
            let st = status(VolumePhase::Degraded, r);
            let s = aggregate(G, &two, Some(&st), &|_| true);
            resolve_done(&two, Some(&st), "n1", generation, &s, &reach)
        };
        let with_gen = |mut r: Vec<ReplicaStatus>, g: i64| {
            for e in &mut r {
                e.observed_generation = Some(g);
            }
            r
        };
        let mut r = with_gen(healthy(), 2);
        split(&mut r);
        assert!(!check(r, 2), "still split");
        // Agents have not seen the resolve generation yet.
        assert!(!check(with_gen(healthy(), 1), 2));
        // n2 discarded its data and resyncs from n1.
        let mut r = with_gen(healthy(), 2);
        r[1].disk_state = Some("Inconsistent".into());
        assert!(!check(r.clone(), 2), "no sync progress reported");
        r[1].sync_percent = Some(12);
        assert!(check(r.clone(), 2));
        r[1].peers.insert("n1".into(), "Connecting".into());
        assert!(!check(r, 2), "not connected to the survivor");
        assert!(check(with_gen(healthy(), 2), 2));
        // The survivor itself must be UpToDate.
        let mut r = with_gen(healthy(), 2);
        r[0].disk_state = Some("Outdated".into());
        assert!(!check(r, 2));
    }

    #[test]
    fn attachments_converge() {
        let mut two = two_with_tb();
        for (i, r) in two.replicas.iter_mut().enumerate() {
            r.port = Some(11000 + u16::try_from(i).unwrap());
        }
        let other = Assignment {
            replicas: vec![AssignedReplica {
                node: "n4".into(),
                pool: Some("p".into()),
                node_id: 0,
                port: Some(11000),
                role: Role::Diskful,
            }],
            minor: Some(50001),
            secret: None,
            initial_up_to_date_node: None,
        };
        let used = Used::from_assignments([&two, &other]);
        let att = |n: &[&str]| n.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        // Nodes with a diskful or tiebreaker replica need nothing.
        assert_eq!(
            converge_attachments(&two, &att(&["n1", "n3"]), &[], &used),
            (None, vec![])
        );
        let a = converge_attachments(&two, &att(&["n4"]), &[], &used)
            .0
            .unwrap();
        let added = a.replicas.last().unwrap();
        assert_eq!(
            (
                added.node.as_str(),
                added.node_id,
                added.port,
                added.role,
                &added.pool
            ),
            ("n4", 3, Some(11001), Role::Diskless, &None)
        );
        assert_eq!(a.replicas[..3], two.replicas[..]);
        assert_eq!(
            converge_attachments(&a, &att(&["n4"]), &[], &used),
            (None, vec![])
        );
        // Unpublished (or a missed unpublish): the diskless entry goes, the
        // tiebreaker stays.
        let back = converge_attachments(&a, &[], &[], &used).0.unwrap();
        assert_eq!(back, two);
        // spec.evict: no new diskless entry; an attached one is a running
        // workload and stays until the attachment goes.
        assert_eq!(
            converge_attachments(&two, &att(&["n4"]), &att(&["n4"]), &used),
            (None, vec![])
        );
        assert_eq!(
            converge_attachments(&a, &att(&["n4"]), &att(&["n4"]), &used),
            (None, vec![])
        );
        let back = converge_attachments(&a, &[], &att(&["n4"]), &used)
            .0
            .unwrap();
        assert_eq!(back, two);
    }

    #[test]
    fn attachment_failures_name_the_node() {
        let mut a = two_with_tb();
        a.replicas.push(AssignedReplica {
            node: "n9".into(),
            pool: None,
            node_id: 3,
            port: Some(11000),
            role: Role::Diskless,
        });
        let full: Vec<Assignment> = model::keys::PORT_RANGE
            .map(|port| Assignment {
                replicas: vec![AssignedReplica {
                    node: "n5".into(),
                    pool: Some("p".into()),
                    node_id: 0,
                    port: Some(port),
                    role: Role::Diskful,
                }],
                minor: None,
                secret: None,
                initial_up_to_date_node: None,
            })
            .collect();
        let used = Used::from_assignments(&full);
        let att = ["n4".to_owned(), "n5".to_owned(), "n6".to_owned()];
        let (next, failed) = converge_attachments(&a, &att, &[], &used);
        let next = next.unwrap();
        // n9 is detached: gone, and its node id 3 goes to n4.
        let ids: Vec<(&str, u8)> = next
            .replicas
            .iter()
            .map(|r| (r.node.as_str(), r.node_id))
            .collect();
        assert_eq!(ids, [("n1", 0), ("n2", 1), ("n3", 2), ("n4", 3), ("n6", 4)]);
        assert_eq!(failed.len(), 1);
        assert_eq!(
            (failed[0].node.as_str(), failed[0].reason),
            ("n5", AttachmentErrorReason::NoPort)
        );
        assert!(
            failed[0].message.contains("on n5:"),
            "{}",
            failed[0].message
        );

        // At the node id limit, a detached entry's id is what the add gets.
        let mut crowded = two_with_tb();
        crowded.replicas = (0..=model::keys::NODE_ID_MAX)
            .map(|id| AssignedReplica {
                node: format!("m{id}"),
                pool: None,
                node_id: id,
                port: Some(11000),
                role: if id == 7 {
                    Role::Diskless
                } else {
                    Role::Diskful
                },
            })
            .collect();
        let (next, failed) = converge_attachments(&crowded, &["n5".into()], &[], &Used::default());
        assert!(failed.is_empty(), "{failed:?}");
        let added = next.unwrap();
        assert!(
            added
                .replicas
                .iter()
                .any(|r| r.node == "n5" && r.node_id == 7)
        );
        // Every id held: n5 fails alone, and the detached entry still goes.
        crowded.replicas[7].role = Role::Diskful;
        crowded.replicas[8].role = Role::Diskless;
        let attached = ["m8".to_owned(), "n5".to_owned()];
        let (next, failed) = converge_attachments(&crowded, &attached, &[], &Used::default());
        assert_eq!(next, None);
        assert_eq!(
            (failed[0].node.as_str(), failed[0].reason),
            ("n5", AttachmentErrorReason::TooManyPeers)
        );
        let (next, failed) =
            converge_attachments(&crowded, &["n5".into(), "n6".into()], &[], &Used::default());
        assert_eq!(failed.len(), 1, "m8 detached frees one id: {failed:?}");
        assert!(next.unwrap().replicas.iter().all(|r| r.node != "m8"));
    }

    #[test]
    fn snapshot_rounds_that_block_eviction() {
        let mut snap = Snapshot::new("s", model::SnapshotSpec { volume: "v".into() });
        assert!(!round_running(&snap), "no status yet");
        for (phase, running) in [
            (SnapshotPhase::Pending, false),
            (SnapshotPhase::Suspending, true),
            (SnapshotPhase::Suspended, true),
            (SnapshotPhase::Taking, true),
            (SnapshotPhase::Taken, true),
            (SnapshotPhase::Resuming, true),
            (SnapshotPhase::Ready, false),
            (SnapshotPhase::Failed, false),
            (SnapshotPhase::Deleting, false),
        ] {
            snap.status = Some(model::SnapshotStatus {
                phase,
                ..model::SnapshotStatus::default()
            });
            assert_eq!(round_running(&snap), running, "{phase:?}");
        }
    }

    #[test]
    fn primary_only_from_the_assignment() {
        // n0 is no longer assigned, its agent not caught up yet; its name
        // sorts first, so the smallest-name rule alone would pick it.
        let two = two_with_tb();
        let mut r = healthy();
        r[0].in_use = false;
        r.push(entry("n0", true, "Diskless", G, true));
        let st = status(VolumePhase::Ready, r.clone());
        let s = aggregate(G, &two, Some(&st), &|_| true);
        assert_eq!(s.primary, None);
        r[1].in_use = true;
        let st = status(VolumePhase::Ready, r);
        let s = aggregate(G, &two, Some(&st), &|_| true);
        assert_eq!(s.primary.as_deref(), Some("n2"));
    }

    #[test]
    fn entries_outside_the_assignment_are_ignored() {
        // n2 was evicted: out of the assignment, its stale entry still in use.
        let a = assignment(&[
            ("n1", Role::Diskful),
            ("n4", Role::Diskful),
            ("n3", Role::Tiebreaker),
        ]);
        let mut n4 = entry("n4", true, "UpToDate", G, false);
        n4.peers = [("n1".to_owned(), "Connected".to_owned())].into();
        let mut n1 = entry("n1", true, "UpToDate", G, false);
        n1.peers = [("n4".to_owned(), "Connected".to_owned())].into();
        let st = status(
            VolumePhase::Degraded,
            vec![
                n1,
                entry("n2", true, "UpToDate", G, true),
                entry("n3", true, "Diskless", 0, false),
                n4,
            ],
        );
        let s = aggregate_nodes(
            G,
            &a,
            Some(&st),
            &Nodes {
                ready: &|_| true,
                reachable: &|n| n != "n2",
                evicting: &[],
                alive_across: &|_, _| true,
            },
            &BTreeSet::new(),
            None,
        );
        assert_eq!(s.phase, VolumePhase::Ready);
        assert_eq!(s.degraded.status, FALSE);
        assert_eq!(s.primary, None);
    }

    #[test]
    fn holders_from_acks() {
        let ack = |node: &str, ok: bool, pool: Option<&str>| model::SnapshotAck {
            node: node.into(),
            phase: SnapshotPhase::Resuming,
            pool: pool.map(str::to_owned),
            ok,
            error: None,
        };
        let mut snap = Snapshot::new("s", model::SnapshotSpec { volume: "v".into() });
        snap.status = Some(model::SnapshotStatus {
            acks: vec![
                ack("n2", true, Some("p2")),
                ack("n1", true, Some("p1")),
                ack("n3", false, Some("p3")),
                ack("n4", true, None),
            ],
            ..model::SnapshotStatus::default()
        });
        assert_eq!(
            snapshot_holders(&snap),
            [
                ("n1".to_owned(), "p1".to_owned()),
                ("n2".to_owned(), "p2".to_owned())
            ]
        );
        assert_eq!(snapshot_holder(&snap), Some(("n1".into(), "p1".into())));
    }

    #[test]
    fn split_brain_latches_until_connected() {
        let two = two_with_tb();
        let all = Nodes {
            ready: &|_| true,
            reachable: &|_| true,
            evicting: &[],
            alive_across: &|_, _| true,
        };
        let with = |a: &str, b: &str| {
            let mut r = healthy();
            r[0].peers.insert("n2".into(), a.into());
            r[1].peers.insert("n1".into(), b.into());
            status(VolumePhase::Degraded, r)
        };
        let s = aggregate_nodes(
            G,
            &two,
            Some(&with("StandAlone", "StandAlone")),
            &all,
            &BTreeSet::new(),
            None,
        );
        let latched = s.split_pairs.clone();
        assert_eq!(latched, BTreeSet::from([pair("n2", "n1")]));
        assert_eq!(pairs_from_message(&s.split_brain.message), latched);
        // The agent re-adjusts: DRBD cycles through Connecting.
        for (a, b) in [
            ("Connecting", "StandAlone"),
            ("Connecting", "Connecting"),
            ("Connected", "Connecting"),
        ] {
            let s = aggregate_nodes(G, &two, Some(&with(a, b)), &all, &latched, None);
            assert_eq!(s.split_brain.status, TRUE, "{a}/{b}");
            assert_eq!(s.split_pairs, latched);
            // Without the latch the same report is no split brain.
            let s = aggregate_nodes(G, &two, Some(&with(a, b)), &all, &BTreeSet::new(), None);
            assert_eq!(s.split_brain.status, FALSE, "{a}/{b}");
        }
        // An unreachable side keeps the latch.
        let s = aggregate_nodes(
            G,
            &two,
            Some(&with("Connected", "Connected")),
            &Nodes {
                ready: &|_| true,
                reachable: &|n| n != "n1",
                evicting: &[],
                alive_across: &|_, _| true,
            },
            &latched,
            None,
        );
        assert_eq!(s.split_brain.status, TRUE);
        let s = aggregate_nodes(
            G,
            &two,
            Some(&with("Connected", "Connected")),
            &all,
            &latched,
            None,
        );
        assert_eq!(s.split_brain.status, FALSE);
        assert!(s.split_pairs.is_empty());
        assert!(pairs_from_message("").is_empty());
        assert_eq!(
            pairs_from_message("split brain between a and b, c and d; set spec.resolve.survivor"),
            BTreeSet::from([pair("a", "b"), pair("c", "d")])
        );
    }

    #[test]
    fn initialized_signal() {
        let mut two = two_with_tb();
        two.minor = Some(50000);
        let init = |r: &[ReplicaStatus], nodes: &Nodes<'_>| {
            let st = status(VolumePhase::Provisioning, r.to_vec());
            aggregate_nodes(G, &two, Some(&st), nodes, &BTreeSet::new(), None).initialized
        };
        let all = Nodes {
            ready: &|_| true,
            reachable: &|_| true,
            evicting: &[],
            alive_across: &|_, _| true,
        };
        let mut r = healthy();
        r[0].disk_state = Some("Inconsistent".into());
        r[1].disk_state = Some("Inconsistent".into());
        assert!(!init(&r, &all));
        // One UpToDate diskful replica is not enough any more.
        r[0].disk_state = Some("UpToDate".into());
        assert!(!init(&r, &all));
        r[1].disk_state = Some("UpToDate".into());
        assert!(init(&r, &all));
        let mut d = r.clone();
        d[1].peers.insert("n1".into(), "Connecting".into());
        assert!(!init(&d, &all));
        // The tiebreaker must be present too (phase Ready).
        assert!(!init(&r[..2], &all));
        let stale = Nodes {
            ready: &|_| true,
            reachable: &|n| n != "n2",
            evicting: &[],
            alive_across: &|_, _| true,
        };
        assert!(!init(&r, &stale));
        // Ready while resizing still counts.
        let st = status(VolumePhase::Provisioning, r.clone());
        let s = aggregate(2 * G, &two, Some(&st), &|_| true);
        assert_eq!(s.ready.reason, reason::RESIZING);
        assert!(s.initialized);
        // replicas 1: present from a fresh agent.
        let one = assignment(&[("n1", Role::Diskful)]);
        let st = status(
            VolumePhase::Provisioning,
            vec![entry("n1", true, "UpToDate", G, false)],
        );
        assert!(aggregate(G, &one, Some(&st), &|_| true).initialized);
        let st = status(
            VolumePhase::Provisioning,
            vec![entry("n1", false, "UpToDate", G, false)],
        );
        assert!(!aggregate(G, &one, Some(&st), &|_| true).initialized);
        assert!(!aggregate(G, &one, None, &|_| true).initialized);
    }

    #[test]
    fn initialized_at_survives_a_lagging_store() {
        let t = |s: i64| Time(k8s_openapi::jiff::Timestamp::from_second(s).unwrap());
        // Written by this process, store copy from before the write.
        assert_eq!(carry_initialized(Some(&t(10)), None, None), Some(t(10)));
        assert_eq!(
            carry_initialized(Some(&t(10)), None, Some(t(20))),
            Some(t(10))
        );
        // After a restart the store is the source.
        assert_eq!(
            carry_initialized(None, Some(&t(5)), Some(t(20))),
            Some(t(5))
        );
        assert_eq!(carry_initialized(None, None, Some(t(20))), Some(t(20)));
        assert_eq!(carry_initialized(None, None, None), None);
    }

    #[test]
    fn resolve_actions() {
        use ResolveAction as A;
        assert_eq!(resolve_action(true, false, false), A::Wait);
        assert_eq!(resolve_action(true, true, true), A::Wait);
        assert_eq!(resolve_action(false, true, false), A::Wait, "resyncing");
        assert_eq!(resolve_action(false, true, true), A::Clear);
        // Set on a volume that never split: shown, never cleared on its own.
        assert_eq!(resolve_action(false, false, true), A::WithoutSplitBrain);
        assert_eq!(resolve_action(false, false, false), A::WithoutSplitBrain);
    }

    #[test]
    fn primary_prefers_the_current_generation() {
        let two = two_with_tb();
        let mut r = healthy();
        // n1 listed first but written before the failover; n2 is current.
        r[0].in_use = true;
        r[0].observed_generation = Some(3);
        r[1].in_use = true;
        r[1].observed_generation = Some(4);
        let st = status(VolumePhase::Ready, r);
        let all = Nodes {
            ready: &|_| true,
            reachable: &|_| true,
            evicting: &[],
            alive_across: &|_, _| true,
        };
        let s = aggregate_nodes(G, &two, Some(&st), &all, &BTreeSet::new(), Some(4));
        assert_eq!(s.primary.as_deref(), Some("n2"));
        // No entry at the current generation: the smallest node name.
        let s = aggregate_nodes(G, &two, Some(&st), &all, &BTreeSet::new(), Some(5));
        assert_eq!(s.primary.as_deref(), Some("n1"));
    }

    #[test]
    fn primary_is_the_smallest_in_use_node() {
        let mut a = two_with_tb();
        a.replicas.push(AssignedReplica {
            node: "n0".into(),
            pool: None,
            node_id: 3,
            port: None,
            role: Role::Diskless,
        });
        let mut r = healthy();
        let mut n0 = entry("n0", true, "Diskless", G, true);
        n0.observed_generation = Some(4);
        r[0].in_use = false;
        r[1].in_use = true;
        r[1].observed_generation = Some(4);
        // ReadWriteMany: n2 wrote first, n0 later; list order must not decide.
        let all = Nodes {
            ready: &|_| true,
            reachable: &|_| true,
            evicting: &[],
            alive_across: &|_, _| true,
        };
        for order in [[1, 3], [3, 1]] {
            let mut list: Vec<ReplicaStatus> = r.clone();
            list.push(n0.clone());
            let list = order.iter().map(|i| list[*i].clone()).collect();
            let st = status(VolumePhase::Ready, list);
            let s = aggregate_nodes(G, &a, Some(&st), &all, &BTreeSet::new(), Some(4));
            assert_eq!(s.primary.as_deref(), Some("n0"), "{order:?}");
        }
        // An unreachable in-use entry is left out.
        let mut list = r.clone();
        list.push(n0);
        let st = status(VolumePhase::Ready, list);
        let s = aggregate_nodes(
            G,
            &a,
            Some(&st),
            &Nodes {
                ready: &|_| true,
                reachable: &|n| n != "n0",
                evicting: &[],
                alive_across: &|_, _| true,
            },
            &BTreeSet::new(),
            Some(4),
        );
        assert_eq!(s.primary.as_deref(), Some("n2"));
    }
}
