//! spec.evict: re-placing the replica a dead or retired node holds.
//! The controller never fills the list itself; an operator or a human
//! does, because only they know a node is fenced. One move per volume and
//! pass: the first listed node by name that can act.

use std::collections::BTreeSet;

use model::{AssignedReplica, Assignment, ReplicaStatus, Role, VolumeSpec};

use crate::alloc::{Used, next_node_id};

/// What the pass knows besides the Volume itself.
pub struct Input<'a> {
    pub name: &'a str,
    pub spec: &'a VolumeSpec,
    pub assignment: &'a Assignment,
    pub entries: &'a [ReplicaStatus],
    /// status.initializedAt is set.
    pub initialized: bool,
    pub split_brain: bool,
    /// A Snapshot of this volume from Suspending to Resuming: a node that
    /// joins now would ack Taking with a blank LV.
    pub snapshot_in_progress: Option<&'a str>,
    /// The node's agent Lease is fresh.
    pub fresh: &'a dyn Fn(&str) -> bool,
    /// The core Node object exists; deleting it is the operator's proof
    /// that a node is dead for good.
    pub node_exists: &'a dyn Fn(&str) -> bool,
    /// Every Pool as placement sees it: `live` folds in `fresh`.
    pub views: &'a [placer::PoolView],
    /// Nodes placement may use (Ready, agent fresh), sorted.
    pub live_nodes: &'a [String],
    pub used: &'a Used,
}

impl std::fmt::Debug for Input<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Input")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// spec.evict is empty.
    Idle,
    /// The node holds nothing and is not attached: drop it from spec.evict.
    Clear {
        node: String,
        rest: Vec<String>,
    },
    /// An attachment or a diskless entry remains: an attached diskless
    /// entry is a running workload, so it goes only with ControllerUnpublish.
    Wait {
        node: String,
    },
    Blocked {
        node: String,
        message: String,
    },
    /// Write `next`; the node leaves spec.evict by `Clear` on a later pass.
    Move {
        node: String,
        next: Assignment,
        /// The (node, pool) of a new diskful replica, for the reservation.
        diskful: Option<(String, String)>,
        event: String,
    },
}

fn blocked(node: &str, why: impl std::fmt::Display) -> Step {
    Step::Blocked {
        node: node.to_owned(),
        message: format!("eviction of {node} blocked: {why}"),
    }
}

fn up_to_date(e: &ReplicaStatus) -> bool {
    e.present
        && e.disk_state
            .as_deref()
            .is_some_and(|d| d.eq_ignore_ascii_case("UpToDate"))
}

/// A status entry of a node outside the assignment: its agent has not torn
/// the old replica down yet, so a new one there would meet stale state.
fn departing(a: &Assignment, entries: &[ReplicaStatus], node: &str) -> bool {
    entries.iter().any(|e| e.node == node) && !a.replicas.iter().any(|r| r.node == node)
}

/// Pools a moved diskful replica may land in: no diskful entry of this
/// volume, not being evicted itself, not departing. The placer checks the
/// rest.
pub fn candidates(
    views: &[placer::PoolView],
    a: &Assignment,
    entries: &[ReplicaStatus],
    evict: &[String],
) -> Vec<placer::PoolView> {
    views
        .iter()
        .filter(|p| !evict.contains(&p.node))
        .filter(|p| !departing(a, entries, &p.node))
        .filter(|p| {
            !a.replicas
                .iter()
                .any(|r| r.node == p.node && r.role == Role::Diskful)
        })
        .cloned()
        .collect()
}

/// What any move needs, the tiebreaker's included: it carries no data, so
/// neither initializedAt nor a survivor matters for it, but a fresh node
/// still serving a workload does.
fn common_precondition(i: &Input<'_>, node: &str) -> Option<String> {
    if i.spec.resolve.is_some() {
        return Some("spec.resolve is pending".into());
    }
    if i.split_brain {
        return Some("split brain".into());
    }
    if let Some(snap) = i.snapshot_in_progress {
        return Some(format!("snapshot {snap} in progress"));
    }
    // A stale Lease shows only that the agent is down, not that the node
    // stopped serving; a deleted Node is the operator's proof it did.
    if !(i.node_exists)(node) {
        return None;
    }
    let hint = if (i.fresh)(node) {
        ""
    } else {
        " (agent stale; delete its Node once fenced)"
    };
    if i.spec.attachments.iter().any(|n| n == node) {
        return Some(format!("attached to {node}; unpublish it first{hint}"));
    }
    if i.entries.iter().any(|e| e.node == node && e.in_use) {
        return Some(format!("in use on {node}{hint}"));
    }
    None
}

/// What a diskful move needs on top, as the blocking reason.
fn diskful_precondition(i: &Input<'_>, node: &str) -> Option<String> {
    // Before initializedAt only the initial node makes data; any other
    // replica joins it as on any move.
    let initial = i.assignment.initial_up_to_date_node.as_deref();
    if !i.initialized && initial.is_none_or(|n| n == node) {
        return Some(format!(
            "status.initializedAt is not set and {node} makes the first data: a volume never Ready holds nothing, delete and recreate it; if its status was lost, an operator may set initializedAt once an UpToDate replica holds the data"
        ));
    }
    let entry = |n: &str| i.entries.iter().find(|e| e.node == n);
    let others: Vec<&str> = i
        .assignment
        .replicas
        .iter()
        .filter(|r| r.role == Role::Diskful && r.node != node)
        .map(|r| r.node.as_str())
        .collect();
    let up = |n: &str| (i.fresh)(n) && entry(n).is_some_and(up_to_date);
    // With N's Node deleted the survivor may lack quorum: N will never
    // come back to hold a newer copy.
    let dead = !(i.node_exists)(node);
    let survivor = |n: &str| up(n) && (dead || entry(n).is_some_and(|e| e.quorum == Some(true)));
    if !others.iter().any(|n| survivor(n)) {
        return Some(format!(
            "no UpToDate replica with quorum outside {node}; if {node} is dead for good, delete its Node object"
        ));
    }
    let lagging: Vec<&str> = others
        .iter()
        .copied()
        .filter(|n| (i.fresh)(n) && !up(n))
        .collect();
    if !lagging.is_empty() {
        return Some(format!(
            "not UpToDate on {} (a rebuild may still run)",
            lagging.join(", ")
        ));
    }
    None
}

/// A tiebreaker for `next` when it has none and two diskful replicas:
/// a live node with no entry, not being evicted.
fn add_tiebreaker(i: &Input<'_>, next: &mut Assignment) -> Option<String> {
    let diskful: Vec<String> = next
        .replicas
        .iter()
        .filter(|r| r.role == Role::Diskful)
        .map(|r| r.node.clone())
        .collect();
    if i.spec.replicas != 2
        || diskful.len() != 2
        || next.replicas.iter().any(|r| r.role == Role::Tiebreaker)
    {
        return None;
    }
    let live: Vec<String> = i
        .live_nodes
        .iter()
        .filter(|n| !i.spec.evict.contains(n))
        .filter(|n| !departing(i.assignment, i.entries, n))
        .filter(|n| !next.replicas.iter().any(|r| &r.node == *n))
        .cloned()
        .collect();
    let node = placer::tiebreaker(&diskful, &live, i.views, &i.spec.tier, &|n| {
        i.used.resources_on(n)
    })?;
    let node_id = next_node_id(i.assignment, next)?;
    let port = i.used.lowest_free_port(&node)?;
    next.replicas.push(AssignedReplica {
        node: node.clone(),
        pool: None,
        node_id,
        port: Some(port),
        role: Role::Tiebreaker,
    });
    Some(node)
}

fn move_diskful(i: &Input<'_>, gone: &AssignedReplica) -> Step {
    let node = gone.node.as_str();
    let pinned_pool_here = i.spec.pool.as_ref().is_some_and(|p| {
        gone.pool.as_ref() == Some(p) || i.views.iter().any(|v| &v.name == p && v.node == node)
    });
    if i.spec.node.as_deref() == Some(node) || pinned_pool_here {
        return blocked(node, format!("pinned to {node}"));
    }
    let pools = candidates(i.views, i.assignment, i.entries, &i.spec.evict);
    let req = placer::Request {
        size_bytes: i.spec.size_bytes,
        replicas: 1,
        tier: i.spec.tier.clone(),
        pool: i.spec.pool.clone(),
        node: None,
    };
    let mut full: Vec<String> = Vec::new();
    let chosen = match placer::place(&req, &pools) {
        Ok(diskful) => diskful.into_iter().next(),
        Err(placer::Insufficient { rejected, .. }) => {
            full.extend(
                rejected
                    .iter()
                    .filter(|r| r.reason == placer::Reason::NoCapacity)
                    .map(|r| r.pool.clone()),
            );
            None
        }
    };
    let Some(chosen) = chosen else {
        // Departing nodes are skipped only until their agent drops the old
        // entry; naming them tells the operator the block is temporary.
        let mut leaving: Vec<&str> = i
            .views
            .iter()
            .filter(|v| v.tier == i.spec.tier && v.live)
            .filter(|v| !i.spec.evict.contains(&v.node))
            .map(|v| v.node.as_str())
            .filter(|n| departing(i.assignment, i.entries, n))
            .collect();
        leaving.sort_unstable();
        leaving.dedup();
        let mut why = format!("no pool for tier {} outside {node}", i.spec.tier);
        if !leaving.is_empty() {
            why = format!("{why} (departing: {})", leaving.join(", "));
        }
        if !full.is_empty() {
            why = format!(
                "{why} (no room under the capacity policy: {})",
                full.join(", ")
            );
        }
        return blocked(node, why);
    };
    let mut next = i.assignment.clone();
    next.replicas.retain(|r| r.node != node);
    if let Some(r) = next.replicas.iter_mut().find(|r| r.node == chosen.node) {
        // A tiebreaker or diskless peer keeps its node id and port.
        r.role = Role::Diskful;
        r.pool = Some(chosen.pool.clone());
    } else {
        let Some(node_id) = next_node_id(i.assignment, &next) else {
            return blocked(node, "no free DRBD node id");
        };
        let port = match i.assignment.minor {
            None => None,
            Some(_) => match i.used.lowest_free_port(&chosen.node) {
                Some(p) => Some(p),
                None => return blocked(node, format!("no free DRBD port on {}", chosen.node)),
            },
        };
        next.replicas.push(AssignedReplica {
            node: chosen.node.clone(),
            pool: Some(chosen.pool.clone()),
            node_id,
            port,
            role: Role::Diskful,
        });
    }
    let tiebreaker = add_tiebreaker(i, &mut next);
    let mut event = format!("replica of {} moved from {node} to {}", i.name, chosen.node);
    if let Some(t) = tiebreaker {
        event = format!("{event}; tiebreaker on {t}");
    }
    Step::Move {
        node: node.to_owned(),
        next,
        diskful: Some((chosen.node, chosen.pool)),
        event,
    }
}

fn move_tiebreaker(i: &Input<'_>, gone: &AssignedReplica) -> Step {
    let node = gone.node.as_str();
    let mut next = i.assignment.clone();
    next.replicas.retain(|r| r.node != node);
    let event = match add_tiebreaker(i, &mut next) {
        Some(t) => format!("tiebreaker of {} moved from {node} to {t}", i.name),
        None => format!(
            "tiebreaker of {} removed from {node}; no node for a new one",
            i.name
        ),
    };
    Step::Move {
        node: node.to_owned(),
        next,
        diskful: None,
        event,
    }
}

/// The first step of spec.evict, by node name, that moves or clears; else
/// the first Blocked, so its Event stays the same pass after pass; else the
/// first Wait.
pub fn decide(i: &Input<'_>) -> Step {
    let nodes: BTreeSet<&String> = i.spec.evict.iter().collect();
    let mut held = None;
    for node in nodes {
        let step = decide_node(i, node);
        match step {
            Step::Move { .. } | Step::Clear { .. } => return step,
            Step::Blocked { .. } if !matches!(held, Some(Step::Blocked { .. })) => {
                held = Some(step);
            }
            _ => {
                held.get_or_insert(step);
            }
        }
    }
    held.unwrap_or(Step::Idle)
}

fn decide_node(i: &Input<'_>, node: &String) -> Step {
    let held = i.assignment.replicas.iter().find(|r| &r.node == node);
    let attached = i.spec.attachments.contains(node);
    let Some(r) = held else {
        if attached {
            return Step::Wait { node: node.clone() };
        }
        let rest: BTreeSet<&String> = i.spec.evict.iter().filter(|n| *n != node).collect();
        return Step::Clear {
            node: node.clone(),
            rest: rest.into_iter().cloned().collect(),
        };
    };
    if r.role == Role::Diskless {
        return Step::Wait { node: node.clone() };
    }
    if r.role == Role::Diskful && i.spec.replicas < 2 {
        return blocked(
            node,
            format!("a local volume has no other copy; remove {node} from spec.evict"),
        );
    }
    let why = common_precondition(i, node).or_else(|| {
        (r.role == Role::Diskful)
            .then(|| diskful_precondition(i, node))
            .flatten()
    });
    if let Some(why) = why {
        return blocked(node, why);
    }
    match r.role {
        Role::Diskful => move_diskful(i, r),
        _ => move_tiebreaker(i, r),
    }
}

/// Indices of status entries the controller removes: the node holds no
/// replica in the assignment and its Node is gone, so no agent will ever
/// drop the entry. A stale agent on an existing Node keeps its entry: it
/// marks the node departing until the agent tears the old replica down.
pub fn orphan_entries(
    a: &Assignment,
    entries: &[ReplicaStatus],
    exists: &dyn Fn(&str) -> bool,
) -> Vec<(usize, String)> {
    entries
        .iter()
        .enumerate()
        .filter(|(_, e)| !a.replicas.iter().any(|r| r.node == e.node))
        .filter(|(_, e)| !exists(&e.node))
        .map(|(i, e)| (i, e.node.clone()))
        .collect()
}

/// JSON patch removing `orphans`, highest index first so the earlier ones
/// keep their place; every removal is guarded by a test of the node name.
pub fn orphan_patch(orphans: &[(usize, String)]) -> serde_json::Value {
    let mut sorted = orphans.to_vec();
    sorted.sort_by_key(|o| std::cmp::Reverse(o.0));
    let ops: Vec<serde_json::Value> = sorted
        .iter()
        .flat_map(|(i, node)| {
            [
                serde_json::json!({ "op": "test", "path": format!("/status/replicas/{i}/node"), "value": node }),
                serde_json::json!({ "op": "remove", "path": format!("/status/replicas/{i}") }),
            ]
        })
        .collect();
    serde_json::Value::Array(ops)
}

#[cfg(test)]
mod tests;
