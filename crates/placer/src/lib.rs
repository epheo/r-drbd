//! Placement of volume replicas and the choice of the quorum tiebreaker.
//!
//! Pure crate, deliberately small: distinct nodes, tier or pool or node
//! filter, capacity from the pool's own policy (see `room`), most room
//! first, deterministic ties.

#![forbid(unsafe_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolView {
    pub name: String,
    pub node: String,
    pub tier: String,
    /// The node is Ready and its agent fresh.
    pub live: bool,
    pub available: bool,
    pub total_bytes: u64,
    pub free_bytes: u64,
    /// Sum of the sizes of volumes already placed in this pool.
    pub committed_bytes: u64,
    pub overcommit_percent: u32,
    pub reserve_percent: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub size_bytes: u64,
    /// Diskful replicas wanted in total (1 local, 2 or 3 replicated).
    /// 0 behaves like already satisfied.
    pub replicas: u32,
    pub tier: String,
    /// Every diskful replica goes to this pool.
    pub pool: Option<String>,
    /// The first diskful replica goes to this node; the others go anywhere.
    pub node: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chosen {
    pub node: String,
    pub pool: String,
}

/// Not enough nodes or capacity; `rejected` names every rejected pool once
/// with the first reason that rejected it, in declaration order of Reason
/// and input order of pools; `missing` counts the replicas still wanted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insufficient {
    pub missing: u32,
    pub rejected: Vec<Rejected>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejected {
    pub node: String,
    pub pool: String,
    pub reason: Reason,
}

/// Checked in declaration order; a pool reports the first reason that
/// rejects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    NodeUnreachable,
    PoolUnavailable,
    WrongTier,
    NotThePinnedPool,
    NoCapacity,
}

/// Largest volume the pool accepts now:
///
/// ```text
/// reserve = ceil(total * reserve_percent / 100)
/// promise = floor((total - reserve) * overcommit_percent / 100) - committed
/// actual  = free - reserve
/// room    = min(promise, actual), never below 0; 0 when total is 0
/// ```
///
/// `promise` bounds what the volumes may claim once fully written.
/// `actual` bounds the new volume by the data blocks free today: snapshots
/// and earlier overcommit consume blocks that `committed` does not count, and
/// a new volume must be writable in full without touching the reserve.
pub fn room(pool: &PoolView) -> u64 {
    let actual = u128::from(pool.free_bytes).saturating_sub(reserve(pool));
    u64::try_from(u128::from(promise(pool)).min(actual)).unwrap_or(u64::MAX)
}

/// The `promise` term of `room` alone: what may still be committed, 0 when
/// total is 0 or the committed sizes already exceed the policy.
pub fn promise(pool: &PoolView) -> u64 {
    if pool.total_bytes == 0 {
        return 0;
    }
    let usable = u128::from(pool.total_bytes).saturating_sub(reserve(pool));
    let promise = (usable * u128::from(pool.overcommit_percent) / 100)
        .saturating_sub(u128::from(pool.committed_bytes));
    u64::try_from(promise).unwrap_or(u64::MAX)
}

fn reserve(pool: &PoolView) -> u128 {
    (u128::from(pool.total_bytes) * u128::from(pool.reserve_percent)).div_ceil(100)
}

/// Placement may use the pool at all: its node is live and the pool
/// Available. Capacity is apart, see `fits`.
pub fn usable(pool: &PoolView) -> bool {
    pool.live && pool.available
}

/// A pool with total 0 (capacity not reported yet) never fits.
pub fn fits(pool: &PoolView, size_bytes: u64) -> bool {
    pool.total_bytes > 0 && size_bytes <= room(pool)
}

/// Diskful placement: filter pools, order by room descending then node
/// then pool name, the pinned node first, pick one pool per node until
/// `replicas` are held. Never partial; with a node pin, Insufficient unless
/// the first choice is on that node.
pub fn place(req: &Request, pools: &[PoolView]) -> Result<Vec<Chosen>, Insufficient> {
    let wanted = usize::try_from(req.replicas).unwrap_or(usize::MAX);
    let mut rejected = Vec::new();
    let mut survivors = Vec::new();
    for pool in pools {
        match reject(req, pool) {
            Some(reason) => rejected.push(Rejected {
                node: pool.node.clone(),
                pool: pool.name.clone(),
                reason,
            }),
            None => survivors.push(pool),
        }
    }
    // Room, not freeBytes: committed grows with every placement of a pass,
    // so a burst spreads instead of filling the pool with most unwritten space.
    let off_pin = |node: &str| req.node.as_deref().is_some_and(|n| n != node);
    survivors.sort_by(|a, b| {
        off_pin(&a.node)
            .cmp(&off_pin(&b.node))
            .then_with(|| room(b).cmp(&room(a)))
            .then_with(|| a.node.cmp(&b.node))
            .then_with(|| a.name.cmp(&b.name))
    });

    // Not with_capacity(wanted): replicas is caller input and u32::MAX would abort.
    let mut diskful: Vec<Chosen> = Vec::new();
    for pool in survivors {
        if diskful.len() == wanted {
            break;
        }
        if diskful.iter().any(|c| c.node == pool.node) {
            continue;
        }
        diskful.push(Chosen {
            node: pool.node.clone(),
            pool: pool.name.clone(),
        });
    }
    // Without a pool on the pinned node, that replica is missing whatever
    // the other nodes offer.
    let held = match diskful.first() {
        Some(c) if off_pin(&c.node) => diskful.len().min(wanted - 1),
        _ => diskful.len(),
    };
    if held < wanted {
        return Err(Insufficient {
            missing: u32::try_from(wanted - held).unwrap_or(u32::MAX),
            rejected,
        });
    }
    Ok(diskful)
}

fn reject(req: &Request, pool: &PoolView) -> Option<Reason> {
    if !pool.live {
        Some(Reason::NodeUnreachable)
    } else if !pool.available {
        Some(Reason::PoolUnavailable)
    } else if pool.tier != req.tier {
        Some(Reason::WrongTier)
    } else if req.pool.as_ref().is_some_and(|p| *p != pool.name) {
        Some(Reason::NotThePinnedPool)
    } else if !fits(pool, req.size_bytes) {
        Some(Reason::NoCapacity)
    } else {
        None
    }
}

/// Tiebreaker node for an even diskful count: a live node that holds no
/// replica, preferring nodes with no pool of this tier (such as a control
/// plane node), then the node with the lowest `load`, then by name.
pub fn tiebreaker(
    diskful_nodes: &[String],
    live_nodes: &[String],
    pools: &[PoolView],
    tier: &str,
    load: &dyn Fn(&str) -> usize,
) -> Option<String> {
    live_nodes
        .iter()
        .filter(|n| !diskful_nodes.contains(n))
        .min_by_key(|n| {
            let has_tier = pools.iter().any(|p| &p.node == *n && p.tier == tier);
            (has_tier, load(n), n.as_str())
        })
        .cloned()
}
