//! Placement of volume replicas and the quorum tiebreaker.
//!
//! Pure crate, deliberately small: distinct nodes, tier or pool or node
//! filter, capacity with a fixed thin oversubscription ratio, most free
//! space first, deterministic ties.

#![forbid(unsafe_code)]

/// Thin pools may be oversubscribed up to this ratio of their total size.
pub const THIN_OVERSUBSCRIPTION_RATIO: u64 = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolView {
    pub name: String,
    pub node: String,
    pub tier: String,
    pub node_ready: bool,
    pub available: bool,
    pub total_bytes: u64,
    pub free_bytes: u64,
    /// Sum of the sizes of volumes already placed in this pool.
    pub reserved_bytes: u64,
    pub volume_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub size_bytes: u64,
    /// Diskful replicas wanted in total (1 local, 2 or 3 replicated).
    /// 0 behaves like already satisfied.
    pub replicas: u32,
    pub tier: String,
    pub pool: Option<String>,
    pub node: Option<String>,
    /// Nodes that already hold a diskful replica (re-placement after a
    /// loss or a split-brain resolve); they are kept and not chosen again.
    /// Duplicates are collapsed; order is irrelevant.
    pub existing: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chosen {
    pub node: String,
    pub pool: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// New diskful replicas, in choice order, plus the tiebreaker node when
    /// the diskful count is even and above one. Never partial.
    Placed {
        diskful: Vec<Chosen>,
        tiebreaker: Option<String>,
    },
    /// Nothing to do: `existing` already satisfies `replicas`.
    AlreadyPlaced,
    /// Not enough nodes or capacity; `rejected` names every rejected pool
    /// once with the first reason that rejected it, in declaration order of
    /// Reason and input order of pools; `missing` counts the replicas still
    /// wanted.
    Insufficient {
        missing: u32,
        rejected: Vec<Rejected>,
    },
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
    NodeNotReady,
    PoolUnavailable,
    WrongTier,
    NotThePinnedPool,
    NotThePinnedNode,
    NodeAlreadyHolds,
    NoCapacity,
}

/// Thin pools: estimate =
/// min(free * ratio, total * ratio - reserved) in saturating u64; a pool fits
/// when estimate >= size and estimate >= ceil(total / 10) (a minimum
/// free space of 10 percent). A pool with total 0 never fits.
pub fn fits(pool: &PoolView, size_bytes: u64) -> bool {
    if pool.total_bytes == 0 {
        return false;
    }
    let estimate = pool
        .free_bytes
        .saturating_mul(THIN_OVERSUBSCRIPTION_RATIO)
        .min(
            pool.total_bytes
                .saturating_mul(THIN_OVERSUBSCRIPTION_RATIO)
                .saturating_sub(pool.reserved_bytes),
        );
    estimate >= size_bytes && estimate >= pool.total_bytes.div_ceil(10)
}

/// Diskful placement: filter pools, order by free bytes descending then
/// node then pool name, pick one pool per node until `replicas` are held.
///
/// The tiebreaker in `Placed` is chosen among nodes with at least one pool
/// whose `node_ready` is true. Nodes without any pool are tiebreaker
/// candidates only through a direct `tiebreaker()` call (the controller does
/// that with the Node list).
pub fn place(req: &Request, pools: &[PoolView]) -> Outcome {
    let mut existing = req.existing.clone();
    existing.sort();
    existing.dedup();
    let wanted = usize::try_from(req.replicas).unwrap_or(usize::MAX);
    if existing.len() >= wanted {
        return Outcome::AlreadyPlaced;
    }

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
    survivors.sort_by(|a, b| {
        b.free_bytes
            .cmp(&a.free_bytes)
            .then_with(|| a.node.cmp(&b.node))
            .then_with(|| a.name.cmp(&b.name))
    });

    let missing = wanted - existing.len();
    // Not with_capacity(missing): replicas is caller input and u32::MAX would abort.
    let mut diskful: Vec<Chosen> = Vec::new();
    for pool in survivors {
        if diskful.len() == missing {
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
    if diskful.len() < missing {
        let short = u32::try_from(missing - diskful.len()).unwrap_or(u32::MAX);
        return Outcome::Insufficient {
            missing: short,
            rejected,
        };
    }

    let all: Vec<String> = existing
        .into_iter()
        .chain(diskful.iter().map(|c| c.node.clone()))
        .collect();
    let tiebreaker = if all.len() >= 2 && all.len().is_multiple_of(2) {
        let mut ready: Vec<String> = pools
            .iter()
            .filter(|p| p.node_ready)
            .map(|p| p.node.clone())
            .collect();
        ready.sort();
        ready.dedup();
        tiebreaker(&all, &ready, pools, &req.tier)
    } else {
        None
    };
    Outcome::Placed {
        diskful,
        tiebreaker,
    }
}

fn reject(req: &Request, pool: &PoolView) -> Option<Reason> {
    if !pool.node_ready {
        Some(Reason::NodeNotReady)
    } else if !pool.available {
        Some(Reason::PoolUnavailable)
    } else if pool.tier != req.tier {
        Some(Reason::WrongTier)
    } else if req.pool.as_ref().is_some_and(|p| *p != pool.name) {
        Some(Reason::NotThePinnedPool)
    } else if req.node.as_ref().is_some_and(|n| *n != pool.node) {
        Some(Reason::NotThePinnedNode)
    } else if req.existing.contains(&pool.node) {
        Some(Reason::NodeAlreadyHolds)
    } else if !fits(pool, req.size_bytes) {
        Some(Reason::NoCapacity)
    } else {
        None
    }
}

/// Tiebreaker node for an even diskful count: a ready node that holds no
/// replica, preferring nodes with no pool of this tier (such as a control plane node),
/// then the node with the fewest volumes, then by name.
pub fn tiebreaker(
    diskful_nodes: &[String],
    ready_nodes: &[String],
    pools: &[PoolView],
    tier: &str,
) -> Option<String> {
    ready_nodes
        .iter()
        .filter(|n| !diskful_nodes.contains(n))
        .min_by_key(|n| {
            let mine = pools.iter().filter(|p| &p.node == *n);
            let has_tier = mine.clone().any(|p| p.tier == tier);
            let volumes: u64 = mine.map(|p| u64::from(p.volume_count)).sum();
            (has_tier, volumes, n.as_str())
        })
        .cloned()
}
