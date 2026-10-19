//! What GetCapacity answers: the largest volume placement accepts now.

use std::collections::HashMap;
use std::sync::Arc;

use model::Pool;
use model::keys::PARAM_TIER;
use placer::{Outcome, PoolView, Request};

use crate::pool::Usage;

/// GetCapacity for the class `parameters` on the node of `segment`, or the
/// whole cluster when None. Without a tier (a bare request, as csi-sanity
/// sends) every tier counts.
// A tier no Pool declares answers 0, not an error: the provisioner keeps an
// object on error, and a stale one would send the scheduler to a node the
// placer refuses. csi-provisioner v6.3.0 keeps an object saying 0; the
// scheduler reads that and a missing object alike, as no capacity.
pub fn answer<S: std::hash::BuildHasher>(
    pools: &[Arc<Pool>],
    ready_nodes: &[String],
    parameters: &HashMap<String, String, S>,
    segment: Option<&str>,
) -> Result<u64, String> {
    let replicas = crate::csi::parse_replicas(parameters)?;
    let tier = parameters
        .get(PARAM_TIER)
        .map(String::as_str)
        .filter(|t| !t.is_empty());
    Ok(largest(pools, ready_nodes, tier, replicas, segment))
}

/// Largest volume of `replicas` diskful replicas of `tier` (every tier
/// when None) that placement accepts now for a pod on `node`, or anywhere
/// when None. 0 when none fits.
pub fn largest(
    pools: &[Arc<Pool>],
    ready_nodes: &[String],
    tier: Option<&str>,
    replicas: u32,
    node: Option<&str>,
) -> u64 {
    // Only a local volume lives on the pod's node. A replicated one is
    // placed by free space anywhere and any node attaches it diskless, so
    // every node sees the cluster-wide answer.
    let node = node.filter(|_| replicas == 1);
    let views: Vec<PoolView> = pools.iter().map(|p| view(p, ready_nodes)).collect();
    let mut tiers: Vec<&str> = match tier {
        Some(t) => vec![t],
        None => views.iter().map(|v| v.tier.as_str()).collect(),
    };
    tiers.sort_unstable();
    tiers.dedup();
    tiers
        .into_iter()
        .map(|t| largest_in(&views, t, replicas, node))
        .max()
        .unwrap_or(0)
}

/// The placer's view of the Pool with placeableBytes as its whole room.
// placeableBytes is the room the pool pass published, counting assignments
// not yet in the store, which the CSI side cannot see; a pool holding exactly
// that much with no policy on top lets place() judge it unchanged.
fn view(p: &Pool, ready_nodes: &[String]) -> PoolView {
    let room = p
        .status
        .as_ref()
        .and_then(|s| s.placeable_bytes)
        .unwrap_or(0);
    PoolView {
        total_bytes: room,
        free_bytes: room,
        committed_bytes: 0,
        overcommit_percent: 100,
        reserve_percent: 0,
        ..crate::pool::view(p, &Usage::new(), ready_nodes)
    }
}

fn largest_in(views: &[PoolView], tier: &str, replicas: u32, node: Option<&str>) -> u64 {
    let mut sizes: Vec<u64> = views
        .iter()
        .filter(|v| v.tier == tier && v.total_bytes > 0)
        .map(|v| v.total_bytes)
        .collect();
    sizes.sort_unstable_by(|a, b| b.cmp(a));
    sizes.dedup();
    // The answer is one pool's room: placement fits by `size <= room`.
    sizes
        .into_iter()
        .find(|size| placed(views, tier, replicas, node, *size))
        .unwrap_or(0)
}

fn placed(views: &[PoolView], tier: &str, replicas: u32, node: Option<&str>, size: u64) -> bool {
    let request = Request {
        size_bytes: size,
        replicas,
        tier: tier.to_owned(),
        pool: None,
        node: node.map(str::to_owned),
        existing: Vec::new(),
        holds_data: false,
    };
    matches!(placer::place(&request, views), Outcome::Placed { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::condition::{self, types};
    use model::{PoolSpec, PoolStatus};

    const G: u64 = 1 << 30;

    fn policy_pool(
        name: &str,
        node: &str,
        tier: &str,
        (total, free): (u64, u64),
        (overcommit, reserve): (u32, u32),
    ) -> Pool {
        let mut p = Pool::new(
            name,
            PoolSpec {
                node: node.into(),
                tier: tier.into(),
                vg: "vg".into(),
                thin_pool: "thin".into(),
                overcommit_percent: overcommit,
                reserve_percent: reserve,
                devices: None,
                chunk_size: None,
                reclaim_policy: None,
            },
        );
        p.status = Some(PoolStatus {
            total_bytes: Some(total),
            free_bytes: Some(free),
            conditions: vec![condition::new(
                types::AVAILABLE,
                condition::TRUE,
                "Ok",
                "",
                None,
                k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                    k8s_openapi::jiff::Timestamp::now(),
                ),
            )],
            ..PoolStatus::default()
        });
        p
    }

    /// Empty, default policy: 5 percent reserve.
    fn pool(name: &str, node: &str, tier: &str, total: u64) -> Pool {
        policy_pool(name, node, tier, (total, total), (100, 5))
    }

    /// Empty, no reserve: room is the size.
    fn bare(name: &str, node: &str, total: u64) -> Pool {
        policy_pool(name, node, "fast", (total, total), (100, 0))
    }

    /// placeableBytes as the pool reconciler shows it, with `held` bytes
    /// committed per Pool name.
    fn published(pools: Vec<Pool>, ready: &[String], held: &[(&str, u64)]) -> Vec<Arc<Pool>> {
        let mut usage = Usage::new();
        for (name, bytes) in held {
            let p = pools
                .iter()
                .find(|p| p.metadata.name.as_deref() == Some(*name));
            usage
                .entry((p.unwrap().spec.node.clone(), (*name).to_owned()))
                .or_default()
                .push(("v".to_owned(), *bytes));
        }
        pools
            .into_iter()
            .map(|mut p| {
                let shown = crate::pool::placeable(&crate::pool::view(&p, &usage, ready));
                p.status.as_mut().unwrap().placeable_bytes = Some(shown);
                Arc::new(p)
            })
            .collect()
    }

    fn ready(nodes: &[&str]) -> Vec<String> {
        nodes.iter().map(|n| (*n).to_owned()).collect()
    }

    fn params(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn one_replica_is_the_largest_pool_of_the_node() {
        let r = ready(&["n1", "n2", "n3"]);
        let pools = published(
            vec![
                pool("a", "n1", "fast", 100 * G),
                pool("b", "n1", "fast", 200 * G),
                pool("c", "n1", "slow", 400 * G),
                pool("d", "n2", "fast", 20 * G),
            ],
            &r,
            &[],
        );
        let fast = Some("fast");
        assert_eq!(largest(&pools, &r, fast, 1, Some("n1")), 190 * G);
        assert_eq!(largest(&pools, &r, fast, 1, Some("n2")), 19 * G);
        assert_eq!(largest(&pools, &r, Some("slow"), 1, Some("n1")), 380 * G);
        // No Pool of the tier, or none at all, on the node.
        assert_eq!(largest(&pools, &r, Some("slow"), 1, Some("n2")), 0);
        assert_eq!(largest(&pools, &r, fast, 1, Some("n3")), 0);
        assert_eq!(largest(&pools, &r, fast, 1, Some("elsewhere")), 0);
        // No segment: the best node; no tier: the best tier.
        assert_eq!(largest(&pools, &r, fast, 1, None), 190 * G);
        assert_eq!(largest(&pools, &r, None, 1, Some("n1")), 380 * G);
        assert_eq!(largest(&pools, &r, None, 1, Some("n3")), 0);
    }

    #[test]
    fn unavailable_pools_and_unready_nodes_hold_nothing() {
        let r = ready(&["n1", "n2"]);
        let pools = published(vec![pool("a", "n1", "fast", 100 * G)], &r, &[]);
        let fast = Some("fast");
        assert_eq!(largest(&pools, &r, fast, 1, Some("n1")), 95 * G);
        // Available False after the last pool pass: its stale number is not used.
        let mut gone = (*pools[0]).clone();
        gone.status.as_mut().unwrap().conditions.clear();
        assert_eq!(largest(&[Arc::new(gone)], &r, fast, 1, Some("n1")), 0);
        let mut deleting = (*pools[0]).clone();
        deleting.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                k8s_openapi::jiff::Timestamp::now(),
            ));
        assert_eq!(largest(&[Arc::new(deleting)], &r, fast, 1, Some("n1")), 0);
        assert_eq!(largest(&pools, &ready(&["n2"]), fast, 1, Some("n1")), 0);
        let mut unreported = (*pools[0]).clone();
        unreported.status = None;
        assert_eq!(largest(&[Arc::new(unreported)], &r, fast, 1, Some("n1")), 0);
    }

    #[test]
    fn two_replicas_on_two_nodes() {
        let r = ready(&["n1", "n2", "n3"]);
        let pools = published(
            vec![bare("a", "n1", 100 * G), bare("b", "n2", 50 * G)],
            &r,
            &[],
        );
        let fast = Some("fast");
        // Every node, n3 without a Pool too: it attaches diskless.
        for node in [Some("n1"), Some("n2"), Some("n3"), None] {
            assert_eq!(largest(&pools, &r, fast, 2, node), 50 * G, "{node:?}");
        }
        // Three replicas need a third node with a Pool.
        assert_eq!(largest(&pools, &r, fast, 3, Some("n1")), 0);
        assert_eq!(largest(&pools, &r, fast, 3, None), 0);
    }

    #[test]
    fn two_replicas_on_three_nodes() {
        let r = ready(&["n1", "n2", "n3", "n4"]);
        let pools = published(
            vec![
                bare("a", "n1", 100 * G),
                bare("b", "n2", 50 * G),
                bare("c", "n3", 80 * G),
            ],
            &r,
            &[],
        );
        let fast = Some("fast");
        // The second best node bounds two replicas, the third best three,
        // whichever node the pod is on.
        for node in [Some("n1"), Some("n2"), Some("n3"), Some("n4"), None] {
            assert_eq!(largest(&pools, &r, fast, 2, node), 80 * G, "{node:?}");
            assert_eq!(largest(&pools, &r, fast, 3, node), 50 * G, "{node:?}");
        }
    }

    #[test]
    fn replicas_need_distinct_nodes() {
        let r = ready(&["n1", "n2"]);
        let pools = published(
            vec![
                bare("a", "n1", 100 * G),
                bare("b", "n1", 90 * G),
                bare("c", "n2", 10 * G),
            ],
            &r,
            &[],
        );
        assert_eq!(largest(&pools, &r, Some("fast"), 2, Some("n1")), 10 * G);
        assert_eq!(largest(&pools, &r, Some("fast"), 2, None), 10 * G);
    }

    #[test]
    fn overcommit_and_reserve_through_placeable_bytes() {
        let r = ready(&["n1", "n2", "n3"]);
        let pools = published(
            vec![
                // promise (100 - 10) * 2 - 150 = 30 under free 100 - 10.
                policy_pool("a", "n1", "fast", (100 * G, 100 * G), (200, 10)),
                // promise 285 above free 20 - 5: written blocks bound it.
                policy_pool("b", "n2", "fast", (100 * G, 20 * G), (300, 5)),
                // Committed past the policy.
                policy_pool("c", "n3", "fast", (100 * G, 100 * G), (100, 5)),
            ],
            &r,
            &[("a", 150 * G), ("c", 96 * G)],
        );
        let fast = Some("fast");
        assert_eq!(largest(&pools, &r, fast, 1, Some("n1")), 30 * G);
        assert_eq!(largest(&pools, &r, fast, 1, Some("n2")), 15 * G);
        assert_eq!(largest(&pools, &r, fast, 1, Some("n3")), 0);
        assert_eq!(largest(&pools, &r, fast, 2, Some("n1")), 15 * G);
        assert_eq!(largest(&pools, &r, fast, 2, Some("n3")), 15 * G);
        assert_eq!(largest(&pools, &r, fast, 3, None), 0);
    }

    #[test]
    fn parameters_are_checked() {
        let r = ready(&["n1"]);
        let pools = published(vec![pool("a", "n1", "fast", 100 * G)], &r, &[]);
        let ask = |kv: &[(&str, &str)]| answer(&pools, &r, &params(kv), Some("n1"));
        assert_eq!(ask(&[("tier", "fast")]), Ok(95 * G));
        assert_eq!(ask(&[("tier", "fast"), ("replicas", "2")]), Ok(0));
        assert_eq!(ask(&[]), Ok(95 * G));
        assert_eq!(ask(&[("tier", "slow")]), Ok(0));
        assert_eq!(ask(&[("tier", "slow"), ("replicas", "2")]), Ok(0));
        assert!(ask(&[("tier", "fast"), ("replicas", "two")]).is_err());
        assert!(ask(&[("tier", "fast"), ("replicas", "4")]).is_err());
    }
}
