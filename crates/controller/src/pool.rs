//! Pool reconciler: finalizer, status.volumes and what placement sees.
//! Capacity and Available belong to the agent.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kube::{Api, ResourceExt};
use model::condition::{self, types};
use model::keys::FINALIZER_POOL;
use model::{Pool, ReclaimPolicy, Role, Volume};
use serde_json::json;

use crate::store::Stores;
use crate::{Ctx, Memory, apply_status, has_finalizer, set_finalizer};

/// (node, pool) to the (name, sizeBytes) of every Volume with a diskful
/// replica there; names sorted.
pub(crate) type Usage = HashMap<(String, String), Vec<(String, u64)>>;

/// One walk over the volumes, counting assignments not yet in the store.
pub(crate) fn usage(volumes: &[Arc<Volume>], memory: &Memory) -> Usage {
    let mut out = Usage::new();
    for v in volumes {
        let Some(a) = memory.assignment(v) else {
            continue;
        };
        for r in a.replicas.iter().filter(|r| r.role == Role::Diskful) {
            if let Some(pool) = &r.pool {
                out.entry((r.node.clone(), pool.clone()))
                    .or_default()
                    .push((v.name_any(), v.spec.size_bytes));
            }
        }
    }
    for held in out.values_mut() {
        held.sort();
    }
    out
}

/// Placer input for one Pool: capacity from the agent, readiness from the
/// core Node, committed sizes from every assignment.
pub(crate) fn view(p: &Pool, usage: &Usage, ready_nodes: &[String]) -> placer::PoolView {
    let name = p.name_any();
    let held = usage.get(&(p.spec.node.clone(), name.clone()));
    let status = p.status.as_ref();
    placer::PoolView {
        node: p.spec.node.clone(),
        tier: p.spec.tier.clone(),
        node_ready: ready_nodes.binary_search(&p.spec.node).is_ok(),
        available: p.metadata.deletion_timestamp.is_none()
            && status.is_some_and(|s| condition::is_true(&s.conditions, types::AVAILABLE)),
        total_bytes: status.and_then(|s| s.total_bytes).unwrap_or(0),
        free_bytes: status.and_then(|s| s.free_bytes).unwrap_or(0),
        committed_bytes: held.map_or(0, |h| h.iter().map(|(_, size)| size).sum()),
        volume_count: held.map_or(0, |h| u32::try_from(h.len()).unwrap_or(u32::MAX)),
        overcommit_percent: p.spec.overcommit_percent,
        reserve_percent: p.spec.reserve_percent,
        name,
    }
}

/// Rounded down to this, placeableBytes changes only on real movement, not
/// on every write the agent reports in freeBytes.
const PLACEABLE_STEP: u64 = 1 << 20;

/// What status.placeableBytes shows: 0 for a pool placement skips.
pub(crate) fn placeable(view: &placer::PoolView) -> u64 {
    if !(view.node_ready && view.available) {
        return 0;
    }
    placer::room(view) / PLACEABLE_STEP * PLACEABLE_STEP
}

/// Why growing `v` to `wanted` bytes is refused, if it is.
///
/// Only the increase is gated, and only by the promise bound of each pool
/// holding a diskful replica: a resize allocates no blocks, it only grows
/// what the volume may claim. A pool already past its policy refuses any
/// growth, since its promise is 0.
pub(crate) fn resize_refusal(
    v: &Volume,
    wanted: u64,
    pools: &[Arc<Pool>],
    usage: &Usage,
) -> Option<String> {
    let growth = wanted.saturating_sub(v.spec.size_bytes);
    if growth == 0 {
        return None;
    }
    let a = v.spec.assignment.as_ref()?;
    a.replicas
        .iter()
        .filter(|r| r.role == Role::Diskful)
        .filter_map(|r| r.pool.as_deref())
        .find_map(|name| {
            let Some(pool) = pools.iter().find(|p| p.name_any() == name) else {
                return Some(format!("pool {name} not found"));
            };
            let promise = placer::promise(&view(pool, usage, &[]));
            (growth > promise).then(|| {
                format!(
                    "pool {name}: growth of {growth} bytes exceeds the {promise} bytes its capacity policy still allows"
                )
            })
        })
}

/// Under reclaimPolicy Delete the agent removes the VG first and says so
/// with Released; switching to Retain lets the Pool go at once.
fn released(p: &Pool) -> bool {
    p.spec.devices.is_none()
        || p.spec.reclaim() == ReclaimPolicy::Retain
        || p.status
            .as_ref()
            .is_some_and(|s| condition::is_true(&s.conditions, types::RELEASED))
}

pub(crate) async fn reconcile_all(ctx: &Ctx<'_>, memory: &Memory) {
    let api: Api<Pool> = Api::all(ctx.client.clone());
    let volumes = Stores::sorted(&ctx.stores.volumes);
    let usage = usage(&volumes, memory);
    let pinned: HashSet<&str> = volumes
        .iter()
        .filter_map(|v| v.spec.pool.as_deref())
        .collect();
    let ready_nodes = ctx.stores.ready_nodes();
    let snapshots = ctx.stores.snapshots.state();
    // A snapshot LV outlives its Volume; the pool must too.
    let snapshot_held: HashSet<(&str, &str)> = snapshots
        .iter()
        .flat_map(|s| s.status.iter().flat_map(|st| &st.acks))
        .filter_map(|a| a.pool.as_deref().map(|p| (a.node.as_str(), p)))
        .collect();
    for pool in Stores::sorted(&ctx.stores.pools) {
        let name = pool.name_any();
        let wanted: Vec<String> = usage
            .get(&(pool.spec.node.clone(), name.clone()))
            .map(|h| h.iter().map(|(n, _)| n.clone()).collect())
            .unwrap_or_default();
        let current = pool
            .status
            .as_ref()
            .map(|s| s.volumes.clone())
            .unwrap_or_default();
        if pool.metadata.deletion_timestamp.is_some() {
            if !has_finalizer(pool.as_ref(), FINALIZER_POOL) {
                continue;
            }
            let held = pinned.contains(name.as_str())
                || snapshot_held.contains(&(pool.spec.node.as_str(), name.as_str()));
            if wanted.is_empty() && current.is_empty() && !held && released(&pool) {
                set_finalizer(&api, pool.as_ref(), FINALIZER_POOL, false).await;
                continue;
            }
        } else if !has_finalizer(pool.as_ref(), FINALIZER_POOL) {
            set_finalizer(&api, pool.as_ref(), FINALIZER_POOL, true).await;
            continue;
        }
        let view = view(&pool, &usage, &ready_nodes);
        let shown = (Some(view.committed_bytes), Some(placeable(&view)));
        let current_shown = pool
            .status
            .as_ref()
            .map(|s| (s.committed_bytes, s.placeable_bytes));
        if pool.status.is_none() || wanted != current || current_shown != Some(shown) {
            let status = json!({
                "volumes": wanted,
                "committedBytes": shown.0,
                "placeableBytes": shown.1,
            });
            apply_status(&api, &name, status).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{AssignedReplica, Assignment, PoolSpec, PoolStatus, VolumeSpec};

    const G: u64 = 1 << 30;

    fn pool(name: &str, node: &str, total: u64, free: u64) -> Arc<Pool> {
        let mut p = Pool::new(
            name,
            PoolSpec {
                node: node.into(),
                tier: "fast".into(),
                vg: "vg".into(),
                thin_pool: "thin".into(),
                overcommit_percent: model::DEFAULT_OVERCOMMIT_PERCENT,
                reserve_percent: model::DEFAULT_RESERVE_PERCENT,
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
        Arc::new(p)
    }

    fn volume(name: &str, size: u64, pools: &[(&str, &str)]) -> Arc<Volume> {
        let mut v = Volume::new(
            name,
            VolumeSpec {
                size_bytes: size,
                replicas: u32::try_from(pools.len()).unwrap(),
                tier: "fast".into(),
                pool: None,
                node: None,
                access_mode: model::AccessMode::ReadWriteOnce,
                attachments: Vec::new(),
                evict: Vec::new(),
                source: None,
                resolve: None,
                assignment: None,
            },
        );
        v.spec.assignment = Some(Assignment {
            replicas: pools
                .iter()
                .enumerate()
                .map(|(i, (node, pool))| AssignedReplica {
                    node: (*node).into(),
                    pool: Some((*pool).into()),
                    node_id: u8::try_from(i).unwrap(),
                    port: None,
                    role: Role::Diskful,
                })
                .collect(),
            minor: None,
            secret: None,
            initial_up_to_date_node: None,
        });
        Arc::new(v)
    }

    #[test]
    fn resize_gates_only_the_increase_against_the_promise() {
        // Promise of each pool: 95 GiB (100 less the 5 percent reserve).
        let pools = [
            pool("p1", "n1", 100 * G, 10 * G),
            pool("p2", "n2", 100 * G, 100 * G),
        ];
        let v = volume("v", 40 * G, &[("n1", "p1"), ("n2", "p2")]);
        let other = volume("o", 40 * G, &[("n1", "p1")]);
        let usage = usage(&[v.clone(), other], &Memory::default());
        // p1 holds 80: 15 more may be promised. Free space does not matter.
        assert_eq!(resize_refusal(&v, 55 * G, &pools, &usage), None);
        let why = resize_refusal(&v, 55 * G + 1, &pools, &usage).unwrap();
        assert!(why.starts_with("pool p1: growth of"), "{why}");
        // Not growing never fails.
        assert_eq!(resize_refusal(&v, 40 * G, &pools, &usage), None);
    }

    #[test]
    fn resize_past_a_lowered_policy_is_refused() {
        let pools = [pool("p1", "n1", 100 * G, 100 * G)];
        let v = volume("v", 99 * G, &[("n1", "p1")]);
        let usage = usage(std::slice::from_ref(&v), &Memory::default());
        assert_eq!(resize_refusal(&v, 99 * G, &pools, &usage), None);
        assert!(resize_refusal(&v, 99 * G + 1, &pools, &usage).is_some());
        let gone = resize_refusal(&v, 100 * G, &[], &usage).unwrap();
        assert_eq!(gone, "pool p1 not found");
    }

    #[test]
    fn placeable_is_zero_where_placement_skips_and_rounds_to_mib() {
        let p = pool("p1", "n1", 100 * G, 50 * G + 12345);
        let ready = ["n1".to_owned()];
        let v = view(&p, &Usage::new(), &ready);
        assert_eq!(placeable(&v), 45 * G);
        assert_eq!(placeable(&view(&p, &Usage::new(), &[])), 0);
        let mut gone = (*p).clone();
        gone.status.as_mut().unwrap().conditions.clear();
        assert_eq!(placeable(&view(&gone, &Usage::new(), &ready)), 0);
    }
}
