//! Pool reconciler: finalizer and status.volumes. Capacity and Available belong to the agent.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kube::{Api, ResourceExt};
use model::keys::FINALIZER_POOL;
use model::{Pool, Role, Volume};
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

pub(crate) async fn reconcile_all(ctx: &Ctx<'_>, memory: &Memory) {
    let api: Api<Pool> = Api::all(ctx.client.clone());
    let volumes = Stores::sorted(&ctx.stores.volumes);
    let usage = usage(&volumes, memory);
    let pinned: HashSet<&str> = volumes
        .iter()
        .filter_map(|v| v.spec.pool.as_deref())
        .collect();
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
            if wanted.is_empty() && current.is_empty() && !held {
                set_finalizer(&api, pool.as_ref(), FINALIZER_POOL, false).await;
                continue;
            }
        } else if !has_finalizer(pool.as_ref(), FINALIZER_POOL) {
            set_finalizer(&api, pool.as_ref(), FINALIZER_POOL, true).await;
            continue;
        }
        if pool.status.is_none() || wanted != current {
            apply_status(&api, &name, json!({ "volumes": wanted })).await;
        }
    }
}
