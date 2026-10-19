//! One reflector per kind and a change counter waiters block on.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::runtime::reflector::{self, ObjectRef, Store, store::Writer};
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Client, Resource, ResourceExt};
use model::{Pool, Snapshot, Volume};
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tracing::warn;

use crate::Error;

#[derive(Clone)]
pub struct Stores {
    pub pools: Store<Pool>,
    pub volumes: Store<Volume>,
    pub snapshots: Store<Snapshot>,
    pub nodes: Store<Node>,
    /// Leases of the product namespace; agent heartbeats among them.
    pub leases: Store<Lease>,
    /// Per agent Lease name: when this process last saw its renewTime
    /// change. Staleness is judged on this clock, as leader.rs does, so
    /// clock skew between nodes cannot mark a live agent unreachable.
    renewals: Arc<Mutex<HashMap<String, Instant>>>,
    lease_stale: Duration,
    changed: watch::Receiver<u64>,
}

impl std::fmt::Debug for Stores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stores")
            .field("pools", &self.pools.len())
            .field("volumes", &self.volumes.len())
            .field("snapshots", &self.snapshots.len())
            .field("nodes", &self.nodes.len())
            .field("leases", &self.leases.len())
            .finish_non_exhaustive()
    }
}

impl Stores {
    /// A receiver that wakes on every applied or deleted object of any kind.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.clone()
    }

    pub async fn wait_ready(&self) -> Result<(), Error> {
        let dropped = |_| Error::WatchEnded("reflector writer");
        self.pools.wait_until_ready().await.map_err(dropped)?;
        self.volumes.wait_until_ready().await.map_err(dropped)?;
        self.snapshots.wait_until_ready().await.map_err(dropped)?;
        self.nodes.wait_until_ready().await.map_err(dropped)?;
        self.leases.wait_until_ready().await.map_err(dropped)?;
        Ok(())
    }

    pub fn pool(&self, name: &str) -> Option<Arc<Pool>> {
        self.pools.get(&ObjectRef::new(name))
    }

    pub fn volume(&self, name: &str) -> Option<Arc<Volume>> {
        self.volumes.get(&ObjectRef::new(name))
    }

    pub fn snapshot(&self, name: &str) -> Option<Arc<Snapshot>> {
        self.snapshots.get(&ObjectRef::new(name))
    }

    pub fn node(&self, name: &str) -> Option<Arc<Node>> {
        self.nodes.get(&ObjectRef::new(name))
    }

    /// When this process last saw the node's agent Lease renewTime change.
    pub fn agent_renewed_at(&self, node: &str) -> Option<Instant> {
        lock(&self.renewals)
            .get(&model::keys::agent_lease_name(node))
            .copied()
    }

    /// The node's agent renewed its Lease within the stale window, as seen
    /// from here; false without a Lease.
    pub fn agent_fresh(&self, node: &str) -> bool {
        lock(&self.renewals)
            .get(&model::keys::agent_lease_name(node))
            .is_some_and(|t| t.elapsed() < self.lease_stale)
    }

    /// Time until the first fresh agent Lease goes stale, so the pass that
    /// marks its node unreachable runs without waiting for an event.
    pub fn next_lease_expiry(&self) -> Option<Duration> {
        lock(&self.renewals)
            .values()
            .filter_map(|t| self.lease_stale.checked_sub(t.elapsed()))
            .filter(|d| !d.is_zero())
            .min()
            .map(|d| d + Duration::from_millis(100))
    }

    /// Names of the Ready nodes, sorted for binary search.
    pub fn ready_nodes(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .nodes
            .state()
            .iter()
            .filter(|n| node_ready(n))
            .map(|n| n.name_any())
            .collect();
        out.sort();
        out
    }

    /// Objects sorted by name, so passes and allocation are deterministic.
    pub fn sorted<K>(store: &Store<K>) -> Vec<Arc<K>>
    where
        K: Resource + Clone + 'static,
        K::DynamicType: Eq + Hash + Clone,
    {
        let mut all = store.state();
        all.sort_by_key(|o| o.name_any());
        all
    }
}

/// Node Ready condition is True.
pub fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
}

/// Agent Lease staleness threshold.
pub const LEASE_STALE: Duration = Duration::from_secs(model::keys::LEASE_STALE_SECS);

/// A poisoned lock only means a panic elsewhere; the map stays usable.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Starts the five reflectors; the future only resolves when one of them
/// ends for good. Leases are watched in `namespace` only; an agent whose
/// renewTime did not change for `lease_stale` counts as unreachable.
pub fn start(
    client: Client,
    namespace: &str,
    lease_stale: Duration,
) -> (Stores, impl Future<Output = Error> + Send + use<>) {
    let (tx, rx) = watch::channel(0_u64);
    let tx = Arc::new(tx);
    let (pools, pw) = reflector::store();
    let (volumes, vw) = reflector::store();
    let (snapshots, sw) = reflector::store();
    let (nodes, nw) = reflector::store();
    let (leases, lw) = reflector::store();
    let stores = Stores {
        pools,
        volumes,
        snapshots,
        nodes,
        leases,
        renewals: Arc::default(),
        lease_stale,
        changed: rx,
    };
    let renewals = stores.renewals.clone();
    let lease_api = Api::<Lease>::namespaced(client.clone(), namespace);
    let fut = {
        let tx1 = tx.clone();
        let tx2 = tx.clone();
        let tx3 = tx.clone();
        let tx4 = tx.clone();
        let c = client;
        async move {
            tokio::select! {
                e = drive(Api::<Pool>::all(c.clone()), pw, tx1, "pools", |_| {}) => e,
                e = drive(Api::<Volume>::all(c.clone()), vw, tx2, "volumes", |_| {}) => e,
                e = drive(Api::<Snapshot>::all(c.clone()), sw, tx3, "snapshots", |_| {}) => e,
                e = drive(Api::<Node>::all(c), nw, tx4, "nodes", slim_node) => e,
                e = drive_leases(lease_api, lw, tx, renewals, lease_stale) => e,
            }
        }
    };
    (stores, fut)
}

/// Only the Ready condition and the name of a Node are read; images and
/// the rest are dropped to keep the cache small.
fn slim_node(node: &mut Node) {
    if let Some(status) = node.status.as_mut() {
        status.images = None;
        status.volumes_attached = None;
        status.volumes_in_use = None;
        status.node_info = None;
    }
}

async fn drive<K>(
    api: Api<K>,
    writer: Writer<K>,
    tx: Arc<watch::Sender<u64>>,
    kind: &'static str,
    slim: fn(&mut K),
) -> Error
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Default + Eq + Hash + Clone + Send + Sync,
{
    let stream = watcher(api, watcher::Config::default())
        .default_backoff()
        .modify(move |o: &mut K| {
            o.managed_fields_mut().clear();
            slim(o);
        })
        .reflect(writer);
    let mut stream = std::pin::pin!(stream);
    while let Some(event) = stream.next().await {
        match event {
            Ok(watcher::Event::Init | watcher::Event::InitApply(_)) => {}
            Ok(_) => tx.send_modify(|n| *n = n.wrapping_add(1)),
            Err(e) => warn!(kind, error = %e, "watch error; backing off"),
        }
    }
    Error::WatchEnded(kind)
}

fn wall_stale(renew: Option<&MicroTime>, stale: Duration) -> bool {
    renew.is_none_or(|r| {
        Duration::try_from(k8s_openapi::jiff::Timestamp::now().duration_since(r.0))
            .is_ok_and(|d| d >= stale)
    })
}

/// Renewals arrive every few seconds per node; only a change of freshness
/// wakes the reconcilers, and expiry is a timer of the reconcile pass.
async fn drive_leases(
    api: Api<Lease>,
    writer: Writer<Lease>,
    tx: Arc<watch::Sender<u64>>,
    renewals: Arc<Mutex<HashMap<String, Instant>>>,
    stale: Duration,
) -> Error {
    type Seen = HashMap<String, (Option<MicroTime>, Instant)>;
    type Listed = Vec<(String, Option<MicroTime>)>;
    let prefix = model::keys::agent_lease_name("");
    let stream = watcher(api, watcher::Config::default())
        .default_backoff()
        .modify(|o: &mut Lease| o.managed_fields_mut().clear())
        .reflect(writer);
    let mut stream = std::pin::pin!(stream);
    let mut seen: Seen = HashMap::new();
    // A relist keeps the observation of Leases whose renewTime it did not
    // change; the others start at InitDone.
    let mut relist: Option<(Seen, Listed)> = None;
    let renew_of = |l: &Lease| l.spec.as_ref().and_then(|s| s.renew_time.clone());
    while let Some(event) = stream.next().await {
        let now = Instant::now();
        let wake = match event {
            Ok(watcher::Event::Init) => {
                relist = Some((std::mem::take(&mut seen), Vec::new()));
                false
            }
            Ok(watcher::Event::InitApply(l)) => {
                let name = l.name_any();
                if let Some((_, listed)) = relist.as_mut()
                    && name.starts_with(&prefix)
                {
                    listed.push((name, renew_of(&l)));
                }
                false
            }
            Ok(watcher::Event::InitDone) => {
                let (old, listed) = relist.take().unwrap_or_default();
                for (name, renew) in listed {
                    let at = match old.get(&name) {
                        Some((r, at)) if *r == renew => *at,
                        // Nothing observed yet (a new leader): a renewTime
                        // already stale by the wall clock starts stale, so a
                        // long-dead agent does not look alive for a window.
                        None if wall_stale(renew.as_ref(), stale) => {
                            now.checked_sub(stale).unwrap_or(now)
                        }
                        _ => now,
                    };
                    seen.insert(name, (renew, at));
                }
                *lock(&renewals) = seen.iter().map(|(n, (_, at))| (n.clone(), *at)).collect();
                true
            }
            Ok(watcher::Event::Apply(l)) => {
                let name = l.name_any();
                if !name.starts_with(&prefix) {
                    continue;
                }
                let renew = renew_of(&l);
                match seen.get(&name) {
                    Some((r, _)) if *r == renew => false,
                    previous => {
                        let was_fresh = previous.is_some_and(|(_, at)| at.elapsed() < stale);
                        seen.insert(name.clone(), (renew, now));
                        lock(&renewals).insert(name, now);
                        !was_fresh
                    }
                }
            }
            Ok(watcher::Event::Delete(l)) => {
                let name = l.name_any();
                lock(&renewals).remove(&name);
                seen.remove(&name).is_some()
            }
            Err(e) => {
                warn!(kind = "leases", error = %e, "watch error; backing off");
                false
            }
        };
        if wake {
            tx.send_modify(|n| *n = n.wrapping_add(1));
        }
    }
    Error::WatchEnded("leases")
}
