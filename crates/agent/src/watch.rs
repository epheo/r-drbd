//! Reflector caches of Pool, Volume, Snapshot and Node (every node: peers'
//! InternalIPs are DRBD addresses), and the trigger that wakes the reconciler.

use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use k8s_openapi::api::core::v1::Node;
use kube::runtime::reflector::{self, Store};
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Client, Resource};
use model::{Pool, Snapshot, Volume};
use serde::de::DeserializeOwned;
use tokio::sync::Notify;
use tokio::task::JoinSet;

/// Changes within this window share one pass.
pub const COALESCE: Duration = Duration::from_millis(250);
/// A pass runs at least this often, so host drift and failed writes heal.
pub const RESYNC: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
pub struct Trigger {
    notify: Notify,
}

impl Trigger {
    pub fn fire(&self) {
        self.notify.notify_one();
    }

    /// Returns after a change (plus the coalescing window) or after `resync`.
    pub async fn wait(&self, coalesce: Duration, resync: Duration) {
        self.changed_or(resync).await;
        tokio::time::sleep(coalesce).await;
        self.drain();
    }

    /// A change or `resync`, without the window. Cancel safe: a dropped
    /// call consumes no change, so it can race another trigger in a select.
    pub async fn changed_or(&self, resync: Duration) {
        tokio::select! {
            () = self.notify.notified() => {}
            () = tokio::time::sleep(resync) => {}
        }
    }

    pub async fn changed(&self) {
        self.notify.notified().await;
    }

    /// Changes during the window are covered by the coming pass.
    pub fn drain(&self) {
        let _ = self.notify.notified().now_or_never();
    }
}

#[derive(Clone)]
pub struct Stores {
    pub pools: Store<Pool>,
    pub volumes: Store<Volume>,
    pub snapshots: Store<Snapshot>,
    pub nodes: Store<Node>,
}

impl std::fmt::Debug for Stores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stores").finish_non_exhaustive()
    }
}

impl Stores {
    /// Waits for the initial list of every watch.
    pub async fn ready(&self) -> Result<(), reflector::store::WriterDropped> {
        self.pools.wait_until_ready().await?;
        self.volumes.wait_until_ready().await?;
        self.snapshots.wait_until_ready().await?;
        self.nodes.wait_until_ready().await
    }
}

fn spawn<K>(
    tasks: &mut JoinSet<()>,
    api: Api<K>,
    config: watcher::Config,
    trigger: Arc<Trigger>,
) -> Store<K>
where
    K: Resource + Clone + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
{
    let (reader, writer) = reflector::store();
    let kind = K::kind(&K::DynamicType::default()).into_owned();
    let stream = reflector::reflector(writer, watcher(api, config).default_backoff());
    tasks.spawn(async move {
        stream
            .for_each(|ev| {
                match ev {
                    Ok(_) => trigger.fire(),
                    Err(e) => tracing::warn!(kind, error = %e, "watch error"),
                }
                futures::future::ready(())
            })
            .await;
    });
    reader
}

/// Starts the watches; the tasks run until the JoinSet is dropped.
pub fn start(client: &Client, trigger: &Arc<Trigger>) -> (Stores, JoinSet<()>) {
    let mut tasks = JoinSet::new();
    let all = watcher::Config::default;
    let stores = Stores {
        pools: spawn(&mut tasks, Api::all(client.clone()), all(), trigger.clone()),
        volumes: spawn(&mut tasks, Api::all(client.clone()), all(), trigger.clone()),
        snapshots: spawn(&mut tasks, Api::all(client.clone()), all(), trigger.clone()),
        nodes: spawn(&mut tasks, Api::all(client.clone()), all(), trigger.clone()),
    };
    (stores, tasks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn coalesces_and_resyncs() {
        let t = Arc::new(Trigger::default());
        t.fire();
        t.fire();
        let started = tokio::time::Instant::now();
        t.wait(COALESCE, RESYNC).await;
        assert_eq!(started.elapsed(), COALESCE);
        let t2 = t.clone();
        let h = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            t2.fire();
        });
        // A change inside the coalescing window does not cause another pass.
        let started = tokio::time::Instant::now();
        t.fire();
        t.wait(COALESCE, RESYNC).await;
        h.await.unwrap();
        assert_eq!(started.elapsed(), COALESCE);
        let started = tokio::time::Instant::now();
        t.wait(COALESCE, RESYNC).await;
        assert_eq!(started.elapsed(), RESYNC + COALESCE);
    }
}
