//! Live reads of a Volume from the API server for the two restore
//! decisions that must not rest on the watch cache.
//!
//! A GET without resourceVersion is served from etcd, so it is
//! linearizable: a fallback peer brings its disk up only after such a read
//! shows its own entry without restoredFrom, and the initial node clears
//! the bitmap only after such a read, taken once it saw every peer
//! connected, shows every peer's proof. The peer's committed entry then
//! precedes its attach, which precedes the initial node's read.

use kube::{Api, Client};
use model::Volume;

#[async_trait::async_trait]
pub trait LiveVolumes: std::fmt::Debug + Send + Sync + 'static {
    /// The Volume straight from the API server, never a cache.
    async fn get(&self, volume: &str) -> Result<Volume, String>;
}

/// Without an API client nothing is proven: every decision that needs a
/// live read waits.
#[derive(Debug, Default)]
pub struct NoLiveVolumes;

#[async_trait::async_trait]
impl LiveVolumes for NoLiveVolumes {
    async fn get(&self, _: &str) -> Result<Volume, String> {
        Err("no API client for a live read".to_owned())
    }
}

#[derive(Clone)]
pub struct KubeLiveVolumes {
    client: Client,
}

impl KubeLiveVolumes {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for KubeLiveVolumes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeLiveVolumes").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl LiveVolumes for KubeLiveVolumes {
    async fn get(&self, volume: &str) -> Result<Volume, String> {
        Api::<Volume>::all(self.client.clone())
            .get(volume)
            .await
            .map_err(|e| e.to_string())
    }
}
