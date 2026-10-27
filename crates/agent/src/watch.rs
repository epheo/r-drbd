//! Reflector caches of Pool, Volume, Snapshot and Node (every node: peers'
//! InternalIPs are DRBD addresses), the agent Leases (peers' runs), and the
//! trigger that wakes the reconciler.

use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::Node;
use kube::client::{ClientBuilder, ConfigExt};
use kube::runtime::reflector::{self, Store};
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Client, Resource};
use model::{Pool, Snapshot, Volume};
use serde::de::DeserializeOwned;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tower::{BoxError, ServiceBuilder};

/// Changes within this window share one pass.
pub const COALESCE: Duration = Duration::from_millis(250);
/// A pass runs at least this often, so host drift and failed writes heal.
pub const RESYNC: Duration = Duration::from_secs(30);

/// A watch reads a quiet socket: a peer gone without FIN or RST (a host
/// killed or cut off) leaves its store stale until kube-runtime's 295 s
/// idle timeout. Keepalive probes find it within USER_TIMEOUT.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(10);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);
const KEEPALIVE_PROBES: u32 = 3;
/// Linux ends a failed keepalive at this bound, and applies it to unacked
/// writes, which keepalive never probes.
const USER_TIMEOUT: Duration =
    KEEPALIVE_IDLE.saturating_add(KEEPALIVE_INTERVAL.saturating_mul(KEEPALIVE_PROBES));

fn connector(connect_timeout: Option<Duration>) -> HttpConnector {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_connect_timeout(connect_timeout);
    http.set_keepalive(Some(KEEPALIVE_IDLE));
    http.set_keepalive_interval(Some(KEEPALIVE_INTERVAL));
    http.set_keepalive_retries(Some(KEEPALIVE_PROBES));
    http.set_tcp_user_timeout(Some(USER_TIMEOUT));
    http
}

/// The watches' client: kube's config-driven layers on keepalive sockets,
/// since kube::Config cannot set keepalive. No read timeout: it would end
/// a quiet watch. No retry or trace layer: the watcher backs off and
/// `spawn` logs its errors.
pub fn client(config: &kube::Config) -> Result<Client, kube::Error> {
    let https = config.rustls_https_connector_with_connector(connector(config.connect_timeout))?;
    let http: hyper_util::client::legacy::Client<_, kube::client::Body> =
        hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(https);
    let service = ServiceBuilder::new()
        .layer(config.base_uri_layer())
        .option_layer(config.auth_layer()?)
        .layer(config.extra_headers_layer()?)
        .map_err(BoxError::from)
        .service(http);
    Ok(ClientBuilder::new(service, config.default_namespace.clone()).build())
}

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
    pub leases: Store<Lease>,
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
        self.nodes.wait_until_ready().await?;
        self.leases.wait_until_ready().await
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
pub fn start(client: &Client, namespace: &str, trigger: &Arc<Trigger>) -> (Stores, JoinSet<()>) {
    let mut tasks = JoinSet::new();
    let all = watcher::Config::default;
    let stores = Stores {
        pools: spawn(&mut tasks, Api::all(client.clone()), all(), trigger.clone()),
        volumes: spawn(&mut tasks, Api::all(client.clone()), all(), trigger.clone()),
        snapshots: spawn(&mut tasks, Api::all(client.clone()), all(), trigger.clone()),
        nodes: spawn(&mut tasks, Api::all(client.clone()), all(), trigger.clone()),
        // Every agent renews each 10 s: no pass per renewal, passes read the cache.
        leases: spawn(
            &mut tasks,
            Api::namespaced(client.clone(), namespace),
            all().labels(&format!("{}=true", model::keys::AGENT_LEASE_LABEL)),
            Arc::new(Trigger::default()),
        ),
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

    #[tokio::test]
    async fn watch_sockets_probe_a_silent_peer() {
        use rustix::net::sockopt;
        use tower::ServiceExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        let io = connector(None).oneshot(uri.parse().unwrap()).await.unwrap();
        let sock = io.inner();
        assert!(sockopt::socket_keepalive(sock).unwrap());
        assert_eq!(sockopt::tcp_keepidle(sock).unwrap(), KEEPALIVE_IDLE);
        assert_eq!(sockopt::tcp_keepintvl(sock).unwrap(), KEEPALIVE_INTERVAL);
        assert_eq!(sockopt::tcp_keepcnt(sock).unwrap(), KEEPALIVE_PROBES);
        let user_timeout = sockopt::tcp_user_timeout(sock).unwrap();
        assert_eq!(u128::from(user_timeout), USER_TIMEOUT.as_millis());
    }

    #[tokio::test]
    async fn watch_client_sends_the_config_token_to_its_server() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let mut config = kube::Config::new(url.parse().unwrap());
        config.auth_info.token = Some(String::from("t0ken").into());
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut buf).await.unwrap();
                assert_ne!(n, 0, "request ended before its head");
                head.extend_from_slice(&buf[..n]);
            }
            let body = r#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"}}"#;
            let len = body.len();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {len}\r\n\r\n{body}"
            );
            s.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8(head).unwrap().to_ascii_lowercase()
        });
        let node = Api::<Node>::all(client(&config).unwrap())
            .get("n1")
            .await
            .unwrap();
        assert_eq!(node.metadata.name.as_deref(), Some("n1"));
        let head = server.await.unwrap();
        assert!(head.starts_with("get /api/v1/nodes/n1 "), "{head}");
        assert!(head.contains("authorization: bearer t0ken"), "{head}");
    }
}
