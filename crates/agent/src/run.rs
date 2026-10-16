//! Agent entry point: startup, then the watch, reconcile and CSI tasks.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kube::{Client, ResourceExt};

use crate::events2::Tracker;
use crate::exec::TokioExec;
use crate::reconcile::{DrbdEnv, Reconciler};
use crate::watch::{self, Trigger};
use crate::{Exec, csi, drbd, node};

const DRBD_CONF_DIR: &str = "/etc/drbd.d";
const DRBD_INCLUDE_FILE: &str = "r-drbd.res";

#[derive(Debug, Clone)]
pub struct Config {
    /// Kubernetes node name; also the DRBD host name and the CSI node id.
    pub node: String,
    /// Namespace of the heartbeat Lease.
    pub namespace: String,
    /// `unix:///path/csi.sock` or a bare path.
    pub csi_endpoint: String,
    /// DRBD resource files, included from /etc/drbd.d/r-drbd.res.
    pub res_dir: PathBuf,
    /// Prepended to every external command, e.g. `[/proc/self/exe,
    /// host-exec, <ns>, --]` to run tools in the host mount namespace.
    pub host_exec_prefix: Option<Vec<String>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("kubernetes client: {0}")]
    Kube(#[from] kube::Error),
    #[error("kubernetes config: {0}")]
    KubeConfig(String),
    #[error("watch caches: {0}")]
    Watch(String),
    #[error("csi server: {0}")]
    Csi(String),
}

impl Error {
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

/// Writes (status, Lease, fileSystem marker) use a client that gives up on
/// a dead connection: connect within 5 s, an answer within 30 s, so the
/// write backoff retries on a fresh connection instead of hanging about
/// 30 s per try. The read timeout is a socket idle timeout, so the
/// watches keep their own client without it.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(30);

async fn clients() -> Result<(Client, Client), Error> {
    let config = kube::Config::infer()
        .await
        .map_err(|e| Error::KubeConfig(e.to_string()))?;
    let watch = Client::try_from(config.clone())?;
    let mut writes = config;
    writes.connect_timeout = Some(CONNECT_TIMEOUT);
    writes.read_timeout = Some(READ_TIMEOUT);
    Ok((Client::try_from(writes)?, watch))
}

/// Bound on finishing the running pass and in-flight CSI operations at shutdown.
const DRAIN: Duration = Duration::from_secs(60);
/// Shutdown when every LVM change ends within its wait: the drain, an LVM
/// change the abandoned pass left running, the CSI server's 5 s. The pod
/// grace must exceed it; a change stuck longer is waited for until the
/// grace runs out, the only hard stop, since killing it could leave a
/// thin pool suspended.
const SHUTDOWN_MAX: Duration = DRAIN
    .saturating_add(crate::exec::LVM_CHANGE_WAIT)
    .saturating_add(Duration::from_secs(5));
/// How often shutdown says it still waits for an LVM change.
const CHANGE_WAIT_LOG: Duration = Duration::from_secs(30);

/// Runs until SIGTERM or Ctrl-C; returns early only on a fatal startup
/// error or when the CSI server dies. On a signal the running pass, its
/// status writes and in-flight CSI operations finish first (bounded).
pub async fn run(cfg: Config) -> Result<(), Error> {
    prepare_host(&cfg.res_dir, Path::new(DRBD_CONF_DIR))?;
    let (client, watch_client) = clients().await?;
    let runner = Arc::new(TokioExec::new(cfg.host_exec_prefix.clone()));
    let exec: Arc<dyn Exec> = runner.clone();
    let tools = node::probe(exec.as_ref()).await;
    // Spawned: the first heartbeat never waits on systemctl.
    let guard_exec = exec.clone();
    tokio::spawn(async move { node::check_shutdown_guard(guard_exec.as_ref()).await });

    let trigger = Arc::new(Trigger::default());
    let changes = Arc::new(Trigger::default());
    let (stores, _watches) = watch::start(&watch_client, &cfg.namespace, &trigger);
    let tracker = Arc::new(Mutex::new(Tracker::default()));
    let features = tools.drbd();
    let events = features.is_some().then(|| {
        tokio::spawn(drbd::events2(
            cfg.host_exec_prefix.clone(),
            tracker.clone(),
            drbd::Notify {
                changes: changes.clone(),
                pass: trigger.clone(),
            },
        ))
    });
    if events.is_none() {
        tracing::warn!("DRBD not found: replicated volumes cannot be served on this node");
    }
    let reconciler = Reconciler::new(cfg.node.clone(), exec.clone()).with_drbd(DrbdEnv::new(
        features,
        cfg.res_dir.clone(),
        tracker,
    ));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let socket = csi::socket_path(&cfg.csi_endpoint);
    let listener = csi::bind(&socket)?;
    let node_service = csi::NodeService::new(
        cfg.node.clone(),
        exec.clone(),
        Arc::new(stores.volumes.clone()),
        Arc::new(csi::KubeFsMarker::new(client.clone())),
    );
    let inflight = node_service.inflight();
    let mut csi_stop = stop_rx.clone();
    let mut csi_task = tokio::spawn(csi::serve(listener, node_service, async move {
        stopped(&mut csi_stop).await;
    }));
    let heartbeat = tokio::spawn(node::heartbeat(
        client.clone(),
        cfg.namespace.clone(),
        cfg.node.clone(),
        tools,
    ));
    let mut reconcile = tokio::spawn(reconcile_loop(
        client,
        exec.clone(),
        reconciler,
        stores,
        Triggers {
            pass: trigger,
            changes,
        },
        stop_rx,
    ));

    let result = tokio::select! {
        r = &mut reconcile => r.unwrap_or_else(|e| Err(Error::Watch(e.to_string()))),
        r = &mut csi_task => Err(Error::Csi(match r {
            Ok(Ok(())) => "stopped".to_owned(),
            Ok(Err(e)) => e.to_string(),
            Err(e) => e.to_string(),
        })),
        r = shutdown() => r.map(|()| tracing::info!(bound = ?SHUTDOWN_MAX, "shutting down")),
    };
    let _ = stop_tx.send(true);
    let pass = async {
        if !reconcile.is_finished() && tokio::time::timeout(DRAIN, &mut reconcile).await.is_err() {
            tracing::warn!("reconcile pass still running after {DRAIN:?}, abandoned");
            reconcile.abort();
        }
    };
    let csi_ops = async {
        if !inflight.wait_idle(DRAIN).await {
            tracing::warn!("CSI operations still running after {DRAIN:?}, abandoned");
        }
    };
    tokio::join!(pass, csi_ops);
    while !runner.wait_idle(CHANGE_WAIT_LOG).await {
        tracing::warn!("waiting for a running LVM change before exit");
    }
    if !csi_task.is_finished() {
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut csi_task).await;
        csi_task.abort();
    }
    heartbeat.abort();
    if let Some(e) = events {
        e.abort();
    }
    result
}

/// The host LVM check, logged. Runs once this node has a Pool, until a
/// version is read: a failed probe blocks nothing and is tried next pass.
async fn check_lvm(exec: &dyn Exec, rec: &mut Reconciler) -> bool {
    let host = crate::storage::probe_host(exec).await;
    if let Some(w) = &host.warning {
        tracing::warn!("{w}");
    }
    if host.version.is_none() {
        tracing::warn!("LVM version unreadable; checking again next pass");
        return false;
    }
    if let Some(e) = &host.unsupported {
        tracing::error!("{e}; every Pool on this node reports Available False");
    }
    rec.set_lvm_unsupported(host.unsupported);
    true
}

async fn stopped(rx: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = rx.wait_for(|s| *s).await;
}

struct Triggers {
    /// Watch changes, the resync timer, events2 becoming available or lost.
    pass: Arc<Trigger>,
    /// events2 lines: a status refresh of the touched volumes only.
    changes: Arc<Trigger>,
}

/// Passes and refreshes run to completion; `stop` is only looked at between them.
async fn reconcile_loop(
    client: Client,
    exec: Arc<dyn Exec>,
    mut rec: Reconciler,
    stores: watch::Stores,
    triggers: Triggers,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<(), Error> {
    tokio::select! {
        r = stores.ready() => r.map_err(|e| Error::Watch(e.to_string()))?,
        () = stopped(&mut stop) => return Ok(()),
    }
    let node = rec.node().to_owned();
    tracing::info!(node, "caches ready, reconciling");
    let mut lvm_checked = false;
    loop {
        rec.set_addresses(addresses(&stores));
        rec.set_peer_leases(peer_leases(&stores));
        let pools = stores.pools.state();
        if !lvm_checked && pools.iter().any(|p| p.spec.node == node) {
            lvm_checked = check_lvm(exec.as_ref(), &mut rec).await;
        }
        let volumes = stores.volumes.state();
        let snapshots = stores.snapshots.state();
        apply(&client, &node, rec.pass(&pools, &volumes, &snapshots).await).await;
        // Fixed per pass so a stream of refreshes cannot push the resync out.
        let resync = tokio::time::Instant::now() + watch::RESYNC;
        loop {
            tokio::select! {
                biased;
                () = stopped(&mut stop) => return Ok(()),
                () = triggers.pass.changed() => {
                    tokio::time::sleep(watch::COALESCE).await;
                    triggers.pass.drain();
                    break;
                }
                () = tokio::time::sleep_until(resync) => break,
                () = triggers.changes.changed() => {
                    tokio::time::sleep(watch::COALESCE).await;
                    triggers.changes.drain();
                    let volumes = stores.volumes.state();
                    apply(&client, &node, rec.refresh(&volumes).await).await;
                    if rec.take_pass_wanted() {
                        break;
                    }
                }
            }
        }
    }
}

fn peer_leases(stores: &watch::Stores) -> BTreeMap<String, crate::peer::PeerLease> {
    stores
        .leases
        .state()
        .iter()
        .filter_map(|l| {
            let node = l
                .name_any()
                .strip_prefix(&model::keys::agent_lease_name(""))?
                .to_owned();
            let spec = l.spec.as_ref()?;
            Some((
                node,
                crate::peer::PeerLease {
                    acquired: spec.acquire_time.as_ref().map(|t| t.0),
                    renewed: spec.renew_time.as_ref().map(|t| t.0),
                },
            ))
        })
        .collect()
}

fn addresses(stores: &watch::Stores) -> BTreeMap<String, std::net::IpAddr> {
    stores
        .nodes
        .state()
        .iter()
        .filter_map(|n| Some((n.name_any(), drbd::internal_ip(n)?)))
        .collect()
}

async fn apply(client: &Client, node: &str, writes: Vec<crate::reconcile::StatusWrite>) {
    let results = crate::retry::each_with_backoff(
        &writes,
        &crate::retry::BACKOFF,
        crate::retry::retryable,
        |w| w.apply(client, node),
    )
    .await;
    let mut skipped = 0;
    for (w, res) in writes.iter().zip(results) {
        match res {
            Some(Err(e)) => {
                tracing::warn!(object = w.name(), error = %e, "status write failed, retried next pass");
            }
            None => skipped += 1,
            Some(Ok(())) => {}
        }
    }
    if skipped > 0 {
        tracing::warn!(
            skipped,
            "API unreachable: remaining status writes left to the next pass"
        );
    }
}

async fn shutdown() -> Result<(), Error> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| Error::io("install SIGTERM handler", e))?;
    tokio::select! {
        _ = term.recv() => {}
        r = tokio::signal::ctrl_c() => r.map_err(|e| Error::io("install SIGINT handler", e))?,
    }
    Ok(())
}

/// Creates the res dir and, when DRBD is configured on the host, the include
/// file; an existing include file is never overwritten.
pub(crate) fn prepare_host(res_dir: &Path, drbd_dir: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(res_dir)
        .map_err(|e| Error::io(format!("create {}", res_dir.display()), e))?;
    if !drbd_dir.is_dir() {
        return Ok(());
    }
    let include = drbd_dir.join(DRBD_INCLUDE_FILE);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&include)
    {
        Ok(mut f) => {
            writeln!(f, "include \"{}/*.res\";", res_dir.display())
                .map_err(|e| Error::io(format!("write {}", include.display()), e))?;
            tracing::info!(file = %include.display(), "wrote DRBD include file");
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(Error::io(format!("create {}", include.display()), e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>(_: &T) {}

    #[test]
    fn run_future_is_send() {
        let fut = run(Config {
            node: "n1".into(),
            namespace: "ns".into(),
            csi_endpoint: "/nonexistent/csi.sock".into(),
            res_dir: PathBuf::from("/nonexistent"),
            host_exec_prefix: None,
        });
        assert_send(&fut);
    }

    #[test]
    fn host_files() {
        let base = std::env::temp_dir().join(format!("agent-prepare-{}", std::process::id()));
        let res = base.join("res");
        let drbd = base.join("drbd.d");
        prepare_host(&res, &drbd).unwrap();
        assert!(res.is_dir());
        assert!(!drbd.join(DRBD_INCLUDE_FILE).exists());

        std::fs::create_dir_all(&drbd).unwrap();
        prepare_host(&res, &drbd).unwrap();
        let text = std::fs::read_to_string(drbd.join(DRBD_INCLUDE_FILE)).unwrap();
        assert_eq!(text, format!("include \"{}/*.res\";\n", res.display()));

        std::fs::write(drbd.join(DRBD_INCLUDE_FILE), "mine\n").unwrap();
        prepare_host(&res, &drbd).unwrap();
        let text = std::fs::read_to_string(drbd.join(DRBD_INCLUDE_FILE)).unwrap();
        assert_eq!(text, "mine\n");
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn pod_grace_exceeds_the_longest_shutdown() {
        let yaml = include_str!("../../../deploy/base/30-agent.yaml");
        let grace: u64 = yaml
            .lines()
            .find_map(|l| l.trim().strip_prefix("terminationGracePeriodSeconds:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap();
        assert!(
            Duration::from_secs(grace) > SHUTDOWN_MAX,
            "{grace} s <= {SHUTDOWN_MAX:?}"
        );
    }
}
