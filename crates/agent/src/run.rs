//! Agent entry point: startup, then the watch, reconcile and CSI tasks.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kube::{Client, ResourceExt};

use crate::devices::{HostDisks, SysDisks};
use crate::events2::Tracker;
use crate::exec::TokioExec;
use crate::reconcile::{DrbdEnv, KubeLiveVolumes, Reconciler};
use crate::watch::{self, Trigger};
use crate::{Exec, csi, drbd, inventory, node, shutdown};

const DRBD_CONF_DIR: &str = "/etc/drbd.d";
const DRBD_INCLUDE_FILE: &str = "r-drbd.res";

#[derive(Debug, Clone)]
pub struct Config {
    /// Kubernetes node name; also the CSI node id and the DRBD name peers
    /// know this host by.
    pub node: String,
    /// Namespace of the heartbeat Lease.
    pub namespace: String,
    /// `unix:///path/csi.sock` or a bare path.
    pub csi_endpoint: String,
    /// DRBD resource files, included from /etc/drbd.d/r-drbd.res.
    pub res_dir: PathBuf,
    /// Prepended to every external command but drbd-utils, e.g. `[/proc/self/exe,
    /// host-exec, <ns>, --]` to run tools in the host mount namespace.
    pub host_exec_prefix: Option<Vec<String>>,
    /// This agent's pod, read at shutdown on hosts without systemd.
    pub pod: Option<String>,
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
/// Shutdown when every LVM change ends within its wait: the guard, the drain, an LVM
/// change the abandoned pass left running, the CSI server's 5 s. The pod
/// grace must exceed it; a change stuck longer is waited for until the
/// grace runs out, the only hard stop, since killing it could leave a
/// thin pool suspended.
const SHUTDOWN_MAX: Duration = DRAIN
    .saturating_add(shutdown::GUARD_MAX)
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
    let (tools, restart) = probe_tools(&exec, &client, &cfg.node).await;
    // Spawned: the first heartbeat never waits on systemctl.
    tokio::spawn(node::startup_checks(exec.clone(), &client, &cfg.node));

    let trigger = Arc::new(Trigger::default());
    let changes = Arc::new(Trigger::default());
    let (stores, _watches) = watch::start(&watch_client, &cfg.namespace, &trigger);
    let tracker = Arc::new(Mutex::new(Tracker::default()));
    let features = tools.drbd();
    let events = features.map(|_| spawn_events2(&tracker, &changes, &trigger));
    if events.is_none() {
        tracing::warn!("DRBD not found: replicated volumes cannot be served on this node");
    }
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let inv = spawn_inventory(&cfg.node, &client, &exec, &stores, &stop_rx);
    let mut drbd_env = DrbdEnv::new(features, cfg.res_dir.clone(), tracker.clone());
    drbd_env.host_name = host_name();
    let reconciler = Reconciler::new(cfg.node.clone(), exec.clone())
        .with_drbd(drbd_env)
        .with_disks(inv.disks.clone())
        .with_disk_lock(inv.lock.clone())
        .with_live(Arc::new(KubeLiveVolumes::new(client.clone())));

    let listener = ::csi::bind_unix(&cfg.csi_endpoint)
        .and_then(tokio::net::UnixListener::from_std)
        .map_err(|e| Error::io("CSI socket", e))?;
    let node_service = csi::NodeService::new(
        cfg.node.clone(),
        exec.clone(),
        Arc::new(stores.volumes.clone()),
        Arc::new(csi::KubeFsMarker::new(client.clone())),
    );
    let inflight = node_service.inflight();
    let guard = shutdown::Guard::new(&exec, tracker, &cfg, &tools, &client, &node_service);
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
            disks: inv.wakes,
            ready: inv.ready,
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
        // The other arms are only borrowed: while the guard runs, the
        // reconcile and CSI tasks keep running and the drain below waits for them.
        r = signalled(&guard) => r,
        () = restart.notified() => Ok(()),
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
    inv.task.abort();
    if let Some(e) = events {
        e.abort();
    }
    result
}

/// The kernel node name (uname -n); the pod shares the host's with hostNetwork.
fn host_name() -> Option<String> {
    let name = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .inspect_err(|e| tracing::warn!(error = %e, "host name unreadable; using the node name"))
        .ok()?;
    Some(name.trim().to_owned()).filter(|n| !n.is_empty())
}

/// `drbdsetup events2` from the image (no host prefix) feeding the tracker.
fn spawn_events2(
    tracker: &Arc<Mutex<Tracker>>,
    changes: &Arc<Trigger>,
    pass: &Arc<Trigger>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(drbd::events2(
        None,
        tracker.clone(),
        drbd::Notify {
            changes: changes.clone(),
            pass: pass.clone(),
        },
    ))
}

/// The tool probe. The DRBD side of the agent is fixed at start, so a
/// module an operator loads later fires the returned restart; nothing
/// waits for it, the heartbeat and the CSI socket come up at once.
async fn probe_tools(
    exec: &Arc<dyn Exec>,
    client: &Client,
    node: &str,
) -> (node::Tools, Arc<tokio::sync::Notify>) {
    let tools = node::probe(exec.as_ref()).await;
    if tools.drbd_kernel.is_some() {
        node::disable_dead_helper(exec.as_ref(), Path::new(node::HOST_ROOT)).await;
    }
    let restart = Arc::new(tokio::sync::Notify::new());
    if tools.drbd_utils.is_some() && tools.drbd_kernel.is_none() {
        let (exec, restart) = (exec.clone(), restart.clone());
        let events = node::KubeNodeEvents {
            client: client.clone(),
            node: node.to_owned(),
        };
        tokio::spawn(async move {
            node::watch_for_module(exec.as_ref(), &events, node::MODULE_RECHECK).await;
            restart.notify_one();
        });
    }
    (tools, restart)
}

/// The NodeDisks task and what the reconciler shares with it.
struct Inventory {
    disks: Arc<dyn HostDisks>,
    lock: Arc<tokio::sync::Mutex<()>>,
    wakes: tokio::sync::mpsc::UnboundedSender<inventory::Wake>,
    ready: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

fn spawn_inventory(
    node: &str,
    client: &Client,
    exec: &Arc<dyn Exec>,
    stores: &watch::Stores,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Inventory {
    let sys = SysDisks::default();
    let mountinfo = sys.mountinfo.clone();
    let disks: Arc<dyn HostDisks> = Arc::new(sys);
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let pools = stores.pools.clone();
    let (watch, watcher) = inventory::events::start(&tx, &mountinfo);
    let scanner = inventory::Scanner::new(
        node.to_owned(),
        exec.clone(),
        disks.clone(),
        lock.clone(),
        Box::new(move || pools.state()),
        watch,
        stop.clone(),
    );
    let (ready, mut ready_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(inventory::run(
        client.clone(),
        scanner,
        watcher,
        async move {
            let _ = ready_rx.wait_for(|r| *r).await;
        },
        rx,
        stop.clone(),
    ));
    Inventory {
        disks,
        lock,
        wakes: tx,
        ready,
        task,
    }
}

/// The host LVM check, logged. Runs once on every node, a node without a
/// Pool still attaches volumes its host must not scan, and again every
/// pass while a node with a Pool gives no version: a failed probe blocks
/// nothing. A node without lvm2 and without a Pool is left alone until a
/// Pool appears; without lvm2 there is no udev scan to warn about either.
async fn check_lvm(exec: &dyn Exec, rec: &mut Reconciler, has_pool: bool) -> bool {
    let host = crate::storage::probe_host(exec).await;
    if host.version.is_none() {
        if has_pool {
            tracing::warn!("LVM version unreadable; checking again next pass");
        } else {
            tracing::info!(
                "LVM version unreadable on a node without a Pool; checked again when one appears"
            );
        }
        return false;
    }
    if let Some(w) = &host.warning {
        tracing::warn!("{w}");
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

/// What of this node's Pools the NodeDisks rows depend on: which VG
/// each owns, and whether by its tag or by its `vg`.
#[derive(Debug, Default)]
struct PoolSet(Option<BTreeMap<String, PoolKey>>);

/// `vg`, `thinPool`, `devices`.
type PoolKey = (String, String, Option<Vec<String>>);

impl PoolSet {
    /// True when the set differs from the last one seen, and for the
    /// first: the inventory's first pass may have read the Pools earlier.
    fn update(&mut self, pools: &[Arc<model::Pool>], node: &str) -> bool {
        let now = pools
            .iter()
            .filter(|p| p.spec.node == node)
            .map(|p| {
                let s = &p.spec;
                (
                    p.name_any(),
                    (s.vg.clone(), s.thin_pool.clone(), s.devices.clone()),
                )
            })
            .collect();
        let changed = self.0.as_ref() != Some(&now);
        self.0 = Some(now);
        changed
    }
}

struct Triggers {
    /// Watch changes, the resync timer, events2 becoming available or lost.
    pass: Arc<Trigger>,
    /// events2 lines: a status refresh of the touched volumes only.
    changes: Arc<Trigger>,
    /// The NodeDisks inventory, after a Pool device change or a change
    /// of this node's Pools.
    disks: tokio::sync::mpsc::UnboundedSender<crate::inventory::Wake>,
    /// The caches are ready, for the inventory: kube's `wait_until_ready`
    /// wakes only the last of several waiters.
    ready: tokio::sync::watch::Sender<bool>,
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
    triggers.ready.send_replace(true);
    let node = rec.node().to_owned();
    tracing::info!(node, "caches ready, reconciling");
    let (mut lvm_checked, mut lvm_probed) = (false, false);
    let mut pool_set = PoolSet::default();
    loop {
        rec.set_addresses(addresses(&stores));
        rec.set_peer_leases(peer_leases(&stores));
        let pools = stores.pools.state();
        let has_pool = pools.iter().any(|p| p.spec.node == node);
        if !lvm_checked && (has_pool || !lvm_probed) {
            lvm_probed = true;
            lvm_checked = check_lvm(exec.as_ref(), &mut rec, has_pool).await;
        }
        let volumes = stores.volumes.state();
        let snapshots = stores.snapshots.state();
        let writes = rec.pass(&pools, &volumes, &snapshots).await;
        for pool in apply(&client, &node, writes).await {
            rec.events_unsent(&pool);
        }
        let pool_set_changed = pool_set.update(&pools, &node);
        if rec.take_disks_changed() || pool_set_changed {
            let _ = triggers.disks.send(crate::inventory::Wake::Lvm);
        }
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

/// Applies the writes; returns the Pools whose Events did not get through.
async fn apply(
    client: &Client,
    node: &str,
    writes: Vec<crate::reconcile::StatusWrite>,
) -> Vec<String> {
    let results = crate::retry::each_with_backoff(
        &writes,
        &crate::retry::BACKOFF,
        crate::retry::retryable,
        |w| w.apply(client, node),
    )
    .await;
    let mut skipped = 0;
    let mut unsent = Vec::new();
    for (w, res) in writes.iter().zip(results) {
        if matches!(w, crate::reconcile::StatusWrite::Event { .. }) && !matches!(res, Some(Ok(())))
        {
            unsent.push(w.name().to_owned());
        }
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
    unsent
}

/// The shutdown guard runs before the drain.
async fn signalled(guard: &shutdown::Guard) -> Result<(), Error> {
    shutdown().await?;
    guard.on_signal().await;
    tracing::info!(bound = ?SHUTDOWN_MAX, "shutting down");
    Ok(())
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
            pod: None,
        });
        assert_send(&fut);
    }

    fn pool(name: &str, node: &str, vg: &str) -> Arc<model::Pool> {
        Arc::new(model::Pool::new(
            name,
            model::PoolSpec {
                node: node.into(),
                tier: "t".into(),
                vg: vg.into(),
                thin_pool: "thin".into(),
                overcommit_percent: model::DEFAULT_OVERCOMMIT_PERCENT,
                reserve_percent: model::DEFAULT_RESERVE_PERCENT,
                devices: None,
                chunk_size: None,
                reclaim_policy: None,
            },
        ))
    }

    fn with(p: &Arc<model::Pool>, f: impl FnOnce(&mut model::Pool)) -> Arc<model::Pool> {
        let mut p = (**p).clone();
        f(&mut p);
        Arc::new(p)
    }

    #[test]
    fn pool_set_wakes_on_this_nodes_pools_only() {
        let a = pool("a", "n1", "vg1");
        let mut set = PoolSet::default();
        assert!(set.update(std::slice::from_ref(&a), "n1"), "first set");
        assert!(!set.update(std::slice::from_ref(&a), "n1"), "same set");

        let b = pool("b", "n1", "vg2");
        assert!(set.update(&[a.clone(), b.clone()], "n1"), "added");
        assert!(set.update(std::slice::from_ref(&a), "n1"), "removed");

        let vg = with(&a, |p| p.spec.vg = "vg9".into());
        assert!(set.update(&[vg], "n1"), "vg");
        assert!(set.update(std::slice::from_ref(&a), "n1"));
        let thin = with(&a, |p| p.spec.thin_pool = "t2".into());
        assert!(set.update(&[thin], "n1"), "thinPool");
        assert!(set.update(std::slice::from_ref(&a), "n1"));
        let devs = with(&a, |p| {
            p.spec.devices = Some(vec!["/dev/disk/by-id/x".into()]);
        });
        assert!(set.update(std::slice::from_ref(&devs), "n1"), "devices");
        let more = with(&devs, |p| {
            p.spec
                .devices
                .as_mut()
                .unwrap()
                .push("/dev/disk/by-id/y".into());
        });
        assert!(set.update(&[more], "n1"), "a device appended");
        assert!(set.update(std::slice::from_ref(&a), "n1"));

        let other = pool("c", "n2", "vg1");
        assert!(
            !set.update(&[a.clone(), other.clone()], "n1"),
            "other node added"
        );
        let moved = with(&other, |p| p.spec.vg = "vg3".into());
        assert!(!set.update(&[a.clone(), moved], "n1"), "other node changed");
        assert!(
            !set.update(std::slice::from_ref(&a), "n1"),
            "other node removed"
        );
        let came = with(&other, |p| p.spec.node = "n1".into());
        assert!(set.update(&[a.clone(), came], "n1"), "moved to this node");
        assert!(set.update(std::slice::from_ref(&a), "n1"), "moved away");

        let status = with(&a, |p| {
            p.status = Some(model::PoolStatus {
                free_bytes: Some(1 << 30),
                ..model::PoolStatus::default()
            });
            p.metadata.resource_version = Some("7".into());
        });
        assert!(!set.update(&[status], "n1"), "status only");
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
