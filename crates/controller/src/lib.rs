//! r-drbd controller: Pool, Volume and Snapshot reconcilers, DRBD allocation,
//! Lease leader election and the CSI controller plugin.
//!
//! One leader, one reflector per kind, one reconcile loop: every pass walks
//! all objects in name order and writes only what changed, so allocation
//! never races with itself inside the process.

#![forbid(unsafe_code)]

pub mod alloc;
pub mod capacity;
pub mod csi;
pub mod evict;
pub mod leader;
pub mod pool;
pub mod snapshot;
pub mod store;
pub mod volume;
pub mod wait;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;

use hyper_timeout::TimeoutConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::{Patch, PatchParams};
use kube::client::{ClientBuilder, ConfigExt};
use kube::{Api, Client, Resource, ResourceExt};
use model::Assignment;
use serde::de::DeserializeOwned;
use tower::{BoxError, ServiceBuilder};
use tracing::{debug, error, info, warn};

use crate::store::Stores;

/// Resync period of the reconcile loop when nothing changes.
pub const RESYNC: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Config {
    /// Namespace of the controller Lease.
    pub namespace: String,
    /// `unix:///path` or a plain path.
    pub csi_endpoint: String,
    /// Lease holder identity: the pod name or the hostname; `run` makes it
    /// unique per process.
    pub identity: String,
    /// Bound of the snapshot take round.
    pub take_timeout: Duration,
    /// Bound of the snapshot suspend round. Never above
    /// model::snapshot::SUSPEND_BOUND: the agents' resume deadline assumes it.
    pub suspend_timeout: Duration,
    /// Bound of every CSI wait on store state.
    pub ready_timeout: Duration,
    /// An agent Lease unrenewed this long marks its node unreachable.
    pub lease_stale: Duration,
    /// PeerUnreachable False this long before its Normal Event.
    pub peer_clear_grace: Duration,
    /// The wait before a volume's next snapshot round after a failed one;
    /// it doubles per failure in a row.
    pub round_backoff: Duration,
}

impl Config {
    pub fn new(
        namespace: impl Into<String>,
        csi_endpoint: impl Into<String>,
        identity: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            csi_endpoint: csi_endpoint.into(),
            identity: identity.into(),
            take_timeout: model::snapshot::TAKE_BOUND,
            suspend_timeout: model::snapshot::SUSPEND_BOUND,
            ready_timeout: Duration::from_secs(30),
            lease_stale: store::LEASE_STALE,
            peer_clear_grace: volume::PEER_CLEAR_GRACE,
            round_backoff: snapshot::BACKOFF_FIRST,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{context}: {source}")]
    Kube {
        context: String,
        #[source]
        source: Box<kube::Error>,
    },
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("CSI server: {0}")]
    Transport(#[from] ::csi::tonic::transport::Error),
    #[error("kube config")]
    KubeConfig(#[source] kube::config::InferConfigError),
    #[error("lost the leader Lease {0}")]
    LostLease(String),
    #[error("watch of {0} ended")]
    WatchEnded(&'static str),
}

impl Error {
    pub(crate) fn kube(context: impl Into<String>, source: kube::Error) -> Self {
        Self::Kube {
            context: context.into(),
            source: Box::new(source),
        }
    }
}

/// Volume and Snapshot names become LV and DRBD resource names on the
/// host; same rule as model::keys::NAME_PATTERN, written out because the
/// crate takes no regex dependency.
pub fn valid_name(name: &str) -> bool {
    let b = name.as_bytes();
    let end = |c: &u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    (1..=63).contains(&b.len())
        && b.first().is_some_and(end)
        && b.last().is_some_and(end)
        && b.iter().all(|c| end(c) || *c == b'-' || *c == b'.')
}

/// A Volume name is also the start of every LV name made from it, so the
/// prefixes LVM reserves fail here, at admission. Snapshot names come after
/// the separator, so `snapshot-<uuid>` stays valid for them.
pub fn valid_volume_name(name: &str) -> bool {
    valid_name(name) && lvm::names::reserved_prefix(name).is_none()
}

/// Bounds of every call but the watches: a half-open connection fails
/// the call instead of stalling the pass, CSI or the Lease for good.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// The read timeout is a socket idle timeout. It sits above kube-runtime's
/// watch idle bound (the server's 290 s plus a 5 s margin), so a quiet
/// watch never trips it.
/// Keepalive sees only the next hop: behind a TCP proxy, an apiserver gone
/// silent leaves a LIST mute, which would otherwise freeze that store.
const WATCH_READ_TIMEOUT: Duration = Duration::from_secs(300);
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

/// The controller's two kube clients.
#[derive(Clone)]
pub struct Clients {
    /// The reflectors' watches.
    pub watch: Client,
    /// Reconcile, CSI and Lease calls.
    pub request: Client,
}

impl std::fmt::Debug for Clients {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Clients").finish_non_exhaustive()
    }
}

impl Clients {
    pub fn new(config: kube::Config) -> Result<Self, Error> {
        let error = |e| Error::kube("building the kube client", e);
        Ok(Self {
            watch: watch_client(&watch_config(config.clone())).map_err(error)?,
            request: Client::try_from(request_config(config)).map_err(error)?,
        })
    }
}

fn keepalive_connector() -> HttpConnector {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_keepalive(Some(KEEPALIVE_IDLE));
    http.set_keepalive_interval(Some(KEEPALIVE_INTERVAL));
    http.set_keepalive_retries(Some(KEEPALIVE_PROBES));
    http.set_tcp_user_timeout(Some(USER_TIMEOUT));
    http
}

/// kube's config-driven stack on keepalive sockets, since kube::Config
/// cannot set keepalive. No retry or trace layer: the watcher backs off
/// and store.rs logs its errors.
fn watch_client(config: &kube::Config) -> Result<Client, kube::Error> {
    let https = config.rustls_https_connector_with_connector(keepalive_connector())?;
    let mut timeouts = TimeoutConnector::new(https);
    timeouts.set_connect_timeout(config.connect_timeout);
    timeouts.set_read_timeout(config.read_timeout);
    timeouts.set_write_timeout(config.write_timeout);
    let http: hyper_util::client::legacy::Client<_, kube::client::Body> =
        hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(timeouts);
    let service = ServiceBuilder::new()
        .layer(config.base_uri_layer())
        .option_layer(config.auth_layer()?)
        .layer(config.extra_headers_layer()?)
        .map_err(BoxError::from)
        .service(http);
    Ok(ClientBuilder::new(service, config.default_namespace.clone()).build())
}

fn watch_config(mut config: kube::Config) -> kube::Config {
    config.connect_timeout = Some(CONNECT_TIMEOUT);
    config.read_timeout = Some(WATCH_READ_TIMEOUT);
    config
}

fn request_config(mut config: kube::Config) -> kube::Config {
    config.connect_timeout = Some(CONNECT_TIMEOUT);
    config.read_timeout = Some(READ_TIMEOUT);
    config
}

/// Connects with the in-cluster or kubeconfig credentials and runs until
/// SIGTERM (Ok, Lease released) or until leadership is lost or a component
/// fails (Err); the caller exits the process.
pub async fn run(mut cfg: Config) -> Result<(), Error> {
    cfg.identity = leader::per_process(&cfg.identity);
    let config = kube::Config::infer().await.map_err(Error::KubeConfig)?;
    Box::pin(run_with_clients(cfg, Clients::new(config)?)).await
}

/// `run` with given clients; tests point them at envtest.
pub async fn run_with_clients(cfg: Config, clients: Clients) -> Result<(), Error> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| Error::Io {
            context: "installing the SIGTERM handler".into(),
            source: e,
        })?;
    Box::pin(run_until(cfg, clients, async move {
        term.recv().await;
    }))
    .await
}

/// `run_with_clients` that stops when `shutdown` resolves: reconcilers and
/// CSI stop first, then the Lease is released for the successor.
pub async fn run_until(
    cfg: Config,
    clients: Clients,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Error> {
    let Clients { watch, request } = clients;
    let mut shutdown = std::pin::pin!(shutdown);
    tokio::select! {
        r = leader::acquire(&request, &cfg.namespace, &cfg.identity) => r?,
        () = &mut shutdown => return Ok(()),
    }
    info!(
        identity = cfg.identity,
        "leader; starting reconcilers and CSI"
    );
    let (stores, watchers) = store::start(watch, &cfg.namespace, cfg.lease_stale);
    let (namespace, identity) = (cfg.namespace.clone(), cfg.identity.clone());
    let hold = leader::hold(request.clone(), &namespace, &identity);
    let reconcile = reconcile_loop(request.clone(), stores.clone(), cfg.clone());
    let csi = csi::serve(request.clone(), stores, cfg);
    let result = tokio::select! {
        e = hold => return Err(e),
        e = watchers => Err(e),
        r = reconcile => r,
        r = csi => r,
        () = &mut shutdown => {
            info!("shutting down");
            Ok(())
        }
    };
    leader::release(request, &namespace, &identity).await;
    result
}

/// State the reconcile passes carry between runs.
#[derive(Debug, Default)]
pub(crate) struct Memory {
    /// Assignments written but not yet seen in the store, by Volume uid,
    /// with the resourceVersion the write was guarded by; allocation must
    /// count them or two volumes get the same minor or port. The uid keeps
    /// a recreated Volume of the same name apart. Once the store moves past
    /// that resourceVersion it holds the write (or a later state).
    pub(crate) assigned: HashMap<String, (Option<String>, Assignment)>,
    /// Assignment writes of unknown outcome, keyed as `assigned`. They hold
    /// their minor and ports but do not make the Volume assigned: once the
    /// store moves past that resourceVersion, the write is in it or never
    /// lands.
    pub(crate) pending: HashMap<String, (Option<String>, Assignment)>,
    /// Split-brain pairs latched per Volume uid (volume::drbd_conditions).
    pub(crate) split: HashMap<String, BTreeSet<volume::Pair>>,
    /// Volume uids whose spec.resolve has seen SplitBrain True; only those
    /// get the field cleared once resynced.
    pub(crate) resolving: HashSet<String>,
    /// initializedAt as last written, per Volume uid, so an apply built
    /// from a store copy that lags behind that write cannot drop it.
    pub(crate) initialized: HashMap<String, k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
    /// Per Volume uid: the EvictionBlocked text last sent as an Event, so a
    /// block that lasts emits one Event per reason, not one per pass.
    pub(crate) evict_blocked: HashMap<String, String>,
    /// Per Volume uid: the kind of the status apply failure last sent as
    /// an Event; cleared once an apply succeeds.
    pub(crate) status_failed: HashMap<String, String>,
    /// Duplicate minors and ports already announced, by message.
    pub(crate) duplicates: BTreeSet<String>,
    /// Per Volume uid: the attached nodes without a replica, and the
    /// message last sent as an Event.
    pub(crate) attach_failed: HashMap<String, (Vec<String>, String)>,
    /// Per Volume uid: when each PeerUnreachable probe record was first seen.
    pub(crate) probes: HashMap<String, volume::ProbesSeen>,
    /// Volume uids with a probe record awaiting a Lease renewal.
    pub(crate) probe_wait: HashSet<String>,
    /// Per Volume uid: what PeerUnreachable last announced as an Event.
    pub(crate) peer_unreachable: HashMap<String, volume::Announced>,
    /// Per Pool uid: the Snapshots its deletion last said, in an Event, it
    /// waits for.
    pub(crate) pool_waits: HashMap<String, Vec<String>>,
    /// Per Volume uid: its snapshot rounds failed since the last Ready one.
    /// Memory outlives the CSI replace of a Failed Snapshot, which deletes
    /// it; a restart allows one early round.
    pub(crate) round_failures: HashMap<String, snapshot::Failures>,
}

impl Memory {
    pub(crate) fn assignment<'a>(&'a self, v: &'a model::Volume) -> Option<&'a Assignment> {
        let pending = v
            .metadata
            .uid
            .as_deref()
            .and_then(|uid| self.assigned.get(uid))
            .filter(|(rv, _)| *rv == v.metadata.resource_version)
            .map(|(_, a)| a);
        pending.or(v.spec.assignment.as_ref())
    }

    /// Minors and ports held by stored, written and pending assignments.
    pub(crate) fn used(&self, volumes: &[std::sync::Arc<model::Volume>]) -> alloc::Used {
        let mut used =
            alloc::Used::from_assignments(volumes.iter().filter_map(|v| self.assignment(v)));
        for v in volumes {
            let pending = v
                .metadata
                .uid
                .as_deref()
                .and_then(|uid| self.pending.get(uid))
                .filter(|(rv, _)| *rv == v.metadata.resource_version);
            if let Some((_, a)) = pending {
                used.add(a);
            }
        }
        used
    }

    /// Latched split pairs; after a restart, the ones the SplitBrain
    /// condition still names.
    pub(crate) fn latched(&self, v: &model::Volume) -> BTreeSet<volume::Pair> {
        if let Some(p) = v.metadata.uid.as_deref().and_then(|u| self.split.get(u)) {
            return p.clone();
        }
        v.status
            .as_ref()
            .and_then(|s| {
                model::condition::get(&s.conditions, model::condition::types::SPLIT_BRAIN)
            })
            .filter(|c| c.status == model::condition::TRUE)
            .map(|c| volume::pairs_from_message(&c.message))
            .unwrap_or_default()
    }

    pub(crate) fn set_probe_wait(&mut self, v: &model::Volume, waiting: bool) {
        if let Some(uid) = v.metadata.uid.clone() {
            if waiting {
                self.probe_wait.insert(uid);
            } else {
                self.probe_wait.remove(&uid);
            }
        }
    }

    /// A pass due soon for PeerUnreachable: a fresh Lease renewal wakes
    /// nothing, yet a waiting record counts only after one, and a cleared
    /// condition waits out its grace. Nothing armed otherwise, so a fault
    /// that lasts adds no passes.
    pub(crate) fn peer_timer(&self) -> Option<Duration> {
        (!self.probe_wait.is_empty()
            || self
                .peer_unreachable
                .values()
                .any(|a| a.false_since.is_some()))
        .then(|| Duration::from_secs(model::keys::LEASE_RENEW_SECS + 1))
    }

    /// Notes the Volume's probe records and returns their first sightings.
    pub(crate) fn probes_seen(&mut self, v: &model::Volume) -> volume::ProbesSeen {
        let Some(uid) = v.metadata.uid.clone() else {
            return volume::ProbesSeen::new();
        };
        let seen = self.probes.entry(uid).or_default();
        volume::note_probes(seen, v, std::time::Instant::now());
        seen.clone()
    }

    pub(crate) fn latch(&mut self, v: &model::Volume, pairs: BTreeSet<volume::Pair>) {
        if let Some(uid) = v.metadata.uid.clone() {
            self.split.insert(uid, pairs);
        }
    }

    pub(crate) fn remember(&mut self, v: &model::Volume, a: Assignment) {
        if let Some(uid) = v.metadata.uid.clone() {
            self.assigned
                .insert(uid, (v.metadata.resource_version.clone(), a));
        }
    }

    /// Notes a failed status apply; true when this kind is new for the
    /// Volume and deserves an Event.
    pub(crate) fn status_failure(&mut self, uid: &str, kind: String) -> bool {
        if self.status_failed.get(uid) == Some(&kind) {
            return false;
        }
        self.status_failed.insert(uid.to_owned(), kind);
        true
    }

    pub(crate) fn reserve(&mut self, v: &model::Volume, a: Assignment) {
        if let Some(uid) = v.metadata.uid.clone() {
            self.pending
                .insert(uid, (v.metadata.resource_version.clone(), a));
        }
    }
}

pub(crate) struct Ctx<'a> {
    pub(crate) client: &'a Client,
    pub(crate) stores: &'a Stores,
    pub(crate) cfg: &'a Config,
}

async fn reconcile_loop(client: Client, stores: Stores, cfg: Config) -> Result<(), Error> {
    let mut changed = stores.subscribe();
    stores.wait_ready().await?;
    info!("stores synced");
    let mut memory = Memory::default();
    loop {
        changed.borrow_and_update();
        let ctx = Ctx {
            client: &client,
            stores: &stores,
            cfg: &cfg,
        };
        let next = pass(&ctx, &mut memory).await;
        let sleep = next.map_or(RESYNC, |d| d.min(RESYNC));
        tokio::select! {
            r = changed.changed() => {
                if r.is_err() {
                    return Err(Error::WatchEnded("store channel"));
                }
                // Coalesce bursts (our own writes echo back as events).
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            () = tokio::time::sleep(sleep) => {}
        }
    }
}

/// One reconcile pass over every object; returns the earliest timer a
/// reconciler asked for.
async fn pass(ctx: &Ctx<'_>, memory: &mut Memory) -> Option<Duration> {
    let seen: HashMap<String, Option<String>> = ctx
        .stores
        .volumes
        .state()
        .iter()
        .filter_map(|v| Some((v.metadata.uid.clone()?, v.metadata.resource_version.clone())))
        .collect();
    let current = |uid: &String, rv: &Option<String>| seen.get(uid).is_some_and(|cur| cur == rv);
    memory.assigned.retain(|uid, (rv, _)| current(uid, rv));
    memory.pending.retain(|uid, (rv, _)| current(uid, rv));
    memory.split.retain(|uid, _| seen.contains_key(uid));
    memory.resolving.retain(|uid| seen.contains_key(uid));
    memory.initialized.retain(|uid, _| seen.contains_key(uid));
    memory.evict_blocked.retain(|uid, _| seen.contains_key(uid));
    memory.attach_failed.retain(|uid, _| seen.contains_key(uid));
    memory.status_failed.retain(|uid, _| seen.contains_key(uid));
    memory
        .peer_unreachable
        .retain(|uid, _| seen.contains_key(uid));
    memory.probes.retain(|uid, _| seen.contains_key(uid));
    memory.probe_wait.retain(|uid| seen.contains_key(uid));
    memory
        .round_failures
        .retain(|uid, _| seen.contains_key(uid));
    report_duplicates(ctx, memory).await;
    volume::reconcile_all(ctx, memory).await;
    pool::reconcile_all(ctx, memory).await;
    let snapshots = snapshot::reconcile_all(ctx, memory).await;
    let peers = memory.peer_timer();
    [snapshots, ctx.stores.next_lease_expiry(), peers]
        .into_iter()
        .flatten()
        .min()
}

/// Every stored minor and per-node port held by more than one Volume, with
/// its holders. Allocation should make this empty.
pub(crate) fn duplicates(
    volumes: &[std::sync::Arc<model::Volume>],
) -> Vec<(String, Vec<std::sync::Arc<model::Volume>>)> {
    let mut minors: BTreeMap<u32, Vec<_>> = BTreeMap::new();
    let mut ports: BTreeMap<(&str, u16), Vec<_>> = BTreeMap::new();
    for v in volumes {
        let Some(a) = &v.spec.assignment else {
            continue;
        };
        if let Some(m) = a.minor {
            minors.entry(m).or_default().push(v.clone());
        }
        for r in &a.replicas {
            if let Some(p) = r.port {
                ports.entry((&r.node, p)).or_default().push(v.clone());
            }
        }
    }
    let names = |held: &[std::sync::Arc<model::Volume>]| {
        held.iter()
            .map(|v| v.name_any())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let minors = minors
        .into_iter()
        .filter(|(_, held)| held.len() > 1)
        .map(|(m, held)| (format!("minor {m} held by {}", names(&held)), held));
    let ports = ports
        .into_iter()
        .filter(|(_, held)| held.len() > 1)
        .map(|((n, p), held)| (format!("port {p} on {n} held by {}", names(&held)), held));
    minors.chain(ports).collect()
}

/// Both values are immutable once stored, so a duplicate is only reported:
/// an error and one Warning Event per holder, once while it lasts.
async fn report_duplicates(ctx: &Ctx<'_>, memory: &mut Memory) {
    let found = duplicates(&ctx.stores.volumes.state());
    for (message, held) in &found {
        if memory.duplicates.contains(message) {
            continue;
        }
        error!(message, "duplicate DRBD assignment; not rewritten");
        for v in held {
            warning_event(ctx.client, v.as_ref(), DUPLICATE_ASSIGNMENT, message).await;
        }
    }
    memory.duplicates = found.into_iter().map(|(m, _)| m).collect();
}

/// Warning Event: two Volumes hold the same minor or port.
pub(crate) const DUPLICATE_ASSIGNMENT: &str = "DuplicateAssignment";

/// Warning Event: the controller's own status apply failed.
pub(crate) const STATUS_APPLY_FAILED: &str = "StatusApplyFailed";

/// A Normal core/v1 Event on one of our objects; cluster-scoped objects
/// get theirs in "default", as kubectl describe looks there. Failure only
/// logs: the RBAC grants core events, not events.k8s.io, hence no kube
/// Recorder.
pub(crate) async fn event<K: Resource<DynamicType = ()>>(
    client: &Client,
    v: &K,
    reason: &str,
    message: &str,
) {
    emit(client, v, "Normal", reason, message).await;
}

/// `event` of type Warning.
pub(crate) async fn warning_event<K: Resource<DynamicType = ()>>(
    client: &Client,
    v: &K,
    reason: &str,
    message: &str,
) {
    emit(client, v, "Warning", reason, message).await;
}

async fn emit<K: Resource<DynamicType = ()>>(
    client: &Client,
    v: &K,
    type_: &str,
    reason: &str,
    message: &str,
) {
    use k8s_openapi::api::core::v1::{Event, EventSource, ObjectReference};
    let name = v.name_any();
    let now = model::time::now();
    let ev = Event {
        metadata: kube::api::ObjectMeta {
            generate_name: Some(format!("{name}.")),
            namespace: Some("default".into()),
            ..kube::api::ObjectMeta::default()
        },
        involved_object: ObjectReference {
            api_version: Some(K::api_version(&()).into_owned()),
            kind: Some(K::kind(&()).into_owned()),
            name: Some(name.clone()),
            uid: v.meta().uid.clone(),
            resource_version: v.meta().resource_version.clone(),
            ..ObjectReference::default()
        },
        reason: Some(reason.into()),
        message: Some(message.into()),
        type_: Some(type_.into()),
        count: Some(1),
        first_timestamp: Some(now.clone()),
        last_timestamp: Some(now),
        source: Some(EventSource {
            component: Some(model::keys::CONTROLLER_FIELD_MANAGER.into()),
            host: None,
        }),
        reporting_component: Some(model::keys::CONTROLLER_FIELD_MANAGER.into()),
        ..Event::default()
    };
    let api: Api<Event> = Api::namespaced(client.clone(), "default");
    if let Err(e) = api.create(&kube::api::PostParams::default(), &ev).await {
        warn!(volume = name, reason, error = %e, "recording the event failed");
    }
}

pub(crate) fn now() -> Time {
    model::time::now()
}

pub(crate) fn has_finalizer<K: Resource>(obj: &K, finalizer: &str) -> bool {
    obj.meta()
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|x| x == finalizer))
}

/// Finalizer lists are edited with a merge patch carrying resourceVersion:
/// the list is replaced whole, so the precondition keeps a concurrent
/// writer's finalizer from being dropped. A conflict is retried next pass.
pub(crate) async fn set_finalizer<K>(api: &Api<K>, obj: &K, finalizer: &str, present: bool)
where
    K: Resource + Clone + DeserializeOwned + std::fmt::Debug,
{
    let mut list: Vec<String> = obj.meta().finalizers.clone().unwrap_or_default();
    let had = list.iter().any(|f| f == finalizer);
    if had == present {
        return;
    }
    if present {
        list.push(finalizer.to_owned());
    } else {
        list.retain(|f| f != finalizer);
    }
    let patch = serde_json::json!({
        "metadata": {
            "resourceVersion": obj.meta().resource_version,
            "finalizers": list,
        }
    });
    let name = obj.name_any();
    match api
        .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
    {
        Ok(_) => debug!(name, finalizer, present, "finalizer updated"),
        Err(kube::Error::Api(s)) if s.is_conflict() || s.is_not_found() => {
            debug!(
                name,
                finalizer, "finalizer update raced; retrying next pass"
            );
        }
        Err(e) => warn!(name, finalizer, error = %e, "finalizer update failed"),
    }
}

/// Server-side apply of the controller's status fields, forced, under the
/// controller field manager. `status` must hold only controller-owned
/// fields: anything in it becomes ours, anything we stop sending is removed.
/// False when it did not apply; a failure is logged.
pub(crate) async fn apply_status<K>(api: &Api<K>, name: &str, status: serde_json::Value) -> bool
where
    K: Resource<DynamicType = ()> + Clone + DeserializeOwned + std::fmt::Debug,
{
    match try_apply_status(api, name, status).await {
        Ok(()) => true,
        Err(kube::Error::Api(s)) if s.is_not_found() => false,
        Err(e) => {
            warn!(kind = %K::kind(&()), name, error = %e, "status apply failed");
            false
        }
    }
}

/// `apply_status` that hands the error back instead of logging it.
pub(crate) async fn try_apply_status<K>(
    api: &Api<K>,
    name: &str,
    status: serde_json::Value,
) -> Result<(), kube::Error>
where
    K: Resource<DynamicType = ()> + Clone + DeserializeOwned + std::fmt::Debug,
{
    let patch = serde_json::json!({
        "apiVersion": K::api_version(&()),
        "kind": K::kind(&()),
        "metadata": { "name": name },
        "status": status,
    });
    let pp = PatchParams::apply(model::keys::CONTROLLER_FIELD_MANAGER).force();
    api.patch_status(name, &pp, &Patch::Apply(&patch)).await?;
    debug!(kind = %K::kind(&()), name, "status applied");
    Ok(())
}

/// What a failed write is filed under, so a lasting failure is announced
/// once per kind, not once per pass.
pub(crate) fn error_kind(e: &kube::Error) -> String {
    match e {
        kube::Error::Api(s) if !s.reason.is_empty() => format!("{} {}", s.code, s.reason),
        kube::Error::Api(s) => s.code.to_string(),
        _ => "request failed".into(),
    }
}

/// `apply_status` that lands only on the object version it was computed
/// from. Returns false when it did not land; a conflict means the store
/// lagged, and the next pass, woken by the newer version, decides again.
pub async fn apply_status_at<K>(
    api: &Api<K>,
    name: &str,
    resource_version: Option<&str>,
    status: serde_json::Value,
) -> bool
where
    K: Resource<DynamicType = ()> + Clone + DeserializeOwned + std::fmt::Debug,
{
    let patch = serde_json::json!({
        "apiVersion": K::api_version(&()),
        "kind": K::kind(&()),
        "metadata": { "name": name, "resourceVersion": resource_version },
        "status": status,
    });
    let pp = PatchParams::apply(model::keys::CONTROLLER_FIELD_MANAGER).force();
    match api.patch_status(name, &pp, &Patch::Apply(&patch)).await {
        Ok(_) => {
            debug!(kind = %K::kind(&()), name, "status applied");
            true
        }
        Err(kube::Error::Api(s)) if s.is_conflict() => {
            debug!(kind = %K::kind(&()), name, "status computed from a stale copy; retrying next pass");
            false
        }
        Err(kube::Error::Api(s)) if s.is_not_found() => false,
        Err(e) => {
            warn!(kind = %K::kind(&()), name, error = %e, "status apply failed");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::time::Duration;

    use super::{
        CONNECT_TIMEOUT, KEEPALIVE_IDLE, KEEPALIVE_INTERVAL, KEEPALIVE_PROBES, Memory,
        READ_TIMEOUT, USER_TIMEOUT, WATCH_READ_TIMEOUT, keepalive_connector, request_config,
        valid_name, valid_volume_name, watch_client, watch_config,
    };
    use crate::volume::Outcome;
    use kube::ResourceExt;
    use model::keys::{MINOR_RANGE, PORT_RANGE};

    fn volume(uid: &str, rv: &str, assignment: Option<serde_json::Value>) -> Arc<model::Volume> {
        let mut spec = serde_json::json!({ "sizeBytes": 1 << 30, "replicas": 2, "tier": "t" });
        if let Some(a) = assignment {
            spec["assignment"] = a;
        }
        Arc::new(
            serde_json::from_value(serde_json::json!({
                "apiVersion": "r-drbd.io/v1alpha1",
                "kind": "Volume",
                "metadata": { "name": uid, "uid": uid, "resourceVersion": rv },
                "spec": spec,
            }))
            .expect("volume"),
        )
    }

    fn assignment(minor: u32, node: &str, port: u16) -> serde_json::Value {
        serde_json::json!({
            "minor": minor,
            "replicas": [{ "node": node, "nodeId": 0, "port": port, "role": "diskful", "pool": "p" }],
        })
    }

    #[test]
    fn write_outcomes() {
        let api = |code| -> Result<(), kube::Error> {
            Err(kube::Error::Api(Box::new(kube::core::Status {
                code,
                ..kube::core::Status::default()
            })))
        };
        assert_eq!(Outcome::of(&Ok::<_, kube::Error>(())), Outcome::Applied);
        for code in [400, 403, 404, 409, 422] {
            assert_eq!(Outcome::of(&api(code)), Outcome::Rejected, "{code}");
        }
        // A 5xx or a lost answer may still commit.
        for code in [0, 500, 503, 504] {
            assert_eq!(Outcome::of(&api(code)), Outcome::Unknown, "{code}");
        }
        let transport = Err::<(), _>(kube::Error::LinesCodecMaxLineLengthExceeded);
        assert_eq!(Outcome::of(&transport), Outcome::Unknown);
    }

    #[test]
    fn an_unknown_write_holds_its_minor_and_port_until_the_volume_moves() {
        let (minor, port) = (*MINOR_RANGE.start(), *PORT_RANGE.start());
        let mut memory = Memory::default();
        let unsure = volume("a", "1", None);
        let a = serde_json::from_value(assignment(minor, "n1", port)).expect("assignment");
        memory.reserve(&unsure, a);
        let used = memory.used(std::slice::from_ref(&unsure));
        assert_eq!(used.lowest_free_minor(), Some(minor + 1));
        assert_eq!(used.lowest_free_port("n1"), Some(port + 1));
        assert_eq!(used.lowest_free_port("n2"), Some(port));
        // Pending only reserves: the Volume itself is still unassigned.
        assert!(memory.assignment(&unsure).is_none());
        // Moved without the write: nothing held.
        let moved = volume("a", "2", None);
        assert_eq!(memory.used(&[moved]).lowest_free_minor(), Some(minor));
        // Moved with it: held by the stored assignment.
        let landed = volume("a", "2", Some(assignment(minor, "n1", port)));
        assert_eq!(memory.used(&[landed]).lowest_free_minor(), Some(minor + 1));
    }

    #[test]
    fn a_lasting_status_failure_is_announced_once_per_kind() {
        let api = |code, reason: &str| {
            kube::Error::Api(Box::new(kube::core::Status {
                code,
                reason: reason.into(),
                ..kube::core::Status::default()
            }))
        };
        let invalid = super::error_kind(&api(422, "Invalid"));
        let forbidden = super::error_kind(&api(403, "Forbidden"));
        assert_eq!(invalid, "422 Invalid");
        assert_eq!(super::error_kind(&api(500, "")), "500");
        let mut memory = Memory::default();
        assert!(memory.status_failure("a", invalid.clone()));
        assert!(!memory.status_failure("a", invalid.clone()));
        assert!(memory.status_failure("b", invalid.clone()));
        assert!(memory.status_failure("a", forbidden));
        // An apply that succeeds forgets it.
        memory.status_failed.remove("a");
        assert!(memory.status_failure("a", invalid));
    }

    #[test]
    fn duplicate_minors_and_ports_are_found() {
        let (minor, port) = (*MINOR_RANGE.start(), *PORT_RANGE.start());
        let vols = [
            volume("a", "1", Some(assignment(minor, "n1", port))),
            volume("b", "1", Some(assignment(minor, "n2", port))),
            volume("c", "1", Some(assignment(minor + 1, "n1", port))),
            volume("d", "1", Some(assignment(minor + 2, "n2", port + 1))),
            volume("e", "1", None),
        ];
        let found: Vec<(String, Vec<String>)> = super::duplicates(&vols)
            .into_iter()
            .map(|(m, held)| (m, held.iter().map(|v| v.name_any()).collect()))
            .collect();
        assert_eq!(
            found,
            [
                (
                    format!("minor {minor} held by a, b"),
                    vec!["a".into(), "b".into()]
                ),
                (
                    format!("port {port} on n1 held by a, c"),
                    vec!["a".into(), "c".into()]
                ),
            ]
        );
        assert_eq!(super::duplicates(&vols[2..]), vec![]);
    }

    #[test]
    fn every_call_times_out() {
        let base = kube::Config::new("https://127.0.0.1:6443".parse().expect("url"));
        assert_eq!(base.read_timeout, None, "kube default");
        let request = request_config(base.clone());
        assert_eq!(request.connect_timeout, Some(CONNECT_TIMEOUT));
        assert_eq!(request.read_timeout, Some(READ_TIMEOUT));
        let watch = watch_config(base);
        assert_eq!(watch.connect_timeout, Some(CONNECT_TIMEOUT));
        assert_eq!(watch.read_timeout, Some(WATCH_READ_TIMEOUT));
        // Above the longest server-side watch timeout plus margin.
        assert!(WATCH_READ_TIMEOUT > Duration::from_secs(295));
    }

    #[tokio::test]
    async fn watch_sockets_probe_a_silent_peer() {
        use rustix::net::sockopt;
        use tower::ServiceExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let uri = format!("http://{}", listener.local_addr().expect("addr"));
        let io = keepalive_connector()
            .oneshot(uri.parse().expect("uri"))
            .await
            .expect("connect");
        let sock = io.inner();
        assert!(sockopt::socket_keepalive(sock).expect("keepalive"));
        assert_eq!(sockopt::tcp_keepidle(sock).expect("idle"), KEEPALIVE_IDLE);
        assert_eq!(
            sockopt::tcp_keepintvl(sock).expect("interval"),
            KEEPALIVE_INTERVAL
        );
        assert_eq!(sockopt::tcp_keepcnt(sock).expect("count"), KEEPALIVE_PROBES);
        let user_timeout = sockopt::tcp_user_timeout(sock).expect("user timeout");
        assert_eq!(u128::from(user_timeout), USER_TIMEOUT.as_millis());
    }

    #[tokio::test]
    async fn watch_client_sends_the_config_token_to_its_server() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let mut config = kube::Config::new(url.parse().expect("url"));
        config.auth_info.token = Some(String::from("t0ken").into());
        let server = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().expect("accept");
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut buf).expect("read");
                assert_ne!(n, 0, "request ended before its head");
                head.extend_from_slice(&buf[..n]);
            }
            let body = r#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"}}"#;
            let len = body.len();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {len}\r\n\r\n{body}"
            );
            s.write_all(resp.as_bytes()).expect("write");
            String::from_utf8(head).expect("utf8").to_ascii_lowercase()
        });
        let client = watch_client(&watch_config(config)).expect("client");
        let node = kube::Api::<k8s_openapi::api::core::v1::Node>::all(client)
            .get("n1")
            .await
            .expect("get");
        assert_eq!(node.metadata.name.as_deref(), Some("n1"));
        let head = server.join().expect("server");
        assert!(head.starts_with("get /api/v1/nodes/n1 "), "{head}");
        assert!(head.contains("authorization: bearer t0ken"), "{head}");
    }

    /// The read timeout stays for what keepalive cannot see: a live peer
    /// that never answers, as a TCP proxy in front of a dead apiserver.
    #[tokio::test]
    async fn watch_client_ends_a_list_its_server_never_answers() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let mut config = kube::Config::new(url.parse().expect("url"));
        config.read_timeout = Some(Duration::from_millis(200));
        // Holds the accepted connection open, unanswered, until joined.
        let server = std::thread::spawn(move || listener.accept().expect("accept"));
        let nodes = kube::Api::<k8s_openapi::api::core::v1::Node>::all(
            watch_client(&config).expect("client"),
        );
        let list = tokio::time::timeout(
            Duration::from_secs(10),
            nodes.list(&kube::api::ListParams::default()),
        )
        .await
        .expect("the read timeout ends the list");
        assert!(list.is_err(), "{list:?}");
        drop(server.join().expect("server"));
    }

    #[test]
    fn names() {
        assert_eq!(
            model::keys::NAME_PATTERN,
            "^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$",
            "valid_name follows the pattern by hand"
        );
        // The pattern's length is the LV name cap.
        assert!(valid_name(&"a".repeat(lvm::names::NAME_MAX)));
        assert!(!valid_name(&"a".repeat(lvm::names::NAME_MAX + 1)));
        // LVM's reserved substrings all need `_`, which no name holds.
        for part in lvm::names::reserved_parts() {
            assert!(!valid_name(&format!("a{part}")), "{part}");
        }
        for bad in ["snapshot", "snapshot-1", "pvmove0"] {
            assert!(valid_name(bad) && !valid_volume_name(bad), "{bad}");
        }
        assert!(valid_volume_name("pvc-1") && valid_volume_name("vol-snapshot"));
        for ok in [
            "pvc-0a1b2c3d-4e5f-6789-abcd-ef0123456789",
            "snapshot-0a1b2c3d-4e5f-6789-abcd-ef0123456789",
            "a",
            "1abc",
            "a.b",
            "a-9",
            &"a".repeat(63),
        ] {
            assert!(valid_name(ok), "{ok}");
        }
        for bad in [
            "",
            "-abc",
            "abc-",
            ".a",
            "A",
            "a_b",
            "a+b",
            "a b",
            "a/b",
            "\u{e9}1",
            &"a".repeat(64),
        ] {
            assert!(!valid_name(bad), "{bad}");
        }
    }
}
