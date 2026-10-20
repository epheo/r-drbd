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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource, ResourceExt};
use model::Assignment;
use serde::de::DeserializeOwned;
use tracing::{debug, info, warn};

use crate::store::Stores;

/// Resync period of the reconcile loop when nothing changes.
pub const RESYNC: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Config {
    /// Namespace of the controller Lease.
    pub namespace: String,
    /// `unix:///path` or a plain path.
    pub csi_endpoint: String,
    /// Lease holder identity: the pod name or the hostname.
    pub identity: String,
    /// Bound of the snapshot take round.
    pub take_timeout: Duration,
    /// Bound of the snapshot suspend round: IO stays frozen meanwhile.
    pub suspend_timeout: Duration,
    /// Bound of every CSI wait on store state.
    pub ready_timeout: Duration,
    /// An agent Lease unrenewed this long marks its node unreachable.
    pub lease_stale: Duration,
    /// PeerUnreachable False this long before its Normal Event.
    pub peer_clear_grace: Duration,
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
            take_timeout: Duration::from_secs(120),
            suspend_timeout: Duration::from_secs(30),
            ready_timeout: Duration::from_secs(30),
            lease_stale: store::LEASE_STALE,
            peer_clear_grace: volume::PEER_CLEAR_GRACE,
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
/// the call instead of stalling the pass, CSI or the Lease for good. The
/// read timeout is a socket idle timeout, which a quiet watch would hit.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(30);

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
        let build = |c| Client::try_from(c).map_err(|e| Error::kube("building the kube client", e));
        Ok(Self {
            watch: build(config.clone())?,
            request: build(request_config(config))?,
        })
    }
}

fn request_config(mut config: kube::Config) -> kube::Config {
    config.connect_timeout = Some(CONNECT_TIMEOUT);
    config.read_timeout = Some(READ_TIMEOUT);
    config
}

/// Connects with the in-cluster or kubeconfig credentials and runs until
/// SIGTERM (Ok, Lease released) or until leadership is lost or a component
/// fails (Err); the caller exits the process.
pub async fn run(cfg: Config) -> Result<(), Error> {
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
    /// Split-brain pairs latched per Volume uid (volume::drbd_conditions).
    pub(crate) split: HashMap<String, BTreeSet<volume::Pair>>,
    /// Volume uids whose spec.resolve has seen SplitBrain True; only those
    /// get the field cleared once resynced.
    pub(crate) resolving: HashSet<String>,
    /// Per Snapshot uid: the reachable replicas listed when Suspending
    /// started; its acks are awaited from those.
    pub(crate) rounds: HashMap<String, Vec<String>>,
    /// initializedAt as last written, per Volume uid, so an apply built
    /// from a store copy that lags behind that write cannot drop it.
    pub(crate) initialized: HashMap<String, k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
    /// Per Volume uid: the EvictionBlocked text last sent as an Event, so a
    /// block that lasts emits one Event per reason, not one per pass.
    pub(crate) evict_blocked: HashMap<String, String>,
    /// Per Volume uid: the attached nodes without a replica, and the
    /// message last sent as an Event.
    pub(crate) attach_failed: HashMap<String, (Vec<String>, String)>,
    /// Per Volume uid: when each PeerUnreachable probe record was first seen.
    pub(crate) probes: HashMap<String, volume::ProbesSeen>,
    /// Volume uids with a probe record awaiting a Lease renewal.
    pub(crate) probe_wait: HashSet<String>,
    /// Per Volume uid: what PeerUnreachable last announced as an Event.
    pub(crate) peer_unreachable: HashMap<String, volume::Announced>,
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
    memory
        .assigned
        .retain(|uid, (rv, _)| seen.get(uid).is_some_and(|cur| cur == rv));
    memory.split.retain(|uid, _| seen.contains_key(uid));
    memory.resolving.retain(|uid| seen.contains_key(uid));
    memory.initialized.retain(|uid, _| seen.contains_key(uid));
    memory.evict_blocked.retain(|uid, _| seen.contains_key(uid));
    memory.attach_failed.retain(|uid, _| seen.contains_key(uid));
    memory
        .peer_unreachable
        .retain(|uid, _| seen.contains_key(uid));
    memory.probes.retain(|uid, _| seen.contains_key(uid));
    memory.probe_wait.retain(|uid| seen.contains_key(uid));
    let snapshots: HashSet<String> = ctx
        .stores
        .snapshots
        .state()
        .iter()
        .filter_map(|s| s.metadata.uid.clone())
        .collect();
    memory.rounds.retain(|uid, _| snapshots.contains(uid));
    volume::reconcile_all(ctx, memory).await;
    pool::reconcile_all(ctx, memory).await;
    let snapshots = snapshot::reconcile_all(ctx, memory).await;
    let peers = memory.peer_timer();
    [snapshots, ctx.stores.next_lease_expiry(), peers]
        .into_iter()
        .flatten()
        .min()
}

/// A Normal core/v1 Event on a Volume; cluster-scoped objects get theirs
/// in "default", as kubectl describe looks there. Failure only logs: the
/// RBAC grants core events, not events.k8s.io, hence no kube Recorder.
pub(crate) async fn event(client: &Client, v: &model::Volume, reason: &str, message: &str) {
    emit(client, v, "Normal", reason, message).await;
}

/// `event` of type Warning.
pub(crate) async fn warning_event(client: &Client, v: &model::Volume, reason: &str, message: &str) {
    emit(client, v, "Warning", reason, message).await;
}

async fn emit(client: &Client, v: &model::Volume, type_: &str, reason: &str, message: &str) {
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
            api_version: Some(format!("{}/{}", model::GROUP, model::VERSION)),
            kind: Some("Volume".into()),
            name: Some(name.clone()),
            uid: v.metadata.uid.clone(),
            resource_version: v.metadata.resource_version.clone(),
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
pub(crate) async fn apply_status<K>(api: &Api<K>, name: &str, status: serde_json::Value)
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
    match api.patch_status(name, &pp, &Patch::Apply(&patch)).await {
        Ok(_) => debug!(kind = %K::kind(&()), name, "status applied"),
        Err(kube::Error::Api(s)) if s.is_not_found() => {}
        Err(e) => warn!(kind = %K::kind(&()), name, error = %e, "status apply failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::{CONNECT_TIMEOUT, READ_TIMEOUT, request_config, valid_name, valid_volume_name};

    #[test]
    fn only_requests_time_out() {
        let watch = kube::Config::new("https://127.0.0.1:6443".parse().expect("url"));
        assert_eq!(watch.read_timeout, None, "kube default; a watch idles");
        let request = request_config(watch);
        assert_eq!(request.connect_timeout, Some(CONNECT_TIMEOUT));
        assert_eq!(request.read_timeout, Some(READ_TIMEOUT));
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
