//! r-drbd controller: Pool, Volume and Snapshot reconcilers, DRBD allocation,
//! Lease leader election and the CSI controller plugin.
//!
//! One leader, one reflector per kind, one reconcile loop: every pass walks
//! all objects in name order and writes only what changed, so allocation
//! never races with itself inside the process.

#![forbid(unsafe_code)]

pub mod alloc;
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
    Transport(#[from] tonic::transport::Error),
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
    let bytes = name.as_bytes();
    let Some((first, rest)) = bytes.split_first() else {
        return false;
    };
    (2..=48).contains(&bytes.len())
        && (first.is_ascii_alphabetic() || *first == b'_')
        && rest
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
}

/// Connects with the in-cluster or kubeconfig credentials and runs until
/// SIGTERM (Ok, Lease released) or until leadership is lost or a component
/// fails (Err); the caller exits the process.
pub async fn run(cfg: Config) -> Result<(), Error> {
    let client = Client::try_default()
        .await
        .map_err(|e| Error::kube("building the kube client", e))?;
    Box::pin(run_with_client(cfg, client)).await
}

/// `run` with a given client; tests point it at envtest.
pub async fn run_with_client(cfg: Config, client: Client) -> Result<(), Error> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| Error::Io {
            context: "installing the SIGTERM handler".into(),
            source: e,
        })?;
    Box::pin(run_until(cfg, client, async move {
        term.recv().await;
    }))
    .await
}

/// `run_with_client` that stops when `shutdown` resolves: reconcilers and
/// CSI stop first, then the Lease is released for the successor.
pub async fn run_until(
    cfg: Config,
    client: Client,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Error> {
    let mut shutdown = std::pin::pin!(shutdown);
    tokio::select! {
        r = leader::acquire(&client, &cfg.namespace, &cfg.identity) => r?,
        () = &mut shutdown => return Ok(()),
    }
    info!(
        identity = cfg.identity,
        "leader; starting reconcilers and CSI"
    );
    let (stores, watchers) = store::start(client.clone(), &cfg.namespace, cfg.lease_stale);
    let (namespace, identity) = (cfg.namespace.clone(), cfg.identity.clone());
    let hold = leader::hold(client.clone(), &namespace, &identity);
    let reconcile = reconcile_loop(client.clone(), stores.clone(), cfg.clone());
    let csi = csi::serve(client.clone(), stores, cfg);
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
    leader::release(client, &namespace, &identity).await;
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
    [snapshots, ctx.stores.next_lease_expiry()]
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
    let now = now();
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

/// Current time as a metav1.Time.
pub(crate) fn now() -> Time {
    Time(k8s_openapi::jiff::Timestamp::now())
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
    use super::valid_name;

    #[test]
    fn names() {
        for ok in [
            "pvc-0a1b2c3d-4e5f-6789-abcd-ef0123456789",
            "_x",
            "ab",
            "A_b-9",
            &"a".repeat(48),
        ] {
            assert!(valid_name(ok), "{ok}");
        }
        for bad in [
            "",
            "a",
            "1abc",
            "-abc",
            "a.b",
            "a b",
            "a/b",
            "\u{e9}1",
            &"a".repeat(49),
        ] {
            assert!(!valid_name(bad), "{bad}");
        }
    }
}
