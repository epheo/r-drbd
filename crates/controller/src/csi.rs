//! CSI Identity and Controller services on the sidecars' Unix socket.
//! Every call maps to a Volume or Snapshot object and ends in a bounded wait on the store.

use std::collections::HashMap;
use std::sync::Arc;

use ::csi::v1::controller_server::{Controller, ControllerServer};
use ::csi::v1::controller_service_capability::{self, rpc};
use ::csi::v1::identity_server::{Identity, IdentityServer};
use ::csi::v1::plugin_capability::{self, service, volume_expansion};
use ::csi::v1::validate_volume_capabilities_response::Confirmed;
use ::csi::v1::volume_capability::access_mode::Mode;
use ::csi::v1::volume_capability::{AccessMode, AccessType};
use ::csi::v1::volume_content_source::Type as SourceType;
use ::csi::v1::{
    CapacityRange, ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
    ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
    ControllerGetVolumeRequest, ControllerGetVolumeResponse, ControllerModifyVolumeRequest,
    ControllerModifyVolumeResponse, ControllerPublishVolumeRequest,
    ControllerPublishVolumeResponse, ControllerServiceCapability, ControllerUnpublishVolumeRequest,
    ControllerUnpublishVolumeResponse, CreateSnapshotRequest, CreateSnapshotResponse,
    CreateVolumeRequest, CreateVolumeResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    DeleteVolumeRequest, DeleteVolumeResponse, GetCapacityRequest, GetCapacityResponse,
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, ListSnapshotsRequest, ListSnapshotsResponse, ListVolumesRequest,
    ListVolumesResponse, PluginCapability, ProbeRequest, ProbeResponse, Topology,
    ValidateVolumeCapabilitiesRequest, ValidateVolumeCapabilitiesResponse, VolumeCapability,
};
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::api::{DeleteParams, ObjectMeta, Patch, PatchParams, PostParams, Preconditions};
use kube::{Api, Client, ResourceExt};
use model::keys::{
    ANNOTATION_PIN_NODE, ANNOTATION_PIN_POOL, CONTROLLER_FIELD_MANAGER, CSI_DRIVER, PARAM_REPLICAS,
    PARAM_TIER, TOPOLOGY_KEY,
};
use model::{
    Role, Snapshot, SnapshotPhase, SnapshotSpec, Volume, VolumePhase, VolumeSource, VolumeSpec,
    condition,
};
use serde_json::json;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::store::Stores;
use crate::{Config, Error, wait};

const PARAM_PVC_NAME: &str = "csi.storage.k8s.io/pvc/name";
const PARAM_PVC_NAMESPACE: &str = "csi.storage.k8s.io/pvc/namespace";
/// publish_context key the node plugin reads.
pub const PUBLISH_DEVICE_PATH: &str = "devicePath";

/// StorageClass parameters the controller understands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Params {
    pub tier: String,
    pub replicas: u32,
    /// (namespace, name) of the PVC, from --extra-create-metadata.
    pub pvc: Option<(String, String)>,
}

pub fn parse_params<S: std::hash::BuildHasher>(
    p: &HashMap<String, String, S>,
) -> Result<Params, String> {
    let tier = p
        .get(PARAM_TIER)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| format!("parameter {PARAM_TIER} is required"))?
        .clone();
    let replicas = parse_replicas(p)?;
    let pvc = match (p.get(PARAM_PVC_NAMESPACE), p.get(PARAM_PVC_NAME)) {
        (Some(ns), Some(n)) if !ns.is_empty() && !n.is_empty() => Some((ns.clone(), n.clone())),
        _ => None,
    };
    Ok(Params {
        tier,
        replicas,
        pvc,
    })
}

pub fn parse_replicas<S: std::hash::BuildHasher>(
    p: &HashMap<String, String, S>,
) -> Result<u32, String> {
    match p.get(PARAM_REPLICAS) {
        None => Ok(1),
        Some(r) => r
            .trim()
            .parse::<u32>()
            .ok()
            .filter(|n| (1..=3).contains(n))
            .ok_or_else(|| format!("parameter {PARAM_REPLICAS} must be 1, 2 or 3, got {r:?}")),
    }
}

/// The Volume access mode a capability needs. MULTI_NODE_* is raw block
/// only: XFS is not a shared file system.
pub fn check_capability(c: &VolumeCapability) -> Result<model::AccessMode, String> {
    let mode = c
        .access_mode
        .as_ref()
        .map(AccessMode::mode)
        .ok_or("access mode missing")?;
    let wanted = match mode {
        Mode::SingleNodeWriter
        | Mode::SingleNodeReaderOnly
        | Mode::SingleNodeSingleWriter
        | Mode::SingleNodeMultiWriter => model::AccessMode::ReadWriteOnce,
        Mode::MultiNodeReaderOnly | Mode::MultiNodeSingleWriter | Mode::MultiNodeMultiWriter => {
            model::AccessMode::ReadWriteMany
        }
        Mode::Unknown => return Err(format!("access mode {} not supported", mode.as_str_name())),
    };
    match c.access_type.as_ref().ok_or("access type missing")? {
        AccessType::Block(_) => Ok(wanted),
        AccessType::Mount(_) if wanted == model::AccessMode::ReadWriteMany => {
            Err("ReadWriteMany needs volumeMode Block".into())
        }
        AccessType::Mount(m) if m.fs_type.is_empty() || m.fs_type == "xfs" => Ok(wanted),
        AccessType::Mount(m) => Err(format!("fs_type {} not supported, only xfs", m.fs_type)),
    }
}

/// ReadWriteMany when any capability needs it; every one must be valid.
pub fn requested_access_mode(caps: &[VolumeCapability]) -> Result<model::AccessMode, String> {
    if caps.is_empty() {
        return Err("volume capabilities missing".into());
    }
    let mut out = model::AccessMode::ReadWriteOnce;
    for c in caps {
        if check_capability(c)? == model::AccessMode::ReadWriteMany {
            out = model::AccessMode::ReadWriteMany;
        }
    }
    Ok(out)
}

/// The capabilities a volume of `volume` mode serves.
fn capabilities_fit(caps: &[VolumeCapability], volume: model::AccessMode) -> Result<(), String> {
    match requested_access_mode(caps)? {
        model::AccessMode::ReadWriteMany if volume != model::AccessMode::ReadWriteMany => {
            Err("volume is ReadWriteOnce; MULTI_NODE_* access needs a ReadWriteMany volume".into())
        }
        _ => Ok(()),
    }
}

/// Requested bytes; None when the caller left the size open.
pub fn requested_bytes(range: Option<&CapacityRange>) -> Result<Option<u64>, String> {
    let Some(r) = range else { return Ok(None) };
    let required = u64::try_from(r.required_bytes).map_err(|_| "required_bytes is negative")?;
    let limit = u64::try_from(r.limit_bytes).map_err(|_| "limit_bytes is negative")?;
    if limit != 0 && required > limit {
        return Err(format!(
            "required_bytes {required} exceeds limit_bytes {limit}"
        ));
    }
    Ok(match (required, limit) {
        (0, 0) => None,
        (0, l) => Some(l),
        (r, _) => Some(r),
    })
}

/// An existing size satisfies a capacity range.
fn size_fits(size: u64, range: Option<&CapacityRange>) -> bool {
    range.is_none_or(|r| {
        let required = u64::try_from(r.required_bytes).unwrap_or(0);
        let limit = u64::try_from(r.limit_bytes).unwrap_or(0);
        size >= required && (limit == 0 || size <= limit)
    })
}

/// Device-mapper bytes (`-` counted twice) a mapped name may take: those of
/// `pvc-<uuid>`, so a mapped name never takes more of the LV name budget
/// than a name CSI sends itself.
const MAPPED_DM_MAX: usize = 45;
/// The hash suffix: `-` (two device-mapper bytes) and 16 hex digits.
const SUFFIX_DM: usize = 2 + 16;

/// Object name for a CSI name. The external-provisioner and snapshotter
/// send pvc-<uuid> and snapshot-<uuid>, which pass unchanged. Anything that
/// is not both a Kubernetes name and an LV name (uppercase, too long) is
/// mapped to a stable lowercase name with a hash suffix; the CO then uses
/// the returned id, so the mapping is only ever applied to `name`.
pub fn object_name(csi_name: &str) -> String {
    let b = csi_name.as_bytes();
    let fits = crate::valid_name(csi_name)
        && b.first().is_some_and(u8::is_ascii_lowercase)
        && b.last()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-');
    if fits {
        return csi_name.to_owned();
    }
    // FNV-1a: stable across builds and platforms, unlike std's hasher.
    let hash = b.iter().fold(0xcbf2_9ce4_8422_2325_u64, |h, c| {
        (h ^ u64::from(*c)).wrapping_mul(0x0100_0000_01b3)
    });
    let mut base: String = csi_name
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                '-'
            }
        })
        .skip_while(|c| !c.is_ascii_lowercase())
        .scan(0, |dm, c| {
            *dm += if c == '-' { 2 } else { 1 };
            (*dm <= MAPPED_DM_MAX - SUFFIX_DM).then_some(c)
        })
        .collect();
    while base.ends_with('-') {
        base.pop();
    }
    if base.is_empty() {
        base.push('v');
    }
    format!("{base}-{hash:016x}")
}

fn socket_path(endpoint: &str) -> &str {
    endpoint
        .strip_prefix("unix://")
        .or_else(|| endpoint.strip_prefix("unix:"))
        .unwrap_or(endpoint)
}

fn kube_status(context: &str, e: &kube::Error) -> Status {
    let msg = format!("{context}: {e}");
    match e {
        kube::Error::Api(s) if s.is_not_found() => Status::not_found(msg),
        kube::Error::Api(s) if s.is_already_exists() => Status::already_exists(msg),
        kube::Error::Api(s) if s.is_invalid() || s.code == 400 => Status::invalid_argument(msg),
        _ => Status::internal(msg),
    }
}

fn required<'a>(value: &'a str, what: &str) -> Result<&'a str, Status> {
    if value.is_empty() {
        Err(Status::invalid_argument(format!("{what} is required")))
    } else {
        Ok(value)
    }
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Topology of a local volume: its one diskful node.
fn local_topology(v: &Volume) -> Vec<Topology> {
    v.spec
        .assignment
        .iter()
        .flat_map(|a| &a.replicas)
        .filter(|r| r.role == Role::Diskful)
        .map(|r| Topology {
            segments: HashMap::from([(TOPOLOGY_KEY.to_owned(), r.node.clone())]),
        })
        .collect()
}

fn timestamp(t: k8s_openapi::jiff::Timestamp) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: t.as_second(),
        nanos: t.subsec_nanosecond(),
    }
}

/// Serves until the socket fails.
pub async fn serve(client: Client, stores: Stores, cfg: Config) -> Result<(), Error> {
    stores.wait_ready().await?;
    let path = socket_path(&cfg.csi_endpoint).to_owned();
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(Error::Io {
                context: format!("removing stale socket {path}"),
                source: e,
            });
        }
    }
    let listener = UnixListener::bind(&path).map_err(|e| Error::Io {
        context: format!("binding {path}"),
        source: e,
    })?;
    info!(path, "CSI controller plugin listening");
    let svc = Arc::new(Csi {
        client,
        stores,
        cfg,
    });
    tonic::transport::Server::builder()
        .add_service(IdentityServer::from_arc(svc.clone()))
        .add_service(ControllerServer::from_arc(svc))
        .serve_with_incoming(UnixListenerStream::new(listener))
        .await?;
    Ok(())
}

pub struct Csi {
    client: Client,
    stores: Stores,
    cfg: Config,
}

impl std::fmt::Debug for Csi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Csi")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl Csi {
    pub fn new(client: Client, stores: Stores, cfg: Config) -> Self {
        Self {
            client,
            stores,
            cfg,
        }
    }

    fn volumes(&self) -> Api<Volume> {
        Api::all(self.client.clone())
    }

    fn snapshots(&self) -> Api<Snapshot> {
        Api::all(self.client.clone())
    }

    /// Pins from the PVC annotations; a failed lookup only loses the pins.
    async fn pvc_pins(&self, pvc: Option<&(String, String)>) -> (Option<String>, Option<String>) {
        let Some((ns, name)) = pvc else {
            return (None, None);
        };
        let api: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), ns);
        match api.get(name).await {
            Ok(pvc) => {
                let a = pvc.annotations();
                let pin = |k: &str| a.get(k).filter(|v| !v.is_empty()).cloned();
                (pin(ANNOTATION_PIN_NODE), pin(ANNOTATION_PIN_POOL))
            }
            Err(e) => {
                warn!(namespace = ns, pvc = name, error = %e, "PVC lookup failed; placing without pins");
                (None, None)
            }
        }
    }

    /// Gets or creates the Volume. An existing one must match size, source,
    /// tier, replicas and access mode; while it has no assignment its pins follow the
    /// latest request (a rescheduled WaitForFirstConsumer call). One being
    /// deleted is waited out first.
    async fn ensure_volume(
        &self,
        name: &str,
        spec: &VolumeSpec,
        range: Option<&CapacityRange>,
    ) -> Result<Volume, Status> {
        let api = self.volumes();
        let check = |v: &Volume| -> Result<(), Status> {
            if v.spec.source != spec.source
                || !size_fits(v.spec.size_bytes, range)
                || v.spec.tier != spec.tier
                || v.spec.replicas != spec.replicas
                || v.spec.access_mode != spec.access_mode
            {
                return Err(Status::already_exists(format!(
                    "volume {name} exists with size {}, tier {}, replicas {}, access mode {:?} and source {:?}",
                    v.spec.size_bytes,
                    v.spec.tier,
                    v.spec.replicas,
                    v.spec.access_mode,
                    v.spec.source
                )));
            }
            Ok(())
        };
        for _ in 0..8 {
            let existing = api
                .get_opt(name)
                .await
                .map_err(|e| kube_status("reading volume", &e))?;
            let Some(v) = existing else {
                let obj = Volume {
                    metadata: ObjectMeta {
                        name: Some(name.to_owned()),
                        ..ObjectMeta::default()
                    },
                    spec: spec.clone(),
                    status: None,
                };
                let pp = PostParams {
                    field_manager: Some(CONTROLLER_FIELD_MANAGER.to_owned()),
                    ..PostParams::default()
                };
                match api.create(&pp, &obj).await {
                    Ok(v) => {
                        info!(
                            volume = name,
                            size = spec.size_bytes,
                            replicas = spec.replicas,
                            tier = spec.tier,
                            node = ?spec.node,
                            pool = ?spec.pool,
                            "volume created"
                        );
                        return Ok(v);
                    }
                    Err(kube::Error::Api(s)) if s.is_already_exists() => continue,
                    Err(e) => return Err(kube_status("creating volume", &e)),
                }
            };
            if v.metadata.deletion_timestamp.is_some() {
                self.wait_volume_gone(name).await?;
                continue;
            }
            check(&v)?;
            if v.spec.assignment.is_some() || (v.spec.node == spec.node && v.spec.pool == spec.pool)
            {
                return Ok(v);
            }
            let patch = json!({
                "metadata": { "resourceVersion": v.metadata.resource_version },
                "spec": { "node": spec.node, "pool": spec.pool },
            });
            match api
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(v) => {
                    info!(volume = name, node = ?spec.node, pool = ?spec.pool, "volume pins updated");
                    return Ok(v);
                }
                Err(kube::Error::Api(s)) if s.is_conflict() => {}
                Err(e) => return Err(kube_status("updating volume pins", &e)),
            }
        }
        Err(Status::aborted(format!("volume {name} kept changing")))
    }

    /// Deletes a Volume that never got an assignment, so a CO that treats
    /// RESOURCE_EXHAUSTED as "nothing exists" (csi-provisioner reschedules
    /// or finishes, and never calls DeleteVolume) leaves no orphan. The
    /// preconditions keep a Volume assigned in the meantime.
    /// `expect` is the (uid, generation) that reported Insufficient: a
    /// Volume recreated or respecified since is a different request and is
    /// left alone. None takes whatever unassigned Volume is there.
    async fn discard_unassigned(&self, name: &str, expect: Option<(String, i64)>) {
        let api = self.volumes();
        let matches = |v: &Volume| {
            expect.as_ref().is_none_or(|(uid, generation)| {
                v.metadata.uid.as_deref() == Some(uid.as_str())
                    && v.metadata.generation == Some(*generation)
            })
        };
        let v = match api.get_opt(name).await {
            Ok(Some(v)) if v.spec.assignment.is_none() && matches(&v) => v,
            Ok(_) => return,
            Err(e) => {
                warn!(volume = name, error = %e, "reading the unplaced volume failed");
                return;
            }
        };
        let dp = DeleteParams {
            preconditions: Some(Preconditions {
                uid: v.metadata.uid.clone(),
                resource_version: v.metadata.resource_version.clone(),
            }),
            ..DeleteParams::default()
        };
        match api.delete(name, &dp).await {
            Ok(_) => info!(volume = name, "unplaced volume removed"),
            Err(e) => {
                warn!(volume = name, error = %e, "removing the unplaced volume failed");
                return;
            }
        }
        if let Err(e) = self.wait_volume_gone(name).await {
            warn!(volume = name, error = %e, "unplaced volume still present");
        }
    }

    async fn wait_snapshot_gone(&self, name: &str) -> Result<(), Status> {
        self.wait_gone("snapshot", |s| s.snapshot(name).is_none())
            .await?;
        if self
            .snapshots()
            .get_opt(name)
            .await
            .map_err(|e| kube_status("reading snapshot", &e))?
            .is_some()
        {
            return Err(Status::deadline_exceeded(format!(
                "snapshot {name} still being removed"
            )));
        }
        Ok(())
    }

    /// The snapshot source of a CreateVolume, the size it implies and, for
    /// a `local` volume, the node holding the snapshot. A replicated restore
    /// takes every holder and has no topology to match.
    async fn restore_source(
        &self,
        name: &str,
        req: &CreateVolumeRequest,
        local: bool,
    ) -> Result<(Option<VolumeSource>, Option<u64>, Option<String>), Status> {
        let mut size =
            requested_bytes(req.capacity_range.as_ref()).map_err(Status::invalid_argument)?;
        let limit = req
            .capacity_range
            .as_ref()
            .and_then(|r| u64::try_from(r.limit_bytes).ok())
            .filter(|l| *l > 0);
        let mut holder_node = None;
        let source = match req
            .volume_content_source
            .as_ref()
            .and_then(|s| s.r#type.as_ref())
        {
            None => None,
            Some(SourceType::Snapshot(s)) => {
                let id = required(&s.snapshot_id, "snapshot_id")?;
                let snap = self
                    .snapshots()
                    .get_opt(id)
                    .await
                    .map_err(|e| kube_status("reading snapshot", &e))?
                    .ok_or_else(|| Status::not_found(format!("snapshot {id} not found")))?;
                let snap_size = snap.status.as_ref().and_then(|s| s.size_bytes).unwrap_or(0);
                if limit.is_some_and(|l| snap_size > l) {
                    return Err(Status::out_of_range(format!(
                        "snapshot {id} holds {snap_size} bytes, above limit_bytes"
                    )));
                }
                size = Some(size.unwrap_or(0).max(snap_size));
                // Restores land where the snapshot lives.
                if let Some((node, _)) = crate::volume::snapshot_holder(&snap).filter(|_| local) {
                    let requisite = req
                        .accessibility_requirements
                        .as_ref()
                        .map(|t| t.requisite.as_slice())
                        .unwrap_or_default();
                    if !requisite.is_empty()
                        && !requisite
                            .iter()
                            .any(|t| t.segments.get(TOPOLOGY_KEY) == Some(&node))
                    {
                        self.discard_unassigned(name, None).await;
                        return Err(Status::resource_exhausted(format!(
                            "snapshot {id} lives on {node}, outside the requisite topology"
                        )));
                    }
                    holder_node = Some(node);
                }
                Some(VolumeSource {
                    snapshot: id.to_owned(),
                })
            }
            Some(SourceType::Volume(_)) => {
                return Err(Status::invalid_argument(
                    "cloning from a volume is not supported",
                ));
            }
        };
        Ok((source, size, holder_node))
    }

    /// Waits until the store and the API no longer have the Volume.
    async fn wait_volume_gone(&self, name: &str) -> Result<(), Status> {
        self.wait_gone("volume", |s| s.volume(name).is_none())
            .await?;
        // The store can lag behind a create it never saw; confirm with the API.
        if self
            .volumes()
            .get_opt(name)
            .await
            .map_err(|e| kube_status("reading volume", &e))?
            .is_some()
        {
            return Err(Status::deadline_exceeded(format!(
                "volume {name} still being removed"
            )));
        }
        Ok(())
    }

    /// Adds or removes `node` in spec.attachments; the merge patch carries
    /// resourceVersion so concurrent publishes do not drop each other. On a
    /// ReadWriteOnce volume an add is refused when the list read in the same
    /// attempt holds another node, so of two racing publishes one wins.
    /// Returns the generation that holds the change.
    async fn edit_attachments(&self, name: &str, node: &str, add: bool) -> Result<i64, Status> {
        let api = self.volumes();
        for attempt in 0..8_u64 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(20 * attempt)).await;
            }
            let v = api
                .get(name)
                .await
                .map_err(|e| kube_status("reading volume", &e))?;
            let mut list = v.spec.attachments.clone();
            let exclusive = v.spec.access_mode == model::AccessMode::ReadWriteOnce;
            if add
                && exclusive
                && let Some(other) = list.iter().find(|n| *n != node)
            {
                return Err(Status::failed_precondition(format!(
                    "volume {name} is already published on {other}"
                )));
            }
            if list.iter().any(|n| n == node) == add {
                return Ok(v.metadata.generation.unwrap_or(0));
            }
            if add {
                list.push(node.to_owned());
            } else {
                list.retain(|n| n != node);
            }
            let patch = json!({
                "metadata": { "resourceVersion": v.metadata.resource_version },
                "spec": { "attachments": list },
            });
            match api
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(v) => return Ok(v.metadata.generation.unwrap_or(0)),
                Err(kube::Error::Api(s)) if s.is_conflict() => {}
                Err(e) => return Err(kube_status("updating attachments", &e)),
            }
        }
        Err(Status::aborted(format!(
            "attachments of {name} kept changing"
        )))
    }

    /// Waits for the Volume `uid` to be usable; Failed maps to the CSI code
    /// the CO acts on.
    /// Insufficient also yields the (uid, generation) it was reported for.
    async fn wait_volume_ready(
        &self,
        name: &str,
        created: &Volume,
    ) -> Result<Arc<Volume>, (Status, Option<(String, i64)>)> {
        let uid = created.metadata.uid.clone();
        // A status from before the latest spec (old pins) must not decide.
        let generation = created.metadata.generation.unwrap_or(0);
        wait::until(&self.stores, self.cfg.ready_timeout, |s| {
            let v = s.volume(name).filter(|v| v.metadata.uid == uid)?;
            let st = v.status.as_ref()?;
            if st.observed_generation.unwrap_or(0) < generation {
                return None;
            }
            let c = condition::get(&st.conditions, condition::types::READY);
            let insufficient = c.is_some_and(|c| c.reason == crate::volume::reason::INSUFFICIENT);
            match st.phase {
                VolumePhase::Ready | VolumePhase::Degraded => Some(Ok(v)),
                VolumePhase::Pending if insufficient => {
                    let reported = v.metadata.uid.clone().zip(st.observed_generation);
                    Some(Err((
                        Status::resource_exhausted(format!(
                            "volume {name}: {}",
                            c.map(|c| c.message.as_str()).unwrap_or_default()
                        )),
                        reported,
                    )))
                }
                VolumePhase::Failed => {
                    let msg = format!(
                        "volume {name} failed: {}",
                        c.map(|c| format!("{}: {}", c.reason, c.message))
                            .unwrap_or_default()
                    );
                    Some(Err((Status::failed_precondition(msg), None)))
                }
                _ => None,
            }
        })
        .await
        .ok_or_else(|| {
            (
                Status::deadline_exceeded(format!("volume {name} not Ready yet")),
                None,
            )
        })?
    }

    /// Creates the Snapshot object unless it exists.
    async fn ensure_snapshot(&self, name: &str, source: &str) -> Result<Snapshot, Status> {
        let api = self.snapshots();
        if let Some(s) = api
            .get_opt(name)
            .await
            .map_err(|e| kube_status("reading snapshot", &e))?
        {
            return Ok(s);
        }
        let volume = self
            .volumes()
            .get_opt(source)
            .await
            .map_err(|e| kube_status("reading volume", &e))?;
        if volume.is_none() {
            return Err(Status::not_found(format!("volume {source} not found")));
        }
        let obj = Snapshot {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                ..ObjectMeta::default()
            },
            spec: SnapshotSpec {
                volume: source.to_owned(),
            },
            status: None,
        };
        let pp = PostParams {
            field_manager: Some(CONTROLLER_FIELD_MANAGER.to_owned()),
            ..PostParams::default()
        };
        match api.create(&pp, &obj).await {
            Ok(s) => {
                info!(snapshot = name, volume = source, "snapshot created");
                Ok(s)
            }
            Err(kube::Error::Api(s)) if s.is_already_exists() => api
                .get(name)
                .await
                .map_err(|e| kube_status("reading snapshot", &e)),
            Err(e) => Err(kube_status("creating snapshot", &e)),
        }
    }

    async fn wait_gone(&self, what: &str, gone: impl Fn(&Stores) -> bool) -> Result<(), Status> {
        wait::until(&self.stores, self.cfg.ready_timeout, |s| {
            gone(s).then_some(())
        })
        .await
        .ok_or_else(|| Status::deadline_exceeded(format!("{what} still being removed")))
    }
}

#[tonic::async_trait]
impl Identity for Csi {
    async fn get_plugin_info(
        &self,
        _: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: CSI_DRIVER.to_owned(),
            vendor_version: env!("CARGO_PKG_VERSION").to_owned(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        let svc = |t: service::Type| PluginCapability {
            r#type: Some(plugin_capability::Type::Service(
                plugin_capability::Service { r#type: t.into() },
            )),
        };
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![
                svc(service::Type::ControllerService),
                svc(service::Type::VolumeAccessibilityConstraints),
                PluginCapability {
                    r#type: Some(plugin_capability::Type::VolumeExpansion(
                        plugin_capability::VolumeExpansion {
                            r#type: volume_expansion::Type::Online.into(),
                        },
                    )),
                },
            ],
        }))
    }

    async fn probe(&self, _: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}

#[tonic::async_trait]
impl Controller for Csi {
    async fn create_volume(
        &self,
        req: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = req.into_inner();
        let name = &object_name(required(&req.name, "name")?);
        let access_mode =
            requested_access_mode(&req.volume_capabilities).map_err(Status::invalid_argument)?;
        let params = parse_params(&req.parameters).map_err(Status::invalid_argument)?;
        if access_mode == model::AccessMode::ReadWriteMany && params.replicas < 2 {
            return Err(Status::invalid_argument(
                "ReadWriteMany needs replicas >= 2 (StorageClass parameter replicas)",
            ));
        }
        let (source, size, holder_node) = self
            .restore_source(name, &req, params.replicas == 1)
            .await?;
        let size = size
            .filter(|s| *s > 0)
            .ok_or_else(|| Status::invalid_argument("capacity_range.required_bytes is required"))?;
        let (pvc_node, pool) = self.pvc_pins(params.pvc.as_ref()).await;
        let mut node = pvc_node.or(holder_node);
        // WaitForFirstConsumer: the scheduler's node, unless the PVC pins placement.
        if node.is_none() && pool.is_none() && params.replicas == 1 {
            node = req
                .accessibility_requirements
                .as_ref()
                .and_then(|t| t.preferred.first())
                .and_then(|t| t.segments.get(TOPOLOGY_KEY))
                .cloned();
        }
        let spec = VolumeSpec {
            size_bytes: size,
            replicas: params.replicas,
            tier: params.tier,
            pool,
            node,
            access_mode,
            attachments: Vec::new(),
            evict: Vec::new(),
            source,
            resolve: None,
            assignment: None,
        };
        let created = self
            .ensure_volume(name, &spec, req.capacity_range.as_ref())
            .await?;
        let v = match self.wait_volume_ready(name, &created).await {
            Ok(v) => v,
            Err((e, reported)) => {
                if reported.is_some() {
                    self.discard_unassigned(name, reported).await;
                }
                return Err(e);
            }
        };
        let accessible_topology = if v.spec.replicas == 1 {
            local_topology(&v)
        } else {
            Vec::new()
        };
        Ok(Response::new(CreateVolumeResponse {
            volume: Some(::csi::v1::Volume {
                capacity_bytes: to_i64(v.spec.size_bytes),
                volume_id: name.to_owned(),
                volume_context: HashMap::new(),
                content_source: req.volume_content_source.clone(),
                accessible_topology,
            }),
        }))
    }

    async fn delete_volume(
        &self,
        req: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = req.into_inner();
        let name = required(&req.volume_id, "volume_id")?.to_owned();
        match self.volumes().delete(&name, &DeleteParams::default()).await {
            Ok(_) => {}
            Err(kube::Error::Api(s)) if s.is_not_found() => {}
            Err(e) => return Err(kube_status("deleting volume", &e)),
        }
        self.wait_volume_gone(&name).await?;
        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        req: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        let req = req.into_inner();
        let name = required(&req.volume_id, "volume_id")?.to_owned();
        let node = required(&req.node_id, "node_id")?.to_owned();
        let cap = req
            .volume_capability
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("volume_capability is required"))?;
        let wanted = check_capability(cap).map_err(Status::invalid_argument)?;
        let v = self
            .volumes()
            .get_opt(&name)
            .await
            .map_err(|e| kube_status("reading volume", &e))?
            .ok_or_else(|| Status::not_found(format!("volume {name} not found")))?;
        // The reconciler never gives it a replica, so the wait would only
        // end in DeadlineExceeded. Already attached: a repeat publish keeps
        // its replica and stays idempotent.
        if v.spec.evict.contains(&node) && !v.spec.attachments.contains(&node) {
            return Err(Status::failed_precondition(format!(
                "node {node} is being evicted from volume {name}"
            )));
        }
        if self.stores.node(&node).is_none() {
            return Err(Status::not_found(format!("node {node} not found")));
        }
        let assignment = v.spec.assignment.as_ref().ok_or_else(|| {
            Status::failed_precondition(format!("volume {name} is not placed yet"))
        })?;
        // A DRBD volume reaches any node through a diskless replica the
        // reconciler appends for the attachment.
        if v.spec.replicas < 2 && !assignment.replicas.iter().any(|r| r.node == node) {
            return Err(Status::failed_precondition(format!(
                "volume {name} has no replica on node {node}"
            )));
        }
        if wanted == model::AccessMode::ReadWriteMany
            && v.spec.access_mode != model::AccessMode::ReadWriteMany
        {
            return Err(Status::invalid_argument(format!(
                "volume {name} is ReadWriteOnce; MULTI_NODE_* access needs a ReadWriteMany volume"
            )));
        }
        // ReadWriteMany volumes take any number of nodes, each through its
        // own replica entry.
        if v.spec.access_mode == model::AccessMode::ReadWriteOnce
            && let Some(other) = v.spec.attachments.iter().find(|n| **n != node)
        {
            return Err(Status::failed_precondition(format!(
                "volume {name} is already published on {other}"
            )));
        }
        let generation = self.edit_attachments(&name, &node, true).await?;
        // A generation from before the attachment could be a replica still
        // tearing down from an earlier publish.
        let path = wait::until(&self.stores, self.cfg.ready_timeout, |s| {
            let v = s.volume(&name)?;
            let entries = &v.status.as_ref()?.replicas;
            if let Some(e) = entries.iter().find(|e| {
                e.node == node && e.present && e.observed_generation.unwrap_or(0) >= generation
            }) {
                return e.device_path.clone().map(Ok);
            }
            // The agent never recreates a lost LV and a replicas 1 volume has
            // no peer to serve it diskless, so waiting cannot succeed. No
            // generation check: an error entry keeps the previous
            // observedGeneration, so the filter above would never match it.
            if v.spec.replicas == 1
                && let Some(err) = entries
                    .iter()
                    .find(|e| e.node == node && !e.present)
                    .and_then(|e| e.last_error.as_deref())
                    .filter(|err| err.starts_with(model::keys::LV_LOST))
            {
                return Some(Err(Status::failed_precondition(format!(
                    "volume {name} on {node}: {err}"
                ))));
            }
            None
        })
        .await
        .unwrap_or_else(|| {
            let last_error = self.stores.volume(&name).and_then(|v| {
                v.status
                    .as_ref()?
                    .replicas
                    .iter()
                    .find(|e| e.node == node)?
                    .last_error
                    .clone()
            });
            Err(Status::deadline_exceeded(match last_error {
                Some(err) => format!("no device for {name} on {node} yet: {err}"),
                None => format!("no device for {name} on {node} yet"),
            }))
        })?;
        Ok(Response::new(ControllerPublishVolumeResponse {
            publish_context: HashMap::from([(PUBLISH_DEVICE_PATH.to_owned(), path)]),
        }))
    }

    async fn controller_unpublish_volume(
        &self,
        req: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        let req = req.into_inner();
        let name = required(&req.volume_id, "volume_id")?;
        let node = required(&req.node_id, "node_id")?;
        match self.edit_attachments(name, node, false).await {
            Err(s) if s.code() == tonic::Code::NotFound => {
                return Ok(Response::new(ControllerUnpublishVolumeResponse {}));
            }
            r => {
                r?;
            }
        }
        // The diskless replica goes with the attachment; a tiebreaker stays.
        // Returning before the agent's teardown is safe: the node plugin
        // already unstaged, so this node no longer holds the device open,
        // and DRBD quorum guards the remaining Primaries.
        wait::until(&self.stores, self.cfg.ready_timeout, |s| {
            let Some(v) = s.volume(name) else {
                return Some(());
            };
            let diskless = v
                .spec
                .assignment
                .iter()
                .flat_map(|a| &a.replicas)
                .any(|r| r.node == node && r.role == Role::Diskless);
            (!diskless && !v.spec.attachments.iter().any(|n| n == node)).then_some(())
        })
        .await
        .ok_or_else(|| {
            Status::deadline_exceeded(format!(
                "diskless replica of {name} on {node} still assigned"
            ))
        })?;
        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        req: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = req.into_inner();
        let name = required(&req.volume_id, "volume_id")?;
        if req.volume_capabilities.is_empty() {
            return Err(Status::invalid_argument("volume_capabilities is required"));
        }
        let v = self
            .volumes()
            .get_opt(name)
            .await
            .map_err(|e| kube_status("reading volume", &e))?
            .ok_or_else(|| Status::not_found(format!("volume {name} not found")))?;
        Ok(Response::new(
            match capabilities_fit(&req.volume_capabilities, v.spec.access_mode) {
                Ok(()) => ValidateVolumeCapabilitiesResponse {
                    confirmed: Some(Confirmed {
                        volume_context: req.volume_context,
                        volume_capabilities: req.volume_capabilities,
                        parameters: req.parameters,
                        mutable_parameters: HashMap::new(),
                    }),
                    message: String::new(),
                },
                Err(message) => ValidateVolumeCapabilitiesResponse {
                    confirmed: None,
                    message,
                },
            },
        ))
    }

    async fn list_volumes(
        &self,
        _: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Err(Status::unimplemented("ListVolumes"))
    }

    async fn get_capacity(
        &self,
        req: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        let req = req.into_inner();
        for c in &req.volume_capabilities {
            check_capability(c).map_err(Status::invalid_argument)?;
        }
        let segment = req
            .accessible_topology
            .as_ref()
            .and_then(|t| t.segments.get(TOPOLOGY_KEY))
            .map(String::as_str);
        let ready_nodes = self.stores.ready_nodes();
        let pools = Stores::sorted(&self.stores.pools);
        let bytes = crate::capacity::answer(&pools, &ready_nodes, &req.parameters, segment)
            .map_err(Status::invalid_argument)?;
        Ok(Response::new(GetCapacityResponse {
            available_capacity: to_i64(bytes),
            maximum_volume_size: Some(to_i64(bytes)),
            minimum_volume_size: None,
        }))
    }

    async fn controller_get_capabilities(
        &self,
        _: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        let cap = |t: rpc::Type| ControllerServiceCapability {
            r#type: Some(controller_service_capability::Type::Rpc(
                controller_service_capability::Rpc { r#type: t.into() },
            )),
        };
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![
                cap(rpc::Type::CreateDeleteVolume),
                cap(rpc::Type::PublishUnpublishVolume),
                cap(rpc::Type::ExpandVolume),
                cap(rpc::Type::CreateDeleteSnapshot),
                cap(rpc::Type::GetCapacity),
            ],
        }))
    }

    async fn create_snapshot(
        &self,
        req: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = req.into_inner();
        let name = object_name(required(&req.name, "name")?);
        let source = required(&req.source_volume_id, "source_volume_id")?.to_owned();
        let mut snap = self.ensure_snapshot(&name, &source).await?;
        if snap.spec.volume != source {
            return Err(Status::already_exists(format!(
                "snapshot {name} exists for volume {}",
                snap.spec.volume
            )));
        }
        // A failed attempt is not reused: replace it, one new attempt per call.
        if snap
            .status
            .as_ref()
            .is_some_and(|s| s.phase == SnapshotPhase::Failed)
        {
            info!(snapshot = name, "replacing the failed snapshot");
            match self
                .snapshots()
                .delete(&name, &DeleteParams::default())
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(s)) if s.is_not_found() => {}
                Err(e) => return Err(kube_status("deleting failed snapshot", &e)),
            }
            self.wait_snapshot_gone(&name).await?;
            snap = self.ensure_snapshot(&name, &source).await?;
        }
        let uid = snap.metadata.uid.clone();
        let ready = wait::until(&self.stores, self.cfg.ready_timeout, |s| {
            let snap = s.snapshot(&name).filter(|x| x.metadata.uid == uid)?;
            let st = snap.status.as_ref()?;
            match st.phase {
                SnapshotPhase::Ready => Some(Ok(snap.clone())),
                SnapshotPhase::Failed => Some(Err(Status::internal(format!(
                    "snapshot {name} failed: {}",
                    st.last_error.as_deref().unwrap_or("unknown error")
                )))),
                _ => None,
            }
        })
        .await
        .transpose()?;
        let current = ready
            .as_deref()
            .cloned()
            .or_else(|| self.stores.snapshot(&name).as_deref().cloned())
            .unwrap_or(snap);
        let status = current.status.clone().unwrap_or_default();
        let size = status
            .size_bytes
            .or_else(|| self.stores.volume(&source).map(|v| v.spec.size_bytes))
            .unwrap_or(0);
        let created = status
            .created_at
            .map(|t| t.0)
            .or_else(|| current.metadata.creation_timestamp.as_ref().map(|t| t.0))
            .unwrap_or_else(k8s_openapi::jiff::Timestamp::now);
        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(::csi::v1::Snapshot {
                size_bytes: to_i64(size),
                snapshot_id: name,
                source_volume_id: source,
                creation_time: Some(timestamp(created)),
                ready_to_use: ready.is_some(),
                group_snapshot_id: String::new(),
            }),
        }))
    }

    async fn delete_snapshot(
        &self,
        req: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = req.into_inner();
        let name = required(&req.snapshot_id, "snapshot_id")?.to_owned();
        match self
            .snapshots()
            .delete(&name, &DeleteParams::default())
            .await
        {
            Ok(_) => {}
            Err(kube::Error::Api(s)) if s.is_not_found() => {}
            Err(e) => return Err(kube_status("deleting snapshot", &e)),
        }
        self.wait_snapshot_gone(&name).await?;
        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn list_snapshots(
        &self,
        _: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("ListSnapshots"))
    }

    async fn controller_expand_volume(
        &self,
        req: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let req = req.into_inner();
        let name = required(&req.volume_id, "volume_id")?.to_owned();
        let wanted = requested_bytes(req.capacity_range.as_ref())
            .map_err(Status::invalid_argument)?
            .ok_or_else(|| Status::invalid_argument("capacity_range is required"))?;
        let api = self.volumes();
        let mut done = false;
        for _ in 0..8 {
            let v = api
                .get_opt(&name)
                .await
                .map_err(|e| kube_status("reading volume", &e))?
                .ok_or_else(|| Status::not_found(format!("volume {name} not found")))?;
            if v.spec.size_bytes >= wanted {
                done = true;
                break;
            }
            // Assignments only the reconciler has in memory are missed; a
            // newer one lands in the store within a pass.
            let volumes = Stores::sorted(&self.stores.volumes);
            let usage = crate::pool::usage(&volumes, &crate::Memory::default());
            let pools = Stores::sorted(&self.stores.pools);
            if let Some(why) = crate::pool::resize_refusal(&v, wanted, &pools, &usage) {
                return Err(Status::resource_exhausted(format!("volume {name}: {why}")));
            }
            let patch = json!({
                "metadata": { "resourceVersion": v.metadata.resource_version },
                "spec": { "sizeBytes": wanted },
            });
            match api
                .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => {
                    info!(volume = name, size = wanted, "volume expanding");
                    done = true;
                    break;
                }
                Err(kube::Error::Api(s)) if s.is_conflict() => {}
                Err(e) => return Err(kube_status("expanding volume", &e)),
            }
        }
        if !done {
            return Err(Status::aborted(format!("volume {name} kept changing")));
        }
        let usable = wait::until(&self.stores, self.cfg.ready_timeout, |s| {
            s.volume(&name)?
                .status
                .as_ref()?
                .usable_bytes
                .filter(|u| *u >= wanted)
        })
        .await
        .ok_or_else(|| {
            Status::deadline_exceeded(format!("volume {name} not grown to {wanted} bytes yet"))
        })?;
        let block = matches!(
            req.volume_capability
                .as_ref()
                .and_then(|c| c.access_type.as_ref()),
            Some(AccessType::Block(_))
        );
        Ok(Response::new(ControllerExpandVolumeResponse {
            capacity_bytes: to_i64(usable.max(wanted)),
            node_expansion_required: !block,
        }))
    }

    async fn controller_get_volume(
        &self,
        _: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerGetVolume"))
    }

    async fn controller_modify_volume(
        &self,
        _: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerModifyVolume"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::csi::v1::volume_capability::{BlockVolume, MountVolume};

    fn params(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn parameters() {
        let p = parse_params(&params(&[("tier", "fast")])).unwrap();
        assert_eq!(
            p,
            Params {
                tier: "fast".into(),
                replicas: 1,
                pvc: None
            }
        );
        let p = parse_params(&params(&[
            ("tier", "fast"),
            ("replicas", "2"),
            (PARAM_PVC_NAME, "data"),
            (PARAM_PVC_NAMESPACE, "ns"),
            ("csi.storage.k8s.io/pv/name", "pvc-1"),
        ]))
        .unwrap();
        assert_eq!(p.replicas, 2);
        assert_eq!(p.pvc, Some(("ns".into(), "data".into())));
        assert!(parse_params(&params(&[])).is_err());
        assert!(parse_params(&params(&[("tier", "")])).is_err());
        for bad in ["0", "4", "x", "-1", ""] {
            assert!(
                parse_params(&params(&[("tier", "t"), ("replicas", bad)])).is_err(),
                "{bad}"
            );
        }
        assert_eq!(
            parse_params(&params(&[("tier", "t"), ("replicas", "3")]))
                .unwrap()
                .replicas,
            3
        );
        // Half of the PVC pair is ignored.
        assert_eq!(
            parse_params(&params(&[("tier", "t"), (PARAM_PVC_NAME, "x")]))
                .unwrap()
                .pvc,
            None
        );
    }

    fn cap(mode: Mode, access: AccessType) -> VolumeCapability {
        VolumeCapability {
            access_mode: Some(AccessMode { mode: mode.into() }),
            access_type: Some(access),
        }
    }

    fn mount(fs: &str) -> AccessType {
        AccessType::Mount(MountVolume {
            fs_type: fs.into(),
            ..MountVolume::default()
        })
    }

    #[test]
    fn capabilities() {
        use model::AccessMode::{ReadWriteMany as Rwx, ReadWriteOnce as Rwo};
        let block = || AccessType::Block(BlockVolume {});
        for m in [
            Mode::SingleNodeWriter,
            Mode::SingleNodeReaderOnly,
            Mode::SingleNodeSingleWriter,
            Mode::SingleNodeMultiWriter,
        ] {
            assert_eq!(check_capability(&cap(m, mount(""))), Ok(Rwo), "{m:?}");
            assert_eq!(check_capability(&cap(m, mount("xfs"))), Ok(Rwo), "{m:?}");
            assert_eq!(check_capability(&cap(m, block())), Ok(Rwo), "{m:?}");
        }
        for m in [
            Mode::MultiNodeReaderOnly,
            Mode::MultiNodeSingleWriter,
            Mode::MultiNodeMultiWriter,
        ] {
            assert_eq!(check_capability(&cap(m, block())), Ok(Rwx), "{m:?}");
            assert_eq!(
                check_capability(&cap(m, mount("xfs"))),
                Err("ReadWriteMany needs volumeMode Block".into()),
                "{m:?}"
            );
        }
        assert!(check_capability(&cap(Mode::SingleNodeWriter, mount("ext4"))).is_err());
        assert!(check_capability(&cap(Mode::Unknown, mount(""))).is_err());
        assert!(check_capability(&cap(Mode::Unknown, block())).is_err());
        assert!(check_capability(&VolumeCapability::default()).is_err());
        assert!(requested_access_mode(&[]).is_err());
        let rwo = cap(Mode::SingleNodeWriter, block());
        let rwx = cap(Mode::MultiNodeMultiWriter, block());
        assert_eq!(requested_access_mode(std::slice::from_ref(&rwo)), Ok(Rwo));
        assert_eq!(requested_access_mode(&[rwo.clone(), rwx.clone()]), Ok(Rwx));
        assert!(
            requested_access_mode(&[rwx.clone(), cap(Mode::SingleNodeWriter, mount("ext4"))])
                .is_err()
        );
        assert!(capabilities_fit(std::slice::from_ref(&rwo), Rwo).is_ok());
        assert!(capabilities_fit(std::slice::from_ref(&rwo), Rwx).is_ok());
        assert!(capabilities_fit(std::slice::from_ref(&rwx), Rwx).is_ok());
        assert!(capabilities_fit(&[rwo, rwx], Rwo).is_err());
    }

    #[test]
    fn sizes() {
        let r = |req, lim| CapacityRange {
            required_bytes: req,
            limit_bytes: lim,
        };
        assert_eq!(requested_bytes(None), Ok(None));
        assert_eq!(requested_bytes(Some(&r(0, 0))), Ok(None));
        assert_eq!(requested_bytes(Some(&r(10, 0))), Ok(Some(10)));
        assert_eq!(requested_bytes(Some(&r(0, 20))), Ok(Some(20)));
        assert_eq!(requested_bytes(Some(&r(10, 20))), Ok(Some(10)));
        assert!(requested_bytes(Some(&r(30, 20))).is_err());
        assert!(requested_bytes(Some(&r(-1, 0))).is_err());
        assert!(size_fits(10, Some(&r(10, 0))));
        assert!(!size_fits(10, Some(&r(20, 0))));
        assert!(!size_fits(30, Some(&r(10, 20))));
        assert!(size_fits(10, None));
    }

    #[test]
    fn object_names() {
        let pvc = "pvc-0a1b2c3d-4e5f-6789-abcd-ef0123456789";
        assert_eq!(object_name(pvc), pvc);
        assert_eq!(object_name("snapshot-1"), "snapshot-1");
        let long = "sanity-controller-create-single-with-capacity-C603B253-8E7C995E";
        let mapped = object_name(long);
        assert_eq!(mapped, object_name(long));
        assert_ne!(
            mapped,
            object_name("sanity-controller-create-single-with-capacity-C603B253-8E7C995F")
        );
        assert_eq!(
            lvm::names::dm_len("pvc-0a1b2c3d-4e5f-6789-abcd-ef0123456789"),
            MAPPED_DM_MAX
        );
        for n in [
            long,
            "A-B-C-D-E-F-G-H-I-J-K-L-M-N-O-P-Q-R-S-T",
            "UPPER",
            "1abc",
            "x",
            &"a".repeat(128),
            "--",
            "a_b",
            "Ab",
        ] {
            let m = object_name(n);
            assert!(crate::valid_name(&m), "{n} -> {m}");
            assert!(lvm::names::dm_len(&m) <= MAPPED_DM_MAX, "{m}");
            assert!(
                m.bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'),
                "{m}"
            );
            assert!(m.starts_with(|c: char| c.is_ascii_lowercase()), "{m}");
            assert_eq!(object_name(&m), m, "mapping is idempotent");
        }
        assert!(object_name(long).starts_with("sanity-controller-create-"));
    }

    #[test]
    fn endpoints() {
        assert_eq!(socket_path("unix:///csi/csi.sock"), "/csi/csi.sock");
        assert_eq!(socket_path("/tmp/x.sock"), "/tmp/x.sock");
        assert_eq!(socket_path("unix:/tmp/x.sock"), "/tmp/x.sock");
    }
}
