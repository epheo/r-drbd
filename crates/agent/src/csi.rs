//! CSI Identity and Node services on a Unix socket. Volume id == Volume
//! name. Every mount tool runs through [`Exec`]; there are no mount
//! syscalls here.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ::csi::tonic::{self, Request, Response, Status};
use ::csi::v1::identity_server::{Identity, IdentityServer};
use ::csi::v1::node_server::{Node, NodeServer};
use ::csi::v1::volume_capability::AccessType;
use ::csi::v1::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, NodeExpandVolumeRequest, NodeExpandVolumeResponse,
    NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse, NodeGetInfoRequest,
    NodeGetInfoResponse, NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse,
    NodePublishVolumeRequest, NodePublishVolumeResponse, NodeServiceCapability,
    NodeStageVolumeRequest, NodeStageVolumeResponse, NodeUnpublishVolumeRequest,
    NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest, NodeUnstageVolumeResponse,
    PluginCapability, ProbeRequest, ProbeResponse, Topology, VolumeCapability, VolumeUsage,
};
use kube::runtime::reflector::{ObjectRef, Store};
use model::Volume;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;

use crate::Exec;
use crate::exec::{CmdError, DEFAULT_TIMEOUT, MKFS_TIMEOUT, argv, failure, run_any, run_ok};
use crate::run::Error;
use crate::shutdown::OpenGate;

/// publish_context key the controller fills from the Volume status.
pub const PUBLISH_DEVICE_PATH: &str = "devicePath";
pub const DEFAULT_FS: &str = "xfs";
/// How long NodeStage waits for the device node to appear.
pub const DEVICE_WAIT: Duration = Duration::from_secs(30);
const DEVICE_POLL: Duration = Duration::from_millis(200);

/// What the Volume status says about this node's device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    VolumeMissing,
    NoDevice,
    Device(String),
}

/// Volume-level facts NodeStage needs before trusting a file system.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VolumeFacts {
    /// status.fileSystem: written only after mkfs (or a restore) completed.
    pub file_system: Option<String>,
    /// spec.source.snapshot is set: the device starts with restored data.
    pub restored: bool,
}

pub trait VolumeLookup: Send + Sync + 'static {
    fn lookup(&self, volume: &str, node: &str) -> Lookup;
    /// `None` when the Volume is not known.
    fn facts(&self, volume: &str) -> Option<VolumeFacts>;
}

/// Durable record that a volume's file system is complete.
#[async_trait::async_trait]
pub trait FsMarker: Send + Sync + 'static {
    async fn write(&self, volume: &str, fs: &str) -> Result<(), String>;
    /// status.fileSystem straight from the API server, never a cache: the
    /// last word before a device with a signature is formatted again.
    async fn read_live(&self, volume: &str) -> Result<Option<String>, String>;
}

/// Server-side apply of status.fileSystem under the shared FS field manager.
#[derive(Clone)]
pub struct KubeFsMarker {
    client: kube::Client,
}

impl KubeFsMarker {
    pub fn new(client: kube::Client) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for KubeFsMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeFsMarker").finish_non_exhaustive()
    }
}

/// The apply body: nothing but the marker.
pub fn fs_marker_body(volume: &str, fs: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": format!("{}/{}", model::GROUP, model::VERSION),
        "kind": "Volume",
        "metadata": { "name": volume },
        "status": { "fileSystem": fs },
    })
}

#[async_trait::async_trait]
impl FsMarker for KubeFsMarker {
    async fn write(&self, volume: &str, fs: &str) -> Result<(), String> {
        kube::Api::<Volume>::all(self.client.clone())
            .patch_status(
                volume,
                &kube::api::PatchParams::apply(model::keys::FS_FIELD_MANAGER).force(),
                &kube::api::Patch::Apply(fs_marker_body(volume, fs)),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn read_live(&self, volume: &str) -> Result<Option<String>, String> {
        kube::Api::<Volume>::all(self.client.clone())
            .get(volume)
            .await
            .map(|v| v.status.and_then(|s| s.file_system))
            .map_err(|e| e.to_string())
    }
}

/// Label mkfs writes and clears once it succeeded: the only proof on the
/// device that a format was cut short.
pub const MKFS_LABEL: &str = "r-drbd-mkfs";

/// blkid's view of a device with a signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Probe {
    pub type_: String,
    pub label: Option<String>,
}

/// What NodeStage does with the device once blkid has spoken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FsAction {
    Mount,
    MarkThenMount,
    FormatMarkMount,
    /// A finished file system without a record (the status was lost).
    Adopt,
}

/// `found` is blkid's probe (`None`: empty device), `fs` the requested type.
pub(crate) fn fs_decision(
    found: Option<&Probe>,
    facts: &VolumeFacts,
    fs: &str,
) -> Result<FsAction, Status> {
    let marker = facts.file_system.as_deref();
    let Some(p) = found else {
        return match marker {
            // The data is already gone; a fresh file system would hide that.
            Some(m) => Err(Status::failed_precondition(format!(
                "volume records {m} but the device is empty"
            ))),
            None if facts.restored => Err(Status::failed_precondition(
                "restored volume has no file system",
            )),
            None => Ok(FsAction::FormatMarkMount),
        };
    };
    let t = p.type_.as_str();
    match marker {
        Some(m) if m != t => Err(Status::failed_precondition(format!(
            "device holds {t} but the volume records {m}"
        ))),
        _ if t != fs => Err(Status::failed_precondition(format!(
            "device holds {t}, {fs} requested"
        ))),
        Some(_) => Ok(FsAction::Mount),
        None if facts.restored => Ok(FsAction::MarkThenMount),
        // The caller confirms with the API server before formatting.
        None if p.label.as_deref() == Some(MKFS_LABEL) => Ok(FsAction::FormatMarkMount),
        // Only a type we would have made ourselves is ours to adopt.
        None if fs == DEFAULT_FS => Ok(FsAction::Adopt),
        None => Err(Status::failed_precondition(format!(
            "device holds {t} without a record; only {DEFAULT_FS} is adopted"
        ))),
    }
}

impl VolumeLookup for Store<Volume> {
    fn facts(&self, volume: &str) -> Option<VolumeFacts> {
        let vol = self.get(&ObjectRef::new(volume))?;
        Some(VolumeFacts {
            file_system: vol.status.as_ref().and_then(|s| s.file_system.clone()),
            restored: vol.spec.source.is_some(),
        })
    }

    fn lookup(&self, volume: &str, node: &str) -> Lookup {
        let Some(vol) = self.get(&ObjectRef::new(volume)) else {
            return Lookup::VolumeMissing;
        };
        vol.status
            .as_ref()
            .and_then(|s| s.replicas.iter().find(|r| r.node == node))
            .and_then(|r| r.device_path.clone())
            .map_or(Lookup::NoDevice, Lookup::Device)
    }
}

/// `unix:///run/csi.sock` or a bare path.
pub fn socket_path(endpoint: &str) -> PathBuf {
    PathBuf::from(endpoint.strip_prefix("unix://").unwrap_or(endpoint))
}

/// A leftover socket from a previous run is replaced; any other file at the
/// path is a configuration error and is never deleted.
pub fn bind(path: &Path) -> Result<UnixListener, Error> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| Error::io(format!("create {}", dir.display()), e))?;
    }
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_socket() => {
            std::fs::remove_file(path)
                .map_err(|e| Error::io(format!("remove {}", path.display()), e))?;
            tracing::info!(socket = %path.display(), "removed stale CSI socket");
        }
        Ok(_) => {
            return Err(Error::io(
                format!("CSI endpoint {}", path.display()),
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "exists and is not a socket",
                ),
            ));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::io(format!("stat {}", path.display()), e)),
    }
    UnixListener::bind(path).map_err(|e| Error::io(format!("bind {}", path.display()), e))
}

/// Serves until `stop` resolves; accepted requests still complete.
pub async fn serve(
    listener: UnixListener,
    node: NodeService,
    stop: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    tracing::info!("CSI node plugin serving");
    tonic::transport::Server::builder()
        .add_service(IdentityServer::new(IdentityService))
        .add_service(NodeServer::new(node))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), stop)
        .await
}

#[derive(Debug, Default, Clone, Copy)]
pub struct IdentityService;

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: model::keys::CSI_DRIVER.to_owned(),
            vendor_version: env!("CARGO_PKG_VERSION").to_owned(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        use ::csi::v1::plugin_capability::{Service, Type, service};
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![PluginCapability {
                r#type: Some(Type::Service(Service {
                    r#type: service::Type::VolumeAccessibilityConstraints.into(),
                })),
            }],
        }))
    }

    async fn probe(&self, _: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}

/// Volume ids with a Node operation running. A second RPC on the same
/// volume gets Aborted; shutdown waits until the set is empty.
#[derive(Debug, Clone, Default)]
pub struct Inflight {
    set: Arc<Mutex<HashSet<String>>>,
}

struct InflightGuard {
    inflight: Inflight,
    volume: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.inflight.set.lock() {
            set.remove(&self.volume);
        }
    }
}

impl Inflight {
    fn begin(&self, volume: &str) -> Option<InflightGuard> {
        let mut set = self.set.lock().ok()?;
        set.insert(volume.to_owned()).then(|| InflightGuard {
            inflight: self.clone(),
            volume: volume.to_owned(),
        })
    }

    pub fn is_idle(&self) -> bool {
        self.set.lock().map_or(true, |s| s.is_empty())
    }

    /// True when idle within `bound`.
    pub async fn wait_idle(&self, bound: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + bound;
        while !self.is_idle() {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        true
    }
}

struct Inner {
    node: String,
    exec: Arc<dyn Exec>,
    volumes: Arc<dyn VolumeLookup>,
    marker: Arc<dyn FsMarker>,
    /// Volumes this process marked: a second guard next to the live read.
    marked: Arc<Mutex<HashSet<String>>>,
    device_wait: Duration,
}

#[derive(Clone)]
pub struct NodeService {
    inner: Arc<Inner>,
    inflight: Inflight,
    opens: OpenGate,
}

impl std::fmt::Debug for NodeService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeService")
            .field("node", &self.inner.node)
            .finish_non_exhaustive()
    }
}

fn internal(e: &CmdError) -> Status {
    Status::internal(e.to_string())
}

fn io_status(what: &str, path: &str, e: &std::io::Error) -> Status {
    Status::internal(format!("{what} {path}: {e}"))
}

fn required<'a>(value: &'a str, what: &str) -> Result<&'a str, Status> {
    if value.is_empty() {
        Err(Status::invalid_argument(format!("{what} is required")))
    } else {
        Ok(value)
    }
}

fn access_type(cap: Option<&VolumeCapability>) -> Result<&AccessType, Status> {
    cap.and_then(|c| c.access_type.as_ref()).ok_or_else(|| {
        Status::invalid_argument("volume_capability with an access type is required")
    })
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Filesystem calls block; keep them off the runtime workers.
async fn blocking<T, F>(f: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, Status> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Status::internal(format!("blocking task: {e}")))?
}

async fn create_dir_all(path: &str) -> Result<(), Status> {
    let p = path.to_owned();
    blocking(move || std::fs::create_dir_all(&p).map_err(|e| io_status("create", &p, &e))).await
}

async fn is_block_device(path: &str) -> Result<bool, Status> {
    let p = path.to_owned();
    blocking(move || Ok(std::fs::metadata(&p).is_ok_and(|m| m.file_type().is_block_device()))).await
}

/// `major:minor` of the device node at `path`, as findmnt prints MAJ:MIN.
async fn dev_majmin(path: &str) -> Result<String, Status> {
    let p = path.to_owned();
    blocking(move || {
        let rdev = std::fs::metadata(&p)
            .map_err(|e| io_status("stat", &p, &e))?
            .rdev();
        Ok(format!(
            "{}:{}",
            rustix::fs::major(rdev),
            rustix::fs::minor(rdev)
        ))
    })
    .await
}

impl NodeService {
    pub fn new(
        node: String,
        exec: Arc<dyn Exec>,
        volumes: Arc<dyn VolumeLookup>,
        marker: Arc<dyn FsMarker>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                node,
                exec,
                volumes,
                marker,
                marked: Arc::default(),
                device_wait: DEVICE_WAIT,
            }),
            inflight: Inflight::default(),
            opens: OpenGate::default(),
        }
    }

    /// Only before the service is shared.
    #[must_use]
    pub fn with_device_wait(self, wait: Duration) -> Self {
        let i = &self.inner;
        Self {
            inner: Arc::new(Inner {
                node: i.node.clone(),
                exec: i.exec.clone(),
                volumes: i.volumes.clone(),
                marker: i.marker.clone(),
                marked: i.marked.clone(),
                device_wait: wait,
            }),
            inflight: self.inflight,
            opens: self.opens,
        }
    }

    pub fn inflight(&self) -> Inflight {
        self.inflight.clone()
    }

    /// Closing it refuses NodeStage and NodePublish; the unstage and
    /// unpublish calls kubelet makes while the node goes down still work.
    pub fn open_gate(&self) -> OpenGate {
        self.opens.clone()
    }

    fn admit_open(&self) -> Result<(), Status> {
        if self.opens.is_closed() {
            Err(Status::unavailable("node shutting down"))
        } else {
            Ok(())
        }
    }

    /// Runs `op` in its own task: a client cancel drops the handler but never
    /// kills mkfs or mount half way. The volume stays busy until the task ends.
    async fn guarded<T, F, Fut>(&self, volume: &str, op: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Inner>) -> Fut,
        Fut: Future<Output = Result<T, Status>> + Send + 'static,
    {
        let volume = required(volume, "volume_id")?;
        let guard = self.inflight.begin(volume).ok_or_else(|| {
            Status::aborted(format!("an operation on volume {volume} is in progress"))
        })?;
        let fut = op(self.inner.clone());
        tokio::spawn(async move {
            let _guard = guard;
            fut.await
        })
        .await
        .map_err(|e| Status::internal(format!("operation task: {e}")))?
    }

    #[cfg(test)]
    async fn stage(&self, req: NodeStageVolumeRequest) -> Result<(), Status> {
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.stage(req).await })
            .await
    }

    #[cfg(test)]
    async fn unstage(&self, req: NodeUnstageVolumeRequest) -> Result<(), Status> {
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.unstage(req).await })
            .await
    }

    #[cfg(test)]
    async fn publish(&self, req: NodePublishVolumeRequest) -> Result<(), Status> {
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.publish(req).await })
            .await
    }

    #[cfg(test)]
    async fn unpublish(&self, req: NodeUnpublishVolumeRequest) -> Result<(), Status> {
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.unpublish(req).await })
            .await
    }

    #[cfg(test)]
    async fn expand(&self, req: NodeExpandVolumeRequest) -> Result<i64, Status> {
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.expand(req).await })
            .await
    }

    #[cfg(test)]
    async fn stats(&self, req: NodeGetVolumeStatsRequest) -> Result<Vec<VolumeUsage>, Status> {
        self.inner.stats(req).await
    }
}

impl Inner {
    fn exec(&self) -> &dyn Exec {
        self.exec.as_ref()
    }

    /// Device path without waiting for the node to appear.
    fn device_path(
        &self,
        volume: &str,
        publish_context: &HashMap<String, String>,
    ) -> Result<String, Status> {
        if let Some(d) = publish_context
            .get(PUBLISH_DEVICE_PATH)
            .filter(|d| !d.is_empty())
        {
            return Ok(d.clone());
        }
        match self.volumes.lookup(volume, &self.node) {
            Lookup::Device(d) => Ok(d),
            Lookup::VolumeMissing => Err(Status::not_found(format!("volume {volume} not found"))),
            Lookup::NoDevice => Err(Status::unavailable(format!(
                "volume {volume} has no device on node {}",
                self.node
            ))),
        }
    }

    async fn device(
        &self,
        volume: &str,
        publish_context: &HashMap<String, String>,
    ) -> Result<String, Status> {
        let dev = self.device_path(volume, publish_context)?;
        let deadline = tokio::time::Instant::now() + self.device_wait;
        loop {
            if tokio::fs::try_exists(&dev).await.unwrap_or(false) {
                return Ok(dev);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::unavailable(format!(
                    "device {dev} did not show up in {:?}",
                    self.device_wait
                )));
            }
            tokio::time::sleep(DEVICE_POLL).await;
        }
    }

    /// MAJ:MIN of what is mounted exactly at `path`; findmnt exits 1 when
    /// nothing is.
    async fn mount_of(&self, path: &str) -> Result<Option<String>, Status> {
        let cmd = argv(&["findmnt", "-n", "-o", "MAJ:MIN", "--mountpoint", path]);
        let out = run_any(self.exec(), &cmd, DEFAULT_TIMEOUT)
            .await
            .map_err(|e| internal(&e))?;
        match out.exit_code {
            Some(0) => Ok(Some(out.stdout.trim().to_owned())),
            Some(1) => Ok(None),
            _ => Err(internal(&failure(&cmd, &out))),
        }
    }

    async fn mount_source(&self, path: &str) -> Result<String, Status> {
        let cmd = argv(&["findmnt", "-n", "-o", "SOURCE", "--mountpoint", path]);
        let out = run_ok(self.exec(), &cmd, DEFAULT_TIMEOUT)
            .await
            .map_err(|e| Status::not_found(format!("{path} is not mounted: {e}")))?;
        Ok(out.stdout.trim().to_owned())
    }

    async fn run(&self, cmd: &[String]) -> Result<String, Status> {
        run_ok(self.exec(), cmd, DEFAULT_TIMEOUT)
            .await
            .map(|o| o.stdout)
            .map_err(|e| internal(&e))
    }

    async fn device_bytes(&self, dev: &str) -> Result<u64, Status> {
        let out = self.run(&lvm::cmd::device_size(dev)).await?;
        lvm::cmd::parse_device_size(&out).ok_or_else(|| {
            Status::internal(format!("blockdev {dev}: unparsable size '{}'", out.trim()))
        })
    }

    /// File system type and label, `None` only for a device blkid calls
    /// empty (exit 2). A partition table or any signature blkid cannot name
    /// is FailedPrecondition: mkfs there would destroy data.
    async fn fs_probe(&self, dev: &str) -> Result<Option<Probe>, Status> {
        let cmd = argv(&[
            "blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-s", "LABEL", "-o", "export", dev,
        ]);
        let out = run_any(self.exec(), &cmd, DEFAULT_TIMEOUT)
            .await
            .map_err(|e| internal(&e))?;
        let field = |key: &str| {
            out.stdout
                .lines()
                .find_map(|l| l.trim().strip_prefix(key))
                .map(str::to_owned)
        };
        match out.exit_code {
            Some(0) => field("TYPE=")
                .map(|type_| {
                    Some(Probe {
                        type_,
                        label: field("LABEL="),
                    })
                })
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "{dev} is not empty and has no file system: {}",
                        out.stdout.trim().replace('\n', " ")
                    ))
                }),
            Some(2) => Ok(None),
            _ => Err(internal(&failure(&cmd, &out))),
        }
    }

    async fn stage(&self, req: NodeStageVolumeRequest) -> Result<(), Status> {
        let volume = required(&req.volume_id, "volume_id")?;
        let staging = required(&req.staging_target_path, "staging_target_path")?;
        let mount = match access_type(req.volume_capability.as_ref())? {
            AccessType::Block(_) => return Ok(()),
            AccessType::Mount(m) => m,
        };
        let fs = if mount.fs_type.is_empty() {
            DEFAULT_FS
        } else {
            mount.fs_type.as_str()
        };
        // Before the device wait, so a retry after success returns at once.
        if self
            .already_staged(volume, staging, &req.publish_context)
            .await?
        {
            return Ok(());
        }
        let dev = self.device(volume, &req.publish_context).await?;
        create_dir_all(staging).await?;
        let facts = self
            .volumes
            .facts(volume)
            .ok_or_else(|| Status::not_found(format!("volume {volume} not found")))?;
        let found = self.fs_probe(&dev).await?;
        let mut action = fs_decision(found.as_ref(), &facts, fs)?;
        if matches!(action, FsAction::FormatMarkMount | FsAction::Adopt) {
            // The cache can lag behind our own marker write: never format or
            // adopt on its word alone. An empty device with a live marker
            // lost its data, and a fresh file system would hide that.
            // Adopt skips the in-process set: a record wiped since our own
            // write must be written again.
            let live = if action == FsAction::FormatMarkMount && self.was_marked(volume) {
                Some(fs.to_owned())
            } else {
                self.marker
                    .read_live(volume)
                    .await
                    .map_err(|e| Status::unavailable(format!("read fileSystem of {volume}: {e}")))?
            };
            if live.is_some() {
                let facts = VolumeFacts {
                    file_system: live,
                    ..facts
                };
                action = fs_decision(found.as_ref(), &facts, fs)?;
            }
        }
        match action {
            FsAction::Mount => {
                tracing::debug!(volume, dev, fs, "file system complete, no mkfs");
            }
            FsAction::MarkThenMount => {
                tracing::info!(volume, dev, fs, "restored file system adopted");
                if fs == DEFAULT_FS {
                    self.new_xfs_uuid(&dev, staging).await?;
                }
                self.mark(volume, fs).await?;
            }
            FsAction::FormatMarkMount => {
                if fs != DEFAULT_FS {
                    return Err(Status::invalid_argument(format!(
                        "fs_type {fs} is not supported, only xfs is formatted"
                    )));
                }
                if found.is_some() {
                    tracing::warn!(
                        volume,
                        dev,
                        "unfinished mkfs (label {MKFS_LABEL}): formatting again"
                    );
                } else {
                    tracing::info!(volume, dev, "formatting xfs");
                }
                self.mkfs_xfs(&dev).await?;
                self.mark(volume, fs).await?;
            }
            FsAction::Adopt => {
                tracing::warn!(volume, dev, fs, "file system without a record adopted");
                self.mark(volume, fs).await?;
            }
        }
        let mut cmd = argv(&["mount", "-t", fs]);
        if !mount.mount_flags.is_empty() {
            cmd.push("-o".to_owned());
            cmd.push(mount.mount_flags.join(","));
        }
        cmd.push(dev);
        cmd.push(staging.to_owned());
        self.run(&cmd).await?;
        if fs == DEFAULT_FS {
            // A restore or a re-created PVC can be larger than its file
            // system, and the kubelet calls NodeExpand only on a PVC resize.
            if let Err(e) = self.run(&argv(&["xfs_growfs", staging])).await {
                tracing::warn!(volume, staging, error = %e.message(), "xfs_growfs after mount failed; NodeExpand can retry");
            }
        }
        Ok(())
    }

    /// The label marks the format unfinished until it is cleared; a stage
    /// that stops in between formats again, losing nothing (the fs is empty).
    async fn mkfs_xfs(&self, dev: &str) -> Result<(), Status> {
        run_ok(
            self.exec(),
            &argv(&["mkfs.xfs", "-q", "-f", "-K", "-L", MKFS_LABEL, dev]),
            MKFS_TIMEOUT,
        )
        .await
        .map_err(|e| internal(&e))?;
        self.run(&argv(&["xfs_admin", "-L", "--", dev]))
            .await
            .map(|_| ())
            .map_err(|e| Status::internal(format!("clear mkfs label: {}", e.message())))
    }

    /// Something is mounted at the staging path: our device, completely
    /// staged, is the idempotent success; anything else is an error.
    async fn already_staged(
        &self,
        volume: &str,
        staging: &str,
        publish_context: &HashMap<String, String>,
    ) -> Result<bool, Status> {
        let Some(mounted) = self.mount_of(staging).await? else {
            return Ok(false);
        };
        if let Ok(dev) = self.device_path(volume, publish_context)
            && tokio::fs::try_exists(&dev).await.unwrap_or(false)
            && dev_majmin(&dev).await? != mounted
        {
            return Err(Status::already_exists(format!(
                "{staging} has another device mounted ({mounted})"
            )));
        }
        self.require_restored_marker(volume, staging).await?;
        Ok(true)
    }

    /// A restored volume is staged only once its new UUID and marker are
    /// done; a mount without the marker is a stage cut short (or made by
    /// hand) and must not pass for success.
    async fn require_restored_marker(&self, volume: &str, staging: &str) -> Result<(), Status> {
        let Some(facts) = self.volumes.facts(volume) else {
            return Ok(());
        };
        if !facts.restored || facts.file_system.is_some() || self.was_marked(volume) {
            return Ok(());
        }
        let live = self
            .marker
            .read_live(volume)
            .await
            .map_err(|e| Status::unavailable(format!("read fileSystem of {volume}: {e}")))?;
        if live.is_some() {
            return Ok(());
        }
        Err(Status::failed_precondition(format!(
            "restored volume mounted without its marker; unmount {staging} to retry"
        )))
    }

    /// A thin-snapshot clone keeps its source's XFS UUID, and the kernel
    /// refuses a second mount of one UUID on a node. Before the marker:
    /// mount with nouuid once to replay the log (xfs_admin refuses a dirty
    /// log), unmount, give it a fresh UUID. Any failure leaves it unmounted.
    async fn new_xfs_uuid(&self, dev: &str, staging: &str) -> Result<(), Status> {
        self.run(&argv(&["mount", "-t", "xfs", "-o", "nouuid", dev, staging]))
            .await?;
        if let Err(e) = self.run(&argv(&["umount", staging])).await {
            // Retry once so the device is not left mounted under nouuid.
            let _ = self.run(&argv(&["umount", staging])).await;
            return Err(Status::internal(format!(
                "unmount after log replay: {}",
                e.message()
            )));
        }
        tracing::info!(dev, "giving the restored xfs a new UUID");
        self.run(&argv(&["xfs_admin", "-U", "generate", dev]))
            .await
            .map(|_| ())
            .map_err(|e| Status::internal(format!("new xfs UUID: {}", e.message())))
    }

    /// Nothing is mounted until the marker is durable: a mount on an
    /// unrecorded file system could be reformatted on the next stage.
    async fn mark(&self, volume: &str, fs: &str) -> Result<(), Status> {
        self.marker
            .write(volume, fs)
            .await
            .map_err(|e| Status::unavailable(format!("record fileSystem of {volume}: {e}")))?;
        if let Ok(mut m) = self.marked.lock() {
            m.insert(volume.to_owned());
        }
        Ok(())
    }

    /// Also forgets volumes gone from the store, so a recreated Volume of
    /// the same name starts unmarked.
    fn was_marked(&self, volume: &str) -> bool {
        self.marked.lock().is_ok_and(|mut m| {
            m.retain(|v| self.volumes.facts(v).is_some());
            m.contains(volume)
        })
    }

    async fn unstage(&self, req: NodeUnstageVolumeRequest) -> Result<(), Status> {
        required(&req.volume_id, "volume_id")?;
        let staging = required(&req.staging_target_path, "staging_target_path")?;
        if self.mount_of(staging).await?.is_some() {
            self.run(&argv(&["umount", staging])).await?;
        }
        Ok(())
    }

    async fn publish(&self, req: NodePublishVolumeRequest) -> Result<(), Status> {
        let volume = required(&req.volume_id, "volume_id")?;
        let target = required(&req.target_path, "target_path")?;
        let source = match access_type(req.volume_capability.as_ref())? {
            AccessType::Block(_) => {
                let dev = self.device(volume, &req.publish_context).await?;
                let (t, d) = (target.to_owned(), dev.clone());
                let rdev = |p: &str| {
                    std::fs::metadata(p)
                        .map(|m| m.rdev())
                        .map_err(|e| io_status("stat", p, &e))
                };
                if self.mount_of(target).await?.is_some() {
                    return blocking(move || {
                        if rdev(&t)? == rdev(&d)? {
                            Ok(())
                        } else {
                            Err(Status::already_exists(format!(
                                "{t} has another device bound"
                            )))
                        }
                    })
                    .await;
                }
                blocking(move || {
                    if let Some(dir) = Path::new(&t).parent() {
                        std::fs::create_dir_all(dir)
                            .map_err(|e| io_status("create", &dir.display().to_string(), &e))?;
                    }
                    std::fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(&t)
                        .map_err(|e| io_status("create", &t, &e))?;
                    Ok(())
                })
                .await?;
                dev
            }
            AccessType::Mount(_) => {
                let staging = required(&req.staging_target_path, "staging_target_path")?;
                let Some(staged) = self.mount_of(staging).await? else {
                    return Err(Status::failed_precondition(format!(
                        "{staging} is not staged"
                    )));
                };
                if let Some(mounted) = self.mount_of(target).await? {
                    return if mounted == staged {
                        Ok(())
                    } else {
                        Err(Status::already_exists(format!(
                            "{target} has another device mounted ({mounted})"
                        )))
                    };
                }
                create_dir_all(target).await?;
                staging.to_owned()
            }
        };
        let opts = if req.readonly { "bind,ro" } else { "bind" };
        self.run(&argv(&["mount", "-o", opts, &source, target]))
            .await?;
        Ok(())
    }

    async fn unpublish(&self, req: NodeUnpublishVolumeRequest) -> Result<(), Status> {
        required(&req.volume_id, "volume_id")?;
        let target = required(&req.target_path, "target_path")?;
        if self.mount_of(target).await?.is_some() {
            self.run(&argv(&["umount", target])).await?;
        }
        let t = target.to_owned();
        blocking(move || {
            let path = Path::new(&t);
            let res = if path.is_dir() {
                std::fs::remove_dir(path)
            } else {
                std::fs::remove_file(path)
            };
            match res {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    Err(io_status("remove", &t, &e))
                }
                _ => Ok(()),
            }
        })
        .await
    }

    async fn stats(&self, req: NodeGetVolumeStatsRequest) -> Result<Vec<VolumeUsage>, Status> {
        use ::csi::v1::volume_usage::Unit;
        required(&req.volume_id, "volume_id")?;
        let path = required(&req.volume_path, "volume_path")?;
        if !tokio::fs::try_exists(path).await.unwrap_or(false) {
            return Err(Status::not_found(format!("{path} does not exist")));
        }
        if is_block_device(path).await? {
            let total = to_i64(self.device_bytes(path).await?);
            return Ok(vec![VolumeUsage {
                total,
                unit: Unit::Bytes.into(),
                ..VolumeUsage::default()
            }]);
        }
        let p = path.to_owned();
        let s = blocking(move || {
            rustix::fs::statvfs(p.as_str())
                .map_err(|e| Status::internal(format!("statvfs {p}: {e}")))
        })
        .await?;
        let frsize = s.f_frsize;
        Ok(vec![
            VolumeUsage {
                available: to_i64(s.f_bavail.saturating_mul(frsize)),
                total: to_i64(s.f_blocks.saturating_mul(frsize)),
                used: to_i64(s.f_blocks.saturating_sub(s.f_bfree).saturating_mul(frsize)),
                unit: Unit::Bytes.into(),
            },
            VolumeUsage {
                available: to_i64(s.f_ffree),
                total: to_i64(s.f_files),
                used: to_i64(s.f_files.saturating_sub(s.f_ffree)),
                unit: Unit::Inodes.into(),
            },
        ])
    }

    /// The LV grows first (the agent follows Volume.spec.sizeBytes); until it
    /// did, return Unavailable so the kubelet retries.
    async fn expand(&self, req: NodeExpandVolumeRequest) -> Result<i64, Status> {
        required(&req.volume_id, "volume_id")?;
        let path = if req.volume_path.is_empty() {
            required(&req.staging_target_path, "volume_path")?
        } else {
            req.volume_path.as_str()
        };
        let want = req.capacity_range.map_or(0, |c| c.required_bytes);
        let block = matches!(
            req.volume_capability
                .as_ref()
                .and_then(|c| c.access_type.as_ref()),
            Some(AccessType::Block(_))
        ) || is_block_device(path).await?;
        let dev = if block {
            path.to_owned()
        } else {
            self.mount_source(path).await?
        };
        let have = to_i64(self.device_bytes(&dev).await?);
        if have < want {
            return Err(Status::unavailable(format!(
                "device {dev} is {have} bytes, {want} requested: not grown yet"
            )));
        }
        if !block {
            self.run(&argv(&["xfs_growfs", path])).await?;
        }
        Ok(have)
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn node_stage_volume(
        &self,
        req: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        self.admit_open()?;
        let req = req.into_inner();
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.stage(req).await })
            .await?;
        Ok(Response::new(NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        req: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let req = req.into_inner();
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.unstage(req).await })
            .await?;
        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        req: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        self.admit_open()?;
        let req = req.into_inner();
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.publish(req).await })
            .await?;
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        req: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = req.into_inner();
        let id = req.volume_id.clone();
        self.guarded(&id, move |i| async move { i.unpublish(req).await })
            .await?;
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        req: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let usage = self.inner.stats(req.into_inner()).await?;
        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage,
            volume_condition: None,
        }))
    }

    async fn node_expand_volume(
        &self,
        req: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        let req = req.into_inner();
        let id = req.volume_id.clone();
        let capacity_bytes = self
            .guarded(&id, move |i| async move { i.expand(req).await })
            .await?;
        Ok(Response::new(NodeExpandVolumeResponse { capacity_bytes }))
    }

    async fn node_get_capabilities(
        &self,
        _: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        use ::csi::v1::node_service_capability::{Rpc, Type, rpc};
        let cap = |t: rpc::Type| NodeServiceCapability {
            r#type: Some(Type::Rpc(Rpc { r#type: t.into() })),
        };
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![
                cap(rpc::Type::StageUnstageVolume),
                cap(rpc::Type::ExpandVolume),
                cap(rpc::Type::GetVolumeStats),
            ],
        }))
    }

    async fn node_get_info(
        &self,
        _: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.inner.node.clone(),
            max_volumes_per_node: 0,
            accessible_topology: Some(Topology {
                segments: HashMap::from([(
                    model::keys::TOPOLOGY_KEY.to_owned(),
                    self.inner.node.clone(),
                )]),
            }),
        }))
    }
}

#[cfg(test)]
mod tests;
