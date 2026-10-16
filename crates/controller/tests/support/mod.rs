//! envtest: a real etcd and kube-apiserver per test, the CRDs installed,
//! the controller running against it, and a fake agent that writes status
//! the way the real one does.

#![allow(dead_code, unreachable_pub, clippy::missing_panics_doc)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{Namespace, Node};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{DeleteParams, ListParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Api, Client, ResourceExt};
use model::keys::{agent_field_manager, agent_lease_name};
use model::{
    Pool, PoolSpec, ReplicaStatus, Role, Snapshot, SnapshotAck, SnapshotPhase, Volume, VolumeSpec,
};
use serde_json::json;
use tempfile::TempDir;

pub const NAMESPACE: &str = "r-drbd-system";
pub const GIB: u64 = 1 << 30;
/// Where `setup-envtest use 1.33.0` installs, relative to the XDG data dir.
const ENVTEST_DIR: &str = "kubebuilder-envtest/k8s/1.33.0-linux-amd64";
/// apiservers running at once in one test binary; more starved the
/// apiserver start on a loaded 32 core machine.
const MAX_PARALLEL: usize = 4;

static SLOTS: Mutex<usize> = Mutex::new(0);
static FREED: Condvar = Condvar::new();

/// The envtest binaries, or None with the reason printed.
pub fn assets() -> Option<PathBuf> {
    let dir = std::env::var_os("KUBEBUILDER_ASSETS")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("XDG_DATA_HOME")
                .map(PathBuf::from)
                .or_else(|| std::env::var_os("HOME").map(|h| Path::new(&h).join(".local/share")))
                .map(|d| d.join(ENVTEST_DIR))
        })
        .unwrap_or_default();
    for bin in ["etcd", "kube-apiserver"] {
        if !dir.join(bin).is_file() {
            eprintln!(
                "SKIPPED: envtest binary {} missing (set KUBEBUILDER_ASSETS)",
                dir.join(bin).display()
            );
            return None;
        }
    }
    Some(dir)
}

/// Returns from the test when the envtest binaries are missing.
#[macro_export]
macro_rules! require_envtest {
    () => {
        match support::assets() {
            Some(a) => a,
            None => return,
        }
    };
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("free port")
}

fn log_file(dir: &Path, name: &str) -> Stdio {
    Stdio::from(std::fs::File::create(dir.join(name)).expect("log file"))
}

pub struct Env {
    pub dir: TempDir,
    pub kubeconfig: PathBuf,
    etcd: Child,
    apiserver: Child,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = self.apiserver.kill();
        let _ = self.etcd.kill();
        let _ = self.apiserver.wait();
        let _ = self.etcd.wait();
        if std::env::var_os("R_DRBD_KEEP_ENVTEST").is_some() {
            let path = self.dir.path().to_owned();
            eprintln!("envtest dir kept: {}", path.display());
            std::mem::forget(std::mem::replace(
                &mut self.dir,
                TempDir::new().expect("tempdir"),
            ));
        }
        let mut n = SLOTS.lock().expect("slots");
        *n -= 1;
        FREED.notify_one();
    }
}

impl Env {
    /// Starts etcd and kube-apiserver, installs the CRDs and the namespace.
    pub async fn start(assets: &Path) -> Self {
        {
            let mut n = SLOTS.lock().expect("slots");
            while *n >= MAX_PARALLEL {
                n = FREED.wait(n).expect("slots");
            }
            *n += 1;
        }
        let dir = tempfile::Builder::new()
            .prefix("r-drbd-envtest-")
            .tempdir()
            .expect("tempdir");
        let d = dir.path();
        let (client_port, peer_port, api_port) = (free_port(), free_port(), free_port());
        let etcd = Command::new(assets.join("etcd"))
            .args([
                "--name=default".to_owned(),
                format!("--data-dir={}", d.join("etcd").display()),
                format!("--listen-client-urls=http://127.0.0.1:{client_port}"),
                format!("--advertise-client-urls=http://127.0.0.1:{client_port}"),
                format!("--listen-peer-urls=http://127.0.0.1:{peer_port}"),
                format!("--initial-advertise-peer-urls=http://127.0.0.1:{peer_port}"),
                format!("--initial-cluster=default=http://127.0.0.1:{peer_port}"),
                "--unsafe-no-fsync=true".to_owned(),
            ])
            .stdout(log_file(d, "etcd.log"))
            .stderr(log_file(d, "etcd.err"))
            .spawn()
            .expect("start etcd");

        let key = d.join("sa.key");
        let status = Command::new("openssl")
            .args(["genrsa", "-out"])
            .arg(&key)
            .arg("2048")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("openssl");
        assert!(status.success(), "openssl genrsa failed");
        std::fs::write(
            d.join("tokens.csv"),
            "test-token,admin,admin,system:masters\n",
        )
        .expect("token file");
        let apiserver = Command::new(assets.join("kube-apiserver"))
            .args([
                format!("--etcd-servers=http://127.0.0.1:{client_port}"),
                format!("--secure-port={api_port}"),
                "--bind-address=127.0.0.1".to_owned(),
                "--advertise-address=127.0.0.1".to_owned(),
                format!("--cert-dir={}", d.join("certs").display()),
                "--service-account-issuer=https://kubernetes.default.svc".to_owned(),
                format!("--service-account-key-file={}", key.display()),
                format!("--service-account-signing-key-file={}", key.display()),
                format!("--token-auth-file={}", d.join("tokens.csv").display()),
                "--authorization-mode=AlwaysAllow".to_owned(),
                "--service-cluster-ip-range=10.0.0.0/24".to_owned(),
                "--disable-admission-plugins=ServiceAccount".to_owned(),
                "--allow-privileged=true".to_owned(),
            ])
            .stdout(log_file(d, "apiserver.log"))
            .stderr(log_file(d, "apiserver.err"))
            .spawn()
            .expect("start kube-apiserver");

        let kubeconfig = d.join("kubeconfig");
        std::fs::write(
            &kubeconfig,
            format!(
                "apiVersion: v1\nkind: Config\nclusters:\n- name: envtest\n  cluster:\n    server: https://127.0.0.1:{api_port}\n    insecure-skip-tls-verify: true\nusers:\n- name: admin\n  user:\n    token: test-token\ncontexts:\n- name: envtest\n  context:\n    cluster: envtest\n    user: admin\ncurrent-context: envtest\n"
            ),
        )
        .expect("kubeconfig");
        let env = Self {
            dir,
            kubeconfig,
            etcd,
            apiserver,
        };
        env.wait_up().await;
        env.install().await;
        env
    }

    pub async fn client(&self) -> Client {
        let kc = Kubeconfig::read_from(&self.kubeconfig).expect("read kubeconfig");
        let cfg = kube::Config::from_custom_kubeconfig(kc, &KubeConfigOptions::default())
            .await
            .expect("kube config");
        Client::try_from(cfg).expect("client")
    }

    async fn wait_up(&self) {
        let client = self.client().await;
        let api: Api<Namespace> = Api::all(client);
        for _ in 0..1200 {
            if api.get_opt("default").await.ok().flatten().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("apiserver not up; logs in {}", self.dir.path().display());
    }

    async fn install(&self) {
        let client = self.client().await;
        let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
        for crd in model::crds() {
            crds.create(&PostParams::default(), &crd)
                .await
                .expect("create CRD");
        }
        for crd in model::crds() {
            let name = crd.name_any();
            eventually(Duration::from_secs(30), || async {
                let c = crds.get(&name).await.ok()?;
                c.status?
                    .conditions?
                    .iter()
                    .any(|c| c.type_ == "Established" && c.status == "True")
                    .then_some(())
            })
            .await;
        }
        let ns: Api<Namespace> = Api::all(client);
        ns.create(
            &PostParams::default(),
            &Namespace {
                metadata: ObjectMeta {
                    name: Some(NAMESPACE.into()),
                    ..ObjectMeta::default()
                },
                ..Namespace::default()
            },
        )
        .await
        .expect("namespace");
    }
}

/// Polls `f` every 100 ms until it yields; panics after `timeout`.
pub async fn eventually<T, F, Fut>(timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("controller=info")),
        )
        .with_test_writer()
        .try_init();
}

/// Runs the controller in the background; aborted on drop.
pub struct Running(pub tokio::task::JoinHandle<Result<(), controller::Error>>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub fn config(env: &Env, identity: &str) -> controller::Config {
    let sock = env.dir.path().join(format!("{identity}.sock"));
    let mut cfg =
        controller::Config::new(NAMESPACE, format!("unix://{}", sock.display()), identity);
    cfg.ready_timeout = Duration::from_secs(20);
    cfg
}

pub fn spawn(client: Client, cfg: controller::Config) -> Running {
    Running(tokio::spawn(controller::run_with_client(cfg, client)))
}

/// Renews `node`'s agent Lease every second like a live agent, until the test
/// runtime ends; a paused or deleted Lease is left alone.
fn heartbeat(client: Client, node: String) {
    tokio::spawn(async move {
        let api: Api<Lease> = Api::namespaced(client, NAMESPACE);
        let name = agent_lease_name(&node);
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let Ok(Some(l)) = api.get_opt(&name).await else {
                continue;
            };
            if l.spec.and_then(|s| s.holder_identity).as_deref() == Some(PAUSED) {
                continue;
            }
            let now = micro(k8s_openapi::jiff::Timestamp::now());
            let _ = api
                .patch(
                    &name,
                    &PatchParams::default(),
                    &Patch::Merge(&json!({ "spec": { "renewTime": now } })),
                )
                .await;
        }
    });
}

const PAUSED: &str = "paused";

/// Lease times are MicroTime: at most six fractional digits.
fn micro(t: k8s_openapi::jiff::Timestamp) -> serde_json::Value {
    serde_json::to_value(k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime(t))
        .expect("MicroTime")
}

/// Creates or rewrites `node`'s agent Lease renewed `ago` in the past;
/// `paused` stops the heartbeat from renewing it.
pub async fn agent_lease(client: &Client, node: &str, ago: Duration, paused: bool) {
    let api: Api<Lease> = Api::namespaced(client.clone(), NAMESPACE);
    let renewed = micro(
        k8s_openapi::jiff::Timestamp::now()
            .checked_sub(ago)
            .expect("time"),
    );
    let lease = json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": { "name": agent_lease_name(node), "namespace": NAMESPACE },
        "spec": {
            "holderIdentity": if paused { PAUSED } else { node },
            "leaseDurationSeconds": model::keys::LEASE_STALE_SECS,
            "renewTime": renewed,
        },
    });
    api.patch(
        &agent_lease_name(node),
        &PatchParams::apply("test-agent").force(),
        &Patch::Apply(&lease),
    )
    .await
    .expect("agent lease");
}

pub async fn delete_agent_lease(client: &Client, node: &str) {
    Api::<Lease>::namespaced(client.clone(), NAMESPACE)
        .delete(&agent_lease_name(node), &DeleteParams::default())
        .await
        .expect("delete lease");
}

pub async fn delete_node(client: &Client, node: &str) {
    Api::<Node>::all(client.clone())
        .delete(node, &DeleteParams::default())
        .await
        .expect("delete node");
}

/// A core Node with the Ready condition set, and its agent's Lease kept
/// fresh.
pub async fn node(client: &Client, name: &str, ready: bool) {
    agent_lease(client, name, Duration::ZERO, false).await;
    heartbeat(client.clone(), name.to_owned());
    let api: Api<Node> = Api::all(client.clone());
    api.create(
        &PostParams::default(),
        &Node {
            metadata: ObjectMeta {
                name: Some(name.into()),
                ..ObjectMeta::default()
            },
            ..Node::default()
        },
    )
    .await
    .expect("create node");
    set_node_ready(client, name, ready).await;
}

/// Sets the Node Ready condition, as the kubelet would.
pub async fn set_node_ready(client: &Client, name: &str, ready: bool) {
    let api: Api<Node> = Api::all(client.clone());
    let now = k8s_openapi::jiff::Timestamp::now().to_string();
    let status = json!({ "status": { "conditions": [{
        "type": "Ready",
        "status": if ready { "True" } else { "False" },
        "reason": "Test",
        "message": "",
        "lastHeartbeatTime": now,
        "lastTransitionTime": now,
    }]}});
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&status))
        .await
        .expect("node status");
}

/// A Pool with capacity and Available reported as its agent would.
pub async fn pool(client: &Client, name: &str, node: &str, tier: &str, free: u64) {
    pool_with_vg(client, name, node, tier, free, "vg").await;
}

pub async fn pool_with_vg(
    client: &Client,
    name: &str,
    node: &str,
    tier: &str,
    free: u64,
    vg: &str,
) {
    let api: Api<Pool> = Api::all(client.clone());
    api.create(
        &PostParams::default(),
        &Pool::new(
            name,
            PoolSpec {
                node: node.into(),
                tier: tier.into(),
                vg: vg.into(),
                thin_pool: "thin".into(),
                overcommit_percent: model::DEFAULT_OVERCOMMIT_PERCENT,
                reserve_percent: model::DEFAULT_RESERVE_PERCENT,
            },
        ),
    )
    .await
    .expect("create pool");
    pool_capacity(client, name, node, free * 2, free).await;
}

/// The agent's capacity report for a Pool.
pub async fn pool_capacity(client: &Client, name: &str, node: &str, total: u64, free: u64) {
    let api: Api<Pool> = Api::all(client.clone());
    let now = k8s_openapi::jiff::Timestamp::now().to_string();
    let status = json!({
        "apiVersion": "r-drbd.io/v1alpha1",
        "kind": "Pool",
        "metadata": { "name": name },
        "status": {
            "totalBytes": total,
            "freeBytes": free,
            "chunkBytes": 65536,
            "conditions": [{
                "type": "Available", "status": "True", "reason": "Ok", "message": "",
                "lastTransitionTime": now,
            }],
        }
    });
    api.patch_status(
        name,
        &PatchParams::apply(&agent_field_manager(node)).force(),
        &Patch::Apply(&status),
    )
    .await
    .expect("pool status");
}

pub fn volume_spec(size: u64, replicas: u32, tier: &str) -> VolumeSpec {
    VolumeSpec {
        size_bytes: size,
        replicas,
        tier: tier.into(),
        pool: None,
        node: None,
        access_mode: model::AccessMode::default(),
        attachments: Vec::new(),
        evict: Vec::new(),
        source: None,
        resolve: None,
        assignment: None,
    }
}

pub async fn create_volume(client: &Client, name: &str, spec: VolumeSpec) {
    let api: Api<Volume> = Api::all(client.clone());
    api.create(&PostParams::default(), &Volume::new(name, spec))
        .await
        .expect("create volume");
}

pub async fn create_snapshot(client: &Client, name: &str, volume: &str) {
    let api: Api<Snapshot> = Api::all(client.clone());
    api.create(
        &PostParams::default(),
        &Snapshot::new(
            name,
            model::SnapshotSpec {
                volume: volume.into(),
            },
        ),
    )
    .await
    .expect("create snapshot");
}

pub async fn get_volume(client: &Client, name: &str) -> Option<Volume> {
    Api::<Volume>::all(client.clone())
        .get_opt(name)
        .await
        .expect("get volume")
}

pub async fn get_snapshot(client: &Client, name: &str) -> Option<Snapshot> {
    Api::<Snapshot>::all(client.clone())
        .get_opt(name)
        .await
        .expect("get snapshot")
}

pub async fn get_pool(client: &Client, name: &str) -> Option<Pool> {
    Api::<Pool>::all(client.clone())
        .get_opt(name)
        .await
        .expect("get pool")
}

/// Waits until `f` holds on the Volume and returns it.
pub async fn volume_where(client: &Client, name: &str, f: impl Fn(&Volume) -> bool) -> Volume {
    eventually(Duration::from_secs(30), || async {
        get_volume(client, name).await.filter(|v| f(v))
    })
    .await
}

pub async fn snapshot_where(
    client: &Client,
    name: &str,
    f: impl Fn(&Snapshot) -> bool,
) -> Snapshot {
    eventually(Duration::from_secs(30), || async {
        get_snapshot(client, name).await.filter(|s| f(s))
    })
    .await
}

pub fn phase_of(v: &Volume) -> model::VolumePhase {
    v.status.as_ref().map(|s| s.phase).unwrap_or_default()
}

pub fn snap_phase(s: &Snapshot) -> SnapshotPhase {
    s.status.as_ref().map(|s| s.phase).unwrap_or_default()
}

pub fn ready_reason(v: &Volume) -> Option<String> {
    let s = v.status.as_ref()?;
    model::condition::get(&s.conditions, "Ready").map(|c| c.reason.clone())
}

/// One node's agent, reduced to its status writes. It reports replicas >= 2
/// volumes too (UpToDate, peers Connected, quorum), diskless and tiebreaker
/// replicas as Diskless, and drops its entry once its node leaves the
/// assignment, as the real agent does. A DRBD diskful
/// replica that appears after status.initializedAt (a rebuild, a node from
/// spec.evict) first reports Inconsistent, as after a just-created
/// metadata, and UpToDate on the next step (the full sync).
#[derive(Clone)]
pub struct FakeAgent {
    pub client: Client,
    pub node: String,
}

impl FakeAgent {
    pub fn new(client: &Client, node: &str) -> Self {
        Self {
            client: client.clone(),
            node: node.into(),
        }
    }

    /// Applies this node's replica entry with the per-node field manager.
    pub async fn report(&self, volume: &str, entry: ReplicaStatus) {
        let status = json!({
            "apiVersion": "r-drbd.io/v1alpha1",
            "kind": "Volume",
            "metadata": { "name": volume },
            "status": { "replicas": [entry] },
        });
        Api::<Volume>::all(self.client.clone())
            .patch_status(
                volume,
                &PatchParams::apply(&agent_field_manager(&self.node)).force(),
                &Patch::Apply(&status),
            )
            .await
            .expect("agent status");
    }

    fn role(&self, v: &Volume) -> Option<Role> {
        v.spec
            .assignment
            .as_ref()?
            .replicas
            .iter()
            .find(|r| r.node == self.node)
            .map(|r| r.role)
    }

    /// Replica present and in sync at `size` bytes.
    pub async fn ready_at(&self, volume: &str, size: Option<u64>) {
        let v = get_volume(&self.client, volume).await.expect("volume");
        let diskful = self.role(&v) == Some(Role::Diskful);
        let peers = v
            .spec
            .assignment
            .iter()
            .flat_map(|a| &a.replicas)
            .filter(|r| r.role == Role::Diskful && r.node != self.node)
            .map(|r| (r.node.clone(), "Connected".to_owned()))
            .collect();
        let minor = v.spec.assignment.as_ref().and_then(|a| a.minor);
        let device = match minor {
            Some(m) => format!("/dev/drbd{m}"),
            None => format!("/dev/vg/{volume}"),
        };
        let initialized = v.status.as_ref().and_then(|s| s.initialized_at.as_ref());
        let mine = v
            .status
            .as_ref()
            .and_then(|s| s.replicas.iter().find(|e| e.node == self.node));
        let rebuild = diskful
            && minor.is_some()
            && initialized.is_some()
            && mine.is_none_or(|e| !e.present || e.disk_state.as_deref() == Some("Diskless"));
        let disk = match (diskful, rebuild) {
            (true, true) => "Inconsistent",
            (true, false) => "UpToDate",
            (false, _) => "Diskless",
        };
        self.report(
            volume,
            ReplicaStatus {
                node: self.node.clone(),
                present: true,
                disk_state: Some(disk.into()),
                sync_percent: rebuild.then_some(0),
                device_path: Some(device),
                allocated_bytes: diskful.then_some(v.spec.size_bytes),
                size_bytes: Some(size.unwrap_or(v.spec.size_bytes)),
                observed_generation: v.metadata.generation,
                peers,
                quorum: v
                    .spec
                    .assignment
                    .as_ref()
                    .and_then(|a| a.minor)
                    .map(|_| true),
                ..ReplicaStatus::default()
            },
        )
        .await;
    }

    /// Removes this node's entry: everything applied under its manager.
    pub async fn forget(&self, volume: &str) {
        let status = json!({
            "apiVersion": "r-drbd.io/v1alpha1",
            "kind": "Volume",
            "metadata": { "name": volume },
            "status": { "replicas": [] },
        });
        Api::<Volume>::all(self.client.clone())
            .patch_status(
                volume,
                &PatchParams::apply(&agent_field_manager(&self.node)).force(),
                &Patch::Apply(&status),
            )
            .await
            .expect("agent status");
    }

    pub async fn ready(&self, volume: &str) {
        self.ready_at(volume, None).await;
    }

    pub async fn gone(&self, volume: &str) {
        let v = get_volume(&self.client, volume).await.expect("volume");
        self.report(
            volume,
            ReplicaStatus {
                node: self.node.clone(),
                present: false,
                observed_generation: v.metadata.generation,
                ..ReplicaStatus::default()
            },
        )
        .await;
    }

    pub async fn ack(&self, snapshot: &str, phase: SnapshotPhase, ok: bool, pool: Option<&str>) {
        let ack = SnapshotAck {
            node: self.node.clone(),
            phase,
            pool: pool.map(str::to_owned),
            ok,
            error: (!ok).then(|| format!("fake failure on {}", self.node)),
        };
        let status = json!({
            "apiVersion": "r-drbd.io/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": snapshot },
            "status": { "acks": [ack] },
        });
        Api::<Snapshot>::all(self.client.clone())
            .patch_status(
                snapshot,
                &PatchParams::apply(&agent_field_manager(&self.node)).force(),
                &Patch::Apply(&status),
            )
            .await
            .expect("agent ack");
    }

    /// Acts on every Volume and Snapshot of this node like a healthy agent,
    /// until the task is aborted.
    pub fn spawn_auto(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.step().await {
                    eprintln!("fake agent {}: {e}", self.node);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    }

    async fn step(&self) -> Result<(), kube::Error> {
        let volumes = Api::<Volume>::all(self.client.clone())
            .list(&ListParams::default())
            .await?;
        for v in &volumes.items {
            let name = v.name_any();
            let mine = v
                .status
                .as_ref()
                .and_then(|s| s.replicas.iter().find(|e| e.node == self.node));
            let Some(role) = self.role(v) else {
                if mine.is_some() && v.spec.assignment.is_some() {
                    self.forget(&name).await;
                }
                continue;
            };
            let generation = v.metadata.generation;
            if v.metadata.deletion_timestamp.is_some() {
                if mine.is_none_or(|e| e.present || e.observed_generation != generation) {
                    self.gone(&name).await;
                }
            } else if mine.is_none_or(|e| {
                !e.present
                    || e.observed_generation != generation
                    || (role == Role::Diskful
                        && (e.size_bytes != Some(v.spec.size_bytes)
                            || e.disk_state.as_deref() != Some("UpToDate")))
            }) {
                self.ready(&name).await;
            }
        }
        let snapshots = Api::<Snapshot>::all(self.client.clone())
            .list(&ListParams::default())
            .await?;
        for s in &snapshots.items {
            let status = s.status.clone().unwrap_or_default();
            let mine = status.acks.iter().find(|a| a.node == self.node);
            if mine.is_some_and(|a| a.phase == status.phase) {
                continue;
            }
            let volume = volumes.items.iter().find(|v| v.name_any() == s.spec.volume);
            let member = volume.and_then(|v| self.role(v)).is_some();
            let my_pool = volume
                .and_then(|v| v.spec.assignment.as_ref())
                .and_then(|a| {
                    a.replicas
                        .iter()
                        .find(|r| r.node == self.node && r.role == Role::Diskful)
                })
                .and_then(|r| r.pool.clone());
            let name = s.name_any();
            match status.phase {
                SnapshotPhase::Suspending | SnapshotPhase::Resuming if member => {
                    let pool = mine.and_then(|a| a.pool.clone());
                    self.ack(&name, status.phase, true, pool.as_deref()).await;
                }
                SnapshotPhase::Taking if my_pool.is_some() => {
                    self.ack(&name, status.phase, true, my_pool.as_deref())
                        .await;
                }
                SnapshotPhase::Deleting if mine.is_some_and(|a| a.pool.is_some()) => {
                    let pool = mine.and_then(|a| a.pool.clone());
                    self.ack(&name, status.phase, true, pool.as_deref()).await;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Field paths the controller must never own: agent-written status.
const AGENT_FIELDS: &[&str] = &[
    "f:replicas",
    "f:acks",
    "f:totalBytes",
    "f:freeBytes",
    "f:chunkBytes",
    "k:{\"type\":\"Available\"}",
];

/// Panics when the controller's status entry in managedFields holds an
/// agent field; returns how many controller status entries were checked.
pub fn assert_controller_status_fields(meta: &ObjectMeta) -> usize {
    let mut checked = 0;
    for m in meta.managed_fields.iter().flatten() {
        if m.manager.as_deref() != Some(model::keys::CONTROLLER_FIELD_MANAGER)
            || m.subresource.as_deref() != Some("status")
        {
            continue;
        }
        let fields = serde_json::to_string(&m.fields_v1).expect("fieldsV1");
        for f in AGENT_FIELDS {
            assert!(!fields.contains(f), "controller owns {f}: {fields}");
        }
        checked += 1;
    }
    checked
}

/// The node plugin's filesystem marker, under the shared fs manager.
pub async fn set_file_system(client: &Client, volume: &str, fs: &str) {
    let status = json!({
        "apiVersion": "r-drbd.io/v1alpha1",
        "kind": "Volume",
        "metadata": { "name": volume },
        "status": { "fileSystem": fs },
    });
    Api::<Volume>::all(client.clone())
        .patch_status(
            volume,
            &PatchParams::apply(model::keys::FS_FIELD_MANAGER).force(),
            &Patch::Apply(&status),
        )
        .await
        .expect("fileSystem marker");
}
