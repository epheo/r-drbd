//! csi-sanity against the CSI controller plugin, with a fake agent on one
//! node. Runs with R_DRBD_CSI_SANITY=1 (tools/harness/csi-sanity.sh) or
//! R_DRBD_REQUIRE_ENVTEST=1; the binary is CSI_SANITY, else csi-sanity in
//! the Go bin dir or on PATH.

mod support;

use std::collections::HashMap;
use std::process::Command;

use csi::tonic::{Request, Response, Status};
use csi::v1::{self, node_server::Node, node_server::NodeServer};
use support::{Env, FakeAgent, GIB, node, pool};

/// Node service of the one fake node: NodeGetInfo gives the Controller
/// suites a node id with a replica, and the cleanup's unpublish and
/// unstage succeed because the stub never mounts. The Node suites are
/// skipped: they would test this stub, not the product.
struct FakeNode;

type R<T> = Result<Response<T>, Status>;

#[csi::tonic::async_trait]
impl Node for FakeNode {
    async fn node_stage_volume(
        &self,
        _: Request<v1::NodeStageVolumeRequest>,
    ) -> R<v1::NodeStageVolumeResponse> {
        Err(Status::unimplemented("fake node"))
    }
    async fn node_unstage_volume(
        &self,
        _: Request<v1::NodeUnstageVolumeRequest>,
    ) -> R<v1::NodeUnstageVolumeResponse> {
        Ok(Response::new(v1::NodeUnstageVolumeResponse {}))
    }
    async fn node_publish_volume(
        &self,
        _: Request<v1::NodePublishVolumeRequest>,
    ) -> R<v1::NodePublishVolumeResponse> {
        Err(Status::unimplemented("fake node"))
    }
    async fn node_unpublish_volume(
        &self,
        _: Request<v1::NodeUnpublishVolumeRequest>,
    ) -> R<v1::NodeUnpublishVolumeResponse> {
        Ok(Response::new(v1::NodeUnpublishVolumeResponse {}))
    }
    async fn node_get_volume_stats(
        &self,
        _: Request<v1::NodeGetVolumeStatsRequest>,
    ) -> R<v1::NodeGetVolumeStatsResponse> {
        Err(Status::unimplemented("fake node"))
    }
    async fn node_expand_volume(
        &self,
        _: Request<v1::NodeExpandVolumeRequest>,
    ) -> R<v1::NodeExpandVolumeResponse> {
        Err(Status::unimplemented("fake node"))
    }
    async fn node_get_capabilities(
        &self,
        _: Request<v1::NodeGetCapabilitiesRequest>,
    ) -> R<v1::NodeGetCapabilitiesResponse> {
        Ok(Response::new(v1::NodeGetCapabilitiesResponse::default()))
    }
    async fn node_get_info(
        &self,
        _: Request<v1::NodeGetInfoRequest>,
    ) -> R<v1::NodeGetInfoResponse> {
        Ok(Response::new(v1::NodeGetInfoResponse {
            node_id: "n1".into(),
            max_volumes_per_node: 0,
            accessible_topology: Some(v1::Topology {
                segments: HashMap::from([(model::keys::TOPOLOGY_KEY.to_owned(), "n1".to_owned())]),
            }),
        }))
    }
}

fn sanity_bin() -> String {
    if let Ok(bin) = std::env::var("CSI_SANITY") {
        return bin;
    }
    let go_bin = std::env::var_os("GOPATH")
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| std::path::Path::new(&h).join("go")))
        .map(|d| d.join("bin/csi-sanity"));
    match go_bin {
        Some(p) if p.is_file() => p.display().to_string(),
        _ => "csi-sanity".to_owned(),
    }
}

/// Runs csi-sanity once; returns (success, summary lines).
async fn sanity(bin: &str, args: Vec<String>) -> (bool, Vec<String>) {
    let mut cmd = Command::new(bin);
    cmd.args(["-ginkgo.no-color", "-ginkgo.v"]).args(args);
    if let Ok(extra) = std::env::var("CSI_SANITY_ARGS") {
        cmd.args(extra.split_whitespace());
    }
    eprintln!("running {cmd:?}");
    let out = tokio::task::spawn_blocking(move || cmd.output())
        .await
        .expect("join")
        .expect("run csi-sanity");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    println!("{stdout}");
    eprintln!("{}", String::from_utf8_lossy(&out.stderr));
    let summary = stdout
        .lines()
        .filter(|l| {
            l.starts_with("Ran ") || l.contains("Passed |") || l.trim_start().starts_with("[FAIL]")
        })
        .map(str::to_owned)
        .collect();
    (out.status.success(), summary)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn csi_sanity() {
    // The require flag covers this test too, so a CI edit cannot turn it
    // into a silent pass.
    if std::env::var_os("R_DRBD_CSI_SANITY").is_none() && !support::envtest_required() {
        eprintln!("SKIPPED: set R_DRBD_CSI_SANITY=1 to run csi-sanity");
        return;
    }
    let assets = require_envtest!();
    let bin = sanity_bin();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "test", 1000 * GIB).await;
    let cfg = support::config(&env, "sanity");
    let endpoint = cfg.csi_endpoint.clone();
    let sock = std::path::PathBuf::from(endpoint.trim_start_matches("unix://"));
    let _ctl = support::spawn(env.clients().await, cfg);
    let agent = FakeAgent::new(&client, "n1").spawn_auto();
    support::eventually(std::time::Duration::from_secs(30), || async {
        sock.exists().then_some(())
    })
    .await;

    let node_sock = env.dir.path().join("node.sock");
    let listener = tokio::net::UnixListener::bind(&node_sock).expect("bind node socket");
    let node_srv = tokio::spawn(
        csi::tonic::transport::Server::builder()
            .add_service(NodeServer::new(FakeNode))
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener)),
    );

    let params = env.dir.path().join("params.yaml");
    std::fs::write(&params, "tier: test\n").expect("params file");
    let params_arg = format!("-csi.testvolumeparameters={}", params.display());
    // Identity, and GroupController capability discovery, against the
    // controller socket itself.
    let (id_ok, id_summary) = sanity(
        &bin,
        vec![
            format!("-csi.endpoint={endpoint}"),
            "-ginkgo.focus=Identity Service".into(),
            "-ginkgo.focus=GroupController Service".into(),
        ],
    )
    .await;
    // Controller suites; node calls (NodeGetInfo) go to the fake node.
    let (ctl_ok, ctl_summary) = sanity(
        &bin,
        vec![
            format!("-csi.endpoint=unix://{}", node_sock.display()),
            format!("-csi.controllerendpoint={endpoint}"),
            params_arg,
            "-ginkgo.skip=Identity Service".into(),
            "-ginkgo.skip=GroupController Service".into(),
            "-ginkgo.skip=Node Service".into(),
        ],
    )
    .await;
    agent.abort();
    node_srv.abort();
    eprintln!(
        "csi-sanity identity:\n{}\ncsi-sanity controller:\n{}",
        id_summary.join("\n"),
        ctl_summary.join("\n")
    );
    assert!(id_ok && ctl_ok, "csi-sanity failed");
}
