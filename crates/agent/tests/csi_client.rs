//! Tiny CSI client for the end-to-end demo: drives a running agent over its
//! Unix socket. Ignored by default; run next to the agent with
//! `CSI_SOCKET=/path/csi.sock CSI_VOLUME=pvc-x CSI_STEP=up|down ./csi_client-<hash> --ignored`.
//! `up` stages and publishes under CSI_MNT; `down` unpublishes and unstages.

use csi::v1::identity_client::IdentityClient;
use csi::v1::node_client::NodeClient;
use csi::v1::volume_capability::{AccessType, MountVolume};
use csi::v1::{
    GetPluginInfoRequest, NodeGetInfoRequest, NodeGetVolumeStatsRequest, NodePublishVolumeRequest,
    NodeStageVolumeRequest, NodeUnpublishVolumeRequest, NodeUnstageVolumeRequest, VolumeCapability,
};
use hyper_util::rt::TokioIo;
use tonic::transport::{Endpoint, Uri};

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

#[tokio::test]
#[ignore = "needs a running agent"]
async fn drive() {
    let Some(socket) = env("CSI_SOCKET") else {
        eprintln!("CSI_SOCKET not set: skipped");
        return;
    };
    let volume = env("CSI_VOLUME").expect("CSI_VOLUME");
    let step = env("CSI_STEP").unwrap_or_else(|| "up".into());
    let mnt = env("CSI_MNT").unwrap_or_else(|| "/var/tmp/r-drbd-hosttest/mnt".into());
    let staging = format!("{mnt}/staging/{volume}");
    let target = format!("{mnt}/pods/{volume}/mount");

    let channel = Endpoint::try_from("http://[::]:0")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                Ok::<_, std::io::Error>(TokioIo::new(
                    tokio::net::UnixStream::connect(socket).await?,
                ))
            }
        }))
        .await
        .unwrap();
    let mut identity = IdentityClient::new(channel.clone());
    let mut node = NodeClient::new(channel);
    let info = identity
        .get_plugin_info(GetPluginInfoRequest {})
        .await
        .unwrap()
        .into_inner();
    println!(
        "GetPluginInfo: name={} version={}",
        info.name, info.vendor_version
    );
    let info = node
        .node_get_info(NodeGetInfoRequest {})
        .await
        .unwrap()
        .into_inner();
    println!(
        "NodeGetInfo: node_id={} topology={:?}",
        info.node_id,
        info.accessible_topology.map(|t| t.segments)
    );
    let cap = Some(VolumeCapability {
        access_mode: None,
        access_type: Some(AccessType::Mount(MountVolume::default())),
    });

    if step == "up" {
        node.node_stage_volume(NodeStageVolumeRequest {
            volume_id: volume.clone(),
            staging_target_path: staging.clone(),
            volume_capability: cap.clone(),
            ..NodeStageVolumeRequest::default()
        })
        .await
        .unwrap();
        println!("NodeStageVolume {volume} -> {staging}: ok (device from Volume status)");
        node.node_publish_volume(NodePublishVolumeRequest {
            volume_id: volume.clone(),
            staging_target_path: staging.clone(),
            target_path: target.clone(),
            volume_capability: cap,
            ..NodePublishVolumeRequest::default()
        })
        .await
        .unwrap();
        println!("NodePublishVolume {volume} -> {target}: ok");
        let stats = node
            .node_get_volume_stats(NodeGetVolumeStatsRequest {
                volume_id: volume.clone(),
                volume_path: target.clone(),
                staging_target_path: staging.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        for u in stats.usage {
            println!(
                "NodeGetVolumeStats: unit={} total={} used={} available={}",
                u.unit, u.total, u.used, u.available
            );
        }
    } else {
        node.node_unpublish_volume(NodeUnpublishVolumeRequest {
            volume_id: volume.clone(),
            target_path: target.clone(),
        })
        .await
        .unwrap();
        println!("NodeUnpublishVolume {target}: ok");
        node.node_unstage_volume(NodeUnstageVolumeRequest {
            volume_id: volume.clone(),
            staging_target_path: staging.clone(),
        })
        .await
        .unwrap();
        println!("NodeUnstageVolume {staging}: ok");
    }
}
