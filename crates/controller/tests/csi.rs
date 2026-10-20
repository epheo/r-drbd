//! CSI controller calls against envtest, for what csi-sanity cannot see:
//! PVC annotation pins, topology, error codes, publish context.

mod support;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use csi::tonic::{Code, Request};
use csi::v1::controller_server::Controller;
use csi::v1::volume_capability::access_mode::Mode;
use csi::v1::volume_capability::{AccessMode, AccessType, BlockVolume, MountVolume};
use csi::v1::volume_content_source::{SnapshotSource, Type as SourceType};
use csi::v1::{
    CapacityRange, ControllerExpandVolumeRequest, ControllerPublishVolumeRequest,
    CreateSnapshotRequest, CreateVolumeRequest, GetCapacityRequest, Topology, TopologyRequirement,
    ValidateVolumeCapabilitiesRequest, VolumeCapability, VolumeContentSource,
};
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::Api;
use kube::api::PostParams;
use model::keys::TOPOLOGY_KEY;
use support::{Env, FakeAgent, GIB, get_volume, node, pool};

fn mount() -> VolumeCapability {
    VolumeCapability {
        access_mode: Some(AccessMode {
            mode: Mode::SingleNodeWriter.into(),
        }),
        access_type: Some(AccessType::Mount(MountVolume::default())),
    }
}

fn create(name: &str, params: &[(&str, &str)], size: u64) -> CreateVolumeRequest {
    CreateVolumeRequest {
        name: name.into(),
        capacity_range: Some(CapacityRange {
            required_bytes: i64::try_from(size).unwrap(),
            limit_bytes: 0,
        }),
        volume_capabilities: vec![mount()],
        parameters: params
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        ..CreateVolumeRequest::default()
    }
}

fn topology(node: &str) -> Topology {
    Topology {
        segments: HashMap::from([(TOPOLOGY_KEY.to_owned(), node.to_owned())]),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn controller_calls() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    for n in ["n1", "n2"] {
        node(&client, n, true).await;
    }
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    pool(&client, "p2a", "n2", "fast", 50 * GIB).await;
    pool(&client, "p2b", "n2", "fast", 40 * GIB).await;
    let cfg = support::config(&env, "ctl");
    let _ctl = support::spawn(env.clients().await, cfg.clone());
    let _agents = [
        FakeAgent::new(&client, "n1").spawn_auto(),
        FakeAgent::new(&client, "n2").spawn_auto(),
    ];
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, cfg);

    // Capacity from placeableBytes, once the pool pass has published it.
    // Rooms: p1 90 GiB, p2a 45, p2b 36 (free less the 5 percent reserve).
    let capacity = |params: &[(&str, &str)], node: Option<&str>| GetCapacityRequest {
        parameters: params
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        accessible_topology: node.map(topology),
        ..GetCapacityRequest::default()
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    let local = [("tier", "fast")];
    let replicated = [("tier", "fast"), ("replicas", "2")];
    let expected = [
        (&local[..], Some("n1"), 90),
        (&local[..], Some("n2"), 45),
        (&local[..], Some("n3"), 0),
        (&local[..], None, 90),
        (&replicated[..], Some("n1"), 45),
        (&replicated[..], Some("n2"), 45),
        // Any node attaches a replicated volume, n3 without a Pool too.
        (&replicated[..], Some("n3"), 45),
        (&replicated[..], None, 45),
        // No Pool has the tier: 0, never a stale number.
        (&[("tier", "slow")][..], Some("n1"), 0),
        (&[("tier", "fast"), ("replicas", "3")][..], None, 0),
    ];
    for (params, node, gib) in expected {
        let want = i64::try_from(gib * GIB).unwrap();
        loop {
            let got = svc
                .get_capacity(Request::new(capacity(params, node)))
                .await
                .unwrap()
                .into_inner();
            if got.available_capacity == want {
                assert_eq!(got.maximum_volume_size, Some(want));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{params:?} on {node:?}: {} bytes, want {want}",
                got.available_capacity
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let err = svc
        .get_capacity(Request::new(capacity(
            &[("tier", "fast"), ("replicas", "4")],
            Some("n1"),
        )))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    // PVC annotations pin the pool (and so the node) over free space.
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), "default");
    let pvc: PersistentVolumeClaim = serde_json::from_value(serde_json::json!({
        "metadata": { "name": "data", "annotations": { "r-drbd.io/pool": "p2b" } },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": "1Gi" } },
        },
    }))
    .unwrap();
    pvcs.create(&PostParams::default(), &pvc).await.unwrap();
    let params = [
        ("tier", "fast"),
        ("csi.storage.k8s.io/pvc/name", "data"),
        ("csi.storage.k8s.io/pvc/namespace", "default"),
    ];
    let mut req = create("pvc-pinned", &params, GIB);
    // The pin wins over the scheduler's preferred node.
    req.accessibility_requirements = Some(TopologyRequirement {
        requisite: vec![],
        preferred: vec![topology("n1")],
    });
    let v = svc
        .create_volume(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .volume
        .unwrap();
    assert_eq!(v.volume_id, "pvc-pinned");
    assert_eq!(v.capacity_bytes, i64::try_from(GIB).unwrap());
    assert_eq!(v.accessible_topology, [topology("n2")]);
    let obj = get_volume(&client, "pvc-pinned").await.unwrap();
    assert_eq!(obj.spec.pool.as_deref(), Some("p2b"));
    let a = obj.spec.assignment.unwrap();
    assert_eq!(a.replicas[0].pool.as_deref(), Some("p2b"));

    // A missing PVC only loses the pins; topology then pins the node.
    let mut req = create(
        "pvc-topo",
        &[
            ("tier", "fast"),
            ("csi.storage.k8s.io/pvc/name", "nope"),
            ("csi.storage.k8s.io/pvc/namespace", "default"),
        ],
        GIB,
    );
    req.accessibility_requirements = Some(TopologyRequirement {
        requisite: vec![topology("n2")],
        preferred: vec![topology("n2")],
    });
    let v = svc
        .create_volume(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .volume
        .unwrap();
    assert_eq!(v.accessible_topology, [topology("n2")]);

    // Replicated: unrestricted topology.
    let v = svc
        .create_volume(Request::new(create(
            "pvc-rep",
            &[("tier", "fast"), ("replicas", "2")],
            GIB,
        )))
        .await
        .unwrap()
        .into_inner()
        .volume
        .unwrap();
    assert_eq!(v.accessible_topology, []);

    // Error codes.
    let err = svc
        .create_volume(Request::new(create(
            "pvc-huge",
            &[("tier", "fast")],
            10_000 * GIB,
        )))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted, "{err}");
    let err = svc
        .create_volume(Request::new(create("pvc-x", &[], GIB)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    let err = svc
        .create_volume(Request::new(create("pvc-pinned", &params, 2 * GIB)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    let mut req = create("pvc-multi", &[("tier", "fast")], GIB);
    req.volume_capabilities[0].access_mode = Some(AccessMode {
        mode: Mode::MultiNodeMultiWriter.into(),
    });
    let err = svc.create_volume(Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // Publish: device path from the agent; no replica on the node is refused.
    let publish = |node: &str| ControllerPublishVolumeRequest {
        volume_id: "pvc-pinned".into(),
        node_id: node.into(),
        volume_capability: Some(mount()),
        ..ControllerPublishVolumeRequest::default()
    };
    let ctx = svc
        .controller_publish_volume(Request::new(publish("n2")))
        .await
        .unwrap()
        .into_inner()
        .publish_context;
    assert_eq!(
        ctx.get("devicePath").map(String::as_str),
        Some("/dev/vg/pvc-pinned")
    );
    assert_eq!(
        get_volume(&client, "pvc-pinned")
            .await
            .unwrap()
            .spec
            .attachments,
        ["n2"]
    );
    let err = svc
        .controller_publish_volume(Request::new(publish("n1")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Expand: waits for the agent, mount needs a node expansion, block not.
    let expand = |size: u64, block: bool| ControllerExpandVolumeRequest {
        volume_id: "pvc-pinned".into(),
        capacity_range: Some(CapacityRange {
            required_bytes: i64::try_from(size).unwrap(),
            limit_bytes: 0,
        }),
        volume_capability: Some(VolumeCapability {
            access_mode: mount().access_mode,
            access_type: Some(if block {
                AccessType::Block(BlockVolume {})
            } else {
                AccessType::Mount(MountVolume::default())
            }),
        }),
        ..ControllerExpandVolumeRequest::default()
    };
    let r = svc
        .controller_expand_volume(Request::new(expand(2 * GIB, false)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.capacity_bytes, i64::try_from(2 * GIB).unwrap());
    assert!(r.node_expansion_required);
    let r = svc
        .controller_expand_volume(Request::new(expand(2 * GIB, true)))
        .await
        .unwrap()
        .into_inner();
    assert!(!r.node_expansion_required);

    // Snapshot through the fake agent's rounds.
    let s = svc
        .create_snapshot(Request::new(CreateSnapshotRequest {
            source_volume_id: "pvc-pinned".into(),
            name: "snapshot-1".into(),
            ..CreateSnapshotRequest::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .snapshot
        .unwrap();
    assert!(s.ready_to_use);
    assert_eq!(s.size_bytes, i64::try_from(2 * GIB).unwrap());
    assert!(s.creation_time.is_some());
    let err = svc
        .create_snapshot(Request::new(CreateSnapshotRequest {
            source_volume_id: "pvc-rep".into(),
            name: "snapshot-1".into(),
            ..CreateSnapshotRequest::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
}

fn restore(
    name: &str,
    snapshot: &str,
    requisite: &[&str],
    preferred: &[&str],
) -> CreateVolumeRequest {
    let mut req = create(name, &[("tier", "fast")], GIB);
    req.volume_content_source = Some(VolumeContentSource {
        r#type: Some(SourceType::Snapshot(SnapshotSource {
            snapshot_id: snapshot.into(),
        })),
    });
    req.accessibility_requirements = Some(TopologyRequirement {
        requisite: requisite.iter().map(|n| topology(n)).collect(),
        preferred: preferred.iter().map(|n| topology(n)).collect(),
    });
    req
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn wffc_reschedule_and_restore_topology() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    for n in ["n1", "n2"] {
        node(&client, n, true).await;
    }
    // n1's pool reports no capacity at all.
    pool(&client, "full", "n1", "fast", 0).await;
    pool(&client, "p2", "n2", "fast", 100 * GIB).await;
    let cfg = support::config(&env, "ctl");
    let _ctl = support::spawn(env.clients().await, cfg.clone());
    let _agents = [
        FakeAgent::new(&client, "n1").spawn_auto(),
        FakeAgent::new(&client, "n2").spawn_auto(),
    ];
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, cfg);

    // The scheduler picked the full node: RESOURCE_EXHAUSTED and no Volume
    // left behind, since csi-provisioner then assumes none exists.
    let wffc = |preferred: &str| {
        let mut req = create("pvc-wffc", &[("tier", "fast")], GIB);
        req.accessibility_requirements = Some(TopologyRequirement {
            requisite: vec![topology("n1"), topology("n2")],
            preferred: vec![topology(preferred)],
        });
        req
    };
    let err = svc
        .create_volume(Request::new(wffc("n1")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted, "{err}");
    assert!(
        get_volume(&client, "pvc-wffc").await.is_none(),
        "orphaned Volume"
    );
    // Rescheduled onto n2.
    let v = svc
        .create_volume(Request::new(wffc("n2")))
        .await
        .unwrap()
        .into_inner()
        .volume
        .unwrap();
    assert_eq!(v.accessible_topology, [topology("n2")]);

    // Same name, another tier.
    let err = svc
        .create_volume(Request::new(create("pvc-wffc", &[("tier", "slow")], GIB)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);

    let snap = svc
        .create_snapshot(Request::new(CreateSnapshotRequest {
            source_volume_id: "pvc-wffc".into(),
            name: "snapshot-w".into(),
            ..CreateSnapshotRequest::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .snapshot
        .unwrap();
    assert!(snap.ready_to_use);

    // The snapshot lives on n2; a requisite without n2 cannot be served.
    let err = svc
        .create_volume(Request::new(restore(
            "pvc-r1",
            "snapshot-w",
            &["n1"],
            &["n1"],
        )))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted, "{err}");
    assert!(get_volume(&client, "pvc-r1").await.is_none());
    // Otherwise the holder wins over the preferred node.
    let v = svc
        .create_volume(Request::new(restore(
            "pvc-r2",
            "snapshot-w",
            &["n1", "n2"],
            &["n1"],
        )))
        .await
        .unwrap()
        .into_inner()
        .volume
        .unwrap();
    assert_eq!(v.accessible_topology, [topology("n2")]);
    assert_eq!(
        get_volume(&client, "pvc-r2")
            .await
            .unwrap()
            .spec
            .node
            .as_deref(),
        Some("n2")
    );

    // The snapshot does not fit the caller's limit.
    let mut req = restore("pvc-r3", "snapshot-w", &[], &[]);
    req.capacity_range = Some(CapacityRange {
        required_bytes: 1,
        limit_bytes: 1,
    });
    let err = svc.create_volume(Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), Code::OutOfRange);
    assert!(get_volume(&client, "pvc-r3").await.is_none());

    // A PVC pinned elsewhere conflicts with the snapshot's node.
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), "default");
    let pvc: PersistentVolumeClaim = serde_json::from_value(serde_json::json!({
        "metadata": { "name": "on-n1", "annotations": { "r-drbd.io/node": "n1" } },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": "1Gi" } },
        },
    }))
    .unwrap();
    pvcs.create(&PostParams::default(), &pvc).await.unwrap();
    let mut req = restore("pvc-r4", "snapshot-w", &[], &[]);
    req.parameters.extend([
        ("csi.storage.k8s.io/pvc/name".to_owned(), "on-n1".to_owned()),
        (
            "csi.storage.k8s.io/pvc/namespace".to_owned(),
            "default".to_owned(),
        ),
    ]);
    let err = svc.create_volume(Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("RestoreNodeConflict"), "{err}");
}

fn block(mode: Mode) -> VolumeCapability {
    VolumeCapability {
        access_mode: Some(AccessMode { mode: mode.into() }),
        access_type: Some(AccessType::Block(BlockVolume {})),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn read_write_many() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    for n in ["n1", "n2", "n3", "n4"] {
        node(&client, n, true).await;
    }
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    pool(&client, "p2", "n2", "fast", 100 * GIB).await;
    let cfg = support::config(&env, "ctl");
    let _ctl = support::spawn(env.clients().await, cfg.clone());
    let _agents: Vec<_> = ["n1", "n2", "n3", "n4"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, cfg);
    let rep = [("tier", "fast"), ("replicas", "2")];
    let rwx = || block(Mode::MultiNodeMultiWriter);

    let mut req = create("pvc-rwx", &rep, GIB);
    req.volume_capabilities = vec![rwx()];
    svc.create_volume(Request::new(req.clone())).await.unwrap();
    let v = get_volume(&client, "pvc-rwx").await.unwrap();
    assert_eq!(v.spec.access_mode, model::AccessMode::ReadWriteMany);
    // Idempotent; the same name asked as ReadWriteOnce is another volume.
    svc.create_volume(Request::new(req)).await.unwrap();
    let mut req = create("pvc-rwx", &rep, GIB);
    req.volume_capabilities = vec![block(Mode::SingleNodeWriter)];
    let err = svc.create_volume(Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists, "{err}");

    // Mount and replicas 1 are refused before any Volume exists.
    let mut req = create("pvc-rwx-fs", &rep, GIB);
    req.volume_capabilities[0].access_mode = Some(AccessMode {
        mode: Mode::MultiNodeMultiWriter.into(),
    });
    let err = svc.create_volume(Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("volumeMode Block"), "{err}");
    let mut req = create("pvc-rwx-1", &[("tier", "fast")], GIB);
    req.volume_capabilities = vec![block(Mode::MultiNodeReaderOnly)];
    let err = svc.create_volume(Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("replicas >= 2"), "{err}");
    for name in ["pvc-rwx-fs", "pvc-rwx-1"] {
        assert!(get_volume(&client, name).await.is_none(), "{name}");
    }

    // Any number of nodes: a diskful one and a client with a diskless entry.
    let publish = |vol: &str, node: &str, cap: VolumeCapability| ControllerPublishVolumeRequest {
        volume_id: vol.into(),
        node_id: node.into(),
        volume_capability: Some(cap),
        ..ControllerPublishVolumeRequest::default()
    };
    for n in ["n1", "n4"] {
        svc.controller_publish_volume(Request::new(publish("pvc-rwx", n, rwx())))
            .await
            .unwrap();
    }
    let v = get_volume(&client, "pvc-rwx").await.unwrap();
    let mut att = v.spec.attachments.clone();
    att.sort();
    assert_eq!(att, ["n1", "n4"]);
    let a = v.spec.assignment.unwrap();
    assert!(
        a.replicas
            .iter()
            .any(|r| r.node == "n4" && r.role == model::Role::Diskless)
    );

    // ReadWriteOnce: the second node is refused, MULTI_NODE_* never fits.
    svc.create_volume(Request::new(create("pvc-rwo", &rep, GIB)))
        .await
        .unwrap();
    let rwo = get_volume(&client, "pvc-rwo").await.unwrap();
    assert_eq!(rwo.spec.access_mode, model::AccessMode::ReadWriteOnce);
    svc.controller_publish_volume(Request::new(publish(
        "pvc-rwo",
        "n1",
        block(Mode::SingleNodeWriter),
    )))
    .await
    .unwrap();
    let err = svc
        .controller_publish_volume(Request::new(publish(
            "pvc-rwo",
            "n2",
            block(Mode::SingleNodeWriter),
        )))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    let err = svc
        .controller_publish_volume(Request::new(publish("pvc-rwo", "n1", rwx())))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    // ValidateVolumeCapabilities follows the volume's mode.
    let validate = |vol: &str| ValidateVolumeCapabilitiesRequest {
        volume_id: vol.into(),
        volume_capabilities: vec![rwx()],
        ..ValidateVolumeCapabilitiesRequest::default()
    };
    let r = svc
        .validate_volume_capabilities(Request::new(validate("pvc-rwx")))
        .await
        .unwrap()
        .into_inner();
    assert!(r.confirmed.is_some(), "{}", r.message);
    let r = svc
        .validate_volume_capabilities(Request::new(validate("pvc-rwo")))
        .await
        .unwrap()
        .into_inner();
    assert!(r.confirmed.is_none());
    assert!(r.message.contains("ReadWriteOnce"), "{}", r.message);
}

/// Publishes on n1 after its agent reported the LV lost; no agent acts.
async fn publish_lost(
    assets: &std::path::Path,
    replicas: u32,
    ready_timeout: Duration,
) -> (csi::tonic::Status, Duration) {
    support::init_tracing();
    let env = Env::start(assets).await;
    let client = env.client().await;
    for n in ["n1", "n2"] {
        node(&client, n, true).await;
    }
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    pool(&client, "p2", "n2", "fast", 100 * GIB).await;
    let mut cfg = support::config(&env, "ctl");
    cfg.ready_timeout = ready_timeout;
    let _ctl = support::spawn(env.clients().await, cfg.clone());
    let mut spec = support::volume_spec(GIB, replicas, "fast");
    if replicas == 1 {
        spec.node = Some("n1".into());
    }
    support::create_volume(&client, "pvc-lost", spec).await;
    let v = support::volume_where(&client, "pvc-lost", |v| {
        v.spec
            .assignment
            .as_ref()
            .is_some_and(|a| a.replicas.iter().any(|r| r.node == "n1"))
    })
    .await;
    FakeAgent::new(&client, "n1")
        .report(
            "pvc-lost",
            model::ReplicaStatus {
                node: "n1".into(),
                present: false,
                observed_generation: v.metadata.generation,
                last_error: Some(model::keys::LV_LOST.into()),
                ..model::ReplicaStatus::default()
            },
        )
        .await;
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, cfg);
    let start = Instant::now();
    let err = svc
        .controller_publish_volume(Request::new(ControllerPublishVolumeRequest {
            volume_id: "pvc-lost".into(),
            node_id: "n1".into(),
            volume_capability: Some(block(Mode::SingleNodeWriter)),
            ..ControllerPublishVolumeRequest::default()
        }))
        .await
        .unwrap_err();
    (err, start.elapsed())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_local_replica_fails_publish_fast() {
    let assets = require_envtest!();
    let timeout = Duration::from_secs(6);
    let (err, elapsed) = publish_lost(&assets, 1, timeout).await;
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("not recreated"), "{err}");
    assert!(elapsed < timeout / 2, "{elapsed:?}");
}

/// DRBD can serve a Primary diskless while the LV is rebuilt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_replica_of_a_replicated_volume_still_waits() {
    let assets = require_envtest!();
    let (err, _) = publish_lost(&assets, 2, Duration::from_secs(2)).await;
    assert_eq!(err.code(), Code::DeadlineExceeded, "{err}");
    assert!(err.message().contains("not recreated"), "{err}");
}

/// An attached node that gets no diskless replica (here the last free DRBD
/// node id went to another node) fails its own publish at once, never
/// another node's, and the failure clears once the node detaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn attachment_failure_is_per_node() {
    use csi::v1::ControllerUnpublishVolumeRequest;
    use model::condition::{self, types};

    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    for n in ["na", "nb"] {
        node(&client, n, true).await;
    }
    let cfg = support::config(&env, "ctl");
    let _ctl = support::spawn(env.clients().await, cfg.clone());
    let _agent = FakeAgent::new(&client, "nb").spawn_auto();
    let mut spec = support::volume_spec(GIB, 2, "fast");
    spec.access_mode = model::AccessMode::ReadWriteMany;
    // Node ids 0 to 30 held: one left for a diskless entry.
    spec.assignment = Some(model::Assignment {
        replicas: (0..model::keys::NODE_ID_MAX)
            .map(|id| model::AssignedReplica {
                node: format!("m{id}"),
                pool: Some("p".into()),
                node_id: id,
                port: Some(*model::keys::PORT_RANGE.start()),
                role: model::Role::Diskful,
            })
            .collect(),
        minor: Some(*model::keys::MINOR_RANGE.start()),
        secret: None,
        initial_up_to_date_node: None,
    });
    support::create_volume(&client, "pvc-crowded", spec).await;
    support::volume_where(&client, "pvc-crowded", |v| v.status.is_some()).await;
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, cfg.clone());
    let publish = |node: &str| ControllerPublishVolumeRequest {
        volume_id: "pvc-crowded".into(),
        node_id: node.into(),
        volume_capability: Some(block(Mode::MultiNodeMultiWriter)),
        ..ControllerPublishVolumeRequest::default()
    };
    let device = |r: csi::v1::ControllerPublishVolumeResponse| {
        r.publish_context
            .values()
            .next()
            .cloned()
            .unwrap_or_default()
    };

    let b = svc
        .controller_publish_volume(Request::new(publish("nb")))
        .await
        .expect("nb takes the last node id");
    let b = device(b.into_inner());
    assert!(b.starts_with("/dev/drbd"), "{b}");

    let start = Instant::now();
    let err = svc
        .controller_publish_volume(Request::new(publish("na")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted, "{err}");
    assert!(err.message().contains("on na: more than 32"), "{err}");
    assert!(
        start.elapsed() < cfg.ready_timeout / 4,
        "{:?}",
        start.elapsed()
    );

    // na's failure is not nb's: a repeat publish on nb still answers.
    let again = svc
        .controller_publish_volume(Request::new(publish("nb")))
        .await
        .expect("nb publish beside na's failure");
    assert_eq!(device(again.into_inner()), b);

    let v = get_volume(&client, "pvc-crowded").await.unwrap();
    let st = v.status.unwrap();
    let nodes: Vec<&str> = st
        .attachment_errors
        .iter()
        .map(|e| e.node.as_str())
        .collect();
    assert_eq!(nodes, ["na"]);
    assert_eq!(
        st.attachment_errors[0].reason,
        model::AttachmentErrorReason::TooManyPeers
    );
    let failed = condition::get(&st.conditions, types::ATTACHMENT_FAILED).unwrap();
    assert_eq!(failed.status, "True");
    assert!(failed.message.contains("na"), "{}", failed.message);
    let ready = condition::get(&st.conditions, types::READY).unwrap();
    assert_ne!(ready.reason, "AttachmentFailed", "Ready keeps its meaning");

    svc.controller_unpublish_volume(Request::new(ControllerUnpublishVolumeRequest {
        volume_id: "pvc-crowded".into(),
        node_id: "na".into(),
        ..ControllerUnpublishVolumeRequest::default()
    }))
    .await
    .expect("unpublish na");
    let v = support::volume_where(&client, "pvc-crowded", |v| {
        v.status.as_ref().is_some_and(|s| {
            s.attachment_errors.is_empty()
                && condition::get(&s.conditions, types::ATTACHMENT_FAILED)
                    .is_some_and(|c| c.status == "False")
        })
    })
    .await;
    assert!(
        v.spec
            .assignment
            .unwrap()
            .replicas
            .iter()
            .any(|r| r.node == "nb"),
        "nb keeps its replica"
    );

    // Several passes ran with na failing; one Event says so.
    let events = Api::<k8s_openapi::api::core::v1::Event>::namespaced(client.clone(), "default")
        .list(&kube::api::ListParams::default())
        .await
        .unwrap();
    let failures = events
        .items
        .iter()
        .filter(|e| {
            e.involved_object.name.as_deref() == Some("pvc-crowded")
                && e.reason.as_deref() == Some("AttachmentFailed")
        })
        .count();
    assert_eq!(failures, 1);
}

/// A grow moves DRBD metadata: never while a snapshot round takes copies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expand_refused_mid_round() {
    use model::SnapshotPhase;
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let cfg = support::config(&env, "ctl");
    let _ctl = support::spawn(env.clients().await, cfg.clone());
    let mut spec = support::volume_spec(GIB, 1, "fast");
    spec.node = Some("n1".into());
    support::create_volume(&client, "pvc-x", spec).await;
    support::volume_where(&client, "pvc-x", |v| v.spec.assignment.is_some()).await;
    let a1 = FakeAgent::new(&client, "n1");
    a1.ready("pvc-x").await;
    support::volume_where(&client, "pvc-x", |v| {
        support::phase_of(v) == model::VolumePhase::Ready
    })
    .await;
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores.clone(), cfg);
    let expand = || ControllerExpandVolumeRequest {
        volume_id: "pvc-x".into(),
        capacity_range: Some(CapacityRange {
            required_bytes: i64::try_from(2 * GIB).unwrap(),
            limit_bytes: 0,
        }),
        volume_capability: Some(block(Mode::SingleNodeWriter)),
        ..ControllerExpandVolumeRequest::default()
    };

    support::create_snapshot(&client, "s1", "pvc-x").await;
    support::snapshot_where(&client, "s1", |s| {
        support::snap_phase(s) == SnapshotPhase::Taking
    })
    .await;
    support::eventually(Duration::from_secs(10), || async {
        stores
            .snapshot("s1")
            .filter(|s| controller::volume::round_running(s))
    })
    .await;
    let err = svc
        .controller_expand_volume(Request::new(expand()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err}");
    assert!(err.message().contains("snapshot s1"), "{err}");
    assert_eq!(
        get_volume(&client, "pvc-x").await.unwrap().spec.size_bytes,
        GIB
    );

    a1.ack("s1", SnapshotPhase::Taking, true, Some("p1")).await;
    support::snapshot_where(&client, "s1", |s| {
        support::snap_phase(s) == SnapshotPhase::Ready
    })
    .await;
    let _auto = a1.spawn_auto();
    support::eventually(Duration::from_secs(20), || async {
        svc.controller_expand_volume(Request::new(expand()))
            .await
            .ok()
    })
    .await;
    assert_eq!(
        get_volume(&client, "pvc-x").await.unwrap().spec.size_bytes,
        2 * GIB
    );
}
