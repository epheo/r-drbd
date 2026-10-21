//! Replicated volumes against envtest: attachments, DRBD conditions,
//! split-brain resolve, stale agents, replicated restore.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use ::csi::v1::controller_server::Controller;
use ::csi::v1::volume_capability::access_mode::Mode;
use ::csi::v1::volume_capability::{AccessMode, AccessType, MountVolume};
use ::csi::v1::{
    ControllerPublishVolumeRequest, ControllerUnpublishVolumeRequest, VolumeCapability,
};
use csi::tonic::{self, Code, Request};
use k8s_openapi::api::core::v1::Event;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams};
use kube::{Api, Client};
use model::condition::{self, types};
use model::{ReplicaStatus, Role, SnapshotPhase, Volume, VolumePhase};
use serde_json::json;

use support::{
    Env, FakeAgent, GIB, Running, create_snapshot, create_volume, get_volume, node, phase_of, pool,
    ready_reason, snapshot_where, volume_spec, volume_where,
};

async fn setup(assets: &std::path::Path) -> (Env, Client, controller::Config, Running) {
    setup_with(assets, |_| {}).await
}

/// Agent Leases stale after this in the Lease tests; the heartbeat renews
/// every second.
const SHORT_STALE: Duration = Duration::from_secs(5);

async fn setup_with(
    assets: &std::path::Path,
    tweak: impl FnOnce(&mut controller::Config),
) -> (Env, Client, controller::Config, Running) {
    support::init_tracing();
    let env = Env::start(assets).await;
    let client = env.client().await;
    let mut cfg = support::config(&env, "ctl");
    tweak(&mut cfg);
    let running = support::spawn(env.clients().await, cfg.clone());
    (env, client, cfg, running)
}

/// n1 and n2 hold pools, n3 has none (the tiebreaker), n4 is a client.
async fn cluster(client: &Client) {
    for n in ["n1", "n2", "n3", "n4"] {
        node(client, n, true).await;
    }
    pool(client, "p1", "n1", "fast", 100 * GIB).await;
    pool(client, "p2", "n2", "fast", 100 * GIB).await;
}

fn cond<'a>(v: &'a Volume, t: &str) -> Option<&'a condition::Condition> {
    condition::get(&v.status.as_ref()?.conditions, t)
}

fn cond_is(v: &Volume, t: &str, status: &str) -> bool {
    cond(v, t).is_some_and(|c| c.status == status)
}

fn role_of(v: &Volume, node: &str) -> Option<Role> {
    v.spec
        .assignment
        .as_ref()?
        .replicas
        .iter()
        .find(|r| r.node == node)
        .map(|r| r.role)
}

fn entry_of<'a>(v: &'a Volume, node: &str) -> Option<&'a ReplicaStatus> {
    v.status.as_ref()?.replicas.iter().find(|e| e.node == node)
}

async fn patch_spec(client: &Client, name: &str, spec: serde_json::Value) {
    Api::<Volume>::all(client.clone())
        .patch(
            name,
            &PatchParams::default(),
            &Patch::Merge(&json!({ "spec": spec })),
        )
        .await
        .expect("patch volume spec");
}

fn mount() -> VolumeCapability {
    VolumeCapability {
        access_mode: Some(AccessMode {
            mode: Mode::SingleNodeWriter.into(),
        }),
        access_type: Some(AccessType::Mount(MountVolume::default())),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn publish_appends_diskless_and_unpublish_removes() {
    let assets = require_envtest!();
    let (_env, client, cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    let _agents: Vec<_> = ["n1", "n2", "n3", "n4"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    assert_eq!(role_of(&v, "n3"), Some(Role::Tiebreaker));
    let minor = v.spec.assignment.as_ref().unwrap().minor.unwrap();
    let diskful_before: Vec<_> = v.spec.assignment.as_ref().unwrap().replicas.clone();

    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, cfg);
    let publish = |node: &str| ControllerPublishVolumeRequest {
        volume_id: "rep".into(),
        node_id: node.into(),
        volume_capability: Some(mount()),
        ..ControllerPublishVolumeRequest::default()
    };
    let unpublish = |node: &str| ControllerUnpublishVolumeRequest {
        volume_id: "rep".into(),
        node_id: node.into(),
        ..ControllerUnpublishVolumeRequest::default()
    };

    // A node without a replica gets a diskless one.
    let ctx = svc
        .controller_publish_volume(Request::new(publish("n4")))
        .await
        .unwrap()
        .into_inner()
        .publish_context;
    assert_eq!(
        ctx.get("devicePath").cloned(),
        Some(format!("/dev/drbd{minor}"))
    );
    let v = get_volume(&client, "rep").await.unwrap();
    assert_eq!(v.spec.attachments, ["n4"]);
    let a = v.spec.assignment.as_ref().unwrap();
    let added = a.replicas.iter().find(|r| r.node == "n4").unwrap();
    assert_eq!(
        (added.role, added.node_id, added.port, added.pool.as_deref()),
        (Role::Diskless, 3, Some(11000), None)
    );
    assert_eq!(a.replicas[..3], diskful_before[..]);
    assert_eq!(phase_of(&v), VolumePhase::Ready);
    support::assert_controller_status_fields(&v.metadata);

    // SINGLE_NODE_*: a second node is refused while published.
    let err = svc
        .controller_publish_volume(Request::new(publish("n1")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");

    // Unpublish removes the diskless entry; the agent then drops its status.
    svc.controller_unpublish_volume(Request::new(unpublish("n4")))
        .await
        .unwrap();
    let v = get_volume(&client, "rep").await.unwrap();
    assert_eq!(v.spec.attachments, Vec::<String>::new());
    assert_eq!(role_of(&v, "n4"), None);
    assert_eq!(v.spec.assignment.as_ref().unwrap().replicas, diskful_before);
    volume_where(&client, "rep", |v| entry_of(v, "n4").is_none()).await;

    // The tiebreaker node needs no new entry and keeps its own on unpublish.
    let ctx = svc
        .controller_publish_volume(Request::new(publish("n3")))
        .await
        .unwrap()
        .into_inner()
        .publish_context;
    assert_eq!(
        ctx.get("devicePath").cloned(),
        Some(format!("/dev/drbd{minor}"))
    );
    assert_eq!(
        get_volume(&client, "rep")
            .await
            .unwrap()
            .spec
            .assignment
            .unwrap()
            .replicas,
        diskful_before
    );
    svc.controller_unpublish_volume(Request::new(unpublish("n3")))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let v = get_volume(&client, "rep").await.unwrap();
    assert_eq!(role_of(&v, "n3"), Some(Role::Tiebreaker));
    assert!(entry_of(&v, "n3").is_some_and(|e| e.present));

    // Moving to a diskful node after the unpublish.
    svc.controller_publish_volume(Request::new(publish("n1")))
        .await
        .unwrap();
    assert_eq!(
        get_volume(&client, "rep").await.unwrap().spec.attachments,
        ["n1"]
    );
    // Unpublishing a node that is not attached is a no-op.
    svc.controller_unpublish_volume(Request::new(unpublish("n4")))
        .await
        .unwrap();

    // Two publishes racing for different nodes: exactly one wins.
    svc.controller_unpublish_volume(Request::new(unpublish("n1")))
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        svc.controller_publish_volume(Request::new(publish("n2"))),
        svc.controller_publish_volume(Request::new(publish("n4"))),
    );
    let codes = [&a, &b].map(|r| r.as_ref().map(|_| ()).map_err(tonic::Status::code));
    assert_eq!(codes.iter().filter(|c| c.is_ok()).count(), 1, "{a:?} {b:?}");
    assert!(codes.contains(&Err(Code::FailedPrecondition)), "{codes:?}");
    assert_eq!(
        get_volume(&client, "rep")
            .await
            .unwrap()
            .spec
            .attachments
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_pin_places_one_of_the_replicas() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    // Equal pools: without the pin n1 would come first.
    let mut spec = volume_spec(GIB, 2, "fast");
    spec.node = Some("n2".into());
    create_volume(&client, "pinned", spec).await;
    let v = volume_where(&client, "pinned", |v| v.spec.assignment.is_some()).await;
    let a = v.spec.assignment.as_ref().unwrap();
    assert_eq!(a.initial_up_to_date_node.as_deref(), Some("n2"));
    assert_eq!(role_of(&v, "n1"), Some(Role::Diskful));
    // A pinned node without a pool: Pending, naming the pin.
    let mut spec = volume_spec(GIB, 2, "fast");
    spec.node = Some("n3".into());
    create_volume(&client, "nowhere", spec).await;
    let v = volume_where(&client, "nowhere", |v| {
        ready_reason(v).as_deref() == Some("Insufficient")
    })
    .await;
    let message = &cond(&v, types::READY).unwrap().message;
    assert!(message.contains("one must be on n3"), "{message}");
    assert!(v.spec.assignment.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_volumes_attach_to_one_node_in_one_pass() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    let _agents: Vec<_> = ["n1", "n2", "n3", "n4"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    // Attached from the start: both get placed, then both get their
    // diskless n4 entry, typically in the same pass. A pool of the tier
    // too small for them keeps the spreading tiebreakers off n4.
    pool(&client, "p4", "n4", "fast", GIB / 2).await;
    for name in ["va", "vb"] {
        let mut spec = volume_spec(GIB, 2, "fast");
        spec.attachments = vec!["n4".into()];
        create_volume(&client, name, spec).await;
    }
    let mut ports = Vec::new();
    for name in ["va", "vb"] {
        let v = volume_where(&client, name, |v| {
            role_of(v, "n4") == Some(Role::Diskless) && phase_of(v) == VolumePhase::Ready
        })
        .await;
        let a = v.spec.assignment.unwrap();
        ports.push(a.replicas.iter().find(|r| r.node == "n4").unwrap().port);
    }
    assert_ne!(ports[0], ports[1], "{ports:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_replicated_with_tiebreaker_node_gone() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    let _agents: Vec<_> = ["n1", "n2"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    let tb = FakeAgent::new(&client, "n3");
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| v.spec.assignment.is_some()).await;
    assert_eq!(role_of(&v, "n3"), Some(Role::Tiebreaker));
    tb.ready("rep").await;
    volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    support::delete_node(&client, "n3").await;
    Api::<Volume>::all(client.clone())
        .delete("rep", &DeleteParams::default())
        .await
        .unwrap();
    support::eventually(Duration::from_secs(20), || async {
        get_volume(&client, "rep").await.is_none().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_converges_to_attachments() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    let _agents: Vec<_> = ["n1", "n2", "n3", "n4"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    let before = v.spec.assignment.unwrap().replicas;

    // Attachments written without the CSI call, then a missed unpublish:
    // the reconciler repairs the assignment from the list either way.
    patch_spec(&client, "rep", json!({ "attachments": ["n4"] })).await;
    let v = volume_where(&client, "rep", |v| {
        role_of(v, "n4") == Some(Role::Diskless) && entry_of(v, "n4").is_some_and(|e| e.present)
    })
    .await;
    assert_eq!(phase_of(&v), VolumePhase::Ready);
    patch_spec(&client, "rep", json!({ "attachments": [] })).await;
    let v = volume_where(&client, "rep", |v| role_of(v, "n4").is_none()).await;
    assert_eq!(v.spec.assignment.unwrap().replicas, before);
    volume_where(&client, "rep", |v| entry_of(v, "n4").is_none()).await;
}

fn healthy(node: &str, peers: &[&str], generation: Option<i64>) -> ReplicaStatus {
    ReplicaStatus {
        node: node.into(),
        present: true,
        disk_state: Some("UpToDate".into()),
        device_path: Some("/dev/drbd1000".into()),
        size_bytes: Some(GIB),
        allocated_bytes: Some(GIB),
        quorum: Some(true),
        observed_generation: generation,
        peers: peers
            .iter()
            .map(|p| ((*p).to_owned(), "Connected".to_owned()))
            .collect::<BTreeMap<_, _>>(),
        ..ReplicaStatus::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn split_brain_quorum_and_resolve() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    let [a1, a2, a3] = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| v.spec.assignment.is_some()).await;
    let g = v.metadata.generation;
    a1.report("rep", healthy("n1", &["n2"], g)).await;
    a2.report("rep", healthy("n2", &["n1"], g)).await;
    let mut tb = healthy("n3", &["n1", "n2"], g);
    tb.disk_state = Some("Diskless".into());
    tb.allocated_bytes = None;
    a3.report("rep", tb).await;
    let v = volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    assert!(cond_is(&v, types::QUORUM_LOST, "False"));
    assert!(cond_is(&v, types::SPLIT_BRAIN, "False"));

    // Both sides StandAlone toward each other; n2 lost quorum.
    let mut s1 = healthy("n1", &[], g);
    s1.peers.insert("n2".into(), "StandAlone".into());
    let mut s2 = healthy("n2", &[], g);
    s2.peers.insert("n1".into(), "StandAlone".into());
    s2.quorum = Some(false);
    a1.report("rep", s1.clone()).await;
    a2.report("rep", s2).await;
    let v = volume_where(&client, "rep", |v| {
        cond_is(v, types::SPLIT_BRAIN, "True") && cond_is(v, types::QUORUM_LOST, "True")
    })
    .await;
    assert_eq!(phase_of(&v), VolumePhase::Degraded);
    assert!(
        cond(&v, types::SPLIT_BRAIN)
            .unwrap()
            .message
            .contains("n1 and n2")
    );
    assert_eq!(
        cond(&v, types::QUORUM_LOST).unwrap().message,
        "no quorum on n2"
    );
    support::assert_controller_status_fields(&v.metadata);

    // The agent re-adjusts StandAlone connections, so DRBD cycles through
    // Connecting; the split stays latched until both sides are Connected.
    for i in 0..6 {
        let (state1, state2) = if i % 2 == 0 {
            ("Connecting", "StandAlone")
        } else {
            ("StandAlone", "Connecting")
        };
        let mut s1 = healthy("n1", &[], g);
        s1.peers.insert("n2".into(), state1.into());
        let mut s2 = healthy("n2", &[], g);
        s2.peers.insert("n1".into(), state2.into());
        a1.report("rep", s1).await;
        a2.report("rep", s2).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let v = get_volume(&client, "rep").await.unwrap();
        assert!(cond_is(&v, types::SPLIT_BRAIN, "True"), "write {i}");
    }

    // A survivor outside the diskful replicas changes nothing but Ready.
    patch_spec(&client, "rep", json!({ "resolve": { "survivor": "n3" } })).await;
    let v = volume_where(&client, "rep", |v| {
        ready_reason(v).as_deref() == Some("InvalidSurvivor")
    })
    .await;
    assert!(v.spec.resolve.is_some());
    assert!(cond(&v, "Ready").unwrap().message.contains("n3"));

    // A valid survivor stays set while the split lasts.
    patch_spec(&client, "rep", json!({ "resolve": { "survivor": "n1" } })).await;
    let v = volume_where(&client, "rep", |v| {
        ready_reason(v).as_deref() != Some("InvalidSurvivor")
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        get_volume(&client, "rep")
            .await
            .unwrap()
            .spec
            .resolve
            .unwrap()
            .survivor,
        "n1"
    );

    // n2 discarded its data and resyncs from n1: the resolve is done.
    let g = v.metadata.generation;
    a1.report("rep", healthy("n1", &["n2"], g)).await;
    let mut s2 = healthy("n2", &["n1"], g);
    s2.disk_state = Some("Inconsistent".into());
    s2.sync_percent = Some(5);
    a2.report("rep", s2).await;
    let v = volume_where(&client, "rep", |v| v.spec.resolve.is_none()).await;
    assert!(cond_is(&v, types::SPLIT_BRAIN, "False"));
    let events: Api<Event> = Api::namespaced(client.clone(), "default");
    support::eventually(Duration::from_secs(10), || async {
        events
            .list(&ListParams::default())
            .await
            .ok()?
            .items
            .iter()
            .any(|e| {
                e.reason.as_deref() == Some("SplitBrainResolved")
                    && e.involved_object.name.as_deref() == Some("rep")
            })
            .then_some(())
    })
    .await;
    let v = volume_where(&client, "rep", |v| {
        cond_is(v, types::QUORUM_LOST, "False") && cond_is(v, types::SPLIT_BRAIN, "False")
    })
    .await;
    assert_eq!(phase_of(&v), VolumePhase::Degraded);

    // A resolve without a split brain is shown, not acted on or cleared.
    let g = v.metadata.generation;
    a2.report("rep", healthy("n2", &["n1"], g)).await;
    volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    patch_spec(&client, "rep", json!({ "resolve": { "survivor": "n2" } })).await;
    let v = volume_where(&client, "rep", |v| {
        ready_reason(v).as_deref() == Some("ResolveWithoutSplitBrain")
    })
    .await;
    assert!(
        cond(&v, "Ready")
            .unwrap()
            .message
            .contains("remove spec.resolve")
    );
    let g = v.metadata.generation;
    a1.report("rep", healthy("n1", &["n2"], g)).await;
    a2.report("rep", healthy("n2", &["n1"], g)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        get_volume(&client, "rep")
            .await
            .unwrap()
            .spec
            .resolve
            .is_some()
    );
    patch_spec(&client, "rep", json!({ "resolve": null })).await;
    volume_where(&client, "rep", |v| {
        ready_reason(v).as_deref() == Some("AllReplicasUpToDate")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_primary_entry_is_not_the_primary() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup_with(&assets, |c| c.lease_stale = SHORT_STALE).await;
    cluster(&client).await;
    let [a1, a2, a3] = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| v.spec.assignment.is_some()).await;
    let g = v.metadata.generation;
    a2.report("rep", healthy("n2", &["n1"], g)).await;
    let mut p1 = healthy("n1", &["n2"], g);
    p1.in_use = true;
    a1.report("rep", p1).await;
    let mut tb = healthy("n3", &["n1", "n2"], g);
    tb.disk_state = Some("Diskless".into());
    tb.allocated_bytes = None;
    a3.report("rep", tb).await;
    let primary = |v: &Volume| v.status.as_ref().and_then(|s| s.primary.clone());
    volume_where(&client, "rep", |v| primary(v).as_deref() == Some("n1")).await;

    // n1 halts: its entry stays UpToDate and in use; n2 promotes.
    support::agent_lease(&client, "n1", Duration::ZERO, true).await;
    let halted = tokio::time::Instant::now();
    let mut p2 = healthy("n2", &[], g);
    p2.peers.insert("n1".into(), "Connecting".into());
    p2.in_use = true;
    a2.report("rep", p2.clone()).await;
    // Both entries read in use; while n1 is reachable the smallest name wins.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let v = get_volume(&client, "rep").await.unwrap();
    assert!(entry_of(&v, "n2").is_some_and(|e| e.in_use));
    assert_eq!(primary(&v).as_deref(), Some("n1"));
    let v = support::eventually(SHORT_STALE + Duration::from_secs(5), || async {
        get_volume(&client, "rep")
            .await
            .filter(|v| primary(v).as_deref() == Some("n2"))
    })
    .await;
    assert!(halted.elapsed() < SHORT_STALE + Duration::from_secs(5));
    let d = cond(&v, types::DEGRADED).unwrap();
    assert_eq!(d.reason, "NodeUnreachable");
    assert!(
        d.message.contains("unknown (agent unreachable): n1"),
        "{}",
        d.message
    );

    // n1 returns as Secondary: n2 stays the primary.
    support::agent_lease(&client, "n1", Duration::ZERO, false).await;
    a1.report("rep", healthy("n1", &["n2"], g)).await;
    p2.peers.insert("n1".into(), "Connected".into());
    a2.report("rep", p2).await;
    let v = volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    assert_eq!(primary(&v).as_deref(), Some("n2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_lease_marks_node_unreachable() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup_with(&assets, |c| c.lease_stale = SHORT_STALE).await;
    cluster(&client).await;
    let _agents: Vec<_> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;

    let unreachable = |v: &Volume| {
        phase_of(v) == VolumePhase::Degraded
            && cond(v, types::DEGRADED).is_some_and(|c| {
                c.status == "True" && c.reason == "NodeUnreachable" && c.message.contains("n2")
            })
    };
    // A renewTime far in the past (clock skew) is a renewal as seen here;
    // only the lack of renewals that follows makes n2 stale, and the
    // pass timer notices it without any event, well under RESYNC.
    support::agent_lease(&client, "n2", Duration::from_secs(3600), true).await;
    let paused = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        phase_of(&get_volume(&client, "rep").await.unwrap()),
        VolumePhase::Ready
    );
    let v = support::eventually(Duration::from_secs(10), || async {
        get_volume(&client, "rep").await.filter(|v| unreachable(v))
    })
    .await;
    assert!(paused.elapsed() < controller::RESYNC / 2);
    assert_eq!(ready_reason(&v).as_deref(), Some("NodeUnreachable"));
    support::agent_lease(&client, "n2", Duration::ZERO, false).await;
    volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;

    // No Lease at all while holding a replica.
    support::delete_agent_lease(&client, "n1").await;
    volume_where(&client, "rep", |v| {
        cond(v, types::DEGRADED)
            .is_some_and(|c| c.reason == "NodeUnreachable" && c.message.contains("n1"))
    })
    .await;
    support::agent_lease(&client, "n1", Duration::ZERO, false).await;
    volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placement_skips_nodes_without_a_live_agent() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    // Ready Nodes whose agent never renewed: neither a pool nor the
    // tiebreaker may land there.
    support::delete_agent_lease(&client, "n2").await;
    support::delete_agent_lease(&client, "n3").await;
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| {
        ready_reason(v).as_deref() == Some("Insufficient")
    })
    .await;
    let message = &cond(&v, types::READY).unwrap().message;
    assert!(message.contains("n2/p2 NodeUnreachable"), "{message}");
    support::agent_lease(&client, "n2", Duration::ZERO, false).await;
    let v = volume_where(&client, "rep", |v| v.spec.assignment.is_some()).await;
    assert_eq!(role_of(&v, "n2"), Some(Role::Diskful));
    assert_eq!(role_of(&v, "n3"), None);
    assert_eq!(role_of(&v, "n4"), Some(Role::Tiebreaker));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deletion_with_gone_or_stale_nodes() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup_with(&assets, |c| c.lease_stale = SHORT_STALE).await;
    cluster(&client).await;
    let a1 = FakeAgent::new(&client, "n1");
    let a2 = FakeAgent::new(&client, "n2");
    for (name, n, agent) in [("on-n1", "n1", &a1), ("on-n2", "n2", &a2)] {
        let mut spec = volume_spec(GIB, 1, "fast");
        spec.node = Some(n.into());
        create_volume(&client, name, spec).await;
        volume_where(&client, name, |v| v.spec.assignment.is_some()).await;
        agent.ready(name).await;
        volume_where(&client, name, |v| phase_of(v) == VolumePhase::Ready).await;
    }
    let volumes: Api<Volume> = Api::all(client.clone());

    // n2 stops heartbeating but its Node stays: deletion waits.
    support::agent_lease(&client, "n2", Duration::from_secs(3600), true).await;
    volume_where(&client, "on-n2", |v| {
        ready_reason(v).as_deref() == Some("NodeUnreachable")
    })
    .await;
    volumes
        .delete("on-n2", &DeleteParams::default())
        .await
        .unwrap();
    volume_where(&client, "on-n2", |v| phase_of(v) == VolumePhase::Deleting).await;

    // n1's Node object is removed: its replica counts as absent.
    support::agent_lease(&client, "n1", Duration::from_secs(3600), true).await;
    support::delete_node(&client, "n1").await;
    volumes
        .delete("on-n1", &DeleteParams::default())
        .await
        .unwrap();
    support::eventually(Duration::from_secs(20), || async {
        get_volume(&client, "on-n1").await.is_none().then_some(())
    })
    .await;
    let v = get_volume(&client, "on-n2").await.expect("still blocked");
    assert!(v.metadata.deletion_timestamp.is_some());
    assert_eq!(phase_of(&v), VolumePhase::Deleting);
    assert!(cond(&v, "Ready").unwrap().message.contains("n2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicated_restore_from_acks() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    // No agent runs on n4: it is no tiebreaker candidate.
    support::delete_agent_lease(&client, "n4").await;
    let _agents: Vec<_> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    create_volume(&client, "src", volume_spec(GIB, 2, "fast")).await;
    let src = volume_where(&client, "src", |v| phase_of(v) == VolumePhase::Ready).await;
    support::set_file_system(&client, "src", "xfs").await;
    create_snapshot(&client, "snap", "src").await;
    snapshot_where(&client, "snap", |s| {
        support::snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await;

    // One holder too full for a new volume: every replica is charged at
    // full size, so the restore waits.
    support::pool_capacity(&client, "p2", "n2", 100 * GIB, 0).await;
    let mut spec = volume_spec(GIB, 2, "fast");
    spec.source = Some(support::snapshot_source(&client, "snap").await);
    create_volume(&client, "restored", spec.clone()).await;
    let r = volume_where(&client, "restored", |v| {
        cond(v, "Ready").is_some_and(|c| c.reason == "Insufficient")
    })
    .await;
    assert!(r.spec.assignment.is_none());
    assert!(cond(&r, "Ready").unwrap().message.contains("pool n2/p2"));
    support::pool_capacity(&client, "p2", "n2", 100 * GIB, 100 * GIB).await;
    let r = volume_where(&client, "restored", |v| phase_of(v) == VolumePhase::Ready).await;
    let a = r.spec.assignment.as_ref().unwrap();
    let got: Vec<_> = a
        .replicas
        .iter()
        .map(|r| (r.node.as_str(), r.pool.as_deref(), r.role, r.node_id))
        .collect();
    assert_eq!(
        got,
        [
            ("n1", Some("p1"), Role::Diskful, 0),
            ("n2", Some("p2"), Role::Diskful, 1),
            ("n3", None, Role::Tiebreaker, 2),
        ]
    );
    assert_eq!(a.initial_up_to_date_node.as_deref(), Some("n1"));
    let restored: BTreeMap<_, _> = r
        .status
        .iter()
        .flat_map(|s| &s.replicas)
        .map(|e| (e.node.as_str(), e.restored_from.as_deref()))
        .collect();
    assert_eq!(
        restored,
        BTreeMap::from([("n1", Some("snap")), ("n2", Some("snap")), ("n3", None)])
    );
    assert_ne!(a.minor, src.spec.assignment.as_ref().unwrap().minor);
    assert!(a.secret.is_some());
    assert!(a.replicas.iter().all(|r| r.port == Some(11001)));
    // The snapshot records the marker; the restore waits for its agents.
    let snap = support::get_snapshot(&client, "snap").await.unwrap();
    assert_eq!(snap.status.unwrap().file_system.as_deref(), Some("xfs"));
    assert_eq!(r.status.and_then(|s| s.file_system), None);

    // Holders and replicas must match.
    spec.replicas = 3;
    create_volume(&client, "restored-3", spec).await;
    let r = volume_where(&client, "restored-3", |v| {
        phase_of(v) == VolumePhase::Failed
    })
    .await;
    let c = cond(&r, "Ready").unwrap();
    assert_eq!(c.reason, "UnsupportedRestore");
    assert_eq!(c.message, "snapshot holders 2, replicas 3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_outlives_an_uninitialized_restore() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    // No agent runs on n4: it is no tiebreaker candidate.
    support::delete_agent_lease(&client, "n4").await;
    let auto: Vec<_> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    create_volume(&client, "src", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "src", |v| phase_of(v) == VolumePhase::Ready).await;
    create_snapshot(&client, "snap", "src").await;
    snapshot_where(&client, "snap", |s| {
        support::snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await;
    for a in auto {
        a.abort();
    }

    let mut spec = volume_spec(GIB, 2, "fast");
    spec.source = Some(support::snapshot_source(&client, "snap").await);
    create_volume(&client, "restored", spec).await;
    volume_where(&client, "restored", |v| v.spec.assignment.is_some()).await;
    Api::<model::Snapshot>::all(client.clone())
        .delete("snap", &DeleteParams::default())
        .await
        .expect("delete snapshot");
    snapshot_where(&client, "snap", |s| {
        support::snap_phase(s) == Some(SnapshotPhase::Deleting)
    })
    .await;
    let agents = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    // Each holder copies its takenPool forward, like the agent.
    for (a, p) in agents[..2].iter().zip(["p1", "p2"]) {
        a.ack("snap", SnapshotPhase::Deleting, true, Some(p)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        support::get_snapshot(&client, "snap").await.is_some(),
        "released under an uninitialized restore"
    );

    for a in &agents {
        a.ready("restored").await;
    }
    volume_where(&client, "restored", |v| {
        v.status
            .as_ref()
            .is_some_and(|s| s.initialized_at.is_some())
    })
    .await;
    support::eventually(Duration::from_secs(20), || async {
        support::get_snapshot(&client, "snap")
            .await
            .is_none()
            .then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn larger_restore_is_ready_only_at_the_full_size() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    // No agent runs on n4: it is no tiebreaker candidate.
    support::delete_agent_lease(&client, "n4").await;
    let auto: Vec<_> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    create_volume(&client, "src", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "src", |v| phase_of(v) == VolumePhase::Ready).await;
    create_snapshot(&client, "snap", "src").await;
    snapshot_where(&client, "snap", |s| {
        support::snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await;
    for a in auto {
        a.abort();
    }

    let mut spec = volume_spec(2 * GIB, 2, "fast");
    spec.source = Some(support::snapshot_source(&client, "snap").await);
    create_volume(&client, "restored", spec).await;
    volume_where(&client, "restored", |v| v.spec.assignment.is_some()).await;
    let agents = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    // Every replica up at the snapshot's size: not Ready yet.
    for a in &agents {
        a.ready_at("restored", Some(GIB)).await;
    }
    let v = volume_where(&client, "restored", |v| {
        v.status
            .as_ref()
            .is_some_and(|s| s.replicas.len() == 3 && s.usable_bytes == Some(GIB))
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let v = get_volume(&client, "restored").await.unwrap_or(v);
    let st = v.status.as_ref().unwrap();
    assert_eq!(phase_of(&v), VolumePhase::Provisioning);
    assert_eq!(ready_reason(&v).as_deref(), Some("Resizing"));
    assert_eq!(st.initialized_at, None);
    Api::<model::Snapshot>::all(client.clone())
        .delete("snap", &DeleteParams::default())
        .await
        .expect("delete snapshot");
    snapshot_where(&client, "snap", |s| {
        support::snap_phase(s) == Some(SnapshotPhase::Deleting)
    })
    .await;
    // Each holder copies its takenPool forward, like the agent.
    for (a, p) in agents[..2].iter().zip(["p1", "p2"]) {
        a.ack("snap", SnapshotPhase::Deleting, true, Some(p)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        support::get_snapshot(&client, "snap").await.is_some(),
        "released under a restore not at its size"
    );

    // Grown: Ready, initialized, the Snapshot goes.
    for a in &agents {
        a.ready("restored").await;
    }
    volume_where(&client, "restored", |v| {
        phase_of(v) == VolumePhase::Ready
            && v.status
                .as_ref()
                .is_some_and(|s| s.initialized_at.is_some() && s.usable_bytes == Some(2 * GIB))
    })
    .await;
    support::eventually(Duration::from_secs(20), || async {
        support::get_snapshot(&client, "snap")
            .await
            .is_none()
            .then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialized_at_is_set_once_and_kept() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    let [a1, a2, a3] = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| v.spec.assignment.is_some()).await;
    let g = v.metadata.generation;
    let initialized = |v: &Volume| v.status.as_ref().and_then(|s| s.initialized_at.clone());

    // Present but not UpToDate anywhere: not yet.
    let mut s1 = healthy("n1", &["n2"], g);
    s1.disk_state = Some("Inconsistent".into());
    a1.report("rep", s1).await;
    let mut tb = healthy("n3", &["n1", "n2"], g);
    tb.disk_state = Some("Diskless".into());
    tb.allocated_bytes = None;
    a3.report("rep", tb).await;
    volume_where(&client, "rep", |v| {
        v.status
            .as_ref()
            .is_some_and(|s| s.replicas.len() == 2 && !s.conditions.is_empty())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        initialized(&get_volume(&client, "rep").await.unwrap()),
        None
    );

    // One UpToDate diskful replica is not Ready: still not.
    a1.report("rep", healthy("n1", &["n2"], g)).await;
    volume_where(&client, "rep", |v| {
        entry_of(v, "n1").is_some_and(|e| e.disk_state.as_deref() == Some("UpToDate"))
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        initialized(&get_volume(&client, "rep").await.unwrap()),
        None
    );

    // The first Ready sets it.
    a2.report("rep", healthy("n2", &["n1"], g)).await;
    let v = volume_where(&client, "rep", |v| initialized(v).is_some()).await;
    assert_eq!(phase_of(&v), VolumePhase::Ready);
    let at = initialized(&v).unwrap();
    // Degraded and back, then deletion: every apply carries the same value.
    let mut s2 = healthy("n2", &["n1"], g);
    s2.disk_state = Some("Outdated".into());
    a2.report("rep", s2).await;
    let v = volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Degraded).await;
    assert_eq!(initialized(&v), Some(at.clone()));
    a2.report("rep", healthy("n2", &["n1"], g)).await;
    let v = volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    assert_eq!(initialized(&v), Some(at.clone()));
    Api::<Volume>::all(client.clone())
        .delete("rep", &DeleteParams::default())
        .await
        .unwrap();
    let v = volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Deleting).await;
    assert_eq!(initialized(&v), Some(at.clone()));
    support::assert_controller_status_fields(&v.metadata);
    let owner = v
        .metadata
        .managed_fields
        .iter()
        .flatten()
        .filter(|m| {
            serde_json::to_string(&m.fields_v1)
                .unwrap()
                .contains("f:initializedAt")
        })
        .filter_map(|m| m.manager.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(owner, [model::keys::CONTROLLER_FIELD_MANAGER]);
}

async fn ready_rep(client: &Client, agents: &[FakeAgent]) {
    create_volume(client, "rep", volume_spec(GIB, 2, "fast")).await;
    volume_where(client, "rep", |v| v.spec.assignment.is_some()).await;
    for a in agents {
        a.ready("rep").await;
    }
    volume_where(client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
}

async fn snap_in(client: &Client, name: &str, phase: SnapshotPhase) -> model::Snapshot {
    snapshot_where(client, name, |s| support::snap_phase(s) == Some(phase)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_suspend_bound_and_mid_round_attach() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) =
        setup_with(&assets, |c| c.suspend_timeout = Duration::from_secs(3)).await;
    cluster(&client).await;
    let agents = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    ready_rep(&client, &agents).await;
    let [a1, a2, a3] = &agents;
    let a4 = FakeAgent::new(&client, "n4");

    // n4 attached during Taking never suspended and may write: its agent
    // says so instead of confirming a freeze, and the round fails.
    create_snapshot(&client, "s-attach", "rep").await;
    snap_in(&client, "s-attach", SnapshotPhase::Suspending).await;
    for a in &agents {
        a.ack("s-attach", SnapshotPhase::Suspending, true, None)
            .await;
    }
    snap_in(&client, "s-attach", SnapshotPhase::Taking).await;
    patch_spec(&client, "rep", json!({ "attachments": ["n4"] })).await;
    volume_where(&client, "rep", |v| role_of(v, "n4") == Some(Role::Diskless)).await;
    a1.ack("s-attach", SnapshotPhase::Taking, true, Some("p1"))
        .await;
    a2.ack("s-attach", SnapshotPhase::Taking, true, Some("p2"))
        .await;
    a3.ack("s-attach", SnapshotPhase::Taking, true, None).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        support::snap_phase(&support::get_snapshot(&client, "s-attach").await.unwrap()),
        Some(SnapshotPhase::Taking),
        "waits for n4"
    );
    a4.ack("s-attach", SnapshotPhase::Taking, false, None).await;
    let s = snap_in(&client, "s-attach", SnapshotPhase::Failed).await;
    let e = s.status.unwrap().last_error.unwrap();
    assert!(e.contains("fake failure on n4"), "{e}");
    patch_spec(&client, "rep", json!({ "attachments": [] })).await;
    volume_where(&client, "rep", |v| role_of(v, "n4").is_none()).await;
    // Rounds start only on a spec every replica applied.
    for a in &agents {
        a.ready("rep").await;
    }

    // n3 never acks Suspending: past the bound the round fails naming n3;
    // the agents resume on their own.
    create_snapshot(&client, "s-bad", "rep").await;
    snap_in(&client, "s-bad", SnapshotPhase::Suspending).await;
    let started = tokio::time::Instant::now();
    a1.ack("s-bad", SnapshotPhase::Suspending, true, None).await;
    a2.ack("s-bad", SnapshotPhase::Suspending, true, None).await;
    let s = snap_in(&client, "s-bad", SnapshotPhase::Failed).await;
    assert!(started.elapsed() < Duration::from_secs(10));
    let e = s.status.unwrap().last_error.unwrap();
    assert!(e.contains("n3") && !e.contains("n1"), "{e}");
}

async fn held_with(client: &Client, name: &str, needle: &str) -> model::Snapshot {
    let s = snapshot_where(client, name, |s| {
        s.status
            .as_ref()
            .and_then(|st| st.last_error.as_deref())
            .is_some_and(|e| e.contains(needle))
    })
    .await;
    assert_eq!(support::snap_phase(&s), Some(SnapshotPhase::Pending));
    s
}

/// A failed round holds the volume's next one: longer after each failure
/// in a row, until a Ready round; and while a node that never acked has
/// not renewed its agent Lease since, though that Lease is still fresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_round_holds_the_next_one() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup_with(&assets, |c| {
        c.suspend_timeout = Duration::from_secs(3);
        c.round_backoff = Duration::from_secs(3);
    })
    .await;
    cluster(&client).await;
    let agents = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    ready_rep(&client, &agents).await;
    let [a1, a2, a3] = &agents;
    let suspend = async |s: &str, n2_ok: bool| {
        snap_in(&client, s, SnapshotPhase::Suspending).await;
        a1.ack(s, SnapshotPhase::Suspending, true, None).await;
        a2.ack(s, SnapshotPhase::Suspending, n2_ok, None).await;
    };

    // n3's agent stops renewing; its Lease stays fresh for 40 s.
    support::agent_lease(&client, "n3", Duration::ZERO, true).await;
    create_snapshot(&client, "s1", "rep").await;
    suspend("s1", true).await;
    snap_in(&client, "s1", SnapshotPhase::Failed).await;
    create_snapshot(&client, "s2", "rep").await;
    held_with(
        &client,
        "s2",
        "1 failed round(s) in a row: the next waits 3 s",
    )
    .await;
    held_with(&client, "s2", "agent not reachable on n3").await;
    support::agent_lease(&client, "n3", Duration::ZERO, false).await;
    suspend("s2", false).await;
    snap_in(&client, "s2", SnapshotPhase::Failed).await;

    create_snapshot(&client, "s3", "rep").await;
    held_with(
        &client,
        "s3",
        "2 failed round(s) in a row: the next waits 6 s",
    )
    .await;
    suspend("s3", true).await;
    a3.ack("s3", SnapshotPhase::Suspending, true, None).await;
    snap_in(&client, "s3", SnapshotPhase::Taking).await;
    a1.ack("s3", SnapshotPhase::Taking, true, Some("p1")).await;
    a2.ack("s3", SnapshotPhase::Taking, true, Some("p2")).await;
    a3.ack("s3", SnapshotPhase::Taking, true, None).await;
    snap_in(&client, "s3", SnapshotPhase::Ready).await;

    // A Ready round starts the count again.
    create_snapshot(&client, "s4", "rep").await;
    suspend("s4", false).await;
    snap_in(&client, "s4", SnapshotPhase::Failed).await;
    create_snapshot(&client, "s5", "rep").await;
    held_with(
        &client,
        "s5",
        "1 failed round(s) in a row: the next waits 3 s",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_round_waits_for_every_agent() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup_with(&assets, |c| c.lease_stale = SHORT_STALE).await;
    cluster(&client).await;
    let agents = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    let a4 = FakeAgent::new(&client, "n4");
    ready_rep(&client, &agents).await;
    let [a1, a2, _] = &agents;
    patch_spec(&client, "rep", json!({ "attachments": ["n4"] })).await;
    volume_where(&client, "rep", |v| role_of(v, "n4") == Some(Role::Diskless)).await;
    a4.ready("rep").await;
    support::agent_lease(&client, "n4", Duration::ZERO, true).await;
    tokio::time::sleep(SHORT_STALE + Duration::from_secs(1)).await;

    // n4 could be writing and would never suspend: the round waits.
    create_snapshot(&client, "s1", "rep").await;
    let s = snapshot_where(&client, "s1", |s| {
        s.status
            .as_ref()
            .and_then(|st| st.last_error.as_deref())
            .is_some_and(|e| e.contains("agent not reachable on n4"))
    })
    .await;
    assert_eq!(support::snap_phase(&s), Some(SnapshotPhase::Pending));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        support::snap_phase(&support::get_snapshot(&client, "s1").await.unwrap()),
        Some(SnapshotPhase::Pending)
    );

    // n4 no longer listed: the round starts and every listed replica acks.
    patch_spec(&client, "rep", json!({ "attachments": [] })).await;
    volume_where(&client, "rep", |v| role_of(v, "n4").is_none()).await;
    for a in &agents {
        a.ready("rep").await;
    }
    snap_in(&client, "s1", SnapshotPhase::Suspending).await;
    for a in &agents {
        a.ack("s1", SnapshotPhase::Suspending, true, None).await;
    }
    snap_in(&client, "s1", SnapshotPhase::Taking).await;
    a1.ack("s1", SnapshotPhase::Taking, true, Some("p1")).await;
    a2.ack("s1", SnapshotPhase::Taking, true, Some("p2")).await;
    agents[2].ack("s1", SnapshotPhase::Taking, true, None).await;
    snap_in(&client, "s1", SnapshotPhase::Ready).await;
}

async fn events_of(client: &Client, volume: &str, reason: &str) -> Vec<Event> {
    Api::<Event>::namespaced(client.clone(), "default")
        .list(&ListParams::default())
        .await
        .expect("list events")
        .items
        .into_iter()
        .filter(|e| {
            e.reason.as_deref() == Some(reason) && e.involved_object.name.as_deref() == Some(volume)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn evict_stale_diskful_to_a_fourth_node() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup_with(&assets, |c| c.lease_stale = SHORT_STALE).await;
    cluster(&client).await;
    pool(&client, "p4", "n4", "fast", 100 * GIB).await;
    let mut agents: BTreeMap<&str, _> = ["n1", "n2", "n3", "n4"]
        .into_iter()
        .map(|n| (n, FakeAgent::new(&client, n).spawn_auto()))
        .collect();
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| {
        phase_of(v) == VolumePhase::Ready
            && v.status
                .as_ref()
                .is_some_and(|s| s.initialized_at.is_some())
    })
    .await;
    let before = v.spec.assignment.clone().unwrap();
    let ids: Vec<_> = before
        .replicas
        .iter()
        .map(|r| (r.node.as_str(), r.node_id, r.role))
        .collect();
    assert_eq!(
        ids,
        [
            ("n1", 0, Role::Diskful),
            ("n2", 1, Role::Diskful),
            ("n3", 2, Role::Tiebreaker),
        ]
    );

    // n1 dies: its agent stops and its Lease goes stale.
    agents.remove("n1").unwrap().abort();
    support::agent_lease(&client, "n1", Duration::ZERO, true).await;
    volume_where(&client, "rep", |v| {
        cond(v, types::DEGRADED).is_some_and(|c| c.reason == "NodeUnreachable")
    })
    .await;
    // A Pending Snapshot (the volume is not Ready) does not block.
    create_snapshot(&client, "s0", "rep").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let s0 = support::get_snapshot(&client, "s0").await.unwrap();
    assert_eq!(support::snap_phase(&s0), Some(SnapshotPhase::Pending));
    patch_spec(&client, "rep", json!({ "evict": ["n1"] })).await;

    let v = volume_where(&client, "rep", |v| role_of(v, "n1").is_none()).await;
    let a = v.spec.assignment.as_ref().unwrap();
    let added = a.replicas.iter().find(|r| r.node == "n4").unwrap();
    // A new node id, never n1's 0; the lowest free port on n4.
    assert_eq!(
        (added.role, added.pool.as_deref(), added.node_id, added.port),
        (Role::Diskful, Some("p4"), 3, Some(11000))
    );
    assert_eq!(role_of(&v, "n2"), Some(Role::Diskful));
    assert_eq!(role_of(&v, "n3"), Some(Role::Tiebreaker));
    assert_eq!(a.minor, before.minor);
    // n1 leaves spec.evict; its stale entry stays while its Node exists,
    // since its agent may return to an old replica, and goes with the Node.
    volume_where(&client, "rep", |v| v.spec.evict.is_empty()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(entry_of(&get_volume(&client, "rep").await.unwrap(), "n1").is_some());
    support::delete_node(&client, "n1").await;
    let v = volume_where(&client, "rep", |v| entry_of(v, "n1").is_none()).await;
    assert!(v.status.as_ref().unwrap().initialized_at.is_some());
    let ev = events_of(&client, "rep", "Evicted").await;
    assert_eq!(ev.len(), 1);
    assert_eq!(
        ev[0].message.as_deref(),
        Some("replica of rep moved from n1 to n4")
    );
    assert_eq!(ev[0].type_.as_deref(), Some("Normal"));
    // The rebuild: Inconsistent first, then Ready once n4 is UpToDate.
    let v = volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    assert_eq!(
        entry_of(&v, "n4").unwrap().disk_state.as_deref(),
        Some("UpToDate")
    );
    assert_eq!(v.status.as_ref().unwrap().nodes.as_deref(), Some("n2,n4"));
    assert!(cond_is(&v, types::DEGRADED, "False"));
    support::assert_controller_status_fields(&v.metadata);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn evict_blocked_then_tiebreaker_takes_over() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    for n in ["n1", "n2", "n3"] {
        node(&client, n, true).await;
    }
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    pool(&client, "p2", "n2", "fast", 100 * GIB).await;
    pool(&client, "p3", "n3", "fast", 50 * GIB).await;
    let mut agents: BTreeMap<&str, _> = ["n1", "n2", "n3"]
        .into_iter()
        .map(|n| (n, FakeAgent::new(&client, n).spawn_auto()))
        .collect();
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    let v = volume_where(&client, "rep", |v| {
        phase_of(v) == VolumePhase::Ready
            && v.status
                .as_ref()
                .is_some_and(|s| s.initialized_at.is_some())
    })
    .await;
    assert_eq!(role_of(&v, "n3"), Some(Role::Tiebreaker));
    let tb = v
        .spec
        .assignment
        .as_ref()
        .unwrap()
        .replicas
        .iter()
        .find(|r| r.node == "n3")
        .cloned()
        .unwrap();

    // The only other diskful replica is not UpToDate: blocked.
    agents.remove("n1").unwrap().abort();
    let a1 = FakeAgent::new(&client, "n1");
    let mut outdated = healthy("n1", &["n2"], v.metadata.generation);
    outdated.disk_state = Some("Outdated".into());
    a1.report("rep", outdated).await;
    patch_spec(&client, "rep", json!({ "evict": ["n2"] })).await;
    let v = volume_where(&client, "rep", |v| {
        cond(v, types::DEGRADED).is_some_and(|c| c.message.contains("eviction of n2 blocked"))
    })
    .await;
    let msg = cond(&v, types::DEGRADED).unwrap().message.clone();
    assert!(
        msg.contains("no UpToDate replica with quorum outside n2"),
        "{msg}"
    );
    assert_eq!(v.spec.evict, ["n2"]);
    assert_eq!(role_of(&v, "n2"), Some(Role::Diskful));
    // One Warning per reason, not one per pass.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let blocked = events_of(&client, "rep", "EvictionBlocked").await;
    assert_eq!(blocked.len(), 1, "{blocked:?}");
    assert_eq!(blocked[0].type_.as_deref(), Some("Warning"));
    assert!(
        blocked[0]
            .message
            .as_deref()
            .is_some_and(|m| m.starts_with("eviction of n2 blocked: ")),
        "{blocked:?}"
    );

    // n1 catches up. Only the tiebreaker's node has a pool left: the
    // tiebreaker becomes diskful with its node id and port, and no node
    // remains for a new tiebreaker.
    agents.insert("n1", a1.spawn_auto());
    let v = volume_where(&client, "rep", |v| role_of(v, "n2").is_none()).await;
    let a = v.spec.assignment.as_ref().unwrap();
    let n3 = a.replicas.iter().find(|r| r.node == "n3").unwrap();
    assert_eq!(
        (n3.role, n3.pool.as_deref(), n3.node_id, n3.port),
        (Role::Diskful, Some("p3"), tb.node_id, tb.port)
    );
    assert!(!a.replicas.iter().any(|r| r.role == Role::Tiebreaker));
    assert_eq!(a.replicas.len(), 2);
    // A status from the pass before the move can stand a moment longer.
    volume_where(&client, "rep", |v| {
        v.spec.evict.is_empty()
            && entry_of(v, "n2").is_none()
            && phase_of(v) == VolumePhase::Ready
            && v.status.as_ref().and_then(|s| s.nodes.as_deref()) == Some("n1,n3")
    })
    .await;
    let ev = events_of(&client, "rep", "Evicted").await;
    assert_eq!(ev.len(), 1);
    assert_eq!(
        ev[0].message.as_deref(),
        Some("replica of rep moved from n2 to n3")
    );
}

/// Waits for an EvictionBlocked Event with exactly `message`.
async fn blocked_event(client: &Client, message: &str) -> Event {
    support::eventually(Duration::from_secs(20), || async {
        events_of(client, "rep", "EvictionBlocked")
            .await
            .into_iter()
            .find(|e| e.message.as_deref() == Some(message))
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn evict_keeps_running_workloads() {
    let assets = require_envtest!();
    let (_env, client, cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    pool(&client, "p4", "n4", "fast", 100 * GIB).await;
    let _agents: Vec<_> = ["n1", "n2", "n3", "n4"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "rep", |v| {
        phase_of(v) == VolumePhase::Ready
            && v.status
                .as_ref()
                .is_some_and(|s| s.initialized_at.is_some())
    })
    .await;

    // An attached diskless entry of an evicted node survives passes.
    patch_spec(&client, "rep", json!({ "attachments": ["n4"] })).await;
    volume_where(&client, "rep", |v| {
        role_of(v, "n4") == Some(Role::Diskless) && entry_of(v, "n4").is_some_and(|e| e.present)
    })
    .await;
    patch_spec(&client, "rep", json!({ "evict": ["n4"] })).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let v = get_volume(&client, "rep").await.unwrap();
    assert_eq!(role_of(&v, "n4"), Some(Role::Diskless));
    assert_eq!(v.spec.evict, ["n4"]);

    // A repeat publish of the attached n4 stays idempotent;
    // a node being evicted and not attached is refused before any edit.
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, cfg);
    let publish = |node: &str| ControllerPublishVolumeRequest {
        volume_id: "rep".into(),
        node_id: node.into(),
        volume_capability: Some(mount()),
        ..ControllerPublishVolumeRequest::default()
    };
    let ctx = svc
        .controller_publish_volume(Request::new(publish("n4")))
        .await
        .unwrap()
        .into_inner()
        .publish_context;
    assert!(
        ctx.get("devicePath")
            .is_some_and(|p| p.starts_with("/dev/drbd"))
    );
    patch_spec(&client, "rep", json!({ "evict": ["n4", "n5"] })).await;
    let err = svc
        .controller_publish_volume(Request::new(publish("n5")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(err.message(), "node n5 is being evicted from volume rep");
    assert_eq!(
        get_volume(&client, "rep").await.unwrap().spec.attachments,
        ["n4"]
    );
    patch_spec(&client, "rep", json!({ "evict": ["n4"] })).await;

    // The attachment goes: so does the entry, then n4 leaves spec.evict.
    patch_spec(&client, "rep", json!({ "attachments": [] })).await;
    volume_where(&client, "rep", |v| {
        role_of(v, "n4").is_none() && v.spec.evict.is_empty()
    })
    .await;

    // A fresh diskful node still attached is not evicted;
    // the volume is Ready, so only the Event carries the text.
    patch_spec(&client, "rep", json!({ "attachments": ["n2"] })).await;
    patch_spec(&client, "rep", json!({ "evict": ["n2"] })).await;
    let e = blocked_event(
        &client,
        "eviction of n2 blocked: attached to n2; unpublish it first",
    )
    .await;
    assert_eq!(e.type_.as_deref(), Some("Warning"));
    let v = get_volume(&client, "rep").await.unwrap();
    assert_eq!(phase_of(&v), VolumePhase::Ready);
    let d = cond(&v, types::DEGRADED).unwrap();
    assert_eq!((d.status.as_str(), d.message.as_str()), ("False", ""));
    assert_eq!(role_of(&v, "n2"), Some(Role::Diskful));

    // Unpublished: n2 moves to n4.
    patch_spec(&client, "rep", json!({ "attachments": [] })).await;
    let v = volume_where(&client, "rep", |v| {
        role_of(v, "n2").is_none() && v.spec.evict.is_empty()
    })
    .await;
    assert_eq!(role_of(&v, "n4"), Some(Role::Diskful));

    // The apiserver's answer to a failed test op is what the orphan
    // removal treats as a race.
    let ops = serde_json::from_value(json!([
        { "op": "test", "path": "/status/replicas/0/node", "value": "nope" },
        { "op": "remove", "path": "/status/replicas/0" },
    ]))
    .unwrap();
    let err = Api::<Volume>::all(client.clone())
        .patch_status("rep", &PatchParams::default(), &Patch::Json::<()>(ops))
        .await
        .unwrap_err();
    let kube::Error::Api(s) = err else {
        panic!("{err}");
    };
    assert!(controller::volume::patch_not_applied(&s), "{s:?}");
    // A schema rejection is not taken for a race.
    let ops = serde_json::from_value(json!([
        { "op": "replace", "path": "/status/phase", "value": "Bogus" },
    ]))
    .unwrap();
    let err = Api::<Volume>::all(client.clone())
        .patch_status("rep", &PatchParams::default(), &Patch::Json::<()>(ops))
        .await
        .unwrap_err();
    let kube::Error::Api(s) = err else {
        panic!("{err}");
    };
    assert_eq!(s.code, 422);
    assert!(!controller::volume::patch_not_applied(&s), "{s:?}");
    // The fake agents report after the assignment moves, so wait for it.
    volume_where(&client, "rep", |v| {
        v.status.as_ref().is_some_and(|s| s.replicas.len() >= 3)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evict_waits_for_the_snapshot_round() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    pool(&client, "p4", "n4", "fast", 100 * GIB).await;
    let agents = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    ready_rep(&client, &agents).await;
    volume_where(&client, "rep", |v| {
        v.status
            .as_ref()
            .is_some_and(|s| s.initialized_at.is_some())
    })
    .await;
    let [a1, a2, _] = &agents;

    // A node joining mid-round would ack Taking with a blank LV.
    create_snapshot(&client, "s1", "rep").await;
    snap_in(&client, "s1", SnapshotPhase::Suspending).await;
    patch_spec(&client, "rep", json!({ "evict": ["n2"] })).await;
    blocked_event(&client, "eviction of n2 blocked: snapshot s1 in progress").await;
    let v = get_volume(&client, "rep").await.unwrap();
    assert_eq!(role_of(&v, "n2"), Some(Role::Diskful));
    assert_eq!(role_of(&v, "n4"), None);

    for a in &agents {
        a.ack("s1", SnapshotPhase::Suspending, true, None).await;
    }
    snap_in(&client, "s1", SnapshotPhase::Taking).await;
    a1.ack("s1", SnapshotPhase::Taking, true, Some("p1")).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let v = get_volume(&client, "rep").await.unwrap();
    assert_eq!(role_of(&v, "n4"), None, "moved during Taking");
    a2.ack("s1", SnapshotPhase::Taking, true, Some("p2")).await;
    agents[2].ack("s1", SnapshotPhase::Taking, true, None).await;
    snap_in(&client, "s1", SnapshotPhase::Ready).await;
    // The round is over: the move goes ahead.
    let v = volume_where(&client, "rep", |v| role_of(v, "n2").is_none()).await;
    assert_eq!(role_of(&v, "n4"), Some(Role::Diskful));
}

fn secs_ago(s: u64) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::Time {
    let now = k8s_openapi::jiff::Timestamp::now().as_second();
    k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        k8s_openapi::jiff::Timestamp::from_second(now - i64::try_from(s).unwrap()).unwrap(),
    )
}

/// `node` probed `peer` just now; its connection is down since 25 s.
fn stuck(node: &str, peer: &str, result: model::ProbeResult) -> ReplicaStatus {
    let mut e = healthy(node, &[], None);
    e.disk_state = Some("Inconsistent".into());
    e.peers.insert(peer.into(), "Connecting".into());
    e.unreachable_peers = vec![model::PeerProbe {
        node: peer.into(),
        address: "192.0.2.2".into(),
        port: 11000,
        since: secs_ago(25),
        result,
        probed_at: secs_ago(1),
        error: None,
        peer_run: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
            support::agent_started(),
        )),
    }];
    e
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn peer_unreachable_while_provisioning() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) =
        setup_with(&assets, |c| c.peer_clear_grace = Duration::from_secs(10)).await;
    cluster(&client).await;
    let [a1, a2, a3] = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n));
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "rep", |v| v.spec.assignment.is_some()).await;
    let mut s2 = healthy("n2", &[], None);
    s2.peers.insert("n1".into(), "Connecting".into());
    a2.report("rep", s2.clone()).await;
    a1.report("rep", stuck("n1", "n2", model::ProbeResult::Filtered))
        .await;
    let fired = |v: &Volume| {
        cond(v, types::PEER_UNREACHABLE)
            .is_some_and(|c| c.status == "True" && c.reason == "Firewall")
    };
    let v = volume_where(&client, "rep", fired).await;
    assert_eq!(phase_of(&v), VolumePhase::Provisioning);
    assert_eq!(
        cond(&v, types::PEER_UNREACHABLE).unwrap().message,
        "rejected by a firewall between n1, n2 (TCP 11000); open TCP 11000-11999 between nodes"
    );
    let warned = support::eventually(Duration::from_secs(10), || async {
        let ev = events_of(&client, "rep", "PeerUnreachable").await;
        (!ev.is_empty()).then_some(ev)
    })
    .await;
    assert_eq!(warned[0].type_.as_deref(), Some("Warning"));

    // A re-probe of the same run keeps the condition True.
    let mut reprobed = stuck("n1", "n2", model::ProbeResult::Filtered);
    reprobed.unreachable_peers[0].probed_at = secs_ago(0);
    a1.report("rep", reprobed).await;
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(fired(&get_volume(&client, "rep").await.unwrap()));
    }

    // The link recovers, then n2 reboots: its Lease stops renewing, still
    // fresh for 40 s, and n1's probe of the rebooting n2 fails. Not a
    // firewall.
    a1.report("rep", healthy("n1", &["n2"], None)).await;
    volume_where(&client, "rep", |v| {
        cond_is(v, types::PEER_UNREACHABLE, "False")
    })
    .await;
    support::agent_lease(&client, "n2", Duration::from_secs(15), true).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    a1.report("rep", stuck("n1", "n2", model::ProbeResult::Filtered))
        .await;
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(cond_is(
            &get_volume(&client, "rep").await.unwrap(),
            types::PEER_UNREACHABLE,
            "False"
        ));
    }
    // Back with a new agent run: n1's old record does not count.
    let run =
        k8s_openapi::jiff::Timestamp::from_second(k8s_openapi::jiff::Timestamp::now().as_second())
            .unwrap();
    support::agent_lease_run(&client, "n2", Duration::ZERO, false, run).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(cond_is(
        &get_volume(&client, "rep").await.unwrap(),
        types::PEER_UNREACHABLE,
        "False"
    ));
    // n1's agent probes again in the new window: True once more.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut again = stuck("n1", "n2", model::ProbeResult::Filtered);
    again.unreachable_peers[0].since = secs_ago(0);
    again.unreachable_peers[0].probed_at = secs_ago(0);
    again.unreachable_peers[0].peer_run =
        Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(run));
    a1.report("rep", again).await;
    volume_where(&client, "rep", fired).await;

    // Connected: False, with one Normal Event after the grace.
    a1.report("rep", healthy("n1", &["n2"], None)).await;
    a2.report("rep", healthy("n2", &["n1"], None)).await;
    a3.ready("rep").await;
    let v = volume_where(&client, "rep", |v| {
        cond_is(v, types::PEER_UNREACHABLE, "False") && phase_of(v) == VolumePhase::Ready
    })
    .await;
    assert_eq!(
        cond(&v, types::PEER_UNREACHABLE).unwrap().reason,
        "PeersReachable"
    );
    support::eventually(Duration::from_secs(30), || async {
        (events_of(&client, "rep", "PeersReachable").await.len() == 1).then_some(())
    })
    .await;
    // The reboot's short False was inside the grace: one Warning only.
    assert_eq!(events_of(&client, "rep", "PeerUnreachable").await.len(), 1);
}

/// A holder whose Node is gone took its snapshot LV with it; one whose
/// agent is only stale may come back holding it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_delete_skips_a_gone_holder() {
    let assets = require_envtest!();
    let (_env, client, _cfg, _ctl) = setup(&assets).await;
    cluster(&client).await;
    let auto: Vec<_> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| FakeAgent::new(&client, n).spawn_auto())
        .collect();
    create_volume(&client, "src", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "src", |v| phase_of(v) == VolumePhase::Ready).await;
    create_snapshot(&client, "snap", "src").await;
    snap_in(&client, "snap", SnapshotPhase::Ready).await;
    for a in auto {
        a.abort();
    }
    Api::<model::Snapshot>::all(client.clone())
        .delete("snap", &DeleteParams::default())
        .await
        .expect("delete snapshot");
    snap_in(&client, "snap", SnapshotPhase::Deleting).await;
    FakeAgent::new(&client, "n1")
        .ack("snap", SnapshotPhase::Deleting, true, Some("p1"))
        .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        support::get_snapshot(&client, "snap").await.is_some(),
        "released while n2 may hold its LV"
    );
    support::delete_node(&client, "n2").await;
    support::eventually(Duration::from_secs(20), || async {
        support::get_snapshot(&client, "snap")
            .await
            .is_none()
            .then_some(())
    })
    .await;
}
