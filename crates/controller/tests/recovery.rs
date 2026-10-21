//! A lost Snapshot status, rebuilt from the copies the agents find by
//! their uid tag, never by a new round.

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Event;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, WatchEvent, WatchParams};
use kube::{Api, Client};
use model::{Snapshot, SnapshotPhase, VolumePhase};
use serde_json::json;
use support::{
    Env, FakeAgent, GIB, Running, create_snapshot, create_volume, get_snapshot, node, phase_of,
    pool, snap_phase, snapshot_where, volume_spec, volume_where,
};

async fn setup(assets: &std::path::Path) -> (Env, Client, Running) {
    support::init_tracing();
    let env = Env::start(assets).await;
    let client = env.client().await;
    let running = support::spawn(env.clients().await, support::config(&env, "ctl"));
    (env, client, running)
}

/// n1, n2 and n3, each with a Pool; their agents run.
async fn cluster(client: &Client) -> Vec<FakeAgent> {
    let mut agents = Vec::new();
    for n in ["n1", "n2", "n3"] {
        node(client, n, true).await;
        pool(client, &format!("p-{n}"), n, "fast", 100 * GIB).await;
        agents.push(FakeAgent::new(client, n));
    }
    agents
}

/// A Ready Snapshot of a 2 replica volume, taken by the auto agents.
async fn ready_snapshot(client: &Client) -> Snapshot {
    create_volume(client, "src", volume_spec(GIB, 2, "fast")).await;
    volume_where(client, "src", |v| phase_of(v) == VolumePhase::Ready).await;
    create_snapshot(client, "snap", "src").await;
    snapshot_where(client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await
}

fn holders(s: &Snapshot) -> Vec<(String, String)> {
    controller::volume::snapshot_holders(s)
}

fn api(client: &Client) -> Api<Snapshot> {
    Api::all(client.clone())
}

/// Removes status fields with a merge patch, as a backup restore or a
/// hand edit might.
async fn clear(client: &Client, status: serde_json::Value) {
    api(client)
        .patch_status(
            "snap",
            &PatchParams::default(),
            &Patch::Merge(&json!({ "status": status })),
        )
        .await
        .expect("clear status");
}

/// Every stored version of the Snapshot from now on.
async fn versions(client: &Client) -> impl futures::Stream<Item = Snapshot> + Unpin {
    let a = api(client);
    let rv = a.list(&ListParams::default()).await.unwrap().metadata;
    a.watch(&WatchParams::default(), &rv.resource_version.unwrap())
        .await
        .unwrap()
        .try_filter_map(|e| async move {
            Ok(match e {
                WatchEvent::Added(s) | WatchEvent::Modified(s) => Some(s),
                _ => None,
            })
        })
        .map(|r| r.expect("watch"))
        .boxed()
}

/// The phases of `seen` up to the outcome of a discovery.
async fn phases_until_outcome(
    seen: &mut (impl futures::Stream<Item = Snapshot> + Unpin),
) -> Vec<Option<SnapshotPhase>> {
    let mut out = Vec::new();
    loop {
        let s = tokio::time::timeout(Duration::from_secs(30), seen.next())
            .await
            .expect("an outcome")
            .expect("watch open");
        let p = snap_phase(&s);
        if p == Some(SnapshotPhase::Discovering) && out.last() != Some(&p) {
            let acks = &s.status.as_ref().unwrap().acks;
            assert!(acks.is_empty(), "a discovery starts without acks: {acks:?}");
        }
        let discovered = out.contains(&Some(SnapshotPhase::Discovering));
        out.push(p);
        if discovered && matches!(p, Some(SnapshotPhase::Ready | SnapshotPhase::Lost)) {
            return out;
        }
    }
}

fn round_seen(phases: &[Option<SnapshotPhase>]) -> bool {
    phases.iter().any(|p| {
        matches!(
            p,
            Some(SnapshotPhase::Pending | SnapshotPhase::Suspending | SnapshotPhase::Taking)
        )
    })
}

async fn events_of(client: &Client, reason: &str) -> Vec<Event> {
    Api::<Event>::namespaced(client.clone(), "default")
        .list(&ListParams::default())
        .await
        .expect("list events")
        .items
        .into_iter()
        .filter(|e| {
            e.reason.as_deref() == Some(reason) && e.involved_object.name.as_deref() == Some("snap")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_status_comes_back_from_the_copies() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets).await;
    let _auto: Vec<_> = cluster(&client)
        .await
        .into_iter()
        .map(FakeAgent::spawn_auto)
        .collect();
    let before = ready_snapshot(&client).await;
    let was = before.status.clone().unwrap();
    assert_eq!(holders(&before).len(), 2);

    // The main case: the source Volume is gone.
    Api::<model::Volume>::all(client.clone())
        .delete("src", &DeleteParams::default())
        .await
        .unwrap();
    support::eventually(Duration::from_secs(20), || async {
        support::get_volume(&client, "src")
            .await
            .is_none()
            .then_some(())
    })
    .await;

    // The whole status, then only the phase.
    for cleared in [json!(null), json!({ "phase": null })] {
        let mut seen = versions(&client).await;
        clear(&client, cleared.clone()).await;
        let phases = phases_until_outcome(&mut seen).await;
        assert!(!round_seen(&phases), "{cleared}: {phases:?}");
        assert_eq!(
            phases.last(),
            Some(&Some(SnapshotPhase::Ready)),
            "{cleared}"
        );
        let after = get_snapshot(&client, "snap").await.unwrap();
        let st = after.status.clone().unwrap();
        assert_eq!(holders(&after), holders(&before), "{cleared}");
        assert_eq!(
            (st.size_bytes, st.source_replicas, &st.created_at),
            (was.size_bytes, was.source_replicas, &was.created_at),
            "{cleared}"
        );
        assert_eq!(st.file_system, None, "never rebuilt");
    }

    // Ready without its size is a lost status as well.
    let mut seen = versions(&client).await;
    clear(&client, json!({ "sizeBytes": null })).await;
    let phases = phases_until_outcome(&mut seen).await;
    assert_eq!(phases.last(), Some(&Some(SnapshotPhase::Ready)));
    let st = get_snapshot(&client, "snap").await.unwrap().status.unwrap();
    assert_eq!(st.size_bytes, was.size_bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_node_holds_the_discovery() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets).await;
    let agents = cluster(&client).await;
    let auto: Vec<_> = agents.iter().cloned().map(FakeAgent::spawn_auto).collect();
    ready_snapshot(&client).await;
    // n3, the tiebreaker, holds no copy, but nobody can know that.
    auto[2].abort();
    clear(&client, json!(null)).await;
    let s = snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Discovering)
            && s.status
                .as_ref()
                .and_then(|st| st.last_error.as_deref())
                .is_some_and(|e| e.ends_with("n3"))
    })
    .await;
    assert_eq!(
        support::ready_reason_of_snapshot(&s).as_deref(),
        Some("Discovering")
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let s = get_snapshot(&client, "snap").await.unwrap();
    assert_eq!(snap_phase(&s), Some(SnapshotPhase::Discovering));

    // Ten minutes in: said in the condition, once in an Event.
    let ago = k8s_openapi::jiff::Timestamp::now()
        .checked_sub(Duration::from_secs(601))
        .unwrap();
    let ago = k8s_openapi::jiff::Timestamp::from_second(ago.as_second()).unwrap();
    clear(&client, json!({ "phaseSince": ago.to_string() })).await;
    let s = snapshot_where(&client, "snap", |s| {
        support::ready_reason_of_snapshot(s).as_deref() == Some("DiscoveryIncomplete")
    })
    .await;
    assert_eq!(snap_phase(&s), Some(SnapshotPhase::Discovering));
    support::eventually(Duration::from_secs(10), || async {
        (events_of(&client, "ImageDiscoveryIncomplete").await.len() == 1).then_some(())
    })
    .await;

    // Its Node object deleted changes nothing.
    support::delete_node(&client, "n3").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let s = get_snapshot(&client, "snap").await.unwrap();
    assert_eq!(snap_phase(&s), Some(SnapshotPhase::Discovering));
    assert_eq!(
        events_of(&client, "ImageDiscoveryIncomplete").await.len(),
        1
    );

    // The agent back: it answers, and the status is whole again.
    let back = agents[2].clone().spawn_auto();
    let s = snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await;
    assert_eq!(holders(&s).len(), 2);

    // Silent again, its agent still live: its Pool's deletion waits.
    back.abort();
    clear(&client, json!(null)).await;
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Discovering)
            && s.status
                .as_ref()
                .and_then(|st| st.last_error.as_deref())
                .is_some_and(|e| e.ends_with("n3"))
    })
    .await;
    Api::<model::Pool>::all(client.clone())
        .delete("p-n3", &DeleteParams::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(support::get_pool(&client, "p-n3").await.is_some(), "held");
    // Its agent stale: deleting the Pool gives up on its copies, and the
    // discovery decides without that node.
    support::delete_agent_lease(&client, "n3").await;
    support::eventually(Duration::from_secs(20), || async {
        support::get_pool(&client, "p-n3")
            .await
            .is_none()
            .then_some(())
    })
    .await;
    let s = snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await;
    assert_eq!(holders(&s).len(), 2);
}

/// A live agent's Pool deletion waits for its answer, not longer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_goes_once_its_node_answered() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets).await;
    let agents = cluster(&client).await;
    let auto: Vec<_> = agents.iter().cloned().map(FakeAgent::spawn_auto).collect();
    ready_snapshot(&client).await;
    auto[2].abort();
    clear(&client, json!(null)).await;
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Discovering)
    })
    .await;
    Api::<model::Pool>::all(client.clone())
        .delete("p-n3", &DeleteParams::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(support::get_pool(&client, "p-n3").await.is_some(), "held");
    // The wait shows on the Pool, once.
    let waits = || async {
        Api::<Event>::namespaced(client.clone(), "default")
            .list(&ListParams::default())
            .await
            .unwrap()
            .items
            .into_iter()
            .filter(|e| {
                e.reason.as_deref() == Some("DeletionWaitsForDiscovery")
                    && e.involved_object.name.as_deref() == Some("p-n3")
            })
            .filter_map(|e| e.message)
            .collect::<Vec<_>>()
    };
    let shown = waits().await;
    assert_eq!(shown.len(), 1, "{shown:?}");
    assert!(
        shown[0].contains("n3") && shown[0].contains("snap"),
        "{shown:?}"
    );
    let s = get_snapshot(&client, "snap").await.unwrap();
    agents[2].answer(&s).await.unwrap();
    support::eventually(Duration::from_secs(20), || async {
        support::get_pool(&client, "p-n3")
            .await
            .is_none()
            .then_some(())
    })
    .await;
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_copy_left_ends_lost_never_in_a_round() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets).await;
    let agents = cluster(&client).await;
    let _auto: Vec<_> = agents.iter().cloned().map(FakeAgent::spawn_auto).collect();
    ready_snapshot(&client).await;
    for a in &agents {
        a.copies.lock().unwrap().clear();
    }
    let mut seen = versions(&client).await;
    clear(&client, json!(null)).await;
    let phases = phases_until_outcome(&mut seen).await;
    assert!(!round_seen(&phases), "{phases:?}");
    assert_eq!(phases.last(), Some(&Some(SnapshotPhase::Lost)));
    let s = get_snapshot(&client, "snap").await.unwrap();
    assert_eq!(
        support::ready_reason_of_snapshot(&s).as_deref(),
        Some("SnapshotLost")
    );
    support::eventually(Duration::from_secs(10), || async {
        (events_of(&client, "ImageLost").await.len() == 1).then_some(())
    })
    .await;
    // The not-found answers go; a Lost Snapshot stays as it is.
    support::eventually(Duration::from_secs(10), || async {
        get_snapshot(&client, "snap")
            .await?
            .status?
            .acks
            .is_empty()
            .then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let s = get_snapshot(&client, "snap").await.unwrap();
    assert_eq!(snap_phase(&s), Some(SnapshotPhase::Lost));

    // Copies whose tags disagree end Lost too, and stay.
    let uid = s.metadata.uid.clone().unwrap();
    for (a, size) in agents.iter().zip([GIB, 2 * GIB]) {
        let copy = support::FakeCopy {
            pool: format!("p-{}", a.node),
            size_bytes: Some(size),
            source_replicas: Some(2),
        };
        a.copies.lock().unwrap().insert(uid.clone(), copy);
    }
    clear(&client, json!({ "phase": null })).await;
    let s = snapshot_where(&client, "snap", |s| {
        support::ready_reason_of_snapshot(s).as_deref() == Some("TagsDisagree")
    })
    .await;
    assert_eq!(snap_phase(&s), Some(SnapshotPhase::Lost));
    assert_eq!(holders(&s).len(), 2, "the copies stay");
    support::eventually(Duration::from_secs(10), || async {
        (events_of(&client, "ImageLost").await.len() == 2).then_some(())
    })
    .await;

    // Deleted: the found copies are its holders; then it goes.
    api(&client)
        .delete("snap", &DeleteParams::default())
        .await
        .unwrap();
    support::eventually(Duration::from_secs(20), || async {
        get_snapshot(&client, "snap").await.is_none().then_some(())
    })
    .await;
    assert!(agents.iter().all(|a| a.copies.lock().unwrap().is_empty()));
}

/// A status cleared on a deleting Snapshot: the discovery ends first, then
/// every copy it found goes and the finalizer with them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleting_snapshot_finishes_its_discovery_first() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets).await;
    let agents = cluster(&client).await;
    let auto: Vec<_> = agents.iter().cloned().map(FakeAgent::spawn_auto).collect();
    ready_snapshot(&client).await;
    for a in &auto {
        a.abort();
    }
    api(&client)
        .delete("snap", &DeleteParams::default())
        .await
        .unwrap();
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Deleting)
    })
    .await;
    clear(&client, json!(null)).await;
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Discovering)
    })
    .await;
    let _back: Vec<_> = agents.iter().cloned().map(FakeAgent::spawn_auto).collect();
    support::eventually(Duration::from_secs(30), || async {
        get_snapshot(&client, "snap").await.is_none().then_some(())
    })
    .await;
    assert!(agents.iter().all(|a| a.copies.lock().unwrap().is_empty()));
}

/// The discovery's first write: one merge patch with phase Discovering and
/// no ack, landing only on the version it was computed from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_discovery_starts_on_the_version_read() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    let a = api(&client);
    create_snapshot(&client, "snap", "vol").await;
    let status =
        json!({ "phase": "Ready", "sizeBytes": GIB, "sourceReplicas": 2, "conditions": [] });
    let rv = get_snapshot(&client, "snap").await.unwrap();
    assert!(
        controller::apply_status_at(&a, "snap", rv.metadata.resource_version.as_deref(), status)
            .await
    );
    let agent = FakeAgent::new(&client, "n1");
    agent
        .ack("snap", SnapshotPhase::Taking, true, Some("p1"))
        .await;
    let stale = get_snapshot(&client, "snap").await.unwrap();
    FakeAgent::new(&client, "n2")
        .ack("snap", SnapshotPhase::Taking, true, Some("p2"))
        .await;
    controller::snapshot::start_discovery(&a, &stale).await;
    let s = get_snapshot(&client, "snap").await.unwrap();
    assert_eq!(
        snap_phase(&s),
        Some(SnapshotPhase::Ready),
        "stale: not applied"
    );
    assert_eq!(s.status.as_ref().unwrap().acks.len(), 2);

    controller::snapshot::start_discovery(&a, &s).await;
    let s = get_snapshot(&client, "snap").await.unwrap();
    let st = s.status.unwrap();
    assert_eq!(st.phase, Some(SnapshotPhase::Discovering));
    assert_eq!(st.acks, []);
    assert_eq!((st.size_bytes, st.source_replicas), (None, None));
    assert!(st.phase_since.is_some());
    // The agents' entries went with the list; their next apply adds them back.
    agent
        .ack("snap", SnapshotPhase::Discovering, true, None)
        .await;
    let st = get_snapshot(&client, "snap").await.unwrap().status.unwrap();
    let nodes: BTreeSet<&str> = st.acks.iter().map(|a| a.node.as_str()).collect();
    assert_eq!(nodes, BTreeSet::from(["n1"]));
}

/// A discovery may have started mid round, with IO still frozen for it on
/// some node: no new round and no resize start on its volume until every
/// agent's own deadline passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_discovery_holds_rounds_and_resizes_of_its_volume() {
    use csi::tonic::{Code, Request};
    use csi::v1::controller_server::Controller;
    let assets = require_envtest!();
    let (env, client, _ctl) = setup(&assets).await;
    let _auto: Vec<_> = cluster(&client)
        .await
        .into_iter()
        .map(FakeAgent::spawn_auto)
        .collect();
    ready_snapshot(&client).await;
    // No agent on n4: the discovery stays open.
    node(&client, "n4", true).await;
    pool(&client, "p-n4", "n4", "fast", 100 * GIB).await;
    clear(&client, json!(null)).await;
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Discovering)
    })
    .await;

    create_snapshot(&client, "next", "src").await;
    let held = snapshot_where(&client, "next", |s| {
        s.status
            .as_ref()
            .and_then(|st| st.last_error.as_deref())
            .is_some_and(|e| e.contains("snapshot snap"))
    })
    .await;
    assert_eq!(snap_phase(&held), Some(SnapshotPhase::Pending));
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, support::config(&env, "ctl"));
    let expand = || csi::v1::ControllerExpandVolumeRequest {
        volume_id: "src".into(),
        capacity_range: Some(csi::v1::CapacityRange {
            required_bytes: i64::try_from(2 * GIB).unwrap(),
            limit_bytes: 0,
        }),
        ..csi::v1::ControllerExpandVolumeRequest::default()
    };
    let e = svc
        .controller_expand_volume(Request::new(expand()))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Aborted, "{e}");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let held = get_snapshot(&client, "next").await.unwrap();
    assert_eq!(snap_phase(&held), Some(SnapshotPhase::Pending));

    // Past every agent's deadline: the round and the resize go ahead.
    let bound = model::snapshot::RESUME_AFTER + model::snapshot::CLOCK_MARGIN;
    let ago = k8s_openapi::jiff::Timestamp::now()
        .checked_sub(bound + Duration::from_secs(1))
        .unwrap();
    let ago = k8s_openapi::jiff::Timestamp::from_second(ago.as_second()).unwrap();
    clear(&client, json!({ "phaseSince": ago.to_string() })).await;
    snapshot_where(&client, "next", |s| {
        snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await;
    support::eventually(Duration::from_secs(20), || async {
        svc.controller_expand_volume(Request::new(expand()))
            .await
            .ok()
    })
    .await;
    assert_eq!(
        snap_phase(&get_snapshot(&client, "snap").await.unwrap()),
        Some(SnapshotPhase::Discovering)
    );
}

fn restore_request(name: &str) -> csi::v1::CreateVolumeRequest {
    use csi::v1::volume_capability::access_mode::Mode;
    use csi::v1::volume_capability::{AccessMode, AccessType, MountVolume};
    use csi::v1::volume_content_source::{SnapshotSource, Type};
    csi::v1::CreateVolumeRequest {
        name: name.into(),
        capacity_range: Some(csi::v1::CapacityRange {
            required_bytes: i64::try_from(GIB).unwrap(),
            limit_bytes: 0,
        }),
        volume_capabilities: vec![csi::v1::VolumeCapability {
            access_mode: Some(AccessMode {
                mode: Mode::SingleNodeWriter.into(),
            }),
            access_type: Some(AccessType::Mount(MountVolume::default())),
        }],
        parameters: [("tier", "fast"), ("replicas", "2")]
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .into(),
        volume_content_source: Some(csi::v1::VolumeContentSource {
            r#type: Some(Type::Snapshot(SnapshotSource {
                snapshot_id: "snap".into(),
            })),
        }),
        ..csi::v1::CreateVolumeRequest::default()
    }
}

/// CreateSnapshot answers not ready while Discovering and refuses a Lost
/// Snapshot without replacing it; CreateVolume waits on a discovery and
/// refuses a Lost image; a placed restore of one fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn csi_and_placement_wait_on_a_discovery_and_refuse_a_lost_image() {
    use csi::tonic::{Code, Request};
    use csi::v1::controller_server::Controller;
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    let mut cfg = support::config(&env, "ctl");
    let _ctl = support::spawn(env.clients().await, cfg.clone());
    let agents = cluster(&client).await;
    let auto: Vec<_> = agents.iter().cloned().map(FakeAgent::spawn_auto).collect();
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    cfg.ready_timeout = Duration::from_secs(3);
    let svc = controller::csi::Csi::new(client.clone(), stores, cfg);
    let snap = ready_snapshot(&client).await;
    let take = || csi::v1::CreateSnapshotRequest {
        name: "snap".into(),
        source_volume_id: "src".into(),
        ..csi::v1::CreateSnapshotRequest::default()
    };

    auto[2].abort();
    clear(&client, json!(null)).await;
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Discovering)
    })
    .await;
    let r = svc.create_snapshot(Request::new(take())).await.unwrap();
    assert!(!r.into_inner().snapshot.unwrap().ready_to_use);
    let e = svc
        .create_volume(Request::new(restore_request("waits")))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::DeadlineExceeded, "{e}");
    assert!(support::get_volume(&client, "waits").await.is_none());

    // n3 back: the copies found before make it Ready; with them gone, a
    // new discovery ends Lost.
    let _back = agents[2].clone().spawn_auto();
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Ready)
    })
    .await;
    for a in &agents {
        a.copies.lock().unwrap().clear();
    }
    clear(&client, json!({ "phase": null })).await;
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == Some(SnapshotPhase::Lost)
    })
    .await;
    let e = svc.create_snapshot(Request::new(take())).await.unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition, "{e}");
    assert!(e.message().contains("SnapshotLost"), "{e}");
    let now = get_snapshot(&client, "snap").await.unwrap();
    assert_eq!(now.metadata.uid, snap.metadata.uid, "never replaced");
    let e = svc
        .create_volume(Request::new(restore_request("refused")))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition, "{e}");
    assert!(support::get_volume(&client, "refused").await.is_none());

    let mut spec = volume_spec(GIB, 2, "fast");
    spec.source = Some(support::snapshot_source(&client, "snap").await);
    create_volume(&client, "placed", spec).await;
    let v = volume_where(&client, "placed", |v| phase_of(v) == VolumePhase::Failed).await;
    assert_eq!(support::ready_reason(&v).as_deref(), Some("SnapshotLost"));
}

/// Holder acks without the tag values were written before snapshot LV
/// tags: nothing proves those LVs are the snapshot's copies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_taken_before_tags_is_refused_as_an_image() {
    use csi::tonic::{Code, Request};
    use csi::v1::controller_server::Controller;
    let assets = require_envtest!();
    let (env, client, _ctl) = setup(&assets).await;
    let agents = cluster(&client).await;
    let auto: Vec<_> = agents.iter().cloned().map(FakeAgent::spawn_auto).collect();
    let snap = ready_snapshot(&client).await;
    for h in auto {
        h.abort();
    }
    for a in &snap.status.unwrap().acks {
        let untagged = model::SnapshotAck {
            tag_size_bytes: None,
            tag_source_replicas: None,
            ..a.clone()
        };
        let agent = agents.iter().find(|x| x.node == a.node).unwrap();
        agent.apply_ack("snap", &untagged).await;
    }
    snapshot_where(&client, "snap", |s| holders(s).is_empty()).await;
    let (stores, watchers) = controller::store::start(
        client.clone(),
        support::NAMESPACE,
        controller::store::LEASE_STALE,
    );
    let _watchers = tokio::spawn(watchers);
    stores.wait_ready().await.unwrap();
    let svc = controller::csi::Csi::new(client.clone(), stores, support::config(&env, "ctl"));
    let e = svc
        .create_volume(Request::new(restore_request("refused")))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition, "{e}");
    assert!(e.message().contains("before snapshot LV tags"), "{e}");
    assert!(support::get_volume(&client, "refused").await.is_none());

    let mut spec = volume_spec(GIB, 2, "fast");
    spec.source = Some(support::snapshot_source(&client, "snap").await);
    create_volume(&client, "placed", spec).await;
    let v = volume_where(&client, "placed", |v| phase_of(v) == VolumePhase::Failed).await;
    assert_eq!(
        support::ready_reason(&v).as_deref(),
        Some("UnsupportedRestore")
    );
}
