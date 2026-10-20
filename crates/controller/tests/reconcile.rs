//! Reconciler tests against envtest.

mod support;

use std::time::Duration;

use controller::volume::Outcome;
use kube::api::{DeleteParams, Patch, PatchParams};
use kube::{Api, Client};
use model::keys::{FINALIZER_POOL, MINOR_RANGE, PORT_RANGE};
use model::{Pool, Role, Snapshot, SnapshotPhase, Volume, VolumePhase, VolumeSource};
use serde_json::json;
use support::{
    Env, FakeAgent, GIB, Running, create_snapshot, create_volume, get_pool, get_snapshot,
    get_volume, node, phase_of, pool, ready_reason, snap_phase, snapshot_where, volume_spec,
    volume_where,
};

async fn setup(
    assets: &std::path::Path,
    cfg: impl FnOnce(&mut controller::Config),
) -> (Env, Client, Running) {
    support::init_tracing();
    let env = Env::start(assets).await;
    let client = env.client().await;
    let mut c = support::config(&env, "ctl");
    cfg(&mut c);
    let running = support::spawn(env.clients().await, c);
    (env, client, running)
}

async fn delete_volume(client: &Client, name: &str) {
    Api::<Volume>::all(client.clone())
        .delete(name, &DeleteParams::default())
        .await
        .expect("delete volume");
}

/// Assigned, then Ready once its only agent reports.
async fn ready_local(client: &Client, agent: &FakeAgent, name: &str, size: u64) -> Volume {
    create_volume(client, name, volume_spec(size, 1, "fast")).await;
    volume_where(client, name, |v| v.spec.assignment.is_some()).await;
    agent.ready(name).await;
    volume_where(client, name, |v| phase_of(v) == VolumePhase::Ready).await
}

/// Once a Volume this leader made Ready is quiet, passes send it nothing:
/// the remembered initializedAt equals the stored copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quiet_volume_gets_no_status_patch() {
    let assets = require_envtest!();
    let (env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "quiet", GIB).await;
    volume_where(&client, "quiet", |v| {
        v.status
            .as_ref()
            .is_some_and(|s| s.initialized_at.is_some())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let before = env.controller_status_patches("quiet");
    assert!(before > 0, "the audit log sees the controller's patches");
    // Each Node change wakes a pass.
    let nodes = Api::<k8s_openapi::api::core::v1::Node>::all(client.clone());
    for i in 0..5 {
        let label = json!({ "metadata": { "labels": { "wake": i.to_string() } } });
        nodes
            .patch("n1", &PatchParams::default(), &Patch::Merge(&label))
            .await
            .expect("label node");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(env.controller_status_patches("quiet"), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_finalizer_and_volumes() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let p = support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "p1").await.filter(|p| {
            p.metadata
                .finalizers
                .iter()
                .flatten()
                .any(|f| f == FINALIZER_POOL)
        })
    })
    .await;
    assert!(p.status.is_some());

    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "v1", GIB).await;
    // Pool total 200 GiB, free 100 GiB, default reserve 5 percent (10 GiB):
    // the free side binds, 100 - 10.
    support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "p1").await.filter(|p| {
            p.status.as_ref().is_some_and(|s| {
                s.volumes == ["v1"]
                    && s.committed_bytes == Some(GIB)
                    && s.placeable_bytes == Some(90 * GIB)
            })
        })
    })
    .await;

    // Deletion is held while the volume uses the pool.
    Api::<Pool>::all(client.clone())
        .delete("p1", &DeleteParams::default())
        .await
        .expect("delete pool");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let p = get_pool(&client, "p1")
        .await
        .expect("pool held by finalizer");
    assert!(p.metadata.deletion_timestamp.is_some());
    assert_eq!(p.status.unwrap().volumes, ["v1"]);

    delete_volume(&client, "v1").await;
    volume_where(&client, "v1", |v| phase_of(v) == VolumePhase::Deleting).await;
    agent.gone("v1").await;
    support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "p1").await.is_none().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placement_and_allocation() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    for n in ["n1", "n2", "n3"] {
        node(&client, n, true).await;
    }
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    pool(&client, "p2", "n2", "fast", 50 * GIB).await;
    // Pool of another tier on n3: n3 is still the preferred tiebreaker,
    // it holds no pool of this tier.
    pool(&client, "p3", "n3", "slow", 500 * GIB).await;

    create_volume(&client, "rep-a", volume_spec(GIB, 2, "fast")).await;
    let a = volume_where(&client, "rep-a", |v| v.spec.assignment.is_some()).await;
    let a = a.spec.assignment.unwrap();
    let got: Vec<_> = a
        .replicas
        .iter()
        .map(|r| (r.node.as_str(), r.pool.as_deref(), r.node_id, r.role))
        .collect();
    assert_eq!(
        got,
        [
            ("n1", Some("p1"), 0, Role::Diskful),
            ("n2", Some("p2"), 1, Role::Diskful),
            ("n3", None, 2, Role::Tiebreaker),
        ]
    );
    assert!(
        a.replicas
            .iter()
            .all(|r| r.port.is_some_and(|p| PORT_RANGE.contains(&p)))
    );
    assert!(a.minor.is_some_and(|m| MINOR_RANGE.contains(&m)));
    assert_eq!(a.secret.as_deref().map(str::len), Some(44));
    assert_eq!(a.initial_up_to_date_node.as_deref(), Some("n1"));

    create_volume(&client, "rep-b", volume_spec(GIB, 2, "fast")).await;
    let b = volume_where(&client, "rep-b", |v| v.spec.assignment.is_some())
        .await
        .spec
        .assignment
        .unwrap();
    assert_ne!(a.minor, b.minor);
    assert_ne!(a.secret, b.secret);
    for r in &b.replicas {
        let other = a.replicas.iter().find(|x| x.node == r.node).unwrap();
        assert_ne!(r.port, other.port, "port reused on {}", r.node);
    }

    // Local volume: plain LV, no DRBD fields; pins are honoured.
    let mut spec = volume_spec(GIB, 1, "fast");
    spec.node = Some("n2".into());
    create_volume(&client, "local-pinned", spec).await;
    let l = volume_where(&client, "local-pinned", |v| v.spec.assignment.is_some())
        .await
        .spec
        .assignment
        .unwrap();
    assert_eq!(l.replicas.len(), 1);
    assert_eq!(
        (l.replicas[0].node.as_str(), l.replicas[0].pool.as_deref()),
        ("n2", Some("p2"))
    );
    assert!(l.minor.is_none() && l.secret.is_none() && l.replicas[0].port.is_none());

    // Two volumes created at once still get distinct minors.
    create_volume(&client, "burst-1", volume_spec(GIB, 2, "fast")).await;
    create_volume(&client, "burst-2", volume_spec(GIB, 2, "fast")).await;
    let m1 = volume_where(&client, "burst-1", |v| v.spec.assignment.is_some()).await;
    let m2 = volume_where(&client, "burst-2", |v| v.spec.assignment.is_some()).await;
    let minors = [
        a.minor,
        b.minor,
        m1.spec.assignment.unwrap().minor,
        m2.spec.assignment.unwrap().minor,
    ];
    let mut uniq = minors.to_vec();
    uniq.sort();
    uniq.dedup();
    assert_eq!(uniq.len(), 4, "{minors:?}");

    // Pool status lists the volumes with a diskful replica there.
    support::eventually(Duration::from_secs(20), || async {
        let p = get_pool(&client, "p2").await?;
        (p.status?.volumes == ["burst-1", "burst-2", "local-pinned", "rep-a", "rep-b"])
            .then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insufficient_then_placed() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    create_volume(&client, "v-slow", volume_spec(GIB, 1, "slow")).await;
    let v = volume_where(&client, "v-slow", |v| {
        ready_reason(v).as_deref() == Some("Insufficient")
    })
    .await;
    assert_eq!(phase_of(&v), VolumePhase::Pending);
    let st = v.status.unwrap();
    let c = model::condition::get(&st.conditions, "Ready").unwrap();
    assert_eq!(
        (c.status.as_str(), c.reason.as_str()),
        ("False", "Insufficient")
    );
    assert!(c.message.contains("1 more replica"), "{}", c.message);
    assert!(c.message.contains("n1/p1 WrongTier"), "{}", c.message);

    create_volume(&client, "v-big", volume_spec(10_000 * GIB, 1, "fast")).await;
    let v = volume_where(&client, "v-big", |v| {
        ready_reason(v).as_deref() == Some("Insufficient")
    })
    .await;
    let st = v.status.unwrap();
    let c = model::condition::get(&st.conditions, "Ready").unwrap();
    assert!(c.message.contains("n1/p1 NoCapacity"), "{}", c.message);

    // A matching pool appears: placed without touching the volume.
    node(&client, "n2", true).await;
    pool(&client, "p2", "n2", "slow", 100 * GIB).await;
    let v = volume_where(&client, "v-slow", |v| v.spec.assignment.is_some()).await;
    assert_eq!(v.spec.assignment.unwrap().replicas[0].node, "n2");
    volume_where(&client, "v-slow", |v| {
        phase_of(v) == VolumePhase::Provisioning
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_name_fails() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    // A valid Kubernetes name, one byte past the LV name bound.
    let bad = "a".repeat(64);
    create_volume(&client, &bad, volume_spec(GIB, 1, "fast")).await;
    let v = volume_where(&client, &bad, |v| phase_of(v) == VolumePhase::Failed).await;
    assert_eq!(ready_reason(&v).as_deref(), Some("InvalidName"));
    assert!(v.spec.assignment.is_none());
    // Deletion is not blocked: there is nothing on any node.
    delete_volume(&client, &bad).await;
    support::eventually(Duration::from_secs(20), || async {
        get_volume(&client, &bad).await.is_none().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn volume_lifecycle() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");

    create_volume(&client, "life", volume_spec(GIB, 1, "fast")).await;
    let v = volume_where(&client, "life", |v| {
        phase_of(v) == VolumePhase::Provisioning
    })
    .await;
    assert_eq!(ready_reason(&v).as_deref(), Some("Waiting"));

    agent.ready("life").await;
    let v = volume_where(&client, "life", |v| phase_of(v) == VolumePhase::Ready).await;
    let st = v.status.clone().unwrap();
    assert_eq!(ready_reason(&v).as_deref(), Some("AllReplicasUpToDate"));
    assert_eq!(st.usable_bytes, Some(GIB));
    assert_eq!(st.nodes.as_deref(), Some("n1"));
    assert_eq!(st.observed_generation, v.metadata.generation);
    assert!(!model::condition::is_true(&st.conditions, "Degraded"));

    // The replica vanishes: Degraded, not back to Provisioning.
    agent.gone("life").await;
    let v = volume_where(&client, "life", |v| phase_of(v) == VolumePhase::Degraded).await;
    assert_eq!(ready_reason(&v).as_deref(), Some("ReplicaMissing"));
    assert!(model::condition::is_true(
        &v.status.unwrap().conditions,
        "Degraded"
    ));
    agent.ready("life").await;
    volume_where(&client, "life", |v| phase_of(v) == VolumePhase::Ready).await;

    // Resize: Resizing until the agent reports the new size.
    Api::<Volume>::all(client.clone())
        .patch(
            "life",
            &PatchParams::default(),
            &Patch::Merge(&json!({ "spec": { "sizeBytes": 2 * GIB } })),
        )
        .await
        .expect("resize");
    volume_where(&client, "life", |v| {
        ready_reason(v).as_deref() == Some("Resizing")
    })
    .await;
    agent.ready("life").await;
    let v = volume_where(&client, "life", |v| {
        ready_reason(v).as_deref() == Some("AllReplicasUpToDate")
    })
    .await;
    assert_eq!(v.status.unwrap().usable_bytes, Some(2 * GIB));

    // Primary follows the agent's inUse.
    let mut entry = get_volume(&client, "life")
        .await
        .unwrap()
        .status
        .unwrap()
        .replicas[0]
        .clone();
    entry.in_use = true;
    agent.report("life", entry).await;
    volume_where(&client, "life", |v| {
        v.status.as_ref().and_then(|s| s.primary.as_deref()) == Some("n1")
    })
    .await;

    // Delete waits for the agent to report the replica gone.
    delete_volume(&client, "life").await;
    volume_where(&client, "life", |v| phase_of(v) == VolumePhase::Deleting).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        get_volume(&client, "life").await.is_some(),
        "finalizer released early"
    );
    agent.gone("life").await;
    support::eventually(Duration::from_secs(20), || async {
        get_volume(&client, "life").await.is_none().then_some(())
    })
    .await;
    support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "p1")
            .await
            .filter(|p| p.status.as_ref().is_some_and(|s| s.volumes.is_empty()))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_blocked_by_stale_generation() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "old", GIB).await;
    // An entry written before the delete does not count, even if absent.
    let before = get_volume(&client, "old").await.unwrap();
    agent
        .report(
            "old",
            model::ReplicaStatus {
                node: "n1".into(),
                present: false,
                observed_generation: before.metadata.generation,
                ..Default::default()
            },
        )
        .await;
    delete_volume(&client, "old").await;
    let v = volume_where(&client, "old", |v| phase_of(v) == VolumePhase::Deleting).await;
    assert!(
        v.metadata.generation > before.metadata.generation,
        "deletion bumps generation"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(get_volume(&client, "old").await.is_some());
    agent.gone("old").await;
    support::eventually(Duration::from_secs(20), || async {
        get_volume(&client, "old").await.is_none().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn volume_lv_name_too_long_for_its_vg_fails_at_placement() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    let vg = "g".repeat(90);
    support::pool_with_vg(&client, "p1", "n1", "fast", 50 * GIB, &vg).await;
    // 63 bytes, 31 dashes: 94 device-mapper bytes, with the VG over 127.
    let vol = format!("v{}", "-v".repeat(31));
    create_volume(&client, &vol, volume_spec(GIB, 1, "fast")).await;
    let v = volume_where(&client, &vol, |v| phase_of(v) == VolumePhase::Failed).await;
    assert_eq!(ready_reason(&v).as_deref(), Some("InvalidName"));
    assert!(v.spec.assignment.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_lv_name_too_long_fails_before_any_round() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 50 * GIB).await;
    // Valid names, but every dash doubles in the device-mapper name.
    let vol = format!("v{}", "-v".repeat(31));
    let snap = format!("s{}", "-s".repeat(31));
    let mut spec = volume_spec(GIB, 1, "fast");
    spec.node = Some("n1".into());
    create_volume(&client, &vol, spec).await;
    volume_where(&client, &vol, |v| v.spec.assignment.is_some()).await;
    create_snapshot(&client, &snap, &vol).await;
    let s = snapshot_where(&client, &snap, |s| snap_phase(s) == SnapshotPhase::Failed).await;
    let e = s.status.unwrap().last_error.unwrap();
    assert!(e.starts_with("pool p1: LV name vg/"), "{e}");
    assert!(e.contains("device-mapper"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_local_and_restore() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    node(&client, "n2", true).await;
    pool(&client, "p1", "n1", "fast", 50 * GIB).await;
    pool(&client, "p2", "n2", "fast", 100 * GIB).await;
    let a1 = FakeAgent::new(&client, "n1");
    let mut spec = volume_spec(GIB, 1, "fast");
    spec.node = Some("n1".into());
    create_volume(&client, "src", spec).await;
    volume_where(&client, "src", |v| v.spec.assignment.is_some()).await;

    // Pending until the volume is Ready.
    create_snapshot(&client, "snap1", "src").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        snap_phase(&get_snapshot(&client, "snap1").await.unwrap()),
        SnapshotPhase::Pending
    );
    // The node plugin's marker after mkfs, under the shared fs manager.
    support::set_file_system(&client, "src", "xfs").await;
    a1.ready("src").await;
    let s = snapshot_where(&client, "snap1", |s| snap_phase(s) == SnapshotPhase::Taking).await;
    let st = s.status.unwrap();
    assert_eq!(st.size_bytes, Some(GIB));
    assert_eq!(st.file_system.as_deref(), Some("xfs"));
    a1.ack("snap1", SnapshotPhase::Taking, true, Some("p1"))
        .await;
    let s = snapshot_where(&client, "snap1", |s| snap_phase(s) == SnapshotPhase::Ready).await;
    assert!(s.status.unwrap().created_at.is_some());

    // Restore lands where the snapshot lives, not on the emptier n2, even
    // with p1 too full for a new volume: it holds the data already.
    support::pool_capacity(&client, "p1", "n1", 100 * GIB, 0).await;
    let mut spec = volume_spec(GIB, 1, "fast");
    spec.source = Some(VolumeSource {
        snapshot: "snap1".into(),
    });
    create_volume(&client, "restored", spec).await;
    let r = volume_where(&client, "restored", |v| v.spec.assignment.is_some()).await;
    let rep = &r.spec.assignment.unwrap().replicas[0];
    assert_eq!((rep.node.as_str(), rep.pool.as_deref()), ("n1", Some("p1")));
    // No seeded marker: the agent's adopt path gives the restored XFS a new
    // UUID and writes the marker itself.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let r = get_volume(&client, "restored").await.unwrap();
    assert_eq!(r.status.and_then(|s| s.file_system), None);
    support::set_file_system(&client, "restored", "xfs").await;
    let r = volume_where(&client, "restored", |v| {
        v.status.as_ref().and_then(|s| s.file_system.as_deref()) == Some("xfs")
    })
    .await;
    let owners: Vec<&str> = r
        .metadata
        .managed_fields
        .iter()
        .flatten()
        .filter(|m| {
            serde_json::to_string(&m.fields_v1)
                .unwrap()
                .contains("f:fileSystem")
        })
        .filter_map(|m| m.manager.as_deref())
        .collect();
    assert_eq!(owners, [model::keys::FS_FIELD_MANAGER]);

    let mut spec = volume_spec(GIB, 2, "fast");
    spec.source = Some(VolumeSource {
        snapshot: "snap1".into(),
    });
    create_volume(&client, "restored-rep", spec).await;
    let r = volume_where(&client, "restored-rep", |v| {
        phase_of(v) == VolumePhase::Failed
    })
    .await;
    assert_eq!(ready_reason(&r).as_deref(), Some("UnsupportedRestore"));

    // A restore that never made its first data would hold the Snapshot.
    a1.ready("restored").await;
    volume_where(&client, "restored", |v| {
        v.status
            .as_ref()
            .is_some_and(|s| s.initialized_at.is_some())
    })
    .await;

    // Delete waits for the holder to remove its LV.
    Api::<Snapshot>::all(client.clone())
        .delete("snap1", &DeleteParams::default())
        .await
        .expect("delete snapshot");
    snapshot_where(&client, "snap1", |s| {
        snap_phase(s) == SnapshotPhase::Deleting
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(get_snapshot(&client, "snap1").await.is_some());
    a1.ack("snap1", SnapshotPhase::Deleting, true, Some("p1"))
        .await;
    support::eventually(Duration::from_secs(20), || async {
        get_snapshot(&client, "snap1").await.is_none().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_replicated_rounds() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    for n in ["n1", "n2", "n3"] {
        node(&client, n, true).await;
    }
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    pool(&client, "p2", "n2", "fast", 100 * GIB).await;
    let agents: Vec<FakeAgent> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| FakeAgent::new(&client, n))
        .collect();
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "rep", |v| v.spec.assignment.is_some()).await;
    for a in &agents {
        a.ready("rep").await;
    }
    volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;

    create_snapshot(&client, "rs", "rep").await;
    snapshot_where(&client, "rs", |s| {
        snap_phase(s) == SnapshotPhase::Suspending
    })
    .await;
    agents[0]
        .ack("rs", SnapshotPhase::Suspending, true, None)
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        snap_phase(&get_snapshot(&client, "rs").await.unwrap()),
        SnapshotPhase::Suspending
    );
    agents[1]
        .ack("rs", SnapshotPhase::Suspending, true, None)
        .await;
    // The tiebreaker n3 suspends IO too and is waited for.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        snap_phase(&get_snapshot(&client, "rs").await.unwrap()),
        SnapshotPhase::Suspending
    );
    agents[2]
        .ack("rs", SnapshotPhase::Suspending, true, None)
        .await;
    snapshot_where(&client, "rs", |s| snap_phase(s) == SnapshotPhase::Taking).await;
    agents[0]
        .ack("rs", SnapshotPhase::Taking, true, Some("p1"))
        .await;
    agents[1]
        .ack("rs", SnapshotPhase::Taking, true, Some("p2"))
        .await;
    // Ready at once: every agent resumes on its own when the round ends.
    let s = snapshot_where(&client, "rs", |s| snap_phase(s) == SnapshotPhase::Ready).await;
    let st = s.status.unwrap();
    assert!(st.created_at.is_some());
    assert_eq!(st.size_bytes, Some(GIB));
    assert!(st.last_error.is_none());

    // A failed suspend fails the round.
    create_snapshot(&client, "rs-bad", "rep").await;
    snapshot_where(&client, "rs-bad", |s| {
        snap_phase(s) == SnapshotPhase::Suspending
    })
    .await;
    agents[0]
        .ack("rs-bad", SnapshotPhase::Suspending, true, None)
        .await;
    agents[1]
        .ack("rs-bad", SnapshotPhase::Suspending, false, None)
        .await;
    let s = snapshot_where(&client, "rs-bad", |s| {
        snap_phase(s) == SnapshotPhase::Failed
    })
    .await;
    assert!(
        s.status
            .unwrap()
            .last_error
            .unwrap()
            .contains("fake failure on n2")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_snapshots_of_one_volume_never_overlap() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    for n in ["n1", "n2", "n3"] {
        node(&client, n, true).await;
    }
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    pool(&client, "p2", "n2", "fast", 100 * GIB).await;
    let _agents = ["n1", "n2", "n3"].map(|n| FakeAgent::new(&client, n).spawn_auto());
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    let names = ["sa", "sb", "sc"];
    for n in names {
        create_snapshot(&client, n, "rep").await;
    }
    let api: Api<Snapshot> = Api::all(client.clone());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut held = false;
    loop {
        let all = api
            .list(&kube::api::ListParams::default())
            .await
            .expect("list");
        let running: Vec<String> = all
            .items
            .iter()
            .filter(|s| controller::volume::round_running(s))
            .map(kube::ResourceExt::name_any)
            .collect();
        assert!(running.len() <= 1, "rounds overlap: {running:?}");
        held |= all.items.iter().any(|s| {
            s.status
                .as_ref()
                .and_then(|st| st.last_error.as_deref())
                .is_some_and(|e| e.contains("of the same volume is in its round"))
        });
        if all.items.len() == names.len()
            && all
                .items
                .iter()
                .all(|s| snap_phase(s) == SnapshotPhase::Ready)
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "not all Ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(held, "no snapshot was ever held for another");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_ack_failure() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "vol", GIB).await;
    create_snapshot(&client, "snap", "vol").await;
    snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Taking).await;
    agent.ack("snap", SnapshotPhase::Taking, false, None).await;
    let s = snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Failed).await;
    assert!(
        s.status
            .unwrap()
            .last_error
            .unwrap()
            .contains("fake failure on n1")
    );
    // The round reached Taking: n1 may hold a partial LV and must ack the delete.
    Api::<Snapshot>::all(client.clone())
        .delete("snap", &DeleteParams::default())
        .await
        .expect("delete snapshot");
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == SnapshotPhase::Deleting
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(get_snapshot(&client, "snap").await.is_some());
    agent.ack("snap", SnapshotPhase::Deleting, true, None).await;
    support::eventually(Duration::from_secs(20), || async {
        get_snapshot(&client, "snap").await.is_none().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_take_timeout() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |c| c.take_timeout = Duration::from_secs(2)).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "vol", GIB).await;
    create_snapshot(&client, "slow", "vol").await;
    snapshot_where(&client, "slow", |s| snap_phase(s) == SnapshotPhase::Taking).await;
    let started = std::time::Instant::now();
    let s = snapshot_where(&client, "slow", |s| snap_phase(s) == SnapshotPhase::Failed).await;
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "timeout took {:?}",
        started.elapsed()
    );
    assert!(s.status.unwrap().last_error.unwrap().contains("within 2 s"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_waits_for_volume() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    create_snapshot(&client, "orphan", "nope").await;
    let s = snapshot_where(&client, "orphan", |s| {
        s.status.as_ref().is_some_and(|st| st.last_error.is_some())
    })
    .await;
    assert_eq!(snap_phase(&s), SnapshotPhase::Pending);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_write_conflict() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    // No pool of this tier: the controller leaves the volume unassigned.
    create_volume(&client, "racy", volume_spec(GIB, 1, "none")).await;
    let stale = volume_where(&client, "racy", |v| {
        ready_reason(v).as_deref() == Some("Insufficient")
    })
    .await;
    let api: Api<Volume> = Api::all(client.clone());
    api.patch(
        "racy",
        &PatchParams::default(),
        &Patch::Merge(&json!({ "spec": { "attachments": ["n1"] } })),
    )
    .await
    .expect("bump resourceVersion");
    let a = model::Assignment {
        replicas: vec![model::AssignedReplica {
            node: "n1".into(),
            pool: Some("p1".into()),
            node_id: 0,
            port: None,
            role: Role::Diskful,
        }],
        minor: None,
        secret: None,
        initial_up_to_date_node: None,
    };
    assert_eq!(
        controller::volume::write_assignment(&api, &stale, &a).await,
        Outcome::Rejected
    );
    assert!(
        get_volume(&client, "racy")
            .await
            .unwrap()
            .spec
            .assignment
            .is_none()
    );
    let fresh = get_volume(&client, "racy").await.unwrap();
    assert_eq!(
        controller::volume::write_assignment(&api, &fresh, &a).await,
        Outcome::Applied
    );
    assert_eq!(
        get_volume(&client, "racy").await.unwrap().spec.assignment,
        Some(a)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn take_bound_survives_restart() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    let mut cfg = support::config(&env, "ctl");
    cfg.take_timeout = Duration::from_secs(6);
    let first = support::spawn(env.clients().await, cfg.clone());
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "vol", GIB).await;
    create_snapshot(&client, "snap", "vol").await;
    let s = snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Taking).await;
    assert!(s.status.unwrap().phase_since.is_some());
    drop(first);
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(
        snap_phase(&get_snapshot(&client, "snap").await.unwrap()),
        SnapshotPhase::Taking
    );
    // Same identity: the lease is ours at once. The bound already passed,
    // so the new process fails the round without waiting another 6 s.
    let started = std::time::Instant::now();
    let _second = support::spawn(env.clients().await, cfg);
    snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Failed).await;
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_deleted_mid_taking() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "vol", GIB).await;
    create_snapshot(&client, "snap", "vol").await;
    snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Taking).await;
    Api::<Snapshot>::all(client.clone())
        .delete("snap", &DeleteParams::default())
        .await
        .expect("delete snapshot");
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == SnapshotPhase::Deleting
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        get_snapshot(&client, "snap").await.is_some(),
        "released without n1"
    );
    agent.ack("snap", SnapshotPhase::Deleting, true, None).await;
    support::eventually(Duration::from_secs(20), || async {
        get_snapshot(&client, "snap").await.is_none().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_held_by_snapshot() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "vol", GIB).await;
    create_snapshot(&client, "snap", "vol").await;
    snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Taking).await;
    agent
        .ack("snap", SnapshotPhase::Taking, true, Some("p1"))
        .await;
    snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Ready).await;
    delete_volume(&client, "vol").await;
    volume_where(&client, "vol", |v| phase_of(v) == VolumePhase::Deleting).await;
    agent.gone("vol").await;
    support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "p1")
            .await
            .filter(|p| p.status.as_ref().is_some_and(|s| s.volumes.is_empty()))
    })
    .await;
    Api::<Pool>::all(client.clone())
        .delete("p1", &DeleteParams::default())
        .await
        .expect("delete pool");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        get_pool(&client, "p1").await.is_some(),
        "pool released under a snapshot"
    );
    Api::<Snapshot>::all(client.clone())
        .delete("snap", &DeleteParams::default())
        .await
        .expect("delete snapshot");
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == SnapshotPhase::Deleting
    })
    .await;
    agent
        .ack("snap", SnapshotPhase::Deleting, true, Some("p1"))
        .await;
    support::eventually(Duration::from_secs(20), || async {
        get_snapshot(&client, "snap").await.is_none().then_some(())
    })
    .await;
    support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "p1").await.is_none().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_not_ready_degrades() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    let agent = FakeAgent::new(&client, "n1");
    ready_local(&client, &agent, "vol", GIB).await;
    support::set_node_ready(&client, "n1", false).await;
    let v = volume_where(&client, "vol", |v| phase_of(v) == VolumePhase::Degraded).await;
    let st = v.status.unwrap();
    let c = model::condition::get(&st.conditions, "Degraded").unwrap();
    assert_eq!(c.status, "True");
    assert!(c.message.contains("node not ready: n1"), "{}", c.message);
    support::set_node_ready(&client, "n1", true).await;
    volume_where(&client, "vol", |v| phase_of(v) == VolumePhase::Ready).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_owns_no_agent_fields() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    for n in ["n1", "n2", "n3"] {
        node(&client, n, true).await;
    }
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    pool(&client, "p2", "n2", "fast", 100 * GIB).await;
    let agents: Vec<FakeAgent> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| FakeAgent::new(&client, n))
        .collect();
    create_volume(&client, "rep", volume_spec(GIB, 2, "fast")).await;
    volume_where(&client, "rep", |v| v.spec.assignment.is_some()).await;
    for a in &agents {
        a.ready("rep").await;
    }
    volume_where(&client, "rep", |v| phase_of(v) == VolumePhase::Ready).await;
    create_snapshot(&client, "snap", "rep").await;
    snapshot_where(&client, "snap", |s| {
        snap_phase(s) == SnapshotPhase::Suspending
    })
    .await;
    // The tiebreaker suspends too.
    for a in &agents {
        a.ack("snap", SnapshotPhase::Suspending, true, None).await;
    }
    snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Taking).await;
    for (a, p) in agents.iter().zip(["p1", "p2"]) {
        a.ack("snap", SnapshotPhase::Taking, true, Some(p)).await;
    }
    snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Ready).await;
    support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "p1")
            .await
            .filter(|p| p.status.as_ref().is_some_and(|s| s.volumes == ["rep"]))
    })
    .await;
    let v = get_volume(&client, "rep").await.unwrap();
    let s = get_snapshot(&client, "snap").await.unwrap();
    let p = get_pool(&client, "p1").await.unwrap();
    assert_eq!(support::assert_controller_status_fields(&v.metadata), 1);
    assert_eq!(support::assert_controller_status_fields(&s.metadata), 1);
    assert_eq!(support::assert_controller_status_fields(&p.metadata), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn taking_without_start_gets_one() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    node(&client, "n1", true).await;
    pool(&client, "p1", "n1", "fast", 100 * GIB).await;
    // State as an older controller left it: Taking, no takingSince.
    create_volume(&client, "vol", volume_spec(GIB, 1, "fast")).await;
    let vols: Api<Volume> = Api::all(client.clone());
    let v = get_volume(&client, "vol").await.unwrap();
    let a = model::Assignment {
        replicas: vec![model::AssignedReplica {
            node: "n1".into(),
            pool: Some("p1".into()),
            node_id: 0,
            port: None,
            role: Role::Diskful,
        }],
        minor: None,
        secret: None,
        initial_up_to_date_node: None,
    };
    assert_eq!(
        controller::volume::write_assignment(&vols, &v, &a).await,
        Outcome::Applied
    );
    create_snapshot(&client, "snap", "vol").await;
    let status = json!({
        "apiVersion": "r-drbd.io/v1alpha1",
        "kind": "Snapshot",
        "metadata": { "name": "snap" },
        "status": { "phase": "Taking", "sizeBytes": GIB },
    });
    Api::<Snapshot>::all(client.clone())
        .patch_status(
            "snap",
            &PatchParams::apply(model::keys::CONTROLLER_FIELD_MANAGER).force(),
            &Patch::Apply(&status),
        )
        .await
        .expect("old status");
    let mut cfg = support::config(&env, "ctl");
    cfg.take_timeout = Duration::from_secs(3);
    let started = std::time::Instant::now();
    let _ctl = support::spawn(env.clients().await, cfg);
    snapshot_where(&client, "snap", |s| {
        s.status.as_ref().is_some_and(|st| st.phase_since.is_some())
    })
    .await;
    assert_eq!(
        snap_phase(&get_snapshot(&client, "snap").await.unwrap()),
        SnapshotPhase::Taking
    );
    let s = snapshot_where(&client, "snap", |s| snap_phase(s) == SnapshotPhase::Failed).await;
    // Bounded from when the controller started the clock, not instantly.
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert!(s.status.unwrap().last_error.unwrap().contains("within 3 s"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_policy_defaults_from_the_api_server() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    let ar = kube::api::ApiResource::erase::<Pool>(&());
    let api: Api<kube::api::DynamicObject> = Api::all_with(client.clone(), &ar);
    let obj: kube::api::DynamicObject = serde_json::from_value(json!({
        "apiVersion": "r-drbd.io/v1alpha1",
        "kind": "Pool",
        "metadata": { "name": "bare" },
        "spec": { "node": "n1", "tier": "fast", "vg": "vg", "thinPool": "thin" },
    }))
    .unwrap();
    let got = api
        .create(&kube::api::PostParams::default(), &obj)
        .await
        .expect("create pool");
    assert_eq!(got.data["spec"]["overcommitPercent"], 100);
    assert_eq!(got.data["spec"]["reservePercent"], 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_delete_policy_waits_for_released() {
    let assets = require_envtest!();
    let (_env, client, _ctl) = setup(&assets, |_| {}).await;
    let ar = kube::api::ApiResource::erase::<Pool>(&());
    let dynamic: Api<kube::api::DynamicObject> = Api::all_with(client.clone(), &ar);
    for name in ["del", "ret"] {
        let obj: kube::api::DynamicObject = serde_json::from_value(json!({
            "apiVersion": "r-drbd.io/v1alpha1",
            "kind": "Pool",
            "metadata": { "name": name },
            "spec": {
                "node": "n1", "tier": "fast", "vg": name, "thinPool": "thin",
                "devices": ["/dev/disk/by-id/virtio-a"], "reclaimPolicy": "Delete",
            },
        }))
        .unwrap();
        dynamic
            .create(&kube::api::PostParams::default(), &obj)
            .await
            .expect("create pool");
    }
    let api = Api::<Pool>::all(client.clone());
    for name in ["del", "ret"] {
        support::eventually(Duration::from_secs(20), || async {
            get_pool(&client, name).await.filter(|p| {
                p.metadata
                    .finalizers
                    .iter()
                    .flatten()
                    .any(|f| f == FINALIZER_POOL)
            })
        })
        .await;
        api.delete(name, &DeleteParams::default())
            .await
            .expect("delete pool");
    }
    // Empty, but held until the agent says Released.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(get_pool(&client, "del").await.is_some());
    assert!(get_pool(&client, "ret").await.is_some());

    // Switching to Retain lets it go as today.
    api.patch(
        "ret",
        &PatchParams::default(),
        &Patch::Merge(json!({ "spec": { "reclaimPolicy": "Retain" } })),
    )
    .await
    .expect("retain");
    support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "ret").await.is_none().then_some(())
    })
    .await;

    // Released False changes nothing; True releases the finalizer.
    let released = |status: &str| {
        json!({
            "apiVersion": "r-drbd.io/v1alpha1",
            "kind": "Pool",
            "metadata": { "name": "del" },
            "status": { "conditions": [{
                "type": "Released", "status": status, "reason": "Test", "message": "",
                "lastTransitionTime": "2026-01-01T00:00:00Z",
            }]},
        })
    };
    let ssa = PatchParams::apply(&model::keys::agent_field_manager("n1")).force();
    api.patch_status("del", &ssa, &Patch::Apply(released("False")))
        .await
        .expect("released false");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(get_pool(&client, "del").await.is_some());
    api.patch_status("del", &ssa, &Patch::Apply(released("True")))
        .await
        .expect("released true");
    support::eventually(Duration::from_secs(20), || async {
        get_pool(&client, "del").await.is_none().then_some(())
    })
    .await;
}
