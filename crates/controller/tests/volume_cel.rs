//! Volume and Snapshot spec CEL rules against a real API server.

mod support;

use kube::Api;
use kube::api::{DynamicObject, Patch, PatchParams, PostParams};
use model::{Snapshot, Volume};
use serde_json::{Value, json};
use support::Env;

fn api<K: kube::Resource<DynamicType = ()>>(client: &kube::Client) -> Api<DynamicObject> {
    let ar = kube::api::ApiResource::erase::<K>(&());
    Api::all_with(client.clone(), &ar)
}

async fn create(api: &Api<DynamicObject>, kind: &str, name: &str, spec: Value) {
    let obj: DynamicObject = serde_json::from_value(json!({
        "apiVersion": "r-drbd.io/v1alpha1",
        "kind": kind,
        "metadata": { "name": name },
        "spec": spec,
    }))
    .unwrap();
    api.create(&PostParams::default(), &obj)
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// The API server's refusal text, or None when it took the patch.
async fn patch(api: &Api<DynamicObject>, name: &str, spec: Value) -> Option<String> {
    api.patch(
        name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "spec": spec })),
    )
    .await
    .err()
    .map(|e| e.to_string())
}

fn replica(node: &str, pool: Option<&str>, id: u8, port: Option<u16>, role: &str) -> Value {
    let mut r = json!({ "node": node, "nodeId": id, "role": role });
    if let Some(p) = pool {
        r["pool"] = json!(p);
    }
    if let Some(p) = port {
        r["port"] = json!(p);
    }
    r
}

/// n1 and n2 diskful, n3 the tiebreaker.
fn replicas() -> Vec<Value> {
    vec![
        replica("n1", Some("p1"), 0, Some(7000), "diskful"),
        replica("n2", Some("p2"), 1, Some(7000), "diskful"),
        replica("n3", None, 2, Some(7000), "tiebreaker"),
    ]
}

fn volume_spec() -> Value {
    json!({
        "sizeBytes": 1_073_741_824u64,
        "replicas": 2,
        "tier": "fast",
        "source": { "snapshot": "s", "snapshotUid": "u1" },
        "assignment": {
            "replicas": replicas(),
            "minor": 1000,
            "secret": "c2VjcmV0",
            "initialUpToDateNode": "n1",
        },
    })
}

/// A merge patch replaces a list whole: the base replicas with `edit` applied.
fn edited(edit: impl FnOnce(&mut Vec<Value>)) -> Value {
    let mut r = replicas();
    edit(&mut r);
    json!({ "assignment": { "replicas": r } })
}

fn holes() -> Vec<(&'static str, Value, &'static str)> {
    const MINOR: &str = "minor is immutable";
    const SOURCE: &str = "source is immutable";
    const DISKFUL: &str = "a diskful replica keeps its role and pool";
    let unset = |r: &mut Vec<Value>, i: usize, f: &str| {
        r[i].as_object_mut().unwrap().remove(f);
    };
    vec![
        (
            "minor removed",
            json!({ "assignment": { "minor": null } }),
            MINOR,
        ),
        (
            "minor changed",
            json!({ "assignment": { "minor": 1001 } }),
            MINOR,
        ),
        (
            "secret removed",
            json!({ "assignment": { "secret": null } }),
            "secret is immutable",
        ),
        (
            "initial node removed",
            json!({ "assignment": { "initialUpToDateNode": null } }),
            "initialUpToDateNode is immutable",
        ),
        ("source removed", json!({ "source": null }), SOURCE),
        (
            "snapshot uid removed",
            json!({ "source": { "snapshotUid": null } }),
            SOURCE,
        ),
        (
            "snapshot uid changed",
            json!({ "source": { "snapshotUid": "u2" } }),
            SOURCE,
        ),
        (
            "assignment removed",
            json!({ "assignment": null }),
            "assignment cannot be removed",
        ),
        (
            "nodeId changed",
            edited(|r| r[1]["nodeId"] = json!(5)),
            "nodeId is immutable",
        ),
        (
            "nodeId duplicated",
            edited(|r| r.push(replica("n4", None, 0, Some(7001), "diskless"))),
            "nodeId must be unique",
        ),
        (
            "port removed",
            edited(|r| unset(r, 0, "port")),
            "port is immutable",
        ),
        (
            "port changed",
            edited(|r| r[2]["port"] = json!(7005)),
            "port is immutable",
        ),
        (
            "diskful pool changed",
            edited(|r| r[0]["pool"] = json!("p9")),
            DISKFUL,
        ),
        (
            "diskful pool removed",
            edited(|r| unset(r, 0, "pool")),
            DISKFUL,
        ),
        (
            "diskful made diskless",
            edited(|r| r[1]["role"] = json!("diskless")),
            DISKFUL,
        ),
        (
            "diskful removed without evict",
            edited(|r| {
                r.remove(1);
            }),
            "a diskful replica leaves the assignment only through evict",
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn volume_spec_holes_are_closed() {
    let assets = require_envtest!();
    let env = Env::start(&assets).await;
    let api = api::<Volume>(&env.client().await);
    create(&api, "Volume", "v", volume_spec()).await;
    for (what, spec, message) in holes() {
        let e = patch(&api, "v", spec)
            .await
            .unwrap_or_else(|| panic!("{what}: accepted"));
        assert!(e.contains(message), "{what}: {e}");
    }

    // A Volume created without a source never gets one.
    let mut plain = volume_spec();
    plain.as_object_mut().unwrap().remove("source");
    create(&api, "Volume", "plain", plain).await;
    let source = json!({ "source": { "snapshot": "s", "snapshotUid": "u1" } });
    let e = patch(&api, "plain", source).await.unwrap();
    assert!(e.contains("source is immutable"), "{e}");

    // A source written before uids were recorded never gets one, and the
    // object still takes other writes.
    let mut legacy = volume_spec();
    legacy["source"] = json!({ "snapshot": "s" });
    create(&api, "Volume", "legacy", legacy).await;
    let e = patch(&api, "legacy", json!({ "source": { "snapshotUid": "u1" } }))
        .await
        .unwrap();
    assert!(e.contains("source is immutable"), "{e}");
    let attach = json!({ "attachments": ["n1"] });
    assert_eq!(patch(&api, "legacy", attach).await, None);

    // The evict list is bounded: the assignment rule walks it.
    let many: Vec<String> = (0..33).map(|i| format!("n{i}")).collect();
    let e = patch(&api, "v", json!({ "evict": many })).await.unwrap();
    assert!(e.contains("must have at most 32 items"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn volume_spec_takes_the_controller_moves() {
    let assets = require_envtest!();
    let env = Env::start(&assets).await;
    let api = api::<Volume>(&env.client().await);
    create(&api, "Volume", "v", volume_spec()).await;
    let ok = |what: &str, e: Option<String>| assert_eq!(e, None, "{what}");

    let diskless = edited(|r| r.push(replica("n4", None, 3, Some(7001), "diskless")));
    ok("diskless added", patch(&api, "v", diskless).await);
    ok("diskless removed", patch(&api, "v", edited(|_| {})).await);
    // Evict n2: the tiebreaker n3 becomes diskful with a pool, keeping its
    // id and port; a new tiebreaker joins.
    ok(
        "evict set",
        patch(&api, "v", json!({ "evict": ["n2"] })).await,
    );
    let moved = edited(|r| {
        r.remove(1);
        r[1]["role"] = json!("diskful");
        r[1]["pool"] = json!("p3");
        r.push(replica("n5", None, 3, Some(7000), "tiebreaker"));
    });
    ok("evict move", patch(&api, "v", moved).await);
    ok(
        "evict cleared",
        patch(&api, "v", json!({ "evict": [] })).await,
    );
    // An evict move to a node without an entry adds a diskful one.
    ok(
        "evict set",
        patch(&api, "v", json!({ "evict": ["n1"] })).await,
    );
    let added = json!({ "assignment": { "replicas": [
        replica("n3", Some("p3"), 2, Some(7000), "diskful"),
        replica("n5", None, 3, Some(7000), "tiebreaker"),
        replica("n6", Some("p6"), 4, Some(7000), "diskful"),
    ] } });
    ok("diskful added", patch(&api, "v", added).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_spec_rules() {
    let assets = require_envtest!();
    let env = Env::start(&assets).await;
    let api = api::<Snapshot>(&env.client().await);
    let bound = json!({ "volume": "v", "volumeUid": "u1" });
    create(&api, "Snapshot", "s", bound).await;
    create(&api, "Snapshot", "legacy", json!({ "volume": "v" })).await;
    for (name, spec) in [
        ("s", json!({ "volumeUid": null })),
        ("s", json!({ "volumeUid": "u2" })),
        ("legacy", json!({ "volumeUid": "u1" })),
    ] {
        let e = patch(&api, name, spec.clone())
            .await
            .unwrap_or_else(|| panic!("{name} {spec}: accepted"));
        assert!(e.contains("volumeUid is immutable"), "{name} {spec}: {e}");
    }
    let e = patch(&api, "s", json!({ "volume": "w" })).await.unwrap();
    assert!(e.contains("volume is immutable"), "{e}");
    // An unchanged spec passes, legacy objects included.
    for name in ["s", "legacy"] {
        let labels = json!({ "metadata": { "labels": { "x": "y" } } });
        api.patch(name, &PatchParams::default(), &Patch::Merge(labels))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}
