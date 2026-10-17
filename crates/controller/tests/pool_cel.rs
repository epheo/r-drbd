//! Pool spec CEL rules against a real API server.

mod support;

use kube::Api;
use kube::api::{DynamicObject, Patch, PatchParams, PostParams};
use model::Pool;
use serde_json::{Value, json};
use support::Env;

fn api(client: &kube::Client) -> Api<DynamicObject> {
    let ar = kube::api::ApiResource::erase::<Pool>(&());
    Api::all_with(client.clone(), &ar)
}

fn pool(name: &str, extra: &Value) -> DynamicObject {
    let mut spec = json!({ "node": "n1", "tier": "fast", "vg": "vg", "thinPool": "thin" });
    for (k, v) in extra.as_object().unwrap() {
        spec[k] = v.clone();
    }
    serde_json::from_value(json!({
        "apiVersion": "r-drbd.io/v1alpha1",
        "kind": "Pool",
        "metadata": { "name": name },
        "spec": spec,
    }))
    .unwrap()
}

/// The API server's refusal text, or None when it took the object.
async fn create(api: &Api<DynamicObject>, name: &str, extra: Value) -> Option<String> {
    api.create(&PostParams::default(), &pool(name, &extra))
        .await
        .err()
        .map(|e| e.to_string())
}

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

const A: &str = "/dev/disk/by-id/virtio-a";
const B: &str = "/dev/disk/by-id/virtio-b";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_devices_rules() {
    let assets = require_envtest!();
    let env = Env::start(&assets).await;
    let api = api(&env.client().await);

    // Creation.
    assert_eq!(create(&api, "vg-mode", json!({})).await, None);
    assert_eq!(
        create(&api, "dev", json!({ "devices": [A], "chunkSize": "64Ki" })).await,
        None
    );
    let e = create(&api, "bare-chunk", json!({ "chunkSize": "64Ki" }))
        .await
        .unwrap();
    assert!(e.contains("chunkSize needs devices"), "{e}");
    let e = create(&api, "bare-policy", json!({ "reclaimPolicy": "Delete" }))
        .await
        .unwrap();
    assert!(e.contains("reclaimPolicy needs devices"), "{e}");
    for bad in ["/dev/sdb", "/dev/disk/by-id/a/b", "/dev/disk/by-path/x"] {
        let e = create(&api, "bad-path", json!({ "devices": [bad] }))
            .await
            .unwrap();
        assert!(e.contains("should match"), "{bad}: {e}");
    }
    let e = create(&api, "dup", json!({ "devices": [A, A] }))
        .await
        .unwrap();
    assert!(e.contains("Duplicate value"), "{e}");
    for (chunk, ok) in [
        (json!("64Ki"), true),
        (json!("1Gi"), true),
        (json!(131_072), true),
        (json!("1Mi"), true),
        (json!("32Ki"), false),
        (json!("2Gi"), false),
        (json!("96Ki"), false),
        (json!("100k"), false),
        (json!("junk"), false),
    ] {
        let r = create(&api, "chunk", json!({ "devices": [A], "chunkSize": chunk })).await;
        assert_eq!(r.is_none(), ok, "{chunk}: {r:?}");
        if ok {
            api.delete("chunk", &kube::api::DeleteParams::default())
                .await
                .unwrap();
        }
    }

    // Updates.
    assert_eq!(patch(&api, "dev", json!({ "devices": [A, B] })).await, None);
    let e = patch(&api, "dev", json!({ "devices": [B] })).await.unwrap();
    assert!(e.contains("devices is append only"), "{e}");
    let e = patch(&api, "dev", json!({ "devices": null }))
        .await
        .unwrap();
    assert!(e.contains("devices mode is fixed at creation"), "{e}");
    let e = patch(&api, "vg-mode", json!({ "devices": [A] }))
        .await
        .unwrap();
    assert!(e.contains("devices mode is fixed at creation"), "{e}");
    let e = patch(&api, "dev", json!({ "chunkSize": "128Ki" }))
        .await
        .unwrap();
    assert!(e.contains("chunkSize is immutable"), "{e}");
    let e = patch(&api, "dev", json!({ "chunkSize": null }))
        .await
        .unwrap();
    assert!(e.contains("chunkSize is immutable"), "{e}");
    assert_eq!(
        patch(&api, "dev", json!({ "reclaimPolicy": "Delete" })).await,
        None
    );
    assert_eq!(
        patch(&api, "dev", json!({ "reclaimPolicy": "Retain" })).await,
        None
    );
    // Absent stays absent: no schema default.
    let got = api.get("vg-mode").await.unwrap();
    for f in ["devices", "chunkSize", "reclaimPolicy"] {
        assert!(got.data["spec"].get(f).is_none(), "{f}");
    }
}
