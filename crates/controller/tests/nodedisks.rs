//! The agent's NodeDisks sink against a real API server: created owned
//! by the Node, read once, then patched.

mod support;

use agent::inventory::{KubeSink, Sink};
use k8s_openapi::api::core::v1::Node;
use kube::{Api, ResourceExt};
use model::{DeviceState, DiskStatus, DiskWatch, NodeDisks};
use support::Env;

fn status(rows: Vec<DiskStatus>) -> model::NodeDisksStatus {
    agent::inventory::status(rows, &std::collections::BTreeSet::new(), DiskWatch::Uevent)
}

fn row(name: &str, state: DeviceState, pool: Option<&str>) -> DiskStatus {
    DiskStatus {
        path: format!("/dev/disk/by-id/virtio-{name}"),
        name: name.into(),
        links: vec![format!("/dev/disk/by-id/virtio-{name}")],
        state,
        pool: pool.map(str::to_owned),
        size_bytes: Some(4 << 30),
        model: None,
        serial: Some(name.into()),
        message: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nodedisks_created_then_patched() {
    let assets = require_envtest!();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    support::node(&client, "n1", true).await;
    let uid = Api::<Node>::all(client.clone())
        .get("n1")
        .await
        .unwrap()
        .metadata
        .uid
        .unwrap();
    let api: Api<NodeDisks> = Api::all(client.clone());

    let first = status(vec![
        row("a", DeviceState::Blank, None),
        row("b", DeviceState::Blank, None),
    ]);
    let sink = KubeSink::new(client.clone(), "n1".into());
    assert_eq!(sink.write(&first).await, Ok(true));
    let o = api.get("n1").await.unwrap();
    let owner = &o.owner_references()[0];
    assert_eq!(
        (owner.kind.as_str(), owner.uid.as_str()),
        ("Node", uid.as_str())
    );
    assert_eq!(o.status.as_ref(), Some(&first));
    assert_eq!(o.status.as_ref().unwrap().blank, 2);

    // A fresh sink (an agent restart) finds the object as written: no patch.
    let rv = o.resource_version();
    let again = KubeSink::new(client.clone(), "n1".into());
    assert_eq!(again.write(&first).await, Ok(false));
    assert_eq!(api.get("n1").await.unwrap().resource_version(), rv);

    // A disk joins a Pool and another is unplugged: the row goes.
    let next = status(vec![row("a", DeviceState::Member, Some("p1"))]);
    assert_eq!(sink.write(&next).await, Ok(true));
    let s = api.get("n1").await.unwrap().status.unwrap();
    assert_eq!(s, next);
    assert_eq!(s.blank, 0);

    // A node without its Node object gets no NodeDisks.
    let ghost = KubeSink::new(client.clone(), "ghost".into());
    let none = ghost.write(&first).await;
    assert!(none.unwrap_err().contains("read node ghost"));
}
