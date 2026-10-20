//! Lease leader election against envtest.

mod support;

use std::path::Path;
use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff::{SignedDuration, Timestamp};
use kube::api::{ObjectMeta, PostParams};
use kube::{Api, Client};
use model::keys::CONTROLLER_LEASE_NAME;
use support::{Env, NAMESPACE};

async fn holder(client: &Client) -> Option<String> {
    Api::<Lease>::namespaced(client.clone(), NAMESPACE)
        .get_opt(CONTROLLER_LEASE_NAME)
        .await
        .expect("get lease")?
        .spec?
        .holder_identity
}

fn socket(cfg: &controller::Config) -> &Path {
    Path::new(cfg.csi_endpoint.trim_start_matches("unix://"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waits_for_an_expired_lease() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    // Another controller renewed 30 s ago by its clock; the lease lasts 40 s
    // counted from our own first sight of it, not from that renewTime.
    let renewed = Timestamp::now() - SignedDuration::from_secs(30);
    Api::<Lease>::namespaced(client.clone(), NAMESPACE)
        .create(
            &PostParams::default(),
            &Lease {
                metadata: ObjectMeta {
                    name: Some(CONTROLLER_LEASE_NAME.into()),
                    ..ObjectMeta::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: Some("ghost".into()),
                    lease_duration_seconds: Some(40),
                    acquire_time: Some(MicroTime(renewed)),
                    renew_time: Some(MicroTime(renewed)),
                    ..LeaseSpec::default()
                }),
            },
        )
        .await
        .expect("create lease");
    let cfg = support::config(&env, "second");
    let sock = socket(&cfg).to_owned();
    let _b = support::spawn(env.clients().await, cfg);
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert_eq!(holder(&client).await.as_deref(), Some("ghost"));
    assert!(!sock.exists(), "CSI started before leadership");
    support::eventually(Duration::from_secs(45), || async {
        (holder(&client).await.as_deref() == Some("second")).then_some(())
    })
    .await;
    support::eventually(Duration::from_secs(10), || async {
        sock.exists().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_controller_stays_standby() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    let a = support::spawn(env.clients().await, support::config(&env, "first"));
    support::eventually(Duration::from_secs(20), || async {
        (holder(&client).await.as_deref() == Some("first")).then_some(())
    })
    .await;
    let cfg = support::config(&env, "second");
    let sock = socket(&cfg).to_owned();
    let _b = support::spawn(env.clients().await, cfg);
    // Longer than a renew period: the first keeps the lease.
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert_eq!(holder(&client).await.as_deref(), Some("first"));
    assert!(!sock.exists(), "standby started its CSI endpoint");
    // The first dies without releasing; the second takes over once it expires.
    drop(a);
    support::eventually(Duration::from_secs(60), || async {
        (holder(&client).await.as_deref() == Some("second")).then_some(())
    })
    .await;
    support::eventually(Duration::from_secs(10), || async {
        sock.exists().then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_on_shutdown() {
    let assets = require_envtest!();
    support::init_tracing();
    let env = Env::start(&assets).await;
    let client = env.client().await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let a = tokio::spawn(controller::run_until(
        support::config(&env, "first"),
        env.clients().await,
        async move {
            let _ = stopped.await;
        },
    ));
    support::eventually(Duration::from_secs(20), || async {
        (holder(&client).await.as_deref() == Some("first")).then_some(())
    })
    .await;
    let _b = support::spawn(env.clients().await, support::config(&env, "second"));
    stop.send(()).expect("signal shutdown");
    let result = tokio::time::timeout(Duration::from_secs(10), a)
        .await
        .expect("shutdown in time")
        .expect("join");
    assert!(result.is_ok(), "{result:?}");
    // A released Lease is free at once: the standby's next poll takes it,
    // far sooner than the 40 s expiry.
    support::eventually(Duration::from_secs(15), || async {
        (holder(&client).await.as_deref() == Some("second")).then_some(())
    })
    .await;
}
