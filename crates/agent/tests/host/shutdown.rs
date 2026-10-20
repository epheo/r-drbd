//! The shutdown guard's host probes on a running host: read-only queries,
//! nothing is demoted or signalled. With `R_DRBD_HOST_NS` set they run
//! through host-exec, as the agent does in its pod.

use std::sync::Arc;

use agent::node;
use agent::shutdown::{self, Answer, PodProbe, Verdict};
use k8s_openapi::api::core::v1::Pod;

use super::enabled;

fn exec() -> Arc<dyn agent::Exec> {
    Arc::new(super::exec())
}

struct NoApi;

#[async_trait::async_trait]
impl PodProbe for NoApi {
    async fn get(&self) -> Result<Pod, String> {
        panic!("a systemd host answers locally");
    }
}

#[tokio::test]
#[ignore = "needs a running systemd host"]
async fn local_probes_say_running() {
    if !enabled() {
        return;
    }
    let x = exec();
    let t0 = std::time::Instant::now();
    assert_eq!(shutdown::probe_preparing(x.as_ref()).await, Answer::No);
    assert_eq!(shutdown::probe_system_state(x.as_ref()).await, Answer::No);
    assert_eq!(shutdown::detect(x.as_ref(), &NoApi).await, Verdict::PodOnly);
    let took = t0.elapsed();
    println!("three probes in {took:?}");
    assert!(took < std::time::Duration::from_secs(3), "{took:?}");
}

#[tokio::test]
#[ignore = "needs a running systemd host"]
async fn inhibitor_list_parses() {
    if !enabled() {
        return;
    }
    let x = exec();
    let out = x
        .run(&shutdown::inhibitors_argv(), node::CHECK_TIMEOUT)
        .await
        .unwrap();
    println!("{}", out.stdout);
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    let list = shutdown::parse_inhibitors(&out.stdout).expect("inhibitor table");
    assert!(!list.is_empty(), "{out:?}");
    assert!(
        list.iter().all(|i| !i.who.is_empty() && !i.mode.is_empty()),
        "{list:?}"
    );
    let on = node::graceful_shutdown_on(x.as_ref()).await;
    println!("inhibitors {list:?}; graceful node shutdown {on:?}");
    assert_eq!(on, Some(shutdown::kubelet_delays_shutdown(&list)));
}

#[tokio::test]
#[ignore = "needs a host with drbd-utils"]
async fn host_units_and_force_option() {
    if !enabled() {
        return;
    }
    let x = exec();
    let out = x
        .run(&node::host_units_argv(), node::CHECK_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    println!("loaded: {:?}", node::loaded_units(&out.stdout));
    let help = x
        .run(&shutdown::help_argv(), node::CHECK_TIMEOUT)
        .await
        .unwrap();
    let force = shutdown::force_option(&format!("{}{}", help.stdout, help.stderr));
    println!("force option: {force:?}");
    assert!(force.is_some(), "{help:?}");
}
