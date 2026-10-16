//! Node-level duties: startup tool probe, Node annotations, heartbeat Lease.

use std::time::Duration;

use drbd_config::Features;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use model::keys;
use serde_json::json;

use crate::Exec;
use crate::exec::{DEFAULT_TIMEOUT, argv, run_any};

/// Version strings (`X.Y.Z` as the tool prints it) found at startup;
/// `None` when the tool is absent or prints no version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tools {
    pub lvm: Option<String>,
    pub drbd_utils: Option<String>,
    pub drbd_kernel: Option<String>,
    /// Optional resource file settings drbdsetup knows; probed only when
    /// drbd-utils are present.
    pub drbd_features: Features,
}

impl Tools {
    pub fn annotations(&self) -> Vec<(&'static str, String)> {
        [
            (keys::ANNOTATION_LVM, &self.lvm),
            (keys::ANNOTATION_DRBD_UTILS, &self.drbd_utils),
            (keys::ANNOTATION_DRBD_KERNEL, &self.drbd_kernel),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.clone().map(|v| (k, v)))
        .collect()
    }

    /// DRBD work needs the tools and the loaded module (drbdadm prints the
    /// kernel version only when the module is loaded).
    pub fn drbd(&self) -> Option<Features> {
        (self.drbd_utils.is_some() && self.drbd_kernel.is_some()).then_some(self.drbd_features)
    }
}

/// First `X.Y.Z` token in `s`: `2.03.41(2)-RHEL10` gives `2.03.41`.
pub fn version_token(s: &str) -> Option<&str> {
    s.split(|c: char| !(c.is_ascii_digit() || c == '.'))
        .find(|tok| is_version(tok))
}

fn is_version(tok: &str) -> bool {
    let parts: Vec<&str> = tok.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}

fn drbdadm_field(stdout: &str, key: &str) -> Option<String> {
    stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix(key)?.strip_prefix('='))
        .and_then(version_token)
        .map(str::to_owned)
}

/// Shipped by drbd-utils, enabled by the host OS: takes every DRBD resource
/// down before shutdown.target, so a Primary with suspended IO cannot hang
/// a node shutdown. The agent installs nothing.
pub const SHUTDOWN_GUARD_UNIT: &str = "drbd-graceful-down.service";

/// A hung systemctl must not hold the agent start.
pub const SHUTDOWN_GUARD_TIMEOUT: crate::ExecMode = crate::ExecMode::Timeout {
    wait: Duration::from_secs(5),
    term: Duration::from_secs(1),
    kill: Duration::from_secs(1),
};

pub fn shutdown_guard_argv() -> Vec<String> {
    argv(&[
        "systemctl",
        "show",
        "-p",
        "LoadState",
        "-p",
        "ActiveState",
        SHUTDOWN_GUARD_UNIT,
    ])
}

/// `systemctl show` prints `LoadState=not-found` for a missing unit, with exit 0.
pub fn shutdown_guard_loaded(stdout: &str) -> bool {
    stdout
        .lines()
        .any(|l| l.trim().strip_prefix("LoadState=") == Some("loaded"))
}

/// Read-only startup check; a host without the unit or without systemd
/// still runs the agent.
pub async fn check_shutdown_guard(exec: &dyn Exec) {
    let warn = |detail: String| {
        tracing::warn!(
            unit = SHUTDOWN_GUARD_UNIT,
            detail,
            "host has no drbd-graceful-down.service: a Primary without quorum can hang the node shutdown"
        );
    };
    match run_any(exec, &shutdown_guard_argv(), SHUTDOWN_GUARD_TIMEOUT).await {
        Ok(out) if out.exit_code == Some(0) && shutdown_guard_loaded(&out.stdout) => {
            tracing::info!(unit = SHUTDOWN_GUARD_UNIT, state = %out.stdout.trim().replace('\n', " "), "shutdown guard present");
        }
        Ok(out) => warn(format!(
            "exit {:?}: {} {}",
            out.exit_code,
            out.stdout.trim().replace('\n', " "),
            out.stderr.trim()
        )),
        Err(e) => warn(e.to_string()),
    }
}

/// `lvm version` and, when installed, `drbdadm --version`.
pub async fn probe(exec: &dyn Exec) -> Tools {
    let mut tools = Tools::default();
    match run_any(exec, &argv(&["lvm", "version"]), DEFAULT_TIMEOUT).await {
        Ok(out) => {
            tools.lvm = out
                .stdout
                .lines()
                .find_map(|l| l.trim().strip_prefix("LVM version:"))
                .and_then(version_token)
                .map(str::to_owned);
        }
        Err(e) => tracing::error!(error = %e, "lvm not usable"),
    }
    if tools.lvm.is_none() {
        tracing::error!("lvm version unknown: pools will report the command errors");
    }
    match run_any(exec, &argv(&["drbdadm", "--version"]), DEFAULT_TIMEOUT).await {
        Ok(out) => {
            tools.drbd_utils = drbdadm_field(&out.stdout, "DRBDADM_VERSION");
            tools.drbd_kernel = drbdadm_field(&out.stdout, "DRBD_KERNEL_VERSION");
        }
        Err(e) => tracing::info!(error = %e, "drbd-utils not found"),
    }
    if tools.drbd_utils.is_some() {
        tools.drbd_features = probe_features(exec).await;
    }
    tracing::info!(?tools, "tool probe");
    tools
}

/// Reads the usage text of the drbdsetup commands that own each optional
/// setting: what the installed tools accept, not what a version implies.
async fn probe_features(exec: &dyn Exec) -> Features {
    let mut texts: [String; 3] = Default::default();
    for (text, (command, _)) in texts.iter_mut().zip(crate::drbd::FEATURE_PROBES) {
        match run_any(exec, &crate::drbd::argv::help(command), DEFAULT_TIMEOUT).await {
            // drbdsetup help exits non-zero on some versions; the text decides.
            Ok(out) => *text = format!("{}{}", out.stdout, out.stderr),
            Err(e) => tracing::warn!(command, error = %e, "drbdsetup help failed"),
        }
    }
    crate::drbd::features([&texts[0], &texts[1], &texts[2]])
}

/// Server-side apply of the r-drbd.io annotations this agent owns; a tool
/// that disappeared loses its annotation.
pub async fn annotate(client: &Client, node: &str, tools: &Tools) -> Result<(), kube::Error> {
    let annotations: serde_json::Map<String, serde_json::Value> = tools
        .annotations()
        .into_iter()
        .map(|(k, v)| (k.to_owned(), json!(v)))
        .collect();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": { "name": node, "annotations": annotations },
    });
    Api::<Node>::all(client.clone())
        .patch(
            node,
            &PatchParams::apply(&keys::agent_field_manager(node)).force(),
            &Patch::Apply(body),
        )
        .await?;
    Ok(())
}

pub fn lease(node: &str, namespace: &str, now: Timestamp) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(keys::agent_lease_name(node)),
            namespace: Some(namespace.to_owned()),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(node.to_owned()),
            lease_duration_seconds: i32::try_from(keys::LEASE_STALE_SECS).ok(),
            renew_time: Some(MicroTime(now)),
            ..LeaseSpec::default()
        }),
    }
}

/// Renews the Lease forever and applies the Node annotations until they
/// stick; failures are logged and retried on the next tick, the controller
/// decides what a stale Lease means.
pub async fn heartbeat(client: Client, namespace: String, node: String, tools: Tools) {
    let api = Api::<Lease>::namespaced(client.clone(), &namespace);
    let params = PatchParams::apply(&keys::agent_field_manager(&node)).force();
    let name = keys::agent_lease_name(&node);
    let mut annotated = false;
    let mut tick = tokio::time::interval(Duration::from_secs(keys::LEASE_RENEW_SECS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let lease = lease(&node, &namespace, Timestamp::now());
        let patch = Patch::Apply(&lease);
        let renewed = crate::retry::with_backoff(
            &name,
            &crate::retry::BACKOFF,
            crate::retry::retryable,
            || api.patch(&name, &params, &patch),
        )
        .await;
        match renewed {
            Ok(_) => tracing::trace!(lease = name, "lease renewed"),
            Err(e) => tracing::warn!(lease = name, error = %e, "lease renewal failed"),
        }
        if !annotated {
            match annotate(&client, &node, &tools).await {
                Ok(()) => annotated = true,
                Err(e) => tracing::warn!(node, error = %e, "node annotation failed"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::scripted::{ScriptedExec, ok};

    const DRBDADM: &str = "DRBDADM_BUILDTAG=GIT-hash:\\ abc\nDRBDADM_API_VERSION=2\nDRBD_KERNEL_VERSION_CODE=0x090210\nDRBD_KERNEL_VERSION=9.2.16\nDRBDADM_VERSION_CODE=0x092100\nDRBDADM_VERSION=9.33.0\n";

    #[test]
    fn shutdown_guard_output() {
        assert!(shutdown_guard_loaded(
            "LoadState=loaded\nActiveState=inactive\n"
        ));
        assert!(shutdown_guard_loaded(
            "ActiveState=active\nLoadState=loaded\n"
        ));
        assert!(!shutdown_guard_loaded(
            "LoadState=not-found\nActiveState=inactive\n"
        ));
        assert!(!shutdown_guard_loaded("LoadState=masked\n"));
        assert!(!shutdown_guard_loaded(""));
    }

    #[test]
    fn shutdown_guard_query_is_bounded() {
        let crate::ExecMode::Timeout { wait, term, kill } = SHUTDOWN_GUARD_TIMEOUT else {
            panic!("bounded mode expected");
        };
        assert!(wait <= Duration::from_secs(5));
        assert!(wait + term + kill <= Duration::from_secs(7));
    }

    #[tokio::test]
    async fn shutdown_guard_check_is_one_read_only_query() {
        let x = ScriptedExec::new();
        x.expect(
            shutdown_guard_argv(),
            ok("LoadState=not-found\nActiveState=inactive\n"),
        );
        check_shutdown_guard(&x).await;
        x.assert_done();
        let x = ScriptedExec::new();
        x.expect(
            shutdown_guard_argv(),
            crate::exec::scripted::code(1, "", "System has not been booted with systemd"),
        );
        check_shutdown_guard(&x).await;
        x.assert_done();
    }

    #[test]
    fn versions() {
        assert_eq!(
            version_token("     2.03.41(2)-RHEL10 (2026-05-15)"),
            Some("2.03.41")
        );
        assert_eq!(version_token("none 1.2"), None);
        assert_eq!(version_token("1.2.3.4 9.35.0"), Some("9.35.0"));
        assert_eq!(
            drbdadm_field(DRBDADM, "DRBD_KERNEL_VERSION").as_deref(),
            Some("9.2.16")
        );
    }

    #[tokio::test]
    async fn probe_and_annotations() {
        let x = ScriptedExec::new();
        x.expect_s(
            &["lvm", "version"],
            ok("  LVM version:     2.03.41(2)-RHEL10 (2026-05-15)\n  Library version: 1.02.215\n"),
        )
        .expect_s(&["drbdadm", "--version"], ok(DRBDADM))
        .expect_s(
            &["drbdsetup", "help", "peer-device-options"],
            ok("    [--resync-without-replication={yes|no}] [--peer-tiebreaker={yes|no}]\n"),
        )
        .expect_s(
            &["drbdsetup", "help", "attach"],
            ok("    [--rs-discard-granularity=(0 ... 1048576)] [--bitmap={yes|no}]\n"),
        )
        .expect_s(
            &["drbdsetup", "help", "resource-options"],
            ok("    [--quorum={off|majority|all}|(1 ... 32)]\n"),
        );
        let tools = probe(&x).await;
        x.assert_done();
        assert_eq!(
            tools.drbd(),
            Some(Features {
                tiebreaker: true,
                discard_granularity: true,
                suspended_primary_outdated: false,
            })
        );
        assert_eq!(
            tools.annotations(),
            vec![
                ("r-drbd.io/lvm", "2.03.41".to_owned()),
                ("r-drbd.io/drbd-utils", "9.33.0".to_owned()),
                ("r-drbd.io/drbd-kernel", "9.2.16".to_owned()),
            ]
        );
    }

    #[test]
    fn lease_shape() {
        let l = lease("n1", "r-drbd-system", Timestamp::UNIX_EPOCH);
        let v = serde_json::to_value(&l).unwrap();
        assert_eq!(v["kind"], "Lease");
        assert_eq!(v["metadata"]["name"], "r-drbd-agent-n1");
        assert_eq!(v["spec"]["leaseDurationSeconds"], 40);
        assert_eq!(v["spec"]["holderIdentity"], "n1");
    }
}
