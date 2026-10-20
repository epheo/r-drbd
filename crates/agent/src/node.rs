//! Node-level duties: startup tool probe, Node annotations, heartbeat Lease.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use drbd_config::Features;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::{Event, EventSource, Node, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use kube::api::{Patch, PatchParams, PostParams};
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
    /// How `drbdsetup secondary` takes its force option; `None` leaves the
    /// shutdown guard without its demote.
    pub secondary_force: Option<&'static str>,
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

/// A hung systemctl or bus must not hold anything up; none of these
/// checks blocks the start or the first heartbeat.
pub const CHECK_TIMEOUT: crate::ExecMode = crate::ExecMode::Timeout {
    wait: Duration::from_secs(5),
    term: Duration::from_secs(1),
    kill: Duration::from_secs(1),
};

/// How often a node without graceful node shutdown is checked again.
pub const GRACEFUL_RECHECK: Duration = Duration::from_secs(3600);

/// Shipped by drbd-utils and pulled in by its udev rule: one disconnects
/// every resource when the network goes down, the other takes them all
/// down at shutdown.
pub const HOST_DRBD_UNITS: [&str; 2] = [
    "drbd-graceful-disconnect.service",
    "drbd-graceful-down.service",
];

pub fn host_units_argv() -> Vec<String> {
    let mut a = argv(&["systemctl", "show", "-p", "LoadState"]);
    a.extend(HOST_DRBD_UNITS.iter().map(|&u| u.to_owned()));
    a
}

/// One `LoadState=` block per unit, in argv order; `not-found` and
/// `masked` are absent units.
pub fn loaded_units(stdout: &str) -> Vec<&'static str> {
    stdout
        .lines()
        .filter_map(|l| l.trim().strip_prefix("LoadState="))
        .zip(HOST_DRBD_UNITS)
        .filter(|(state, _)| *state == "loaded")
        .map(|(_, u)| u)
        .collect()
}

pub const GRACEFUL_OFF: &str =
    "graceful node shutdown is off: pods are not stopped before the node shuts down";

pub fn host_units_message(units: &[&str]) -> String {
    format!(
        "host drbd-utils units loaded ({}): r-drbd does not need them; with graceful node shutdown on, their shutdown hooks find no Primary in use; on hosts whose network is managed by systemd-networkd or netplan, a network restart can disconnect an in-use Primary and outdate its peers",
        units.join(", ")
    )
}

/// Layer one of the shutdown guard: kubelet holds logind's delay lock.
/// `None` when the host cannot tell (no systemd-inhibit, no bus).
pub async fn graceful_shutdown_on(exec: &dyn Exec) -> Option<bool> {
    let out = run_any(exec, &crate::shutdown::inhibitors_argv(), CHECK_TIMEOUT)
        .await
        .ok()?;
    if out.exit_code != Some(0) {
        return None;
    }
    crate::shutdown::parse_inhibitors(&out.stdout)
        .map(|list| crate::shutdown::kubelet_delays_shutdown(&list))
}

/// Records the warning where `kubectl describe node` shows it.
#[async_trait::async_trait]
pub trait NodeEvents: Send + Sync {
    async fn warn(&self, reason: &str, message: &str);
}

/// A core/v1 Event on this Node, in "default" where kubectl describe
/// looks for cluster-scoped objects. Failure only logs.
pub struct KubeNodeEvents {
    pub client: Client,
    pub node: String,
}

impl std::fmt::Debug for KubeNodeEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeNodeEvents")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

/// One Event per node and reason, patched on every repeat, so an hourly
/// warning never piles up objects.
pub fn event_name(node: &str, reason: &str) -> String {
    format!("{node}.r-drbd.{}", reason.to_ascii_lowercase())
}

/// What a repeat changes on the existing Event.
pub fn event_repeat(count: i32, message: &str) -> serde_json::Value {
    json!({
        "count": count.saturating_add(1),
        "message": message,
        "lastTimestamp": model::time::now(),
    })
}

pub fn node_event(node: &str, uid: Option<String>, reason: &str, message: &str) -> Event {
    let now = model::time::now();
    Event {
        metadata: ObjectMeta {
            name: Some(event_name(node, reason)),
            namespace: Some("default".into()),
            ..ObjectMeta::default()
        },
        involved_object: ObjectReference {
            api_version: Some("v1".into()),
            kind: Some("Node".into()),
            name: Some(node.to_owned()),
            uid,
            ..ObjectReference::default()
        },
        reason: Some(reason.into()),
        message: Some(message.into()),
        type_: Some("Warning".into()),
        count: Some(1),
        first_timestamp: Some(now.clone()),
        last_timestamp: Some(now),
        source: Some(EventSource {
            component: Some(AGENT_COMPONENT.into()),
            host: Some(node.to_owned()),
        }),
        reporting_component: Some(AGENT_COMPONENT.into()),
        reporting_instance: Some(node.to_owned()),
        ..Event::default()
    }
}

pub const AGENT_COMPONENT: &str = "r-drbd-agent";

#[async_trait::async_trait]
impl NodeEvents for KubeNodeEvents {
    async fn warn(&self, reason: &str, message: &str) {
        let api: Api<Event> = Api::namespaced(self.client.clone(), "default");
        let name = event_name(&self.node, reason);
        let done = match api.get_opt(&name).await {
            Ok(Some(ev)) => api
                .patch(
                    &name,
                    &PatchParams::default(),
                    &Patch::Merge(event_repeat(ev.count.unwrap_or(1), message)),
                )
                .await
                .map(drop),
            Ok(None) => {
                let uid = Api::<Node>::all(self.client.clone())
                    .get(&self.node)
                    .await
                    .ok()
                    .and_then(|n| n.metadata.uid);
                let ev = node_event(&self.node, uid, reason, message);
                api.create(&PostParams::default(), &ev).await.map(drop)
            }
            Err(e) => Err(e),
        };
        if let Err(e) = done {
            tracing::warn!(reason, error = %e, "recording the node event failed");
        }
    }
}

/// Read-only host checks for the shutdown guard, run once at start; the
/// graceful shutdown check repeats while it finds it off.
pub fn startup_checks(
    exec: Arc<dyn Exec>,
    client: &Client,
    node: &str,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    let events = KubeNodeEvents {
        client: client.clone(),
        node: node.to_owned(),
    };
    async move { startup_checks_with(exec, events).await }
}

async fn startup_checks_with(exec: Arc<dyn Exec>, events: KubeNodeEvents) {
    tokio::join!(
        check_host_units(exec.as_ref(), &events),
        check_graceful(exec.as_ref(), &events, GRACEFUL_RECHECK),
    );
}

pub async fn check_host_units(exec: &dyn Exec, events: &dyn NodeEvents) {
    let out = match run_any(exec, &host_units_argv(), CHECK_TIMEOUT).await {
        Ok(out) if out.exit_code == Some(0) => out,
        Ok(out) => {
            tracing::debug!(exit = ?out.exit_code, stderr = out.stderr.trim(), "host units unknown");
            return;
        }
        Err(e) => {
            tracing::debug!(error = %e, "host units unknown");
            return;
        }
    };
    let units = loaded_units(&out.stdout);
    if units.is_empty() {
        return;
    }
    let message = host_units_message(&units);
    tracing::warn!("{message}");
    events.warn("HostDrbdUnits", &message).await;
}

pub async fn check_graceful(exec: &dyn Exec, events: &dyn NodeEvents, every: Duration) {
    loop {
        match graceful_shutdown_on(exec).await {
            Some(true) => {
                tracing::info!("graceful node shutdown on: kubelet delays shutdown");
                return;
            }
            Some(false) => {
                tracing::warn!("{GRACEFUL_OFF}");
                events.warn("GracefulShutdownOff", GRACEFUL_OFF).await;
            }
            None => return,
        }
        tokio::time::sleep(every).await;
    }
}

/// A node where an operator builds and loads the module (OpenShift with
/// Kernel Module Management) can bring it up after the agent. An agent
/// without it looks again this often, and restarts once it is there.
pub const MODULE_RECHECK: Duration = Duration::from_secs(10);

pub const MODULE_MISSING: &str = "DRBD kernel module not loaded: replicated volumes cannot be served on this node; the agent restarts by itself once it is loaded";

/// Init's root: the host's file system, for files the pod does not mount.
pub const HOST_ROOT: &str = "/proc/1/root";
/// The DRBD module's helper path, read and set through sysfs.
pub const HELPER_PARAM: &str = "sys/module/drbd/parameters/usermode_helper";

/// drbdadm prints the kernel version only when the module is loaded.
pub async fn module_loaded(exec: &dyn Exec) -> bool {
    match run_any(exec, &argv(&["drbdadm", "--version"]), DEFAULT_TIMEOUT).await {
        Ok(out) => drbdadm_field(&out.stdout, "DRBD_KERNEL_VERSION").is_some(),
        Err(_) => false,
    }
}

/// An agent started without the module keeps looking, and a Node Event
/// says why replication is off. Returns once the module is loaded: the
/// caller restarts the agent, whose DRBD side is fixed at start.
pub async fn watch_for_module(exec: &dyn Exec, events: &dyn NodeEvents, every: Duration) {
    tracing::warn!("{MODULE_MISSING}");
    events.warn("DrbdModuleMissing", MODULE_MISSING).await;
    loop {
        tokio::time::sleep(every).await;
        if module_loaded(exec).await {
            tracing::warn!(
                "DRBD module loaded after start: restarting to serve replicated volumes"
            );
            return;
        }
    }
}

/// r-drbd sets no handlers, so the kernel's helper calls do nothing for
/// it, and on a host without drbdadm each one fails with a warning in
/// the kernel log; `disabled` ends them. A helper that exists stays: it
/// may serve the host's own resources. Existence is asked on the host,
/// where its symlinks resolve; an unknown answer leaves it alone.
pub async fn disable_dead_helper(exec: &dyn Exec, host_root: &Path) {
    let param = host_root.join(HELPER_PARAM);
    let Ok(current) = std::fs::read_to_string(&param) else {
        return;
    };
    let helper = current.trim();
    if helper.is_empty() || helper == "disabled" {
        return;
    }
    match run_any(exec, &argv(&["test", "-e", helper]), CHECK_TIMEOUT).await {
        Ok(out) if out.exit_code == Some(1) => {}
        Ok(out) if out.exit_code == Some(0) => {
            tracing::info!(
                helper,
                "DRBD usermode helper exists on the host, left as is"
            );
            return;
        }
        Ok(out) => {
            tracing::warn!(
                helper,
                exit = ?out.exit_code,
                "DRBD usermode helper not checked, left as is"
            );
            return;
        }
        Err(e) => {
            tracing::warn!(helper, error = %e, "DRBD usermode helper not checked, left as is");
            return;
        }
    }
    match std::fs::write(&param, "disabled") {
        Ok(()) => tracing::info!(
            helper,
            "DRBD usermode helper absent on the host: set to disabled"
        ),
        Err(e) => tracing::warn!(
            helper,
            error = %e,
            "DRBD usermode helper absent on the host and could not be disabled"
        ),
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
        tools.secondary_force = probe_secondary_force(exec).await;
    }
    tracing::info!(?tools, "tool probe");
    tools
}

/// Only usage text that parses and lacks the option disables the guard:
/// a help call that failed says nothing about the option, and the guard
/// is worth more than a failed demote costs.
async fn probe_secondary_force(exec: &dyn Exec) -> Option<&'static str> {
    let text = match run_any(exec, &crate::shutdown::help_argv(), DEFAULT_TIMEOUT).await {
        Ok(out) => format!("{}{}", out.stdout, out.stderr),
        Err(e) => format!("error: {e}"),
    };
    let force = match crate::shutdown::force_option(&text) {
        Some(option) => Some(option),
        None if crate::shutdown::is_secondary_usage(&text) => None,
        None => {
            tracing::warn!(
                output = text.trim(),
                "drbdsetup secondary --help unreadable, assuming --force=yes"
            );
            Some("--force=yes")
        }
    };
    if let Some(option) = force {
        tracing::info!(
            option,
            "shutdown guard: drbdsetup secondary takes a force option"
        );
    } else {
        tracing::warn!("shutdown guard unavailable: drbdsetup secondary has no --force");
    }
    force
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

/// `started` is this process's start, as acquireTime: a peer's probe
/// result only counts while the same agent run renewed across it.
pub fn lease(node: &str, namespace: &str, now: Timestamp, started: Timestamp) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(keys::agent_lease_name(node)),
            namespace: Some(namespace.to_owned()),
            labels: Some([(keys::AGENT_LEASE_LABEL.to_owned(), "true".to_owned())].into()),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(node.to_owned()),
            lease_duration_seconds: i32::try_from(keys::LEASE_STALE_SECS).ok(),
            renew_time: Some(MicroTime(now)),
            acquire_time: Some(MicroTime(started)),
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
    // Whole seconds: probe records carry the run as a Time, which keeps no
    // more, and the controller compares the two for equality. Fixed for
    // this process, so overlapping pods of one node alternate between two
    // runs instead of minting a new one per renewal.
    let started = model::time::now().0;
    let mut tick = tokio::time::interval(Duration::from_secs(keys::LEASE_RENEW_SECS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let lease = lease(&node, &namespace, Timestamp::now(), started);
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

    #[derive(Default)]
    struct Recorded(std::sync::Mutex<Vec<(String, String)>>);

    #[async_trait::async_trait]
    impl NodeEvents for Recorded {
        async fn warn(&self, reason: &str, message: &str) {
            self.0
                .lock()
                .unwrap()
                .push((reason.to_owned(), message.to_owned()));
        }
    }

    impl Recorded {
        fn reasons(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|(r, _)| r.clone())
                .collect()
        }
    }

    #[test]
    fn host_unit_states() {
        assert_eq!(
            host_units_argv(),
            argv(&[
                "systemctl",
                "show",
                "-p",
                "LoadState",
                "drbd-graceful-disconnect.service",
                "drbd-graceful-down.service",
            ])
        );
        assert_eq!(
            loaded_units("LoadState=loaded\n\nLoadState=loaded\n"),
            HOST_DRBD_UNITS
        );
        assert_eq!(
            loaded_units("LoadState=not-found\n\nLoadState=loaded\n"),
            ["drbd-graceful-down.service"]
        );
        assert_eq!(
            loaded_units("LoadState=masked\n\nLoadState=not-found\n"),
            [] as [&str; 0]
        );
        assert_eq!(loaded_units(""), [] as [&str; 0]);
    }

    #[tokio::test]
    async fn host_units_warn_once() {
        let x = ScriptedExec::new();
        x.expect(
            host_units_argv(),
            ok("LoadState=loaded\n\nLoadState=loaded\n"),
        );
        let ev = Recorded::default();
        check_host_units(&x, &ev).await;
        x.assert_done();
        assert_eq!(ev.reasons(), ["HostDrbdUnits"]);
        assert!(ev.0.lock().unwrap()[0].1.contains("systemd-networkd"));

        let x = ScriptedExec::new();
        x.expect(
            host_units_argv(),
            crate::exec::scripted::code(1, "", "System has not been booted with systemd"),
        );
        let ev = Recorded::default();
        check_host_units(&x, &ev).await;
        assert_eq!(ev.reasons(), [] as [String; 0]);
    }

    const OFF: &str = "WHO            UID USER PID  COMM           WHAT  WHY                                       MODE\nNetworkManager 0   root 2204 NetworkManager sleep NetworkManager needs to turn off networks delay\n\n1 inhibitors listed.\n";
    const ON: &str = "WHO     UID USER PID  COMM       WHAT     WHY                                        MODE\nkubelet 0   root 1585 microshift shutdown Kubelet needs time to handle node shutdown delay\n\n1 inhibitors listed.\n";

    #[tokio::test(start_paused = true)]
    async fn graceful_off_rechecked_until_on() {
        let x = ScriptedExec::new();
        let inhibit = crate::shutdown::inhibitors_argv();
        x.expect(inhibit.clone(), ok(OFF))
            .expect(inhibit.clone(), ok(OFF))
            .expect(inhibit, ok(ON));
        let ev = Recorded::default();
        check_graceful(&x, &ev, GRACEFUL_RECHECK).await;
        x.assert_done();
        assert_eq!(ev.reasons(), ["GracefulShutdownOff", "GracefulShutdownOff"]);
        assert_eq!(ev.0.lock().unwrap()[0].1, GRACEFUL_OFF);
    }

    const NO_MODULE: &str = "DRBDADM_BUILDTAG=GIT-hash:\\ abc\nDRBDADM_API_VERSION=2\nDRBDADM_VERSION_CODE=0x092100\nDRBDADM_VERSION=9.33.0\n";

    #[tokio::test(start_paused = true)]
    async fn late_module_reported_once_then_returns() {
        let x = ScriptedExec::new();
        let version = argv(&["drbdadm", "--version"]);
        x.expect(version.clone(), ok(NO_MODULE))
            .expect(version, ok(DRBDADM));
        let ev = Recorded::default();
        watch_for_module(&x, &ev, MODULE_RECHECK).await;
        x.assert_done();
        assert_eq!(ev.reasons(), ["DrbdModuleMissing"]);
        assert_eq!(ev.0.lock().unwrap()[0].1, MODULE_MISSING);
    }

    #[tokio::test]
    async fn dead_helper_disabled_live_helper_kept() {
        let root = std::env::temp_dir().join(format!("r-drbd-helper-{}", std::process::id()));
        let param = root.join(HELPER_PARAM);
        std::fs::create_dir_all(param.parent().unwrap()).unwrap();
        let x = ScriptedExec::new();
        // No module: nothing to read, nothing asked, nothing written.
        disable_dead_helper(&x, &root).await;
        assert!(!param.exists());
        // The default helper, absent on the host.
        std::fs::write(&param, "/sbin/drbdadm\n").unwrap();
        x.expect(
            argv(&["test", "-e", "/sbin/drbdadm"]),
            crate::exec::scripted::code(1, "", ""),
        );
        disable_dead_helper(&x, &root).await;
        assert_eq!(std::fs::read_to_string(&param).unwrap(), "disabled");
        // Already disabled: stays, nothing asked.
        disable_dead_helper(&x, &root).await;
        assert_eq!(std::fs::read_to_string(&param).unwrap(), "disabled");
        // A helper the host has (its symlinks resolve there): left to it.
        std::fs::write(&param, "/usr/sbin/drbdadm\n").unwrap();
        x.expect(argv(&["test", "-e", "/usr/sbin/drbdadm"]), ok(""));
        disable_dead_helper(&x, &root).await;
        assert_eq!(
            std::fs::read_to_string(&param).unwrap(),
            "/usr/sbin/drbdadm\n"
        );
        // Unknown, a failed spawn or an odd exit code: left alone too.
        x.expect(
            argv(&["test", "-e", "/usr/sbin/drbdadm"]),
            Err("spawn test: no such file".into()),
        )
        .expect(
            argv(&["test", "-e", "/usr/sbin/drbdadm"]),
            crate::exec::scripted::code(127, "", "test: not found"),
        );
        disable_dead_helper(&x, &root).await;
        disable_dead_helper(&x, &root).await;
        assert_eq!(
            std::fs::read_to_string(&param).unwrap(),
            "/usr/sbin/drbdadm\n"
        );
        x.assert_done();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn graceful_unknown_is_silent() {
        let x = ScriptedExec::new();
        x.expect(
            crate::shutdown::inhibitors_argv(),
            Err("spawn systemd-inhibit: No such file or directory".into()),
        );
        let ev = Recorded::default();
        check_graceful(&x, &ev, GRACEFUL_RECHECK).await;
        x.assert_done();
        assert_eq!(ev.reasons(), [] as [String; 0]);
    }

    #[tokio::test]
    async fn force_probe_falls_back_only_when_unreadable() {
        let help = crate::shutdown::help_argv();
        let x = ScriptedExec::new();
        x.expect(help.clone(), Err("timeout after 60s".into()))
            .expect(help.clone(), crate::exec::scripted::code(1, "", "segfault"))
            .expect(
                help.clone(),
                ok("drbdsetup secondary - Change the role\n\nUSAGE: drbdsetup secondary {resource}\n"),
            )
            .expect(help, ok("USAGE: drbdsetup secondary {resource}\n    [--force={yes|no}]\n"));
        assert_eq!(probe_secondary_force(&x).await, Some("--force=yes"));
        assert_eq!(probe_secondary_force(&x).await, Some("--force=yes"));
        assert_eq!(probe_secondary_force(&x).await, None);
        assert_eq!(probe_secondary_force(&x).await, Some("--force=yes"));
        x.assert_done();
    }

    #[test]
    fn checks_are_bounded() {
        let crate::ExecMode::Timeout { wait, .. } = CHECK_TIMEOUT else {
            panic!("bounded mode expected");
        };
        assert!(wait <= Duration::from_secs(5));
    }

    #[test]
    fn event_on_the_node() {
        let v = serde_json::to_value(node_event("n1", Some("u1".into()), "HostDrbdUnits", "m"))
            .unwrap();
        assert_eq!(v["metadata"]["namespace"], "default");
        assert_eq!(v["involvedObject"]["kind"], "Node");
        assert_eq!(v["involvedObject"]["name"], "n1");
        assert_eq!(v["involvedObject"]["uid"], "u1");
        assert_eq!(v["type"], "Warning");
        assert_eq!(v["reason"], "HostDrbdUnits");
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
        )
        .expect_s(
            &["drbdsetup", "secondary", "--help"],
            ok("USAGE: drbdsetup secondary {resource}\n    [--force={yes|no}]\n"),
        );
        let tools = probe(&x).await;
        x.assert_done();
        assert_eq!(tools.secondary_force, Some("--force=yes"));
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
        let l = lease(
            "n1",
            "r-drbd-system",
            Timestamp::UNIX_EPOCH,
            Timestamp::UNIX_EPOCH,
        );
        let v = serde_json::to_value(&l).unwrap();
        assert_eq!(v["kind"], "Lease");
        assert_eq!(v["metadata"]["name"], "r-drbd-agent-n1");
        assert_eq!(v["spec"]["leaseDurationSeconds"], 40);
        assert_eq!(v["spec"]["holderIdentity"], "n1");
        assert_eq!(v["metadata"]["labels"]["r-drbd.io/agent-lease"], "true");
        assert_eq!(v["spec"]["acquireTime"], "1970-01-01T00:00:00.000000Z");
    }
}
