//! Node shutdown guard. On SIGTERM the agent refuses new volume opens,
//! decides whether the node is going down and, if so, forces every Primary
//! it owns to Secondary: a Primary that stays until the host tears DRBD
//! down either disconnects on purpose (its peers become Outdated) or holds
//! the shutdown on suspended I/O. A Secondary that drops off outdates no one.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client};

use crate::events2::Tracker;
use crate::exec::{argv, run_any};
use crate::{Exec, ExecMode};

/// Each local probe; both run at once, so a systemd host answers in well
/// under a second and a hung bus costs at most this.
pub const PROBE_MODE: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(2),
    term: Duration::from_millis(500),
    kill: Duration::from_millis(500),
};
/// Whole detection, the pod probe included.
pub const DETECT_BOUND: Duration = Duration::from_secs(6);
/// kubelet writes the DisruptionTarget condition asynchronously.
pub const POD_TRIES: u32 = 3;
const POD_GET: Duration = Duration::from_millis(1500);
const POD_RETRY: Duration = Duration::from_millis(500);
/// One forced demote; kubelet's critical pod window is 30 s and the
/// detection and the drain need their share of it.
pub const DEMOTE_MODE: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(12),
    term: Duration::from_secs(2),
    kill: Duration::from_secs(1),
};
pub const DEMOTE_BOUND: Duration = Duration::from_secs(15);
/// How long the demoted roles may take to show in the events2 stream.
const SETTLE: Duration = Duration::from_secs(2);
/// The guard's share of the shutdown, before the drain.
pub const GUARD_MAX: Duration = DETECT_BOUND
    .saturating_add(DEMOTE_BOUND)
    .saturating_add(SETTLE);

/// The node shutdown message kubelet puts on the DisruptionTarget condition
/// (`nodeShutdownMessage` in pkg/kubelet/nodeshutdown, Kubernetes v1.35.0).
/// A constant in kubelet, not an API: matched loosely.
pub const SHUTDOWN_MESSAGE_PART: &str = "node shutdown";

/// One probe's answer to "is the node going down".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Yes,
    No,
    /// The probe could not tell: no tool, no systemd, a timeout, output
    /// it does not know.
    Unavailable(String),
}

/// `busctl get-property` of logind's PreparingForShutdown: true from the
/// moment logind announces the shutdown through kubelet's delay window.
pub fn preparing_argv() -> Vec<String> {
    argv(&[
        "busctl",
        "get-property",
        "org.freedesktop.login1",
        "/org/freedesktop/login1",
        "org.freedesktop.login1.Manager",
        "PreparingForShutdown",
    ])
}

/// busctl prints the D-Bus signature, then the value: `b true`.
pub fn parse_preparing(stdout: &str) -> Option<bool> {
    match stdout.trim() {
        "b true" => Some(true),
        "b false" => Some(false),
        _ => None,
    }
}

/// Stays `running` through the logind delay window: logind holds the
/// shutdown before systemd starts its job. `stopping` means systemd is
/// already stopping units, the container runtime among them.
pub fn system_state_argv() -> Vec<String> {
    argv(&["systemctl", "is-system-running"])
}

/// Exits non-zero for every state but running, so only stdout decides.
/// `offline` and `unknown` mean no usable manager.
pub fn parse_system_state(stdout: &str) -> Option<bool> {
    match stdout.lines().next().map(str::trim) {
        Some("stopping") => Some(true),
        Some("initializing" | "starting" | "running" | "degraded" | "maintenance") => Some(false),
        _ => None,
    }
}

async fn local(exec: &dyn Exec, argv: &[String], parse: fn(&str) -> Option<bool>) -> Answer {
    match run_any(exec, argv, PROBE_MODE).await {
        Ok(out) => match parse(&out.stdout) {
            Some(true) => Answer::Yes,
            Some(false) => Answer::No,
            None => Answer::Unavailable(format!(
                "exit {:?}: {} {}",
                out.exit_code,
                out.stdout.trim(),
                out.stderr.trim()
            )),
        },
        Err(e) => Answer::Unavailable(e.to_string()),
    }
}

pub async fn probe_preparing(exec: &dyn Exec) -> Answer {
    local(exec, &preparing_argv(), parse_preparing).await
}

pub async fn probe_system_state(exec: &dyn Exec) -> Answer {
    local(exec, &system_state_argv(), parse_system_state).await
}

/// kubelet marks its own pod terminations: node shutdown and node-pressure
/// eviction share the reason, only the message tells them apart.
pub fn pod_marked_for_node_shutdown(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter().any(|c| {
                c.type_ == "DisruptionTarget"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("TerminationByKubelet")
                    && c.message
                        .as_deref()
                        .is_some_and(|m| m.contains(SHUTDOWN_MESSAGE_PART))
            })
        })
}

/// The API side, for hosts without a usable systemd.
#[async_trait::async_trait]
pub trait PodProbe: Send + Sync {
    /// This agent's own pod as the API server has it now.
    async fn get(&self) -> Result<Pod, String>;
}

pub struct KubePod {
    api: Api<Pod>,
    name: Option<String>,
}

impl std::fmt::Debug for KubePod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubePod")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl KubePod {
    pub fn new(client: Client, namespace: &str, name: Option<String>) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
            name,
        }
    }
}

#[async_trait::async_trait]
impl PodProbe for KubePod {
    async fn get(&self) -> Result<Pod, String> {
        let name = self.name.as_deref().ok_or("POD_NAME is not set")?;
        match tokio::time::timeout(POD_GET, self.api.get(name)).await {
            Ok(r) => r.map_err(|e| e.to_string()),
            Err(_) => Err(format!("no answer within {POD_GET:?}")),
        }
    }
}

pub async fn probe_pod(pods: &dyn PodProbe) -> Answer {
    let mut read = false;
    let mut last = String::new();
    for i in 0..POD_TRIES {
        if i > 0 {
            tokio::time::sleep(POD_RETRY).await;
        }
        match pods.get().await {
            Ok(p) if pod_marked_for_node_shutdown(&p) => return Answer::Yes,
            Ok(_) => read = true,
            Err(e) => last = e,
        }
    }
    if read {
        Answer::No
    } else {
        Answer::Unavailable(last)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Which probe said so.
    NodeDown(&'static str),
    /// The pod alone stops: an update, a delete, a drain.
    PodOnly,
}

/// The local probes are authoritative on a systemd host, since kubelet's
/// graceful shutdown always goes through logind; the API is asked only
/// when neither can tell. Node Ready is never looked at: a node with an
/// unhealthy kubelet is NotReady too.
pub async fn detect(exec: &dyn Exec, pods: &dyn PodProbe) -> Verdict {
    let run = async {
        let (a, b) = tokio::join!(probe_preparing(exec), probe_system_state(exec));
        tracing::info!(preparing_for_shutdown = ?a, system_state = ?b, "shutdown probes");
        if a == Answer::Yes {
            return Verdict::NodeDown("logind PreparingForShutdown");
        }
        if b == Answer::Yes {
            return Verdict::NodeDown("systemd stopping");
        }
        if a == Answer::No || b == Answer::No {
            return Verdict::PodOnly;
        }
        let c = probe_pod(pods).await;
        tracing::info!(pod_condition = ?c, "shutdown probe from the API");
        if c == Answer::Yes {
            Verdict::NodeDown("pod DisruptionTarget node shutdown")
        } else {
            Verdict::PodOnly
        }
    };
    tokio::time::timeout(DETECT_BOUND, run)
        .await
        .unwrap_or_else(|_| {
            tracing::warn!(bound = ?DETECT_BOUND, "shutdown detection timed out");
            Verdict::PodOnly
        })
}

/// From `drbdsetup secondary --help`; the argv follows what the usage shows.
pub fn help_argv() -> Vec<String> {
    argv(&["drbdsetup", "secondary", "--help"])
}

/// The force option as the installed drbdsetup spells it, `None` when it
/// has none.
pub fn force_option(help: &str) -> Option<&'static str> {
    if help.contains("--force={yes|no}") {
        Some("--force=yes")
    } else if help
        .split(|c: char| c.is_whitespace() || c == '[' || c == ']')
        .any(|t| t == "--force")
    {
        Some("--force")
    } else {
        None
    }
}

pub fn demote_argv(force: &str, res: &str) -> Vec<String> {
    argv(&["drbdsetup", "secondary", force, res])
}

/// Resources this agent owns: the names of the installed resource files.
/// Candidates start with a dot and never count.
pub fn owned(res_dir: &Path) -> BTreeSet<String> {
    let Ok(entries) = std::fs::read_dir(res_dir) else {
        return BTreeSet::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            let res = name.strip_suffix(".res")?;
            (!res.is_empty() && !res.starts_with('.')).then(|| res.to_owned())
        })
        .collect()
}

/// Owned resources the tracker shows Primary. With the tracker down every
/// owned resource is a candidate: a forced Secondary on a Secondary is a
/// no-op, a missed Primary is the failure this guards against.
pub fn demote_plan(owned: &BTreeSet<String>, tracker: &Tracker) -> Vec<String> {
    owned
        .iter()
        .filter(|r| {
            !tracker.is_available()
                || tracker
                    .resource(r)
                    .is_some_and(crate::events2::Resource::is_primary)
        })
        .cloned()
        .collect()
}

/// Closed on SIGTERM: a new open after a forced Secondary promotes again.
#[derive(Debug, Clone, Default)]
pub struct OpenGate(Arc<AtomicBool>);

impl OpenGate {
    pub fn close(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

pub struct Guard {
    pub exec: Arc<dyn Exec>,
    pub tracker: Arc<Mutex<Tracker>>,
    pub res_dir: std::path::PathBuf,
    /// `None`: drbdsetup cannot force a Secondary, there is nothing to do.
    pub force: Option<&'static str>,
    pub pods: Box<dyn PodProbe>,
    pub gate: OpenGate,
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("res_dir", &self.res_dir)
            .field("force", &self.force)
            .finish_non_exhaustive()
    }
}

impl Guard {
    pub fn new(
        exec: &Arc<dyn Exec>,
        tracker: Arc<Mutex<Tracker>>,
        cfg: &crate::Config,
        tools: &crate::node::Tools,
        client: &Client,
        csi: &crate::csi::NodeService,
    ) -> Self {
        Self {
            exec: exec.clone(),
            tracker,
            res_dir: cfg.res_dir.clone(),
            force: tools.secondary_force,
            pods: Box::new(KubePod::new(
                client.clone(),
                &cfg.namespace,
                cfg.pod.clone(),
            )),
            gate: csi.open_gate(),
        }
    }

    /// The signal path, before the drain: gate, detect, demote. Never
    /// from the reconcile pass, which may sit behind a command on a
    /// suspended device.
    pub async fn on_signal(&self) {
        self.gate.close();
        tracing::info!("refusing new volume opens");
        match detect(self.exec.as_ref(), self.pods.as_ref()).await {
            Verdict::PodOnly => tracing::info!("node is not going down: pod stop only"),
            Verdict::NodeDown(by) => {
                tracing::info!(by, "node is going down");
                if let Some(force) = self.force {
                    self.demote(force).await;
                } else {
                    tracing::warn!("no forced demote: drbdsetup secondary --force unavailable");
                }
            }
        }
    }

    fn primaries(&self, among: &BTreeSet<String>) -> Vec<String> {
        let t = self
            .tracker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        demote_plan(among, &t)
    }

    async fn demote(&self, force: &str) {
        let owned = owned(&self.res_dir);
        let plan = self.primaries(&owned);
        if plan.is_empty() {
            tracing::info!(owned = owned.len(), "no Primary to demote");
            return;
        }
        tracing::warn!(resources = ?plan, "forcing Primary resources to Secondary");
        let runs = plan.iter().map(|r| {
            let cmd = demote_argv(force, r);
            async move { (r, run_any(self.exec.as_ref(), &cmd, DEMOTE_MODE).await) }
        });
        let Ok(results) = tokio::time::timeout(DEMOTE_BOUND, futures::future::join_all(runs)).await
        else {
            tracing::warn!(bound = ?DEMOTE_BOUND, "forced demotes still running");
            self.report(plan).await;
            return;
        };
        for (r, res) in results {
            let error = match res {
                Ok(out) if out.exit_code == Some(0) => continue,
                Ok(out) => crate::exec::failure(&demote_argv(force, r), &out).to_string(),
                Err(e) => e.to_string(),
            };
            tracing::warn!(resource = r, error, "forced demote failed");
        }
        self.report(plan).await;
    }

    /// The roles after the demotes, from the events2 stream.
    async fn report(&self, plan: Vec<String>) {
        let planned: BTreeSet<String> = plan.into_iter().collect();
        if !self.tracker.lock().is_ok_and(|t| t.is_available()) {
            tracing::warn!(resources = ?planned, "roles after the forced demote unknown: no DRBD event stream");
            return;
        }
        let deadline = tokio::time::Instant::now() + SETTLE;
        let mut left = self.primaries(&planned);
        while !left.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
            left = self.primaries(&planned);
        }
        for r in &planned {
            if left.contains(r) {
                tracing::warn!(resource = r, "still Primary after the forced demote");
            } else {
                tracing::info!(resource = r, "demoted to Secondary");
            }
        }
    }
}

/// One shutdown inhibitor lock as `systemd-inhibit --list` prints it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Inhibitor {
    pub who: String,
    pub comm: String,
    pub what: String,
    pub why: String,
    pub mode: String,
}

pub fn inhibitors_argv() -> Vec<String> {
    argv(&["systemd-inhibit", "--list", "--no-pager"])
}

/// A table whose columns start where their header names do; WHY holds
/// spaces, so splitting on whitespace is wrong. `None` when the text is
/// not that table.
pub fn parse_inhibitors(stdout: &str) -> Option<Vec<Inhibitor>> {
    const COLUMNS: [&str; 8] = ["WHO", "UID", "USER", "PID", "COMM", "WHAT", "WHY", "MODE"];
    let mut lines = stdout.lines();
    let Some(header) = lines.by_ref().find(|l| !l.trim().is_empty()) else {
        return Some(Vec::new());
    };
    if !header.trim_start().starts_with("WHO") {
        return stdout.contains("No inhibitors").then(Vec::new);
    }
    let mut starts = Vec::with_capacity(COLUMNS.len());
    let mut from = 0;
    for c in COLUMNS {
        let at = from + header.get(from..)?.find(c)?;
        starts.push(at);
        from = at + c.len();
    }
    let mut found = Vec::new();
    for line in lines.take_while(|l| !l.trim().is_empty()) {
        let chars: Vec<char> = line.chars().collect();
        let cell = |i: usize| -> String {
            let end = starts
                .get(i + 1)
                .copied()
                .unwrap_or(chars.len())
                .min(chars.len());
            let start = starts[i].min(end);
            chars[start..end]
                .iter()
                .collect::<String>()
                .trim()
                .to_owned()
        };
        found.push(Inhibitor {
            who: cell(0),
            comm: cell(4),
            what: cell(5),
            why: cell(6),
            mode: cell(7),
        });
    }
    Some(found)
}

/// kubelet's graceful node shutdown holds a delay lock on shutdown.
pub fn kubelet_delays_shutdown(inhibitors: &[Inhibitor]) -> bool {
    inhibitors.iter().any(|i| {
        i.mode == "delay"
            && i.what.split(':').any(|w| w == "shutdown")
            && (i.who.to_lowercase().contains("kubelet")
                || i.why.to_lowercase().contains("kubelet"))
    })
}

#[cfg(test)]
mod tests;
