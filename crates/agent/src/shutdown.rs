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

// The bounds below add up to GUARD_MAX, which must fit kubelet's 30 s
// window for node-critical pods with a second to spare.

/// Each local probe; both run at once, so a systemd host answers in well
/// under a second and a hung bus costs at most this.
pub const PROBE_MODE: ExecMode = ExecMode::Timeout {
    wait: Duration::from_millis(1500),
    term: Duration::from_millis(250),
    kill: Duration::from_millis(250),
};
/// Whole detection, the pod probe included.
pub const DETECT_BOUND: Duration = Duration::from_secs(4);
/// kubelet writes the DisruptionTarget condition asynchronously.
pub const POD_TRIES: u32 = 3;
const POD_GET: Duration = Duration::from_secs(1);
const POD_RETRY: Duration = Duration::from_millis(500);
/// In-flight CSI calls: a release that ends demotes by itself, an open
/// that ends is forced with the rest.
pub const INFLIGHT_WAIT: Duration = Duration::from_secs(3);
/// One `sync -f`; a flush that cannot finish is cut by the force anyway.
pub const FLUSH_MODE: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(3),
    term: Duration::from_millis(500),
    kill: Duration::from_millis(500),
};
pub const FLUSH_BOUND: Duration = Duration::from_secs(4);
/// One forced demote.
pub const DEMOTE_MODE: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(9),
    term: Duration::from_secs(2),
    kill: Duration::from_secs(1),
};
pub const DEMOTE_BOUND: Duration = Duration::from_secs(12);
/// How long the demoted roles may take to show in the events2 stream.
const SETTLE: Duration = Duration::from_secs(2);
/// The second round, for what is Primary again after the settle.
pub const AGAIN_MODE: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(2),
    term: Duration::from_secs(1),
    kill: Duration::from_secs(1),
};
pub const AGAIN_BOUND: Duration = Duration::from_secs(4);
/// The guard's share of the shutdown, before the drain.
pub const GUARD_MAX: Duration = DETECT_BOUND
    .saturating_add(INFLIGHT_WAIT)
    .saturating_add(FLUSH_BOUND)
    .saturating_add(DEMOTE_BOUND)
    .saturating_add(SETTLE)
    .saturating_add(AGAIN_BOUND);
/// drbdsetup's exit code for a resource that is not up.
const NOT_UP: i32 = 10;

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

/// When systemd stops the container's scope it signals every process in
/// it, the probe just spawned included, until the scope holds no new ones;
/// a moment later a new child runs.
const KILLED_RETRY: Duration = Duration::from_millis(300);

async fn local(exec: &dyn Exec, argv: &[String], parse: fn(&str) -> Option<bool>) -> Answer {
    let mut out = run_any(exec, argv, PROBE_MODE).await;
    if matches!(&out, Ok(o) if o.exit_code.is_none()) {
        tracing::info!(argv = ?argv, "probe killed by a signal, trying again");
        tokio::time::sleep(KILLED_RETRY).await;
        out = run_any(exec, argv, PROBE_MODE).await;
    }
    match out {
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

/// The usage text drbdsetup prints for `secondary`, not an error.
pub fn is_secondary_usage(text: &str) -> bool {
    text.contains("USAGE:") && text.contains("secondary")
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
    pub inflight: crate::csi::Inflight,
    /// The host's mount table: hostPID makes PID 1 the host's init.
    pub mounts: std::path::PathBuf,
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("res_dir", &self.res_dir)
            .field("force", &self.force)
            .finish_non_exhaustive()
    }
}

/// `sync -f` flushes one file system only: a global sync would block on a
/// suspended device.
pub fn flush_argv(mountpoint: &str) -> Vec<String> {
    argv(&["sync", "-f", mountpoint])
}

/// The first mount point of each `/dev/drbd<minor>` in a mounts table; a
/// bind mount of the same file system needs no second flush.
pub fn mountpoints(table: &str) -> std::collections::BTreeMap<u32, String> {
    let mut found = std::collections::BTreeMap::new();
    for line in table.lines() {
        let mut f = line.split(' ');
        let (Some(src), Some(at)) = (f.next(), f.next()) else {
            continue;
        };
        if let Some(minor) = src
            .strip_prefix("/dev/drbd")
            .and_then(|m| m.parse::<u32>().ok())
        {
            found.entry(minor).or_insert_with(|| unescape_mount(at));
        }
    }
    found
}

/// The kernel writes space, tab, newline and backslash as octal escapes.
fn unescape_mount(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let oct = b
            .get(i + 1..i + 4)
            .filter(|d| b[i] == b'\\' && d.iter().all(|c| (b'0'..=b'7').contains(c)));
        if let Some(d) = oct {
            out.push((d[0] - b'0') * 64 + (d[1] - b'0') * 8 + (d[2] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Owned, tracked, not suspended and mounted: (resource, mount point).
pub fn flush_plan(
    owned: &BTreeSet<String>,
    tracker: &Tracker,
    mounts: &std::collections::BTreeMap<u32, String>,
) -> Vec<(String, String)> {
    owned
        .iter()
        .filter_map(|r| {
            let res = tracker.resource(r)?;
            if !res.suspended.is_empty() {
                return None;
            }
            let at = res.devices.values().find_map(|d| mounts.get(&d.minor?))?;
            Some((r.clone(), at.clone()))
        })
        .collect()
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
            inflight: csi.inflight(),
            mounts: "/proc/1/mounts".into(),
        }
    }

    /// The signal path, before the drain: gate, detect, then wait, flush
    /// and force. Never from the reconcile pass, which may sit behind a
    /// command on a suspended device.
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

    /// A poisoned lock still holds the last state, which is all this needs.
    fn tracker(&self) -> std::sync::MutexGuard<'_, Tracker> {
        self.tracker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn primaries(&self, among: &BTreeSet<String>) -> Vec<String> {
        demote_plan(among, &self.tracker())
    }

    async fn demote(&self, force: &str) {
        let owned = owned(&self.res_dir);
        if !self.inflight.wait_idle(INFLIGHT_WAIT).await {
            tracing::warn!(bound = ?INFLIGHT_WAIT, "CSI calls still running, forcing anyway");
        }
        self.flush(&owned).await;
        let plan = self.primaries(&owned);
        if plan.is_empty() {
            tracing::info!(owned = owned.len(), "no Primary to demote");
            return;
        }
        tracing::warn!(resources = ?plan, "forcing Primary resources to Secondary");
        self.force_all(force, &plan, DEMOTE_MODE, DEMOTE_BOUND)
            .await;
        if !self.tracker().is_available() {
            tracing::warn!(resources = ?plan, "roles after the forced demote unknown: no DRBD event stream");
            return;
        }
        let deadline = tokio::time::Instant::now() + SETTLE;
        while !self.primaries(&owned).is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let again = self.primaries(&owned);
        for r in &plan {
            if !again.contains(r) {
                tracing::info!(resource = r, "demoted to Secondary");
            }
        }
        if again.is_empty() {
            return;
        }
        tracing::warn!(resources = ?again, "still or again Primary, forcing once more");
        self.force_all(force, &again, AGAIN_MODE, AGAIN_BOUND).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        for r in self.primaries(&again.iter().cloned().collect()) {
            tracing::warn!(resource = r, "still Primary after the forced demote");
        }
    }

    /// A forced Secondary fails every write still in the page cache, as a
    /// crash would; whatever can be flushed first is not lost.
    async fn flush(&self, owned: &BTreeSet<String>) {
        let table = std::fs::read_to_string(&self.mounts).unwrap_or_else(|e| {
            tracing::warn!(file = %self.mounts.display(), error = %e, "mount table unreadable, no flush");
            String::new()
        });
        let plan = flush_plan(owned, &self.tracker(), &mountpoints(&table));
        if plan.is_empty() {
            return;
        }
        let runs = plan.iter().map(|(r, at)| async move {
            let t0 = tokio::time::Instant::now();
            let res = run_any(self.exec.as_ref(), &flush_argv(at), FLUSH_MODE).await;
            match res {
                Ok(out) if out.exit_code == Some(0) => {
                    tracing::info!(resource = r, mountpoint = at, took = ?t0.elapsed(), "flushed");
                }
                Ok(out) => {
                    let error = crate::exec::failure(&flush_argv(at), &out);
                    tracing::warn!(resource = r, mountpoint = at, %error, "flush failed");
                }
                Err(e) => tracing::warn!(resource = r, mountpoint = at, error = %e, "flush failed"),
            }
        });
        if tokio::time::timeout(FLUSH_BOUND, futures::future::join_all(runs))
            .await
            .is_err()
        {
            tracing::warn!(bound = ?FLUSH_BOUND, "flushes still running, forcing anyway");
        }
    }

    async fn force_all(&self, force: &str, plan: &[String], mode: ExecMode, bound: Duration) {
        let tracked = self.tracker().is_available();
        let runs = plan.iter().map(|r| {
            let cmd = demote_argv(force, r);
            async move { (r, run_any(self.exec.as_ref(), &cmd, mode).await) }
        });
        let Ok(results) = tokio::time::timeout(bound, futures::future::join_all(runs)).await else {
            tracing::warn!(?bound, "forced demotes still running");
            return;
        };
        for (r, res) in results {
            match res {
                Ok(out) if out.exit_code == Some(0) => {}
                // Without the event stream every owned resource was tried.
                Ok(out) if !tracked && out.exit_code == Some(NOT_UP) => {
                    tracing::info!(resource = r, "not up, nothing to demote");
                }
                Ok(out) => {
                    let error = crate::exec::failure(&demote_argv(force, r), &out);
                    tracing::warn!(resource = r, %error, "forced demote failed");
                }
                Err(e) => tracing::warn!(resource = r, error = %e, "forced demote failed"),
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
