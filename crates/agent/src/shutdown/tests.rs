use std::collections::BTreeSet;
use std::sync::atomic::AtomicU32;

use super::*;
use crate::exec::scripted::{ScriptedExec, code, ok};

/// Kubernetes v1.35.0, pkg/kubelet/nodeshutdown/nodeshutdown_manager.go.
const KUBELET_SHUTDOWN_MESSAGE: &str = "Pod was terminated in response to imminent node shutdown.";
/// Kubernetes v1.35.0, pkg/kubelet/eviction/helpers.go: nodeLowMessageFmt
/// and thresholdMetMessageFmt, same condition type and reason.
const KUBELET_EVICTION_MESSAGE: &str =
    "The node was low on resource: memory. Threshold quantity: 100Mi, available: 49Mi. ";

const MICROSHIFT_INHIBITORS: &str = "\
WHO            UID USER PID  COMM           WHAT     WHY                                        MODE
NetworkManager 0   root 1304 NetworkManager sleep    NetworkManager needs to turn off networks  delay
kubelet        0   root 1585 microshift     shutdown Kubelet needs time to handle node shutdown delay

2 inhibitors listed.
";

const NO_KUBELET_INHIBITORS: &str = "\
WHO            UID USER PID  COMM           WHAT  WHY                                       MODE
NetworkManager 0   root 2204 NetworkManager sleep NetworkManager needs to turn off networks delay

1 inhibitors listed.
";

fn pod(conditions: &serde_json::Value) -> Pod {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "r-drbd-agent-x"},
        "status": {"conditions": conditions},
    }))
    .unwrap()
}

fn disruption(message: &str) -> Pod {
    pod(&serde_json::json!([
        {"type": "Ready", "status": "True"},
        {"type": "DisruptionTarget", "status": "True", "reason": "TerminationByKubelet", "message": message},
    ]))
}

#[test]
fn busctl_output() {
    assert_eq!(parse_preparing("b true\n"), Some(true));
    assert_eq!(parse_preparing("b false\n"), Some(false));
    assert_eq!(parse_preparing(""), None);
    assert_eq!(parse_preparing("s \"yes\"\n"), None);
    assert_eq!(
        preparing_argv(),
        argv(&[
            "busctl",
            "get-property",
            "org.freedesktop.login1",
            "/org/freedesktop/login1",
            "org.freedesktop.login1.Manager",
            "PreparingForShutdown",
        ])
    );
}

#[test]
fn system_state_output() {
    assert_eq!(parse_system_state("stopping\n"), Some(true));
    for s in [
        "running",
        "degraded",
        "starting",
        "initializing",
        "maintenance",
    ] {
        assert_eq!(parse_system_state(&format!("{s}\n")), Some(false), "{s}");
    }
    assert_eq!(parse_system_state("offline\n"), None);
    assert_eq!(parse_system_state("unknown\n"), None);
    assert_eq!(parse_system_state(""), None);
    assert_eq!(
        system_state_argv(),
        argv(&["systemctl", "is-system-running"])
    );
}

#[test]
fn inhibitor_table() {
    let list = parse_inhibitors(MICROSHIFT_INHIBITORS).unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(
        list[1],
        Inhibitor {
            who: "kubelet".into(),
            comm: "microshift".into(),
            what: "shutdown".into(),
            why: "Kubelet needs time to handle node shutdown".into(),
            mode: "delay".into(),
        }
    );
    assert!(kubelet_delays_shutdown(&list));

    let list = parse_inhibitors(NO_KUBELET_INHIBITORS).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].why, "NetworkManager needs to turn off networks");
    assert!(!kubelet_delays_shutdown(&list));

    assert_eq!(parse_inhibitors("No inhibitors.\n"), Some(Vec::new()));
    assert_eq!(parse_inhibitors(""), Some(Vec::new()));
    assert_eq!(parse_inhibitors("Failed to connect to bus\n"), None);
    assert_eq!(
        inhibitors_argv(),
        argv(&["systemd-inhibit", "--list", "--no-pager"])
    );
}

#[test]
fn kubelet_lock_must_delay_shutdown() {
    let lock = |what: &str, mode: &str| Inhibitor {
        who: "kubelet".into(),
        what: what.into(),
        mode: mode.into(),
        ..Inhibitor::default()
    };
    assert!(kubelet_delays_shutdown(&[lock("sleep:shutdown", "delay")]));
    assert!(!kubelet_delays_shutdown(&[lock("shutdown", "block")]));
    assert!(!kubelet_delays_shutdown(&[lock("sleep", "delay")]));
    let by_why = Inhibitor {
        who: "k3s".into(),
        why: "Kubelet needs time to handle node shutdown".into(),
        what: "shutdown".into(),
        mode: "delay".into(),
        ..Inhibitor::default()
    };
    assert!(kubelet_delays_shutdown(&[by_why]));
}

#[test]
fn pod_condition_names_node_shutdown() {
    assert!(pod_marked_for_node_shutdown(&disruption(
        KUBELET_SHUTDOWN_MESSAGE
    )));
    // Node-pressure eviction: same type and reason, another message.
    assert!(!pod_marked_for_node_shutdown(&disruption(
        KUBELET_EVICTION_MESSAGE
    )));
    assert!(!pod_marked_for_node_shutdown(&pod(&serde_json::json!([
        {"type": "DisruptionTarget", "status": "True", "reason": "DeletionByTaintManager", "message": KUBELET_SHUTDOWN_MESSAGE},
    ]))));
    assert!(!pod_marked_for_node_shutdown(&pod(&serde_json::json!([
        {"type": "DisruptionTarget", "status": "False", "reason": "TerminationByKubelet", "message": KUBELET_SHUTDOWN_MESSAGE},
    ]))));
    assert!(!pod_marked_for_node_shutdown(&pod(&serde_json::json!([]))));
}

#[test]
fn force_option_from_help() {
    let help = "drbdsetup secondary - Change the role of a node in a resource to secondary.\n\nUSAGE: drbdsetup secondary {resource}\n    [--force={yes|no}]\n";
    assert_eq!(force_option(help), Some("--force=yes"));
    assert_eq!(
        force_option("USAGE: drbdsetup secondary {resource} [--force]\n"),
        Some("--force")
    );
    assert_eq!(
        force_option("USAGE: drbdsetup secondary {resource}\n"),
        None
    );
    assert!(is_secondary_usage(help));
    assert!(!is_secondary_usage(
        "error: timeout after 60s: drbdsetup secondary --help"
    ));
    assert_eq!(help_argv(), argv(&["drbdsetup", "secondary", "--help"]));
    assert_eq!(
        demote_argv("--force=yes", "pvc-1"),
        argv(&["drbdsetup", "secondary", "--force=yes", "pvc-1"])
    );
}

fn tracker(lines: &[&str]) -> Tracker {
    let mut t = Tracker::default();
    for l in lines {
        t.feed(l).unwrap();
    }
    t
}

fn res(name: &str, role: &str) -> String {
    format!(
        "exists resource name:{name} role:{role} suspended:no force-io-failures:no may_promote:no promotion_score:0"
    )
}

#[test]
fn plan_is_owned_primaries_only() {
    let dir = std::env::temp_dir().join(format!("r-drbd-shutdown-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for f in ["pvc-a.res", "pvc-b.res", ".pvc-c.candidate", "notes.txt"] {
        std::fs::write(dir.join(f), "").unwrap();
    }
    let owned = owned(&dir);
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(
        owned.iter().map(String::as_str).collect::<Vec<_>>(),
        ["pvc-a", "pvc-b"]
    );
    let t = tracker(&[
        &res("pvc-a", "Primary"),
        &res("pvc-b", "Secondary"),
        // A Primary the host owns: no resource file of ours.
        &res("host-r0", "Primary"),
        "exists -",
    ]);
    assert_eq!(demote_plan(&owned, &t), ["pvc-a"]);
    // Tracker down: every owned resource, still never the host's.
    assert_eq!(demote_plan(&owned, &Tracker::default()), ["pvc-a", "pvc-b"]);
    assert!(owned_missing_dir_is_empty());
}

fn owned_missing_dir_is_empty() -> bool {
    owned(Path::new("/nonexistent/r-drbd-res")).is_empty()
}

#[test]
fn bounds_fit_the_critical_pod_window() {
    let total = |m: ExecMode| match m {
        ExecMode::Timeout { wait, term, kill } => wait + term + kill,
        _ => panic!("bounded mode expected"),
    };
    assert!(total(PROBE_MODE) < DETECT_BOUND);
    assert!(total(DEMOTE_MODE) <= DEMOTE_BOUND);
    assert!(total(FLUSH_MODE) <= FLUSH_BOUND);
    assert!(total(AGAIN_MODE) <= AGAIN_BOUND);
    // kubelet's critical pod grace, with room left for the drain.
    assert!(GUARD_MAX <= Duration::from_secs(29), "{GUARD_MAX:?}");
}

/// Counts calls; answers with `pods` in turn, the last one repeated.
struct Pods {
    answers: Vec<Result<Pod, String>>,
    calls: AtomicU32,
}

impl Pods {
    fn new(answers: Vec<Result<Pod, String>>) -> Self {
        Self {
            answers,
            calls: AtomicU32::new(0),
        }
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl PodProbe for Pods {
    async fn get(&self) -> Result<Pod, String> {
        let i = self.calls.fetch_add(1, Ordering::SeqCst) as usize;
        self.answers[i.min(self.answers.len() - 1)].clone()
    }
}

/// The two local probes run concurrently, so their order is not fixed.
fn local_exec(
    busctl: Result<crate::Output, String>,
    state: Result<crate::Output, String>,
) -> Arc<dyn Exec> {
    Arc::new(Unordered(Mutex::new(vec![
        (preparing_argv(), busctl),
        (system_state_argv(), state),
    ])))
}

type Step = (Vec<String>, Result<crate::Output, String>);

struct Unordered(Mutex<Vec<Step>>);

#[async_trait::async_trait]
impl Exec for Unordered {
    async fn run(&self, argv: &[String], _: ExecMode) -> Result<crate::Output, crate::ExecError> {
        let mut steps = self.0.lock().unwrap();
        let i = steps
            .iter()
            .position(|(a, _)| a.as_slice() == argv)
            .unwrap_or_else(|| panic!("unexpected command {argv:?}"));
        steps
            .remove(i)
            .1
            .map_err(|e| crate::ExecError::Spawn(argv[0].clone(), std::io::Error::other(e)))
    }
}

#[tokio::test]
async fn local_yes_skips_the_api() {
    let pods = Pods::new(vec![Ok(disruption(KUBELET_SHUTDOWN_MESSAGE))]);
    let x = local_exec(ok("b true\n"), code(0, "running\n", ""));
    assert_eq!(
        detect(x.as_ref(), &pods).await,
        Verdict::NodeDown("logind PreparingForShutdown")
    );
    let x = local_exec(Err("busctl: not found".into()), code(1, "stopping\n", ""));
    assert_eq!(
        detect(x.as_ref(), &pods).await,
        Verdict::NodeDown("systemd stopping")
    );
    assert_eq!(pods.calls(), 0);
}

#[tokio::test]
async fn stopping_wins_over_logind_false() {
    // With no delay lock logind is done preparing once systemd stops units.
    let pods = Pods::new(vec![Ok(disruption(KUBELET_SHUTDOWN_MESSAGE))]);
    let x = local_exec(ok("b false\n"), code(1, "stopping\n", ""));
    assert_eq!(
        detect(x.as_ref(), &pods).await,
        Verdict::NodeDown("systemd stopping")
    );
    assert_eq!(pods.calls(), 0);
}

#[tokio::test]
async fn local_no_is_an_update_without_the_api() {
    let pods = Pods::new(vec![Ok(disruption(KUBELET_SHUTDOWN_MESSAGE))]);
    let x = local_exec(ok("b false\n"), code(0, "running\n", ""));
    assert_eq!(detect(x.as_ref(), &pods).await, Verdict::PodOnly);
    // One authoritative No is enough.
    let x = local_exec(
        code(1, "", "Failed to connect to bus"),
        code(1, "degraded\n", ""),
    );
    assert_eq!(detect(x.as_ref(), &pods).await, Verdict::PodOnly);
    assert_eq!(pods.calls(), 0);
}

#[tokio::test(start_paused = true)]
async fn api_only_when_both_local_probes_fail() {
    let none = || {
        local_exec(
            Err("spawn busctl: No such file or directory".into()),
            code(1, "offline\n", "System has not been booted with systemd"),
        )
    };
    // kubelet writes the condition late: found on the third read.
    let plain = pod(&serde_json::json!([{"type": "Ready", "status": "True"}]));
    let pods = Pods::new(vec![
        Err("connection refused".into()),
        Ok(plain.clone()),
        Ok(disruption(KUBELET_SHUTDOWN_MESSAGE)),
    ]);
    assert_eq!(
        detect(none().as_ref(), &pods).await,
        Verdict::NodeDown("pod DisruptionTarget node shutdown")
    );
    assert_eq!(pods.calls(), 3);

    let pods = Pods::new(vec![Ok(disruption(KUBELET_EVICTION_MESSAGE))]);
    assert_eq!(detect(none().as_ref(), &pods).await, Verdict::PodOnly);
    assert_eq!(pods.calls(), POD_TRIES);

    let pods = Pods::new(vec![Err("forbidden".into())]);
    assert_eq!(
        probe_pod(&pods).await,
        Answer::Unavailable("forbidden".into())
    );
}

fn killed() -> crate::Output {
    crate::Output {
        exit_code: None,
        stdout: String::new(),
        stderr: String::new(),
        duration: Duration::ZERO,
    }
}

#[tokio::test(start_paused = true)]
async fn probe_killed_early_runs_again() {
    let x = ScriptedExec::new();
    x.expect(system_state_argv(), Ok(killed()))
        .expect(system_state_argv(), code(1, "stopping\n", ""));
    assert_eq!(probe_system_state(&x).await, Answer::Yes);
    x.assert_done();
}

#[tokio::test(start_paused = true)]
async fn probe_killed_late_is_not_retried() {
    /// Killed 600 ms into the run: not the scope kill at the SIGTERM.
    struct LateKill(AtomicU32);
    #[async_trait::async_trait]
    impl Exec for LateKill {
        async fn run(&self, _: &[String], _: ExecMode) -> Result<crate::Output, crate::ExecError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(600)).await;
            Ok(killed())
        }
    }
    let x = LateKill(AtomicU32::new(0));
    assert!(matches!(
        probe_system_state(&x).await,
        Answer::Unavailable(_)
    ));
    assert_eq!(x.0.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn detection_is_bounded() {
    struct Hung;
    #[async_trait::async_trait]
    impl PodProbe for Hung {
        async fn get(&self) -> Result<Pod, String> {
            std::future::pending().await
        }
    }
    let x = local_exec(Err("timeout".into()), Err("timeout".into()));
    let t0 = tokio::time::Instant::now();
    assert_eq!(detect(x.as_ref(), &Hung).await, Verdict::PodOnly);
    assert_eq!(t0.elapsed(), DETECT_BOUND);
}

/// A host for the guard: answers the probes, records every command,
/// holds each demote until `parties` demotes have started, and turns a
/// demoted resource Secondary unless it is sticky for the first round.
struct GuardHost {
    tracker: Arc<Mutex<Tracker>>,
    seen: Mutex<Vec<Vec<String>>>,
    barrier: tokio::sync::Barrier,
    sticky: Mutex<BTreeSet<String>>,
    flush_fails: bool,
}

impl GuardHost {
    fn new(tracker: &Arc<Mutex<Tracker>>, parties: usize) -> Arc<Self> {
        Arc::new(Self {
            tracker: tracker.clone(),
            seen: Mutex::default(),
            barrier: tokio::sync::Barrier::new(parties),
            sticky: Mutex::default(),
            flush_fails: false,
        })
    }

    fn seen(&self, program: &str) -> Vec<Vec<String>> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a[0] == program)
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl Exec for GuardHost {
    async fn run(&self, argv: &[String], _: ExecMode) -> Result<crate::Output, crate::ExecError> {
        self.seen.lock().unwrap().push(argv.to_vec());
        let out = |s: &str| {
            code(0, s, "").map_err(|e| crate::ExecError::Spawn(e, std::io::Error::other("x")))
        };
        if argv == preparing_argv().as_slice() {
            return out("b true\n");
        }
        if argv == system_state_argv().as_slice() {
            return out("running\n");
        }
        if argv[0] == "sync" {
            return if self.flush_fails {
                code(1, "", "sync: error syncing")
                    .map_err(|e| crate::ExecError::Spawn(e, std::io::Error::other("x")))
            } else {
                out("")
            };
        }
        assert_eq!(
            argv[..3],
            argv_s(&["drbdsetup", "secondary", "--force=yes"])
        );
        let res = argv[3].clone();
        let first = self.sticky.lock().unwrap().remove(&res);
        if !first {
            self.barrier.wait().await;
            let mut t = self.tracker.lock().unwrap();
            // An untracked host has no stream to report the role on.
            if t.is_available() {
                t.feed(&format!("change resource name:{res} role:Secondary"))
                    .unwrap();
            }
        }
        out("")
    }
}

fn argv_s(parts: &[&str]) -> Vec<String> {
    argv(parts)
}

struct ResDir(std::path::PathBuf);

impl ResDir {
    fn new(name: &str, files: &[&str]) -> Self {
        let dir = std::env::temp_dir().join(format!("r-drbd-guard-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for f in files {
            std::fs::write(dir.join(f), "").unwrap();
        }
        Self(dir)
    }

    fn mounts(&self, table: &str) -> std::path::PathBuf {
        let p = self.0.join("mounts");
        std::fs::write(&p, table).unwrap();
        p
    }
}

impl Drop for ResDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn guard(
    host: &Arc<GuardHost>,
    dir: &ResDir,
    mounts: std::path::PathBuf,
    force: Option<&'static str>,
) -> Guard {
    Guard {
        exec: host.clone(),
        tracker: host.tracker.clone(),
        res_dir: dir.0.clone(),
        force,
        pods: Box::new(Pods::new(vec![Err("unused".into())])),
        gate: OpenGate::default(),
        inflight: crate::csi::Inflight::default(),
        mounts,
    }
}

fn dev(name: &str, minor: u32) -> String {
    format!(
        "exists device name:{name} volume:0 minor:{minor} backing_dev:/dev/vg/{name} disk:UpToDate client:no open:yes quorum:yes"
    )
}

const MOUNTS: &str = "\
/dev/vda3 / xfs rw 0 0
/dev/drbd50000 /var/lib/kubelet/plugins/kubernetes.io/csi/r-drbd.io/abc/globalmount xfs rw 0 0
/dev/drbd50000 /var/lib/kubelet/pods/uid/volumes/kubernetes.io~csi/pvc-a/mount xfs rw 0 0
/dev/drbd50001 /var/lib/kubelet/plugins/kubernetes.io/csi/r-drbd.io/def/globalmount xfs rw 0 0
/dev/drbd50003 /mnt/host\\040r0 xfs rw 0 0
";

#[test]
fn mount_table() {
    let m = mountpoints(MOUNTS);
    assert_eq!(
        m[&50000],
        "/var/lib/kubelet/plugins/kubernetes.io/csi/r-drbd.io/abc/globalmount"
    );
    assert_eq!(m[&50003], "/mnt/host r0");
    assert_eq!(m.len(), 3);
    assert_eq!(flush_argv("/m"), argv(&["sync", "-f", "/m"]));
}

/// pvc-a mounted, pvc-b mounted but suspended, pvc-c a block volume (no
/// mount), host-r0 mounted but not ours.
fn flush_tracker() -> Tracker {
    tracker(&[
        &res("pvc-a", "Primary"),
        &dev("pvc-a", 50000),
        "exists resource name:pvc-b role:Primary suspended:quorum force-io-failures:no may_promote:no promotion_score:0",
        &dev("pvc-b", 50001),
        &res("pvc-c", "Primary"),
        &dev("pvc-c", 50002),
        &res("host-r0", "Primary"),
        &dev("host-r0", 50003),
        "exists -",
    ])
}

#[test]
fn flush_only_mounted_owned_unsuspended() {
    let owned: BTreeSet<String> = ["pvc-a", "pvc-b", "pvc-c"].map(String::from).into();
    let none = std::collections::BTreeMap::new();
    let a = (
        "pvc-a".to_owned(),
        "/var/lib/kubelet/plugins/kubernetes.io/csi/r-drbd.io/abc/globalmount".to_owned(),
    );
    assert_eq!(
        flush_plan(&owned, &flush_tracker(), &none, &mountpoints(MOUNTS)),
        std::slice::from_ref(&a)
    );
    // Event stream gone: minors from the resource files, suspension unknown.
    let minors = [("pvc-a", 50000), ("pvc-b", 50001), ("pvc-c", 50002)]
        .map(|(r, m)| (r.to_owned(), m))
        .into();
    let b = (
        "pvc-b".to_owned(),
        "/var/lib/kubelet/plugins/kubernetes.io/csi/r-drbd.io/def/globalmount".to_owned(),
    );
    assert_eq!(
        flush_plan(&owned, &Tracker::default(), &minors, &mountpoints(MOUNTS)),
        [a, b]
    );
    assert_eq!(
        flush_plan(&owned, &Tracker::default(), &none, &mountpoints(MOUNTS)),
        []
    );
    assert_eq!(
        res_minor(
            "resource \"r\" {\n    on \"n1\" {\n        volume 0 {\n            device minor 50001;\n"
        ),
        Some(50001)
    );
    assert_eq!(res_minor("resource \"r\" {}\n"), None);
}

#[tokio::test(start_paused = true)]
async fn guard_flushes_then_forces_in_parallel() {
    let dir = ResDir::new("flush", &["pvc-a.res", "pvc-b.res", "pvc-c.res"]);
    let t = Arc::new(Mutex::new(flush_tracker()));
    // All three demotes must be running at once to pass the barrier.
    let host = GuardHost::new(&t, 3);
    let g = guard(&host, &dir, dir.mounts(MOUNTS), Some("--force=yes"));
    // In a serial run the first demote waits on the barrier until its
    // round's bound and the other two never start: the argv check below
    // catches that, not this timeout.
    tokio::time::timeout(Duration::from_secs(40), g.on_signal())
        .await
        .expect("guard ends within its bounds");
    assert!(g.gate.is_closed());
    let seen = host.seen.lock().unwrap().clone();
    let first_sync = seen.iter().position(|a| a[0] == "sync").unwrap();
    let first_demote = seen.iter().position(|a| a[0] == "drbdsetup").unwrap();
    assert!(first_sync < first_demote, "flush before force: {seen:?}");
    assert_eq!(
        host.seen("sync"),
        [flush_argv(
            "/var/lib/kubelet/plugins/kubernetes.io/csi/r-drbd.io/abc/globalmount"
        )]
    );
    let mut demoted = host.seen("drbdsetup");
    demoted.sort();
    assert_eq!(
        demoted,
        ["pvc-a", "pvc-b", "pvc-c"].map(|r| demote_argv("--force=yes", r))
    );
}

#[tokio::test(start_paused = true)]
async fn failed_flush_still_forces() {
    let dir = ResDir::new("flushfail", &["pvc-a.res"]);
    let t = Arc::new(Mutex::new(flush_tracker()));
    let mut host = GuardHost::new(&t, 1);
    Arc::get_mut(&mut host).unwrap().flush_fails = true;
    let g = guard(&host, &dir, dir.mounts(MOUNTS), Some("--force=yes"));
    g.on_signal().await;
    assert_eq!(host.seen("sync").len(), 1);
    assert_eq!(
        host.seen("drbdsetup"),
        [demote_argv("--force=yes", "pvc-a")]
    );
}

#[tokio::test(start_paused = true)]
async fn still_primary_is_forced_once_more() {
    let dir = ResDir::new("again", &["pvc-a.res"]);
    let t = Arc::new(Mutex::new(tracker(&[&res("pvc-a", "Primary"), "exists -"])));
    let host = GuardHost::new(&t, 1);
    host.sticky.lock().unwrap().insert("pvc-a".into());
    let g = guard(&host, &dir, dir.mounts(""), Some("--force=yes"));
    g.on_signal().await;
    assert_eq!(
        host.seen("drbdsetup"),
        [
            demote_argv("--force=yes", "pvc-a"),
            demote_argv("--force=yes", "pvc-a")
        ]
    );
    assert!(!t.lock().unwrap().resource("pvc-a").unwrap().is_primary());
}

#[tokio::test(start_paused = true)]
async fn guard_waits_for_inflight_csi_calls() {
    let dir = ResDir::new("inflight", &["pvc-a.res"]);
    let t = Arc::new(Mutex::new(tracker(&[&res("pvc-a", "Primary"), "exists -"])));
    let host = GuardHost::new(&t, 1);
    let g = guard(&host, &dir, dir.mounts(""), Some("--force=yes"));
    let busy = g.inflight.hold_for_test("pvc-a");
    let t0 = tokio::time::Instant::now();
    let release = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(busy);
    };
    tokio::join!(g.on_signal(), release);
    assert!(t0.elapsed() >= Duration::from_secs(1));
    assert_eq!(host.seen("drbdsetup").len(), 1);
}

#[tokio::test]
async fn guard_without_force_or_shutdown_touches_nothing() {
    let dir = ResDir::new("noforce", &["pvc-a.res"]);
    let t = Arc::new(Mutex::new(tracker(&[&res("pvc-a", "Primary"), "exists -"])));
    for (busctl, force) in [("b false\n", Some("--force=yes")), ("b true\n", None)] {
        let x = ScriptedExec::new();
        x.expect(preparing_argv(), ok(busctl))
            .expect(system_state_argv(), ok("running\n"));
        let x = Arc::new(x);
        let g = Guard {
            exec: x.clone(),
            tracker: t.clone(),
            res_dir: dir.0.clone(),
            force,
            pods: Box::new(Pods::new(vec![Err("unused".into())])),
            gate: OpenGate::default(),
            inflight: crate::csi::Inflight::default(),
            mounts: dir.mounts(MOUNTS),
        };
        g.on_signal().await;
        x.assert_done();
        assert!(g.gate.is_closed());
    }
}

#[tokio::test(start_paused = true)]
async fn untracked_forces_every_owned_name_twice() {
    let dir = ResDir::new("untracked", &["pvc-a.res", "pvc-b.res"]);
    let t = Arc::new(Mutex::new(Tracker::default()));
    let host = GuardHost::new(&t, 1);
    host.sticky
        .lock()
        .unwrap()
        .extend(["pvc-a".to_owned(), "pvc-b".to_owned()]);
    let g = guard(&host, &dir, dir.mounts(""), Some("--force=yes"));
    g.on_signal().await;
    assert_eq!(
        host.seen("drbdsetup"),
        ["pvc-a", "pvc-b", "pvc-a", "pvc-b"].map(|r| demote_argv("--force=yes", r))
    );
}
