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
    // kubelet's critical pod grace, with room left for the drain.
    assert!(GUARD_MAX < Duration::from_secs(30));
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

#[tokio::test]
async fn guard_demotes_owned_primaries_in_parallel() {
    let dir = std::env::temp_dir().join(format!("r-drbd-guard-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("pvc-a.res"), "").unwrap();
    let t = Arc::new(Mutex::new(tracker(&[
        &res("pvc-a", "Primary"),
        &res("host-r0", "Primary"),
        "exists -",
    ])));
    let x = ScriptedExec::new();
    x.expect(preparing_argv(), ok("b true\n"))
        .expect(system_state_argv(), ok("running\n"))
        .expect(demote_argv("--force=yes", "pvc-a"), ok(""));
    let x = Arc::new(x);
    let gate = OpenGate::default();
    let guard = Guard {
        exec: x.clone(),
        tracker: t.clone(),
        res_dir: dir.clone(),
        force: Some("--force=yes"),
        pods: Box::new(Pods::new(vec![Err("unused".into())])),
        gate: gate.clone(),
    };
    let feeder = {
        let t = t.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            t.lock()
                .unwrap()
                .feed("change resource name:pvc-a role:Secondary")
                .unwrap();
        })
    };
    guard.on_signal().await;
    feeder.await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    x.assert_done();
    assert!(gate.is_closed());
    assert!(!t.lock().unwrap().resource("pvc-a").unwrap().is_primary());
    assert!(t.lock().unwrap().resource("host-r0").unwrap().is_primary());
}

#[tokio::test]
async fn guard_without_force_or_shutdown_touches_nothing() {
    let t = Arc::new(Mutex::new(tracker(&[&res("pvc-a", "Primary"), "exists -"])));
    for (busctl, force) in [("b false\n", Some("--force=yes")), ("b true\n", None)] {
        let x = ScriptedExec::new();
        x.expect(preparing_argv(), ok(busctl))
            .expect(system_state_argv(), ok("running\n"));
        let x = Arc::new(x);
        let guard = Guard {
            exec: x.clone(),
            tracker: t.clone(),
            res_dir: std::path::PathBuf::from("/nonexistent"),
            force,
            pods: Box::new(Pods::new(vec![Err("unused".into())])),
            gate: OpenGate::default(),
        };
        guard.on_signal().await;
        x.assert_done();
        assert!(guard.gate.is_closed());
    }
}
