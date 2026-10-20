//! Tokio process runner behind [`Exec`], plus helpers shared by the callers.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

use crate::{Exec, ExecError, ExecMode, Output};

/// Kernel-facing tools (drbdadm, blockdev, blkid, mount): milliseconds,
/// unless a device or a DRBD state change holds them for tens of seconds.
/// Past a minute the device is stuck and the next pass retries.
/// Stopping them leaves nothing half done, so the grace is short.
pub const DEFAULT_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(60),
    term: Duration::from_secs(10),
    kill: Duration::from_secs(5),
};

/// lvs and vgs read one VG's metadata from a few devices and wait only on
/// the VG lock. The agent runs one LVM command at a time, so only an
/// outside command holds that lock; 30 s means the VG is stuck.
pub const LVM_REPORT_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(30),
    term: Duration::from_secs(10),
    kill: Duration::from_secs(5),
};

/// lvcreate, lvextend, lvremove and lvchange suspend and resume the thin
/// pool, commit its metadata with synchronous writes and wait for udev;
/// on a pool busy with writes each step queues behind them.
/// `Finish`: a SIGKILL while the pool is suspended leaves every LV in it
/// hanging, so the process is never killed; 90 s bounds only how long the
/// caller waits for it.
pub const LVM_CHANGE_TIMEOUT: ExecMode = ExecMode::Finish {
    wait: LVM_CHANGE_WAIT,
};
pub const LVM_CHANGE_WAIT: Duration = Duration::from_secs(90);

/// After a `Finish` process exits, how long a grandchild may hold its pipes.
const FINISH_PIPE_DRAIN: Duration = Duration::from_secs(5);

/// mkfs.xfs -K skips the discard but still writes the log (up to 2 GiB) and
/// every AG header, through DRBD protocol C at replication link speed:
/// 2 GiB at a degraded 10 MB/s is about 200 s, doubled for margin.
/// Stopping it is harmless: the mkfs label marks the format unfinished.
pub const MKFS_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(420),
    term: Duration::from_secs(10),
    kill: Duration::from_secs(5),
};

/// Run from the image even when a host prefix is set: the image carries
/// drbd-utils, so the host needs only the kernel module.
const IMAGE_TOOLS: [&str; 3] = ["drbdadm", "drbdsetup", "drbdmeta"];

/// The prefix for [`TokioExec::new`] that runs each command in the mount
/// namespace under `ns` (e.g. /proc/1/ns) through the host-exec role of the
/// r-drbd binary `exe`.
pub fn host_exec_prefix(exe: &str, ns: &str) -> Vec<String> {
    argv(&[exe, "host-exec", ns, "--"])
}

#[derive(Debug, Clone)]
pub struct TokioExec {
    prefix: Vec<String>,
    /// Count of `Finish` processes still running.
    finishing: Arc<tokio::sync::watch::Sender<usize>>,
}

impl Default for TokioExec {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Counts one `Finish` process until it is reaped, whatever ends the task.
struct Finishing(Arc<tokio::sync::watch::Sender<usize>>);

impl Drop for Finishing {
    fn drop(&mut self) {
        self.0.send_modify(|n| *n = n.saturating_sub(1));
    }
}

impl TokioExec {
    /// `prefix` goes in front of every argv at spawn time, e.g.
    /// [`host_exec_prefix`] to run in the host mount namespace.
    /// Logs and errors keep naming the original command.
    pub fn new(prefix: Option<Vec<String>>) -> Self {
        Self {
            prefix: prefix.unwrap_or_default(),
            finishing: Arc::new(tokio::sync::watch::channel(0).0),
        }
    }

    async fn finish(&self, cmd: Spawn, wait: Duration) -> Result<Output, ExecError> {
        let argv = cmd.argv.clone();
        self.finishing.send_modify(|n| *n += 1);
        let guard = Finishing(self.finishing.clone());
        // Spawned: dropping this future drops only the JoinHandle, never the
        // task that owns and reaps the child.
        let mut task = tokio::spawn(async move {
            let _guard = guard;
            cmd.finish().await
        });
        if let Ok(joined) = tokio::time::timeout(wait, &mut task).await {
            return joined.unwrap_or_else(|e| {
                Err(ExecError::Spawn(
                    argv[0].clone(),
                    std::io::Error::other(e.to_string()),
                ))
            });
        }
        // Never signalled: the child may already be reaped (pipe drain), and
        // with hostPID a recycled pid can be any host process. The command
        // may still complete after this error; callers re-read on the next pass.
        tracing::warn!(argv = ?argv, ?wait, "command still running after its wait, left to finish");
        Err(ExecError::Timeout(wait, argv.join(" ")))
    }

    /// True once no `Finish` process runs, false if some still do after `limit`.
    pub async fn wait_idle(&self, limit: Duration) -> bool {
        let mut rx = self.finishing.subscribe();
        tokio::time::timeout(limit, rx.wait_for(|n| *n == 0))
            .await
            .is_ok_and(|r| r.is_ok())
    }
}

#[async_trait::async_trait]
impl Exec for TokioExec {
    async fn run(&self, argv: &[String], mode: ExecMode) -> Result<Output, ExecError> {
        let Some(program) = argv.first().cloned() else {
            return Err(ExecError::Spawn(
                String::new(),
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "empty argv"),
            ));
        };
        let prefix = if IMAGE_TOOLS.contains(&program.as_str()) {
            &[][..]
        } else {
            &self.prefix[..]
        };
        let (spawn_program, spawn_args) = match prefix.split_first() {
            Some((first, rest)) => (first.clone(), [rest, argv].concat()),
            None => (program, argv[1..].to_vec()),
        };
        let cmd = Spawn {
            argv: argv.to_vec(),
            program: spawn_program,
            args: spawn_args,
        };
        match mode {
            ExecMode::Timeout { wait, term, kill } => cmd.supervise(wait, term, kill).await,
            ExecMode::Finish { wait } => self.finish(cmd, wait).await,
            _ => Err(ExecError::Spawn(
                argv[0].clone(),
                std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    format!("exec mode {mode:?} is not implemented yet"),
                ),
            )),
        }
    }
}

/// One command: the argv logs and errors name, and what is spawned.
struct Spawn {
    argv: Vec<String>,
    program: String,
    args: Vec<String>,
}

impl Spawn {
    /// Runs to the end, never signalled.
    async fn finish(self) -> Result<Output, ExecError> {
        let name = self.argv[0].clone();
        let started = Instant::now();
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false)
            .spawn()
            .map_err(|e| ExecError::Spawn(name.clone(), e))?;
        let stdout = Pipe::spawn(child.stdout.take());
        let stderr = Pipe::spawn(child.stderr.take());
        let status = child.wait().await.map_err(|e| {
            stdout.abort();
            stderr.abort();
            ExecError::Spawn(name, e)
        })?;
        let out = Output {
            exit_code: status.code(),
            stdout: stdout.finish(FINISH_PIPE_DRAIN).await,
            stderr: stderr.finish(FINISH_PIPE_DRAIN).await,
            duration: started.elapsed(),
        };
        tracing::debug!(argv = ?self.argv, exit = ?out.exit_code, duration = ?out.duration, "exec");
        Ok(out)
    }

    async fn supervise(
        self,
        wait: Duration,
        term: Duration,
        kill: Duration,
    ) -> Result<Output, ExecError> {
        let argv = self.argv;
        let name = argv[0].clone();
        let started = Instant::now();
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| ExecError::Spawn(name.clone(), e))?;
        let stdout = Pipe::spawn(child.stdout.take());
        let stderr = Pipe::spawn(child.stderr.take());

        let status = tokio::select! {
            status = child.wait() => status,
            () = tokio::time::sleep(wait) => {
                terminate(&mut child, term, kill).await;
                stdout.abort();
                stderr.abort();
                tracing::warn!(argv = ?argv, ?wait, "command timed out");
                return Err(ExecError::Timeout(wait, argv.join(" ")));
            }
        };
        let status = match status {
            Ok(s) => s,
            Err(e) => {
                stdout.abort();
                stderr.abort();
                return Err(ExecError::Spawn(name, e));
            }
        };
        // A grandchild can keep the pipes open after the child exits.
        let grace = term + kill;
        let out = Output {
            exit_code: status.code(),
            stdout: stdout.finish(grace).await,
            stderr: stderr.finish(grace).await,
            duration: started.elapsed(),
        };
        tracing::debug!(argv = ?argv, exit = ?out.exit_code, duration = ?out.duration, "exec");
        Ok(out)
    }
}

/// Reader of one pipe; what it read survives an abort.
struct Pipe {
    buf: Arc<Mutex<Vec<u8>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Pipe {
    fn spawn<R>(pipe: Option<R>) -> Self
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = buf.clone();
        let task = tokio::spawn(async move {
            let Some(mut p) = pipe else { return };
            let mut chunk = [0u8; 8192];
            while let Ok(n) = p.read(&mut chunk).await {
                if n == 0 {
                    break;
                }
                if let Ok(mut b) = sink.lock() {
                    b.extend_from_slice(&chunk[..n]);
                }
            }
        });
        Self { buf, task }
    }

    fn abort(&self) {
        self.task.abort();
    }

    async fn finish(mut self, grace: Duration) -> String {
        if tokio::time::timeout(grace, &mut self.task).await.is_err() {
            self.task.abort();
            tracing::warn!("output pipe still open after the command exited");
        }
        let bytes = self.buf.lock().map(|b| b.clone()).unwrap_or_default();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

async fn terminate(child: &mut Child, term: Duration, kill: Duration) {
    let pid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(rustix::process::Pid::from_raw);
    if let Some(pid) = pid {
        let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
        if tokio::time::timeout(term, child.wait()).await.is_ok() {
            return;
        }
    }
    let _ = child.start_kill();
    let _ = tokio::time::timeout(kill, child.wait()).await;
}

/// A command that ran but exited non-zero, or could not run at all.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{program}: {message}")]
pub struct CmdError {
    pub program: String,
    pub message: String,
}

impl CmdError {
    pub fn new(program: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            message: message.into(),
        }
    }
}

/// Run and require exit 0; the error carries stderr (stdout when stderr is empty).
pub async fn run_ok(exec: &dyn Exec, argv: &[String], mode: ExecMode) -> Result<Output, CmdError> {
    let out = run_any(exec, argv, mode).await?;
    if out.exit_code == Some(0) {
        Ok(out)
    } else {
        Err(failure(argv, &out))
    }
}

/// Run and return the output whatever the exit code; only spawn and timeout fail.
pub async fn run_any(exec: &dyn Exec, argv: &[String], mode: ExecMode) -> Result<Output, CmdError> {
    exec.run(argv, mode)
        .await
        .map_err(|e| CmdError::new(program(argv), e.to_string()))
}

pub fn failure(argv: &[String], out: &Output) -> CmdError {
    let text = if out.stderr.trim().is_empty() {
        out.stdout.trim()
    } else {
        out.stderr.trim()
    };
    let code = out
        .exit_code
        .map_or_else(|| "signal".to_owned(), |c| c.to_string());
    CmdError::new(program(argv), format!("exit {code}: {text}"))
}

fn program(argv: &[String]) -> String {
    argv.first().cloned().unwrap_or_default()
}

pub fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|&s| s.to_owned()).collect()
}

#[cfg(test)]
pub(crate) mod scripted {
    //! Test double: expects argv in order, returns scripted outputs.
    //! An unexpected command is recorded, not panicked on: the CSI node
    //! runs its ops in a spawned task, which turns a panic into an ordinary
    //! Internal status a test may well expect.

    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::{Exec, ExecError, ExecMode, Output};

    type Step = (Vec<String>, Result<Output, String>);

    #[derive(Debug, Default)]
    pub(crate) struct ScriptedExec {
        steps: Mutex<VecDeque<Step>>,
        pub(crate) seen: Mutex<Vec<Vec<String>>>,
        unexpected: Mutex<Vec<String>>,
    }

    pub(crate) fn ok(stdout: &str) -> Result<Output, String> {
        code(0, stdout, "")
    }

    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn code(exit: i32, stdout: &str, stderr: &str) -> Result<Output, String> {
        Ok(Output {
            exit_code: Some(exit),
            stdout: stdout.to_owned(),
            stderr: stderr.to_owned(),
            duration: Duration::ZERO,
        })
    }

    impl ScriptedExec {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn expect(&self, argv: Vec<String>, out: Result<Output, String>) -> &Self {
            self.steps.lock().unwrap().push_back((argv, out));
            self
        }

        pub(crate) fn expect_s(&self, argv: &[&str], out: Result<Output, String>) -> &Self {
            self.expect(argv.iter().map(|&s| s.to_owned()).collect(), out)
        }

        pub(crate) fn assert_done(&self) {
            self.assert_none_unexpected();
            let left = self.steps.lock().unwrap();
            assert!(left.is_empty(), "unconsumed scripted steps: {left:?}");
        }

        fn assert_none_unexpected(&self) {
            let bad = self
                .unexpected
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(bad.is_empty(), "unexpected commands: {bad:#?}");
        }
    }

    /// Catches a test that never calls `assert_done`.
    impl Drop for ScriptedExec {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                self.assert_none_unexpected();
            }
        }
    }

    /// LVM commands that change metadata may suspend a thin pool; a kill
    /// then hangs every LV in it, so they must run in `Finish` mode.
    const LVM_CHANGES: &[&str] = &[
        "lvcreate",
        "lvextend",
        "lvremove",
        "lvchange",
        "lvconvert",
        "vgcreate",
        "vgextend",
        "vgreduce",
        "vgremove",
        "pvcreate",
        "pvremove",
    ];

    #[async_trait::async_trait]
    impl Exec for ScriptedExec {
        async fn run(&self, argv: &[String], mode: ExecMode) -> Result<Output, ExecError> {
            self.seen.lock().unwrap().push(argv.to_vec());
            let change = argv
                .first()
                .is_some_and(|p| LVM_CHANGES.contains(&p.as_str()));
            if change != matches!(mode, ExecMode::Finish { .. }) {
                self.unexpected
                    .lock()
                    .unwrap()
                    .push(format!("{argv:?} in mode {mode:?}"));
            }
            let mut steps = self.steps.lock().unwrap();
            let why = match steps.front() {
                Some((want, _)) if argv == want.as_slice() => {
                    let (_, out) = steps.pop_front().expect("front");
                    return out
                        .map_err(|e| ExecError::Spawn(argv[0].clone(), std::io::Error::other(e)));
                }
                // Left in place: the order check goes on for later commands
                // and `assert_done` reports it as unconsumed.
                Some((want, _)) => format!("{argv:?}, expected {want:?}"),
                None => format!("{argv:?}, script empty"),
            };
            drop(steps);
            self.unexpected.lock().unwrap().push(why);
            Err(ExecError::Spawn(
                argv[0].clone(),
                std::io::Error::other("unexpected command"),
            ))
        }
    }

    #[tokio::test]
    #[should_panic(expected = "unexpected commands")]
    async fn wrong_command_fails_assert_done() {
        let x = ScriptedExec::new();
        x.expect_s(&["blkid", "/dev/x"], ok(""));
        assert!(
            x.run(
                &super::argv(&["mkfs.xfs", "/dev/x"]),
                super::DEFAULT_TIMEOUT
            )
            .await
            .is_err()
        );
        // The expected step stays first, so the order check goes on.
        x.run(&super::argv(&["blkid", "/dev/x"]), super::DEFAULT_TIMEOUT)
            .await
            .unwrap();
        x.assert_done();
    }

    #[tokio::test]
    #[should_panic(expected = "unexpected commands")]
    async fn lvm_change_outside_finish_fails() {
        let x = ScriptedExec::new();
        x.expect_s(&["lvremove", "vg/lv"], ok(""));
        x.run(&super::argv(&["lvremove", "vg/lv"]), super::DEFAULT_TIMEOUT)
            .await
            .unwrap();
        x.assert_done();
    }

    #[tokio::test]
    #[should_panic(expected = "unexpected commands")]
    async fn extra_command_fails_on_drop() {
        let x = ScriptedExec::new();
        let _ = x
            .run(
                &super::argv(&["mkfs.xfs", "/dev/x"]),
                super::DEFAULT_TIMEOUT,
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_and_captures() {
        let out = TokioExec::default()
            .run(
                &argv(&["sh", "-c", "echo out; echo err >&2; exit 3"]),
                DEFAULT_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(out.exit_code, Some(3));
        assert_eq!(out.stdout, "out\n");
        assert_eq!(out.stderr, "err\n");
        let e = failure(&argv(&["sh"]), &out);
        assert_eq!(e.to_string(), "sh: exit 3: err");
    }

    #[tokio::test]
    async fn timeout_terminates() {
        let mode = ExecMode::Timeout {
            wait: Duration::from_millis(100),
            term: Duration::from_millis(500),
            kill: Duration::from_millis(500),
        };
        let started = Instant::now();
        let r = TokioExec::default()
            .run(&argv(&["sleep", "30"]), mode)
            .await;
        assert!(matches!(r, Err(ExecError::Timeout(..))));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn hung_pipe_is_bounded() {
        let mode = ExecMode::Timeout {
            wait: Duration::from_secs(10),
            term: Duration::from_millis(200),
            kill: Duration::from_millis(200),
        };
        let started = Instant::now();
        let out = TokioExec::default()
            .run(&argv(&["sh", "-c", "sleep 5 & echo hi"]), mode)
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(3));
        assert_eq!(out.exit_code, Some(0));
        assert_eq!(out.stdout, "hi\n");
    }

    #[test]
    fn host_exec_prefix_shape() {
        assert_eq!(
            host_exec_prefix("/r-drbd", "/proc/1/ns"),
            ["/r-drbd", "host-exec", "/proc/1/ns", "--"]
        );
    }

    #[tokio::test]
    async fn prefix_is_prepended_once() {
        let x = TokioExec::new(Some(argv(&["echo", "PREFIX"])));
        let out = x
            .run(&argv(&["lvs", "vg1"]), DEFAULT_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(out.stdout, "PREFIX lvs vg1\n");
        let out = TokioExec::new(None)
            .run(&argv(&["echo", "plain"]), DEFAULT_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(out.stdout, "plain\n");
    }

    #[tokio::test]
    async fn drbd_tools_skip_the_prefix() {
        let x = TokioExec::new(Some(argv(&["echo", "PREFIX"])));
        let r = x
            .run(&argv(&["drbdsetup", "--version"]), DEFAULT_TIMEOUT)
            .await;
        assert!(
            !matches!(&r, Ok(out) if out.stdout.starts_with("PREFIX")),
            "{r:?}"
        );
    }

    #[tokio::test]
    async fn prefix_never_shows_in_errors() {
        let x = TokioExec::new(Some(argv(&["/nonexistent/r-drbd", "host-exec"])));
        match x.run(&argv(&["lvs", "vg1"]), DEFAULT_TIMEOUT).await {
            Err(ExecError::Spawn(program, _)) => assert_eq!(program, "lvs"),
            r => panic!("spawn error expected: {r:?}"),
        }
        let mode = ExecMode::Timeout {
            wait: Duration::from_millis(100),
            term: Duration::from_millis(500),
            kill: Duration::from_millis(500),
        };
        let x = TokioExec::new(Some(argv(&["env"])));
        match x.run(&argv(&["sleep", "30"]), mode).await {
            Err(ExecError::Timeout(_, cmd)) => assert_eq!(cmd, "sleep 30"),
            r => panic!("timeout expected: {r:?}"),
        }
    }

    #[tokio::test]
    async fn finish_outlives_a_dropped_caller_and_its_wait() {
        let dir = std::env::temp_dir().join(format!("r-drbd-finish-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let x = TokioExec::default();
        let mode = ExecMode::Finish {
            wait: Duration::from_secs(10),
        };
        // Dropped caller.
        let done = dir.join("done");
        let script = format!("sleep 0.5; touch {}", done.display());
        let cut = tokio::time::timeout(
            Duration::from_millis(100),
            x.run(&argv(&["sh", "-c", &script]), mode),
        )
        .await;
        assert!(cut.is_err(), "caller dropped before the end");
        assert!(!x.wait_idle(Duration::from_millis(10)).await);
        assert!(x.wait_idle(Duration::from_secs(5)).await);
        assert!(done.exists(), "the process ran to its end");

        // Past its wait: a Timeout error, never a signal, so a reaped pid
        // is never signalled either.
        let late = dir.join("late");
        let termed = dir.join("termed");
        let script = format!(
            "trap 'touch {}' TERM; sleep 0.5; touch {}",
            termed.display(),
            late.display()
        );
        let mode = ExecMode::Finish {
            wait: Duration::from_millis(100),
        };
        let r = x.run(&argv(&["sh", "-c", &script]), mode).await;
        assert!(matches!(r, Err(ExecError::Timeout(..))), "{r:?}");
        assert!(x.wait_idle(Duration::from_secs(5)).await);
        assert!(late.exists(), "still completed after the error");
        assert!(!termed.exists(), "no signal after the wait");

        // The same under Timeout is killed with the caller.
        let gone = dir.join("gone");
        let script = format!("sleep 0.5; touch {}", gone.display());
        let _ = tokio::time::timeout(
            Duration::from_millis(100),
            x.run(&argv(&["sh", "-c", &script]), DEFAULT_TIMEOUT),
        )
        .await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!gone.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unsupported_modes() {
        let r = TokioExec::default()
            .run(&argv(&["true"]), ExecMode::Daemon)
            .await;
        assert!(matches!(r, Err(ExecError::Spawn(..))));
    }
}
