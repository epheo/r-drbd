//! Tokio process runner behind [`Exec`], plus helpers shared by the callers.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

use crate::{Exec, ExecError, ExecMode, Output};

/// Defaults: wait 45 s, then SIGTERM and 15 s, then SIGKILL and 5 s.
pub const DEFAULT_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(45),
    term: Duration::from_secs(15),
    kill: Duration::from_secs(5),
};

/// mkfs on a large thin LV with -K writes only metadata, but leave room.
pub const MKFS_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(300),
    term: Duration::from_secs(15),
    kill: Duration::from_secs(5),
};

#[derive(Debug, Default, Clone)]
pub struct TokioExec {
    prefix: Vec<String>,
}

impl TokioExec {
    /// `prefix` goes in front of every argv at spawn time, e.g.
    /// `[/proc/self/exe, host-exec, <ns>, --]` to run in the host mount
    /// namespace. Logs and errors keep naming the original command.
    pub fn new(prefix: Option<Vec<String>>) -> Self {
        Self {
            prefix: prefix.unwrap_or_default(),
        }
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
        let ExecMode::Timeout { wait, term, kill } = mode else {
            return Err(ExecError::Spawn(
                program,
                std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    format!("exec mode {mode:?} is not implemented yet"),
                ),
            ));
        };
        let started = Instant::now();
        let (spawn_program, spawn_args) = match self.prefix.split_first() {
            Some((first, rest)) => (first, [rest, argv].concat()),
            None => (&program, argv[1..].to_vec()),
        };
        let mut child = Command::new(spawn_program)
            .args(&spawn_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| ExecError::Spawn(program.clone(), e))?;
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
                return Err(ExecError::Spawn(program, e));
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

    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::{Exec, ExecError, ExecMode, Output};

    type Step = (Vec<String>, Result<Output, String>);

    #[derive(Debug, Default)]
    pub(crate) struct ScriptedExec {
        steps: Mutex<VecDeque<Step>>,
        pub(crate) seen: Mutex<Vec<Vec<String>>>,
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
            let left = self.steps.lock().unwrap();
            assert!(left.is_empty(), "unconsumed scripted steps: {left:?}");
        }
    }

    #[async_trait::async_trait]
    impl Exec for ScriptedExec {
        async fn run(&self, argv: &[String], _mode: ExecMode) -> Result<Output, ExecError> {
            self.seen.lock().unwrap().push(argv.to_vec());
            let next = self.steps.lock().unwrap().pop_front();
            let Some((want, out)) = next else {
                panic!("unexpected command (script empty): {argv:?}");
            };
            assert_eq!(argv, want.as_slice(), "unexpected command");
            out.map_err(|e| ExecError::Spawn(argv[0].clone(), std::io::Error::other(e)))
        }
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
    async fn unsupported_modes() {
        let r = TokioExec::default()
            .run(&argv(&["true"]), ExecMode::Daemon)
            .await;
        assert!(matches!(r, Err(ExecError::Spawn(..))));
    }
}
