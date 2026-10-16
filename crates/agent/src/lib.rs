//! r-drbd agent: one per node. Reconciles thin LVM and DRBD from the
//! r-drbd.io Pool, Volume and Snapshot objects, writes this node's status
//! entries with server-side apply, keeps the heartbeat Lease and serves the
//! CSI node plugin.
//!
//! Pure helpers: crates/lvm, crates/drbd-config. CRD types: crates/model.
//!
//! Every external command goes through [`Exec`], so tests script outputs.

#![forbid(unsafe_code)]

pub mod csi;
pub mod drbd;
pub mod events2;
pub mod exec;
pub mod node;
pub mod reconcile;
pub mod retry;
pub mod storage;
pub mod watch;

mod run;

pub use run::{Config, Error, run};

use std::time::Duration;

/// How an external command is supervised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecMode {
    /// Fixed timeout, then terminate, then kill.
    Timeout {
        wait: Duration,
        term: Duration,
        kill: Duration,
    },
    /// Timeout restarts while the process or its devices show IO
    /// (create-md, dd copies).
    IoProgress { wait: Duration },
    /// Detached, no timeout (clone copies).
    Daemon,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Output {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration: Duration,
}

/// Process execution boundary so every executor can be tested with
/// scripted outputs. stdin is always /dev/null; argv is never a shell line.
#[async_trait::async_trait]
pub trait Exec: Send + Sync {
    async fn run(&self, argv: &[String], mode: ExecMode) -> Result<Output, ExecError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("spawn {0}: {1}")]
    Spawn(String, std::io::Error),
    #[error("timeout after {0:?}: {1}")]
    Timeout(Duration, String),
}
