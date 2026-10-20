//! DRBD mechanics for replicated volumes (spec.replicas >= 2): command
//! lines, ownership tags, resource files, state derivation and the events2
//! process. The per-volume decisions live in `reconcile::replicated`.

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drbd_config::{Disk, Features, Host as FileHost, Resource as FileResource};
use model::{AssignedReplica, Assignment, ReplicaStatus, Role};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::ExecMode;
use crate::events2::{Resource, Tracker};
use crate::watch::Trigger;

pub use drbd_config::VOLUME;

/// create-md writes the activity log and zeroes the bitmap: 32 MiB per
/// TiB of volume per peer slot, 192 MiB per TiB with 6 slots. At 20 MiB/s,
/// a slow thin pool allocating every chunk it writes, two minutes covers
/// a 12 TiB volume.
pub const CREATE_MD_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(120),
    term: Duration::from_secs(10),
    kill: Duration::from_secs(5),
};

/// drbdadm only parses files and talks netlink here; a hang means a stuck
/// kernel state change, which a retry next pass handles better than a wait.
pub const DRBD_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(30),
    term: Duration::from_secs(5),
    kill: Duration::from_secs(5),
};

pub fn device_path(minor: u32) -> String {
    format!("/dev/drbd{minor}")
}

/// Ownership of the backing LV's DRBD metadata: the Volume UID and this
/// node's DRBD node id. A Volume reusing a name has another UID, so it
/// never takes over an old LV's metadata.
pub const MD_TAG_PREFIX: &str = "r-drbd.md.";

pub fn md_tag(volume_uid: &str, node_id: u8) -> String {
    format!("{MD_TAG_PREFIX}{volume_uid}.{node_id}")
}

pub fn is_md_tag(tag: &str) -> bool {
    tag.starts_with(MD_TAG_PREFIX)
}

/// Host access that is not an external command, behind a trait so the
/// reconciler tests can fake it.
#[async_trait::async_trait]
pub trait Host: Send + Sync + std::fmt::Debug {
    /// /sys/block/drbd<minor>/size, 512 byte sectors.
    fn device_sectors(&self, minor: u32) -> std::io::Result<u64>;
    /// Bind test with SO_REUSEADDR; an address not assigned here
    /// falls back to the wildcard.
    fn port_in_use(&self, addr: IpAddr, port: u16) -> bool;
    /// A TCP connect from `from` (any port), closed at once.
    async fn connect(
        &self,
        from: Option<IpAddr>,
        to: SocketAddr,
        timeout: Duration,
    ) -> std::io::Result<()>;
}

#[derive(Debug, Default)]
pub struct RealHost;

#[async_trait::async_trait]
impl Host for RealHost {
    fn device_sectors(&self, minor: u32) -> std::io::Result<u64> {
        let text = std::fs::read_to_string(format!("/sys/block/drbd{minor}/size"))?;
        text.trim()
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    fn port_in_use(&self, addr: IpAddr, port: u16) -> bool {
        // std sets SO_REUSEADDR on Unix listeners, as the bind test needs.
        match std::net::TcpListener::bind((addr, port)) {
            Ok(_) => false,
            Err(e) if e.kind() == std::io::ErrorKind::AddrNotAvailable => {
                let any: IpAddr = if addr.is_ipv4() {
                    std::net::Ipv4Addr::UNSPECIFIED.into()
                } else {
                    std::net::Ipv6Addr::UNSPECIFIED.into()
                };
                std::net::TcpListener::bind((any, port)).is_err()
            }
            Err(_) => true,
        }
    }

    async fn connect(
        &self,
        from: Option<IpAddr>,
        to: SocketAddr,
        timeout: Duration,
    ) -> std::io::Result<()> {
        let socket = if to.is_ipv4() {
            tokio::net::TcpSocket::new_v4()?
        } else {
            tokio::net::TcpSocket::new_v6()?
        };
        if let Some(ip) = from {
            socket
                .bind(SocketAddr::new(ip, 0))
                .map_err(|e| std::io::Error::new(e.kind(), format!("bind {ip}: {e}")))?;
        }
        match tokio::time::timeout(timeout, socket.connect(to)).await {
            Ok(r) => r.map(drop),
            Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
        }
    }
}

/// Command lines, program first, options before positional names.
pub mod argv {
    use super::VOLUME;
    use crate::exec::argv as a;

    fn vol(res: &str) -> String {
        format!("{res}/{VOLUME}")
    }

    /// --force: runs only on an LV whose tags prove it fresh or stale, and
    /// drbdmeta would otherwise ask before overwriting metadata and, with
    /// no terminal, cancel. --max-peers: the bitmap slots.
    pub fn create_md(res: &str, peer_slots: u8) -> Vec<String> {
        a(&[
            "drbdadm",
            "create-md",
            "--force",
            &format!("--max-peers={peer_slots}"),
            &vol(res),
        ])
    }

    pub fn adjust(res: &str) -> Vec<String> {
        a(&["drbdadm", "adjust", res])
    }

    /// Prints the commands adjust would run, nothing when converged.
    pub fn adjust_dry_run(res: &str) -> Vec<String> {
        a(&["drbdadm", "--dry-run", "adjust", res])
    }

    /// Parses `candidate` in place of `installed` together with the rest of
    /// the host's DRBD configuration, so a clash with another resource's
    /// minor fails here too.
    pub fn validate(candidate: &str, installed: &str, res: &str) -> Vec<String> {
        a(&[
            "drbdadm",
            &format!("--config-to-test={candidate}"),
            &format!("--config-to-exclude={installed}"),
            "dump",
            res,
        ])
    }

    /// Every diskful replica is connected and just created: they all hold
    /// the same nothing, so none syncs.
    pub fn start_without_sync(res: &str) -> Vec<String> {
        a(&["drbdadm", "new-current-uuid", "--clear-bitmap", &vol(res)])
    }

    /// This replica's data becomes current and every peer full-syncs from it.
    pub fn start_as_source(res: &str) -> Vec<String> {
        a(&["drbdadm", "new-current-uuid", "--force-resync", &vol(res)])
    }

    /// drbdsetup needs no resource file: down also works after the file is gone.
    pub fn down(res: &str) -> Vec<String> {
        a(&["drbdsetup", "down", res])
    }

    /// drbdsetup: the departed peer is no longer in the file drbdadm reads.
    pub fn del_peer(res: &str, peer_node_id: u8) -> Vec<String> {
        a(&["drbdsetup", "del-peer", res, &peer_node_id.to_string()])
    }

    /// Frees the departed peer's bitmap slot.
    pub fn forget_peer(res: &str, peer_node_id: u8) -> Vec<String> {
        a(&["drbdsetup", "forget-peer", res, &peer_node_id.to_string()])
    }

    /// The metadata superblock, peer slots included, read from the backing
    /// device. --force: the attached kernel holds the device exclusively,
    /// and drbdmeta asks before reading it anyway.
    pub fn dump_superblock(minor: u32, dev: &str) -> Vec<String> {
        a(&[
            "drbdmeta",
            "--force",
            &minor.to_string(),
            "v09",
            dev,
            "internal",
            "dump-superblock",
            "--output-format=json",
        ])
    }

    pub fn suspend_io(res: &str) -> Vec<String> {
        a(&["drbdadm", "suspend-io", res])
    }

    pub fn resume_io(res: &str) -> Vec<String> {
        a(&["drbdadm", "resume-io", res])
    }

    /// --assume-clean: the grown part of every thin backing LV reads as
    /// zeros where no old metadata lies, so there is nothing to sync. Not
    /// so for a restore growing from its snapshot's size: see
    /// [`resize_synced`].
    pub fn resize(res: &str) -> Vec<String> {
        a(&["drbdadm", "resize", "--assume-clean", &vol(res)])
    }

    /// A resize whose new area syncs: it holds each node's old metadata.
    /// Zero blocks travel as discards (rs-discard-granularity).
    pub fn resize_synced(res: &str) -> Vec<String> {
        a(&["drbdadm", "resize", &vol(res)])
    }

    pub fn disconnect(res: &str) -> Vec<String> {
        a(&["drbdadm", "disconnect", res])
    }

    pub fn connect_discard(res: &str) -> Vec<String> {
        a(&["drbdadm", "connect", "--discard-my-data", res])
    }

    /// One connection only: the survivor may be connected to others.
    pub fn connect_peer(res: &str, peer: &str) -> Vec<String> {
        a(&["drbdadm", "connect", &format!("{res}:{peer}")])
    }

    /// Exit 0 while the resource exists.
    pub fn status(res: &str) -> Vec<String> {
        a(&["drbdsetup", "status", res])
    }

    pub fn status_json(res: &str) -> Vec<String> {
        a(&["drbdsetup", "status", "--json", res])
    }

    pub fn events2() -> Vec<String> {
        a(&["drbdsetup", "events2", "all"])
    }

    /// Usage text of one drbdsetup command, read for the options it knows.
    pub fn help(command: &str) -> Vec<String> {
        a(&["drbdsetup", "help", command])
    }
}

/// Node ids that hold a bitmap slot in `drbdmeta dump-superblock` JSON:
/// the peers array is indexed by node id, a slot index of -1 is none.
pub fn slot_holders(stdout: &str) -> Result<Vec<u8>, String> {
    // drbdmeta may print notices before the document.
    let json = stdout
        .find('{')
        .map(|i| &stdout[i..])
        .ok_or("no superblock in drbdmeta output")?;
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("superblock JSON: {e}"))?;
    let peers = v["peers"].as_array().ok_or("superblock without peers")?;
    Ok(peers
        .iter()
        .enumerate()
        .filter(|(_, p)| p["bitmap_index"].as_i64().is_some_and(|i| i >= 0))
        .filter_map(|(id, _)| u8::try_from(id).ok())
        .collect())
}

/// (drbdsetup command, option) for each optional setting.
pub const FEATURE_PROBES: [(&str, &str); 3] = [
    ("peer-device-options", "--peer-tiebreaker="),
    ("attach", "--rs-discard-granularity="),
    ("resource-options", "--on-suspended-primary-outdated="),
];

/// Features from the `drbdsetup help` texts, in [`FEATURE_PROBES`] order.
pub fn features(help: [&str; 3]) -> Features {
    let has = |i: usize| help[i].contains(FEATURE_PROBES[i].1);
    Features {
        tiebreaker: has(0),
        discard_granularity: has(1),
        suspended_primary_outdated: has(2),
    }
}

/// Inputs of one resource file beyond the assignment.
#[derive(Debug, Clone)]
pub struct FileInputs<'a> {
    pub node: &'a str,
    /// The name of this host's `on` section; peers keep their node names.
    pub host_name: &'a str,
    pub volume: &'a str,
    pub assignment: &'a Assignment,
    /// Backing LV path when this replica is diskful.
    pub backing: Option<&'a str>,
    pub addresses: &'a BTreeMap<String, IpAddr>,
    pub chunk_bytes: Option<u64>,
    pub access_mode: model::AccessMode,
}

pub fn is_diskful(r: &AssignedReplica) -> bool {
    r.role == Role::Diskful
}

/// Two diskless replicas never connect.
pub fn connects(a: &AssignedReplica, b: &AssignedReplica) -> bool {
    is_diskful(a) || is_diskful(b)
}

/// The nodes this replica keeps a connection to.
pub fn peers<'a>(a: &'a Assignment, me: &'a AssignedReplica) -> Vec<&'a AssignedReplica> {
    a.replicas
        .iter()
        .filter(|r| r.node != me.node && connects(me, r))
        .collect()
}

pub(crate) fn socket(
    node: &str,
    port: Option<u16>,
    addresses: &BTreeMap<String, IpAddr>,
) -> Result<SocketAddr, String> {
    let ip = addresses
        .get(node)
        .ok_or_else(|| format!("node {node} has no InternalIP"))?;
    let port = port.ok_or_else(|| format!("assignment has no port for node {node}"))?;
    Ok(SocketAddr::new(*ip, port))
}

/// The resource as this node sees it: itself and the peers it connects to.
pub fn resource<'a>(i: &'a FileInputs<'a>) -> Result<FileResource<'a>, String> {
    let a = i.assignment;
    let minor = a.minor.ok_or("assignment has no minor")?;
    let secret = a.secret.as_deref().ok_or("assignment has no secret")?;
    let me = a
        .replicas
        .iter()
        .find(|r| r.node == i.node)
        .ok_or_else(|| format!("node {} not in the assignment", i.node))?;
    let host = |r: &'a AssignedReplica, disk: Disk<'a>| -> Result<FileHost<'a>, String> {
        Ok(FileHost {
            name: &r.node,
            node_id: r.node_id,
            address: socket(&r.node, r.port, i.addresses)?,
            disk,
            // An attachment's client must not decide quorum ties; the
            // assigned tiebreaker and diskful replicas may.
            tiebreaker: r.role != Role::Diskless,
        })
    };
    let local_disk = match (is_diskful(me), i.backing) {
        (true, Some(p)) => Disk::Local(p),
        (true, None) => return Err("diskful replica without a backing LV".to_owned()),
        (false, _) => Disk::None,
    };
    if a.replicas
        .iter()
        .any(|r| r.node != i.node && r.node == i.host_name)
    {
        return Err(format!(
            "host name {} is also the node name of a peer",
            i.host_name
        ));
    }
    let peers = peers(a, me)
        .into_iter()
        .map(|p| {
            host(
                p,
                if is_diskful(p) {
                    Disk::Peer
                } else {
                    Disk::None
                },
            )
        })
        .collect::<Result<_, _>>()?;
    Ok(FileResource {
        name: i.volume,
        version: env!("CARGO_PKG_VERSION"),
        minor,
        secret,
        local: FileHost {
            name: i.host_name,
            ..host(me, local_disk)?
        },
        peers,
        two_primaries: i.access_mode == model::AccessMode::ReadWriteMany,
        discard_granularity: i.chunk_bytes,
    })
}

/// Resource files under the res dir. A new file is written next to the
/// installed one under a name drbdadm's include never matches, checked,
/// then renamed over it: a file that fails the check is never installed.
#[derive(Debug, Clone)]
pub struct ResFiles {
    dir: PathBuf,
}

impl ResFiles {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn path(&self, res: &str) -> PathBuf {
        self.dir.join(format!("{res}.res"))
    }

    /// Outside the `*.res` include glob: a leading dot and another suffix.
    pub fn candidate_path(&self, res: &str) -> PathBuf {
        self.dir.join(format!(".{res}.candidate"))
    }

    pub fn exists(&self, res: &str) -> bool {
        self.path(res).exists()
    }

    /// Whether the installed file already holds `text`.
    pub fn installed(&self, res: &str, text: &str) -> Result<bool, String> {
        let path = self.path(res);
        match std::fs::read_to_string(&path) {
            Ok(have) => Ok(drbd_config::same_content(&have, text)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(format!("read {}: {e}", path.display())),
        }
    }

    /// Writes the candidate, synced to disk.
    pub fn write_candidate(&self, res: &str, text: &str) -> Result<PathBuf, String> {
        let path = self.candidate_path(res);
        let ctx = |e: std::io::Error| format!("write {}: {e}", path.display());
        std::fs::create_dir_all(&self.dir).map_err(ctx)?;
        let mut f = std::fs::File::create(&path).map_err(ctx)?;
        f.write_all(text.as_bytes()).map_err(ctx)?;
        f.sync_all().map_err(ctx)?;
        Ok(path)
    }

    /// Renames the candidate over the installed file; the directory sync
    /// makes the rename survive a crash.
    pub fn install(&self, res: &str) -> Result<(), String> {
        let (from, to) = (self.candidate_path(res), self.path(res));
        std::fs::rename(&from, &to).map_err(|e| format!("install {}: {e}", to.display()))?;
        std::fs::File::open(&self.dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| format!("sync {}: {e}", self.dir.display()))
    }

    pub fn remove(&self, res: &str) -> Result<(), String> {
        for p in [self.path(res), self.candidate_path(res)] {
            match std::fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("remove {}: {e}", p.display())),
            }
        }
        Ok(())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Disk states that prove metadata exists on an attached disk.
pub fn disk_has_metadata(disk: Option<&str>) -> bool {
    matches!(
        disk,
        Some("UpToDate" | "Consistent" | "Inconsistent" | "Outdated" | "Attaching")
    )
}

/// What events2 says about this replica, in status contract terms.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Derived {
    pub configured: bool,
    pub disk_state: Option<String>,
    /// Lowest `done` of the SyncTarget peer devices.
    pub sync_percent: Option<u32>,
    /// SyncTarget without a `done` value: events2 only prints it with
    /// statistics, so the caller asks `drbdsetup status --json`.
    pub sync_unknown: bool,
    pub peers: BTreeMap<String, String>,
    pub in_use: bool,
    pub quorum: Option<bool>,
}

pub fn derive(res: Option<&Resource>) -> Derived {
    let Some(r) = res else {
        return Derived::default();
    };
    let dev = r.devices.get(&VOLUME);
    let syncing: Vec<Option<f64>> = r
        .peer_devices
        .iter()
        .filter(|((_, v), p)| *v == VOLUME && p.replication.as_deref() == Some("SyncTarget"))
        .map(|(_, p)| p.done)
        .collect();
    let quorum = (!r.devices.is_empty())
        .then(|| {
            r.devices
                .values()
                .map(|d| d.quorum)
                .collect::<Option<Vec<_>>>()
        })
        .flatten()
        .map(|q| q.iter().all(|b| *b));
    Derived {
        configured: dev.is_some(),
        disk_state: dev.map(|d| d.disk.clone().unwrap_or_else(|| "DUnknown".to_owned())),
        sync_percent: syncing.iter().flatten().copied().map(whole_percent).min(),
        sync_unknown: syncing.iter().any(Option::is_none),
        peers: r
            .connections
            .iter()
            .map(|(k, c)| (k.clone(), c.state.clone()))
            .collect(),
        in_use: r.is_primary(),
        quorum,
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn whole_percent(done: f64) -> u32 {
    done.clamp(0.0, 100.0).floor() as u32
}

/// Lowest percent-in-sync of the SyncTarget peer devices in
/// `drbdsetup status --json <res>` output.
pub fn json_sync_percent(stdout: &str) -> Option<u32> {
    let v: serde_json::Value = serde_json::from_str(stdout).ok()?;
    v.as_array()?
        .iter()
        .flat_map(|r| r["connections"].as_array().into_iter().flatten())
        .flat_map(|c| c["peer_devices"].as_array().into_iter().flatten())
        .filter(|p| p["volume"] == VOLUME && p["replication-state"] == "SyncTarget")
        .filter_map(|p| p["percent-in-sync"].as_f64())
        .map(whole_percent)
        .min()
}

/// Facts of the last pass that events alone cannot tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    pub minor: u32,
    pub diskful: bool,
    pub allocated_bytes: Option<u64>,
    pub observed_generation: Option<i64>,
    pub last_error: Option<String>,
    /// The source Snapshot the backing LV provably came from.
    pub restored_from: Option<String>,
}

/// This node's status entry for a DRBD volume. `sectors` is the sysfs
/// size of the device; `sync_fallback` fills the percent when events2 gave none.
pub fn entry(
    node: &str,
    res: Option<&Resource>,
    facts: &Facts,
    sectors: Option<u64>,
    sync_fallback: Option<u32>,
) -> ReplicaStatus {
    let d = derive(res);
    let size_bytes = sectors.filter(|s| d.configured && *s > 0).map(|s| s * 512);
    ReplicaStatus {
        node: node.to_owned(),
        present: d.configured && (!facts.diskful || facts.allocated_bytes.is_some()),
        disk_state: d.disk_state,
        sync_percent: match (d.sync_percent, sync_fallback.filter(|_| d.sync_unknown)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        },
        peers: d.peers,
        in_use: d.in_use,
        quorum: d.quorum,
        device_path: d.configured.then(|| device_path(facts.minor)),
        allocated_bytes: facts.allocated_bytes.filter(|_| facts.diskful),
        size_bytes,
        observed_generation: facts.observed_generation,
        last_error: facts.last_error.clone(),
        restored_from: facts.restored_from.clone(),
        unreachable_peers: Vec::new(),
    }
}

/// Wakes the reconciler: `changes` for a status refresh of the dirty
/// resources, `pass` when the state became available or was lost.
#[derive(Debug, Clone)]
pub struct Notify {
    pub changes: Arc<Trigger>,
    pub pass: Arc<Trigger>,
}

/// Restart delays: 1 s, doubling to at most 30 s. A stream end is usually
/// a one-off (a module reload, a killed process), so the first retry is
/// quick; the cap bounds how stale status can get while drbdsetup keeps
/// failing.
const RESTART_FIRST: Duration = Duration::from_secs(1);
pub(crate) const RESTART_MAX: Duration = Duration::from_secs(30);
/// A stream that ran this long without ending was healthy: the next end
/// starts the schedule over.
const QUIET: Duration = Duration::from_secs(60);

pub fn restart_delay(consecutive: u32) -> Duration {
    RESTART_FIRST
        .saturating_mul(1 << consecutive.min(5))
        .min(RESTART_MAX)
}

/// Runs `drbdsetup events2 all` forever. Tracked state is cleared at every
/// restart and rebuilt from the next initial dump.
pub async fn events2(prefix: Option<Vec<String>>, tracker: Arc<Mutex<Tracker>>, notify: Notify) {
    let mut consecutive: u32 = 0;
    loop {
        let started = tokio::time::Instant::now();
        let why = stream_once(prefix.as_deref(), &tracker, &notify).await;
        let lost = tracker.lock().map(|mut t| {
            let was = t.is_available();
            t.reset();
            let racing = t.before_dump_restarts();
            if racing > 0 {
                tracing::warn!(
                    in_a_row = racing,
                    "events2 dump raced a change; changes queue after {}",
                    crate::events2::QUEUE_AFTER_RESTARTS
                );
            }
            was
        });
        if lost.unwrap_or(true) {
            notify.pass.fire();
        }
        if started.elapsed() >= QUIET {
            consecutive = 0;
        }
        let delay = restart_delay(consecutive);
        consecutive = consecutive.saturating_add(1);
        tracing::warn!(reason = why, ?delay, "events2 stream ended, restarting");
        tokio::time::sleep(delay).await;
    }
}

/// One stream until it ends; returns why.
async fn stream_once(
    prefix: Option<&[String]>,
    tracker: &Arc<Mutex<Tracker>>,
    notify: &Notify,
) -> String {
    let full: Vec<String> = prefix
        .unwrap_or_default()
        .iter()
        .cloned()
        .chain(argv::events2())
        .collect();
    let Some((program, rest)) = full.split_first() else {
        return "empty argv".to_owned();
    };
    let mut child = match tokio::process::Command::new(program)
        .args(rest)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return format!("spawn drbdsetup events2: {e}"),
    };
    let (Some(out), Some(err)) = (child.stdout.take(), child.stderr.take()) else {
        return "events2 pipes missing".to_owned();
    };
    let mut out = BufReader::new(out).lines();
    let mut err = BufReader::new(err).lines();
    let mut err_open = true;
    let why = loop {
        tokio::select! {
            line = out.next_line() => match line {
                Ok(Some(l)) => {
                    if let Some(e) = on_line(tracker, notify, &l) {
                        break e;
                    }
                }
                Ok(None) => break "events2 exited".to_owned(),
                Err(e) => break format!("events2 stdout: {e}"),
            },
            line = err.next_line(), if err_open => match line {
                Ok(Some(l)) => break format!("events2 stderr: {l}"),
                Ok(None) => err_open = false,
                Err(e) => break format!("events2 stderr: {e}"),
            },
        }
    };
    let _ = child.start_kill();
    let _ = child.wait().await;
    why
}

fn on_line(tracker: &Mutex<Tracker>, notify: &Notify, line: &str) -> Option<String> {
    let applied = match tracker.lock() {
        Ok(mut t) => t.feed(line),
        Err(e) => return Some(e.to_string()),
    };
    match applied {
        Ok(a) if a.became_available => {
            tracing::info!("events2 initial dump complete");
            notify.pass.fire();
        }
        Ok(a) if !a.resources.is_empty() => notify.changes.fire(),
        Ok(_) => {}
        Err(e) => return Some(format!("{e}: {line}")),
    }
    None
}

/// The InternalIP of a Node, the DRBD address.
pub fn internal_ip(node: &k8s_openapi::api::core::v1::Node) -> Option<IpAddr> {
    node.status
        .as_ref()?
        .addresses
        .as_ref()?
        .iter()
        .filter(|a| a.type_ == "InternalIP")
        .find_map(|a| a.address.parse().ok())
}

#[cfg(test)]
mod tests;
