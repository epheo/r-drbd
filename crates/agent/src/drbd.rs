//! DRBD mechanics for replicated volumes (spec.replicas >= 2): command
//! lines, generation ids, resource files, state derivation and the events2
//! process. The per-volume decisions live in `reconcile::replicated`.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drbd_config::{
    AddrFamily, Connection as FileConnection, DiskPath, Endpoint, Endpoints, FixedOptions, Header,
    HostEntry, ResourceFile, Tiebreaker, ToolVersions, Version, VolumeEntry,
};
use model::{AssignedReplica, Assignment, ReplicaStatus, Role};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::ExecMode;
use crate::events2::{Resource, Tracker};
use crate::watch::Trigger;

/// One volume per resource in the product.
pub const VOLUME: u32 = 0;
/// DRBD v09 superblock magic, big-endian at byte 60 of the last 4 KiB
/// block of the backing device (read back after a real create-md).
pub const MD_MAGIC: [u8; 4] = [0x83, 0x74, 0x02, 0x6D];
const MD_MAGIC_OFFSET: usize = 60;
pub const MD_BLOCK: usize = 4096;
pub const BITMAP_BLOCK_BYTES: u64 = 4096;
const DISCARD_MIN_UTILS: Version = Version {
    major: 9,
    minor: 34,
    patch: 0,
};
const BITMAP_FLAG_MIN_UTILS: Version = Version {
    major: 9,
    minor: 33,
    patch: 0,
};

/// create-md writes the bitmap of the whole device; IoProgress mode is not
/// implemented by the executor yet, so a generous fixed bound stands in.
pub const CREATE_MD_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(600),
    term: Duration::from_secs(15),
    kill: Duration::from_secs(5),
};

/// A superblock read is one small IO.
pub const SUPERBLOCK_TIMEOUT: ExecMode = ExecMode::Timeout {
    wait: Duration::from_secs(10),
    term: Duration::from_secs(5),
    kill: Duration::from_secs(5),
};

pub fn device_path(minor: u32) -> String {
    format!("/dev/drbd{minor}")
}

/// Host access that is not an external command, behind a trait so the
/// reconciler tests can fake it.
pub trait Host: Send + Sync + std::fmt::Debug {
    /// The last 4 KiB aligned block of a block device.
    fn read_tail(&self, dev: &str) -> std::io::Result<Vec<u8>>;
    /// /sys/block/drbd<minor>/size, 512 byte sectors.
    fn device_sectors(&self, minor: u32) -> std::io::Result<u64>;
    /// Bind test with SO_REUSEADDR; an address not assigned here
    /// falls back to the wildcard.
    fn port_in_use(&self, addr: IpAddr, port: u16) -> bool;
    /// 64 random bits for a fresh current generation id.
    fn random(&self) -> std::io::Result<u64>;
}

#[derive(Debug, Default)]
pub struct RealHost;

impl Host for RealHost {
    fn read_tail(&self, dev: &str) -> std::io::Result<Vec<u8>> {
        let mut f = std::fs::File::open(dev)?;
        let size = f.seek(SeekFrom::End(0))?;
        let block = MD_BLOCK as u64;
        let aligned = size / block * block;
        if aligned < block {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{dev} is smaller than 4 KiB"),
            ));
        }
        f.seek(SeekFrom::Start(aligned - block))?;
        let mut buf = vec![0; MD_BLOCK];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

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

    fn random(&self) -> std::io::Result<u64> {
        let mut b = [0u8; 8];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
        Ok(u64::from_ne_bytes(b))
    }
}

pub fn has_md_magic(tail: &[u8]) -> bool {
    tail.get(MD_MAGIC_OFFSET..MD_MAGIC_OFFSET + 4) == Some(&MD_MAGIC[..])
}

/// A current generation id: lowest bit cleared, never 0 or 4, uppercase hex.
pub fn fresh_gi(random: u64) -> String {
    let mut v = random & !1;
    if v == 0 || v == 4 {
        v = 0x0123_4567_89AB_CDEE;
    }
    format!("{v:016X}")
}

/// set-gi value for `node_id`. `current` is Some for the
/// initial UpToDate winner; thin backing means the sync skip always applies.
pub fn gi_value(node_id: u8, own_id: u8, day0: &str, current: Option<&str>) -> String {
    match (current, node_id == own_id) {
        (Some(r), true) => format!("{r}:{day0}:::1:1"),
        (Some(_), false) => format!(":{day0}::::"),
        (None, true) => format!("{day0}::::1:1"),
        (None, false) => format!("{day0}:::::"),
    }
}

/// Command lines, program first.
pub mod argv {
    use super::{BITMAP_BLOCK_BYTES, BITMAP_FLAG_MIN_UTILS, VOLUME};
    use crate::exec::argv as a;
    use drbd_config::ToolVersions;

    pub fn adjust(res: &str) -> Vec<String> {
        a(&["drbdadm", "-vvv", "adjust", res])
    }

    pub fn create_md(res: &str, peer_slots: u8, versions: &ToolVersions) -> Vec<String> {
        let mut v = a(&["drbdadm", "-vvv", "--max-peers"]);
        v.push(peer_slots.to_string());
        if versions.utils >= BITMAP_FLAG_MIN_UTILS {
            v.push("--bitmap-block-size".to_owned());
            v.push(BITMAP_BLOCK_BYTES.to_string());
        }
        v.extend(a(&["--", "--force", "create-md"]));
        v.push(format!("{res}/{VOLUME}"));
        v
    }

    pub fn set_gi(node_id: u8, minor: u32, dev: &str, gi: &str) -> Vec<String> {
        let mut v = a(&["drbdmeta", "--force", "--node-id"]);
        v.push(node_id.to_string());
        v.push(minor.to_string());
        v.extend(a(&["v09", dev, "internal", "set-gi", gi]));
        v
    }

    pub fn get_gi(node_id: u8, minor: u32, dev: &str) -> Vec<String> {
        let mut v = a(&["drbdmeta"]);
        v.push(minor.to_string());
        v.extend(a(&["v09", dev, "internal", "get-gi", "--node-id"]));
        v.push(node_id.to_string());
        v.push("--force".to_owned());
        v
    }

    /// The superblock only (ids included, about 7 KiB); dump-md would
    /// print the whole bitmap.
    pub fn dump_superblock(minor: u32, dev: &str) -> Vec<String> {
        let mut v = a(&["drbdmeta"]);
        v.push(minor.to_string());
        v.extend(a(&[
            "v09",
            dev,
            "internal",
            "dump-superblock",
            "--output-format=json",
        ]));
        v
    }

    pub fn check_resize(minor: u32, dev: &str) -> Vec<String> {
        let mut v = a(&["drbdmeta"]);
        v.push(minor.to_string());
        v.extend(a(&["v09", dev, "internal", "check-resize"]));
        v
    }

    pub fn wipe_md(minor: u32, dev: &str) -> Vec<String> {
        let mut v = a(&["drbdmeta", "--force"]);
        v.push(minor.to_string());
        v.extend(a(&["v09", dev, "internal", "wipe-md"]));
        v
    }

    pub fn status(res: &str) -> Vec<String> {
        a(&["drbdsetup", "status", res])
    }

    pub fn down(res: &str) -> Vec<String> {
        a(&["drbdsetup", "down", res])
    }

    pub fn del_peer(res: &str, peer_node_id: u8) -> Vec<String> {
        let mut v = a(&["drbdsetup", "del-peer", res]);
        v.push(peer_node_id.to_string());
        v
    }

    pub fn forget_peer(res: &str, peer_node_id: u8) -> Vec<String> {
        let mut v = a(&["drbdsetup", "forget-peer", res]);
        v.push(peer_node_id.to_string());
        v
    }

    pub fn suspend_io(res: &str) -> Vec<String> {
        a(&["drbdadm", "suspend-io", res])
    }

    pub fn resume_io(res: &str) -> Vec<String> {
        a(&["drbdadm", "resume-io", res])
    }

    pub fn wait_connect(res: &str) -> Vec<String> {
        a(&[
            "drbdsetup",
            "wait-connect-resource",
            "--wait-after-sb=yes",
            "--wfc-timeout=10",
            res,
        ])
    }

    pub fn resize(res: &str) -> Vec<String> {
        a(&[
            "drbdadm",
            "-vvv",
            "--",
            "--assume-clean",
            "resize",
            &format!("{res}/{VOLUME}"),
        ])
    }

    pub fn disconnect(res: &str) -> Vec<String> {
        a(&["drbdadm", "-vvv", "disconnect", res])
    }

    pub fn connect_discard(res: &str) -> Vec<String> {
        a(&["drbdadm", "-vvv", "--", "--discard-my-data", "connect", res])
    }

    /// One connection only: the survivor may be connected to others.
    pub fn connect_peer(res: &str, peer: &str) -> Vec<String> {
        a(&["drbdadm", "-vvv", "connect", &format!("{res}:{peer}")])
    }

    pub fn events2() -> Vec<String> {
        a(&["drbdsetup", "events2", "all"])
    }
}

/// Inputs of one resource file beyond the assignment.
#[derive(Debug, Clone)]
pub struct FileInputs<'a> {
    pub node: &'a str,
    pub volume: &'a str,
    pub assignment: &'a Assignment,
    /// Backing LV path when this replica is diskful.
    pub backing: Option<&'a str>,
    pub addresses: &'a BTreeMap<String, IpAddr>,
    pub versions: &'a ToolVersions,
    pub replicas: u32,
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

fn endpoint(
    host: &str,
    port: Option<u16>,
    addresses: &BTreeMap<String, IpAddr>,
) -> Result<Endpoint, String> {
    let ip = addresses
        .get(host)
        .ok_or_else(|| format!("node {host} has no InternalIP"))?;
    let port = port.ok_or_else(|| format!("assignment has no port for node {host}"))?;
    Ok(Endpoint {
        host: host.to_owned(),
        family: if ip.is_ipv4() {
            AddrFamily::Ipv4
        } else {
            AddrFamily::Ipv6
        },
        address: ip.to_string(),
        port,
    })
}

/// The resource file for this node: one volume, this node
/// first, peers by name.
pub fn resource_file(i: &FileInputs<'_>) -> Result<ResourceFile, String> {
    let a = i.assignment;
    let minor = a.minor.ok_or("assignment has no minor")?;
    let secret = a.secret.clone().ok_or("assignment has no secret")?;
    let me = a
        .replicas
        .iter()
        .find(|r| r.node == i.node)
        .ok_or_else(|| format!("node {} not in the assignment", i.node))?;
    // Discard options need drbd-utils 9.34.0.
    let chunk = i
        .chunk_bytes
        .filter(|_| i.versions.utils >= DISCARD_MIN_UTILS);
    let fixed = FixedOptions::new(
        i.replicas,
        chunk,
        i.access_mode == model::AccessMode::ReadWriteMany,
    );
    let tb_supported = drbd_config::tiebreaker_supported(i.versions);
    let volume = |r: &AssignedReplica, local: bool| {
        let disk = match (local, is_diskful(r)) {
            (true, true) => i
                .backing
                .map(|p| DiskPath::Backing(p.to_owned()))
                .ok_or_else(|| "diskful replica without a backing LV".to_owned()),
            (false, true) => Ok(DiskPath::PeerPlaceholder),
            (_, false) => Ok(DiskPath::None),
        }?;
        // Diskless replicas for attachments are explicit clients; a
        // tiebreaker and diskful replicas count as tiebreakers.
        let tiebreaker = match (tb_supported, r.role) {
            (false, _) => Tiebreaker::Unsupported,
            (true, Role::Diskless) => Tiebreaker::No,
            (true, _) => Tiebreaker::Yes,
        };
        Ok::<_, String>(HostEntry {
            host: r.node.clone(),
            node_id: r.node_id,
            volumes: vec![VolumeEntry {
                number: VOLUME,
                disk,
                tiebreaker,
                disk_options: fixed.disk.clone(),
                minor,
            }],
        })
    };
    let mut peers: Vec<&AssignedReplica> = a.replicas.iter().filter(|r| r.node != i.node).collect();
    peers.sort_by(|x, y| x.node.cmp(&y.node));
    let local_ep = endpoint(i.node, me.port, i.addresses)?;
    let mut connections = Vec::new();
    for p in peers.iter().filter(|p| connects(me, p)) {
        connections.push(FileConnection {
            peer: p.node.clone(),
            net_options: Vec::new(),
            endpoints: Endpoints {
                local: local_ep.clone(),
                peer: endpoint(&p.node, p.port, i.addresses)?,
            },
        });
    }
    let now = k8s_openapi::jiff::Timestamp::now().to_string();
    Ok(ResourceFile {
        name: i.volume.to_owned(),
        header: Header {
            generator: format!("r-drbd agent {}", env!("CARGO_PKG_VERSION")),
            node_name: i.node.to_owned(),
            hostname: i.node.to_owned(),
            local_time: now.clone(),
            utc_time: now,
        },
        handlers: Vec::new(),
        options: fixed.resource,
        secret,
        net: fixed.net,
        disk: Vec::new(),
        local: volume(me, true)?,
        peers: peers
            .iter()
            .map(|p| volume(p, false))
            .collect::<Result<_, _>>()?,
        connections,
    })
}

/// Resource files under the res dir, with a backup copy to restore after a failed adjust.
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

    /// Outside the `*.res` glob of the include file.
    pub fn backup_path(&self, res: &str) -> PathBuf {
        self.dir.join(".backup").join(format!("{res}.res"))
    }

    pub fn exists(&self, res: &str) -> bool {
        self.path(res).exists()
    }

    /// Writes through a temporary file and a rename, only when the body
    /// differs. Returns whether it wrote.
    pub fn write_if_differs(&self, res: &str, text: &str) -> Result<bool, String> {
        let path = self.path(res);
        let ctx = |e: std::io::Error| format!("write {}: {e}", path.display());
        match std::fs::read_to_string(&path) {
            Ok(old) if !drbd_config::body_differs(&old, text) => return Ok(false),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(ctx(e)),
        }
        std::fs::create_dir_all(&self.dir).map_err(ctx)?;
        let tmp = self.dir.join(format!("{res}.res_tmp"));
        let mut f = std::fs::File::create(&tmp).map_err(ctx)?;
        f.write_all(text.as_bytes()).map_err(ctx)?;
        f.sync_all().map_err(ctx)?;
        std::fs::rename(&tmp, &path).map_err(ctx)?;
        Ok(true)
    }

    pub fn backup(&self, res: &str) -> Result<(), String> {
        let to = self.backup_path(res);
        let ctx = |e: std::io::Error| format!("backup {}: {e}", to.display());
        if let Some(d) = to.parent() {
            std::fs::create_dir_all(d).map_err(ctx)?;
        }
        std::fs::copy(self.path(res), &to).map_err(ctx)?;
        Ok(())
    }

    /// Puts the backup back after a failed adjust; no backup, no change.
    pub fn restore_backup(&self, res: &str) -> Result<(), String> {
        let from = self.backup_path(res);
        if !from.exists() {
            return Ok(());
        }
        std::fs::copy(&from, self.path(res))
            .map(|_| ())
            .map_err(|e| format!("restore {}: {e}", from.display()))
    }

    pub fn remove(&self, res: &str) -> Result<(), String> {
        for p in [self.path(res), self.backup_path(res)] {
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

const BAD_CONNECTION: &[&str] = &[
    "StandAlone",
    "Disconnecting",
    "Unconnected",
    "Timeout",
    "BrokenPipe",
    "NetworkFailure",
    "ProtocolError",
    "TearDown",
    "Unknown",
];

/// Adjust triggers for an unchanged file. `peers` are the
/// node names this replica must have a connection to.
pub fn needs_adjust(res: Option<&Resource>, peers: &[&str], diskful: bool) -> bool {
    let Some(r) = res else { return true };
    if r.role.as_deref().is_none_or(|role| role == "Unknown") {
        return true;
    }
    let peer_bad = peers.iter().any(|p| {
        r.connections.get(*p).is_none_or(|c| {
            // After a split brain, StandAlone waits for spec.resolve: an
            // adjust would only reconnect into the same split brain.
            BAD_CONNECTION.contains(&c.state.as_str())
                && !(c.split_brain && c.state == "StandAlone")
        })
    });
    if peer_bad || r.devices.keys().any(|v| *v != VOLUME) {
        return true;
    }
    let Some(d) = r.devices.get(&VOLUME) else {
        return true;
    };
    match d.disk.as_deref() {
        Some("Diskless") => diskful || d.client == Some(false),
        Some("Detaching" | "Failed" | "Negotiating" | "DUnknown") | None => true,
        Some(_) => !diskful,
    }
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
    }
}

/// Wakes the reconciler: `changes` for a status refresh of the dirty
/// resources, `pass` when the state became available or was lost.
#[derive(Debug, Clone)]
pub struct Notify {
    pub changes: Arc<Trigger>,
    pub pass: Arc<Trigger>,
}

const RESTART_LATER: Duration = Duration::from_secs(5);
const RESTART_MAX: Duration = Duration::from_secs(60);
/// A stream that lived this long resets the backoff.
const HEALTHY_RUN: Duration = Duration::from_secs(30);

enum Ended {
    /// EOF, stderr output or a spawn failure: restart after 5 s.
    Later(String),
    /// Parse or tracking error: restart at once, backoff aside.
    Now(String),
}

/// Runs `drbdsetup events2 all` forever, restarting with backoff. Tracked
/// state is cleared at every restart.
pub async fn events2(prefix: Option<Vec<String>>, tracker: Arc<Mutex<Tracker>>, notify: Notify) {
    let mut failures: u32 = 0;
    loop {
        let started = tokio::time::Instant::now();
        let ended = stream_once(prefix.as_deref(), &tracker, &notify).await;
        let lost = tracker.lock().map(|mut t| {
            let was = t.is_available();
            t.reset();
            was
        });
        if lost.unwrap_or(true) {
            notify.pass.fire();
        }
        if started.elapsed() >= HEALTHY_RUN {
            failures = 0;
        }
        let (base, why) = match ended {
            Ended::Later(why) => (RESTART_LATER, why),
            Ended::Now(why) => (Duration::ZERO, why),
        };
        let backoff = Duration::from_secs(1u64 << failures.min(6)).min(RESTART_MAX);
        let delay = if failures == 0 {
            base
        } else {
            base.max(backoff)
        };
        failures = failures.saturating_add(1);
        tracing::warn!(reason = why, ?delay, "events2 stream ended, restarting");
        tokio::time::sleep(delay).await;
    }
}

async fn stream_once(
    prefix: Option<&[String]>,
    tracker: &Arc<Mutex<Tracker>>,
    notify: &Notify,
) -> Ended {
    let full: Vec<String> = prefix
        .unwrap_or_default()
        .iter()
        .cloned()
        .chain(argv::events2())
        .collect();
    let Some((program, rest)) = full.split_first() else {
        return Ended::Later("empty argv".to_owned());
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
        Err(e) => return Ended::Later(format!("spawn drbdsetup events2: {e}")),
    };
    let (Some(out), Some(err)) = (child.stdout.take(), child.stderr.take()) else {
        return Ended::Later("events2 pipes missing".to_owned());
    };
    let mut out = BufReader::new(out).lines();
    let mut err = BufReader::new(err).lines();
    let mut err_open = true;
    let ended = loop {
        tokio::select! {
            line = out.next_line() => match line {
                Ok(Some(l)) => {
                    if let Some(e) = on_line(tracker, notify, &l) {
                        break e;
                    }
                }
                Ok(None) => break Ended::Later("events2 exited".to_owned()),
                Err(e) => break Ended::Later(format!("events2 stdout: {e}")),
            },
            line = err.next_line(), if err_open => match line {
                Ok(Some(l)) => break Ended::Later(format!("events2 stderr: {l}")),
                Ok(None) => err_open = false,
                Err(e) => break Ended::Later(format!("events2 stderr: {e}")),
            },
        }
    };
    let _ = child.start_kill();
    let _ = child.wait().await;
    ended
}

fn on_line(tracker: &Mutex<Tracker>, notify: &Notify, line: &str) -> Option<Ended> {
    let applied = match tracker.lock() {
        Ok(mut t) => t.feed(line),
        Err(e) => return Some(Ended::Later(e.to_string())),
    };
    match applied {
        Ok(a) if a.became_available => {
            tracing::info!("events2 initial dump complete");
            notify.pass.fire();
        }
        Ok(a) if !a.resources.is_empty() => notify.changes.fire(),
        Ok(_) => {}
        Err(e) => return Some(Ended::Now(format!("{e}: {line}"))),
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
mod tests {
    use super::*;
    use crate::events2::Tracker;

    fn v(major: u32, minor: u32, patch: u32) -> Version {
        Version {
            major,
            minor,
            patch,
        }
    }

    #[test]
    fn magic_and_gi() {
        let mut tail = vec![0u8; MD_BLOCK];
        assert!(!has_md_magic(&tail));
        tail[60..64].copy_from_slice(&MD_MAGIC);
        assert!(has_md_magic(&tail));
        assert!(!has_md_magic(&tail[..10]));
        assert_eq!(fresh_gi(0xABCD_EF01_2345_6789), "ABCDEF0123456788");
        assert_eq!(fresh_gi(5), "0123456789ABCDEE");
        assert_eq!(gi_value(0, 0, "D0", Some("R1")), "R1:D0:::1:1");
        assert_eq!(gi_value(3, 0, "D0", Some("R1")), ":D0::::");
        assert_eq!(gi_value(1, 1, "D0", None), "D0::::1:1");
        assert_eq!(gi_value(0, 1, "D0", None), "D0:::::");
    }

    #[test]
    fn create_md_bitmap_flag_by_version() {
        let new = ToolVersions {
            kernel: v(9, 3, 4),
            utils: v(9, 35, 0),
        };
        assert_eq!(
            argv::create_md("r", 7, &new).join(" "),
            "drbdadm -vvv --max-peers 7 --bitmap-block-size 4096 -- --force create-md r/0"
        );
        let old = ToolVersions {
            kernel: v(9, 2, 0),
            utils: v(9, 32, 0),
        };
        assert_eq!(
            argv::create_md("r", 7, &old).join(" "),
            "drbdadm -vvv --max-peers 7 -- --force create-md r/0"
        );
    }

    fn tracked(lines: &[&str]) -> Tracker {
        let mut t = Tracker::default();
        for l in lines {
            t.feed(l).unwrap();
        }
        t.feed("exists -").unwrap();
        t
    }

    #[test]
    fn derivation_into_status() {
        let t = tracked(&[
            "exists resource name:r role:Primary suspended:no",
            "exists connection name:r peer-node-id:1 conn-name:b connection:Connected role:Secondary",
            "exists connection name:r peer-node-id:2 conn-name:c connection:Connecting role:Unknown",
            "exists device name:r volume:0 minor:1000 disk:Inconsistent client:no quorum:yes",
            "exists peer-device name:r peer-node-id:1 conn-name:b volume:0 replication:SyncTarget peer-disk:UpToDate done:42.97",
            "exists peer-device name:r peer-node-id:2 conn-name:c volume:0 replication:Off peer-disk:DUnknown",
        ]);
        let facts = Facts {
            minor: 1000,
            diskful: true,
            allocated_bytes: Some(1_052_672 * 1024),
            observed_generation: Some(4),
            last_error: None,
        };
        let e = entry("a", t.resource("r"), &facts, Some(2_104_816), None);
        assert!(e.present && e.in_use);
        assert_eq!(e.disk_state.as_deref(), Some("Inconsistent"));
        assert_eq!(e.sync_percent, Some(42));
        assert_eq!(
            e.peers,
            BTreeMap::from([
                ("b".into(), "Connected".into()),
                ("c".into(), "Connecting".into())
            ])
        );
        assert_eq!(e.quorum, Some(true));
        assert_eq!(e.device_path.as_deref(), Some("/dev/drbd1000"));
        assert_eq!(e.size_bytes, Some(2_104_816 * 512));
        assert_eq!(e.allocated_bytes, Some(1_052_672 * 1024));
        assert_eq!(e.observed_generation, Some(4));

        // Diskless replica: present, no allocation, quorum lost.
        let t = tracked(&[
            "exists resource name:r role:Secondary suspended:quorum",
            "exists device name:r volume:0 minor:1000 disk:Diskless client:yes quorum:no",
        ]);
        let facts = Facts {
            diskful: false,
            allocated_bytes: None,
            ..facts
        };
        let e = entry("c", t.resource("r"), &facts, Some(8), None);
        assert!(e.present && !e.in_use);
        assert_eq!(e.disk_state.as_deref(), Some("Diskless"));
        assert_eq!((e.sync_percent, e.quorum), (None, Some(false)));
        assert!(e.allocated_bytes.is_none() && e.peers.is_empty());

        // Not up: nothing configured.
        let e = entry("c", None, &facts, Some(8), None);
        assert!(!e.present && e.device_path.is_none() && e.size_bytes.is_none());
    }

    #[test]
    fn sync_target_without_done_uses_the_fallback() {
        let t = tracked(&[
            "exists resource name:r role:Secondary",
            "exists connection name:r peer-node-id:1 conn-name:b connection:Connected",
            "exists device name:r volume:0 minor:1 disk:Inconsistent",
            "exists peer-device name:r conn-name:b volume:0 replication:SyncTarget",
        ]);
        let d = derive(t.resource("r"));
        assert_eq!((d.sync_percent, d.sync_unknown), (None, true));
        let facts = Facts {
            minor: 1,
            diskful: true,
            allocated_bytes: Some(1),
            observed_generation: None,
            last_error: None,
        };
        let e = entry("a", t.resource("r"), &facts, Some(8), Some(37));
        assert_eq!(e.sync_percent, Some(37));
        let json = r#"[{"name":"r","connections":[{"name":"b","peer_devices":[
            {"volume":0,"replication-state":"SyncTarget","percent-in-sync":37.42},
            {"volume":0,"replication-state":"Established","percent-in-sync":100.00}]}]}]"#;
        assert_eq!(json_sync_percent(json), Some(37));
        assert_eq!(json_sync_percent("not json"), None);
    }

    #[test]
    fn adjust_triggers() {
        let t = tracked(&[
            "exists resource name:r role:Secondary",
            "exists connection name:r peer-node-id:1 conn-name:b connection:Connecting",
            "exists device name:r volume:0 minor:1 disk:UpToDate client:no",
        ]);
        let r = t.resource("r");
        assert!(!needs_adjust(r, &["b"], true));
        assert!(needs_adjust(None, &["b"], true));
        assert!(needs_adjust(r, &["b", "c"], true), "missing connection");
        assert!(needs_adjust(r, &["b"], false), "disk where none is wanted");
        let t = tracked(&[
            "exists resource name:r role:Secondary",
            "exists connection name:r peer-node-id:1 conn-name:b connection:StandAlone",
            "exists device name:r volume:0 minor:1 disk:UpToDate",
        ]);
        assert!(needs_adjust(t.resource("r"), &["b"], true));
        let t = tracked(&[
            "exists resource name:r role:Secondary",
            "exists device name:r volume:0 minor:1 disk:Diskless client:yes",
        ]);
        assert!(!needs_adjust(t.resource("r"), &[], false));
        assert!(
            needs_adjust(t.resource("r"), &[], true),
            "unintended diskless"
        );
        let t = tracked(&[
            "exists resource name:r role:Unknown",
            "exists device name:r volume:0 minor:1 disk:Diskless client:yes",
        ]);
        assert!(needs_adjust(t.resource("r"), &[], false));
    }

    fn replica(node: &str, id: u8, role: Role) -> AssignedReplica {
        AssignedReplica {
            node: node.into(),
            pool: (role == Role::Diskful).then(|| "p".to_owned()),
            node_id: id,
            port: Some(7000 + u16::from(id)),
            role,
        }
    }

    #[test]
    fn file_for_two_diskful_and_tiebreaker() {
        let a = Assignment {
            replicas: vec![
                replica("node-c", 2, Role::Tiebreaker),
                replica("node-b", 1, Role::Diskful),
                replica("node-a", 0, Role::Diskful),
                replica("node-d", 3, Role::Diskless),
            ],
            minor: Some(1000),
            secret: Some("s3cr3t".into()),
            gi: Some("ABCD".into()),
            initial_up_to_date_node: Some("node-a".into()),
        };
        let addresses: BTreeMap<String, IpAddr> = [
            ("node-a", "10.0.0.11"),
            ("node-b", "10.0.0.12"),
            ("node-c", "10.0.0.13"),
            ("node-d", "fd00::4"),
        ]
        .into_iter()
        .map(|(n, ip)| (n.to_owned(), ip.parse().unwrap()))
        .collect();
        let versions = ToolVersions {
            kernel: v(9, 3, 4),
            utils: v(9, 35, 0),
        };
        let inputs = FileInputs {
            node: "node-c",
            volume: "pvc-1",
            assignment: &a,
            backing: None,
            addresses: &addresses,
            versions: &versions,
            replicas: 2,
            chunk_bytes: Some(65536),
            access_mode: model::AccessMode::ReadWriteOnce,
        };
        let text = drbd_config::render(&resource_file(&inputs).unwrap());
        let body = &text[text.find("resource \"").unwrap()..];
        let on: Vec<&str> = body
            .lines()
            .filter(|l| l.trim_start().starts_with("on "))
            .collect();
        assert_eq!(
            on,
            [
                "    on \"node-c\"",
                "    on \"node-a\"",
                "    on \"node-b\"",
                "    on \"node-d\""
            ]
        );
        assert!(body.contains("quorum majority;"));
        assert!(body.contains("shared-secret     \"s3cr3t\";"));
        assert!(body.contains("rs-discard-granularity 65536;"));
        assert!(body.contains("tiebreaker no;"), "explicit diskless client");
        // The tiebreaker connects to the diskful nodes only.
        assert_eq!(body.matches("connection\n").count(), 2);
        assert!(body.contains("host \"node-c\" address ipv4 10.0.0.13:7002;"));
        assert!(body.contains("host \"node-b\" address ipv4 10.0.0.12:7001;"));
        assert!(!body.contains("fd00::4"));

        // A diskful node connects to everyone, ipv6 included.
        let inputs = FileInputs {
            node: "node-a",
            backing: Some("/dev/vg/pvc-1"),
            ..inputs
        };
        let text = drbd_config::render(&resource_file(&inputs).unwrap());
        assert!(text.contains("disk        /dev/vg/pvc-1;"));
        assert!(text.contains("host \"node-d\" address ipv6 [fd00::4]:7003;"));
        assert_eq!(text.matches("    connection\n").count(), 3);
        assert!(!text.contains("allow-two-primaries"));

        // ReadWriteMany: in the resource net, which every connection inherits.
        let rwx = FileInputs {
            access_mode: model::AccessMode::ReadWriteMany,
            ..inputs
        };
        let text = drbd_config::render(&resource_file(&rwx).unwrap());
        assert_eq!(text.matches("allow-two-primaries yes;").count(), 1);
        assert!(text.contains("\n        allow-two-primaries yes;\n        protocol C;"));

        let mut missing = addresses.clone();
        missing.remove("node-b");
        let inputs = FileInputs {
            addresses: &missing,
            ..inputs
        };
        assert_eq!(
            resource_file(&inputs).unwrap_err(),
            "node node-b has no InternalIP"
        );
    }

    /// A fake events2 through the prefix: `sh -c <script> -- drbdsetup events2 all`.
    #[tokio::test]
    async fn events2_process_gate_and_restart() {
        let script = "echo 'exists resource name:r role:Secondary'; \
            echo 'change resource name:r role:Primary'; \
            echo 'exists -'; sleep 0.3; \
            echo 'change resource name:r role:Secondary'; sleep 0.3";
        let prefix = vec![
            "sh".to_owned(),
            "-c".to_owned(),
            script.to_owned(),
            "--".to_owned(),
        ];
        let tracker = Arc::new(Mutex::new(Tracker::default()));
        let notify = Notify {
            changes: Arc::new(Trigger::default()),
            pass: Arc::new(Trigger::default()),
        };
        let task = tokio::spawn(events2(Some(prefix), tracker.clone(), notify.clone()));
        let wait = |want: fn(&Tracker) -> bool| {
            let tracker = tracker.clone();
            async move {
                for _ in 0..50 {
                    if want(&tracker.lock().unwrap()) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                panic!("events2 state not reached");
            }
        };
        // The queued change replays after the dump.
        wait(|t| t.resource("r").is_some_and(Resource::is_primary)).await;
        assert!(tracker.lock().unwrap().is_available());
        tokio::time::timeout(Duration::from_secs(1), notify.pass.changed())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), notify.changes.changed())
            .await
            .unwrap();
        wait(|t| t.resource("r").is_some_and(|r| !r.is_primary())).await;
        // The process exits: state is cleared until the next dump.
        wait(|t| !t.is_available() && t.resources().is_empty()).await;
        task.abort();
    }

    #[test]
    fn files_write_backup_restore_remove() {
        let dir = std::env::temp_dir().join(format!("agent-resfiles-{}", std::process::id()));
        let f = ResFiles::new(&dir);
        assert!(f.write_if_differs("r", "# a\nresource \"r\" {}\n").unwrap());
        assert!(!f.write_if_differs("r", "# b\nresource \"r\" {}\n").unwrap());
        assert_eq!(
            std::fs::read_to_string(f.path("r")).unwrap(),
            "# a\nresource \"r\" {}\n"
        );
        f.restore_backup("r").unwrap();
        f.backup("r").unwrap();
        assert!(
            f.write_if_differs("r", "# c\nresource \"r\" {x}\n")
                .unwrap()
        );
        f.restore_backup("r").unwrap();
        assert_eq!(
            std::fs::read_to_string(f.path("r")).unwrap(),
            "# a\nresource \"r\" {}\n"
        );
        assert!(!dir.join("r.res_tmp").exists());
        f.remove("r").unwrap();
        f.remove("r").unwrap();
        assert!(!f.exists("r") && !f.backup_path("r").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
