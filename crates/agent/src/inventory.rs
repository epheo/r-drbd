//! The node's NodeDisks: every whole disk with its state, so the admin
//! picks a Pool's disks from kubectl.
//!
//! Event driven, in its own task so a slow disk never holds up Pools and
//! Volumes. Kernel uevents mark single disks dirty (udev turns a close
//! after write, as by mkfs or pvcreate, into a change uevent); a mount
//! table change, a dm map or a Pool device change refresh the cheap
//! facts of every disk. Only dirty disks, and disks whose cheap facts
//! changed, are read (wipefs) or opened; every disk is at start and
//! hourly. Without a uevent socket, every 5 minutes.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Patch, PatchParams, PostParams};
use kube::{Api, Client};
use model::{DeviceState, DiskStatus, DiskWatch, NodeDisks, NodeDisksSpec, NodeDisksStatus};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

pub mod events;
mod scan;

pub use scan::{Lvm, Scanner};

/// Full check with uevents on.
pub const FULL_EVERY: Duration = Duration::from_secs(3600);
/// Full check without uevents.
pub const POLL_EVERY: Duration = Duration::from_secs(300);
/// Events within this window share one pass.
pub const DEBOUNCE: Duration = Duration::from_secs(2);
/// A failed write is retried with the same status, waiting this long,
/// doubling up to [`POLL_EVERY`]; scans keep their own schedule.
pub const RETRY_FIRST: Duration = Duration::from_secs(30);
pub const RETRY_MAX: Duration = Duration::from_secs(300);
/// Disks not reached by then are named in `status.message` and tried
/// again [`RETRY_FIRST`] later.
pub const SCAN_MAX: Duration = Duration::from_secs(120);
/// Keeps the object well under the API's size limit.
pub const MAX_ROWS: usize = 512;

/// by-id names that do not name the disk itself: a PV, a dm or md map
/// on it, or an NVMe id with no model or serial in it.
const NOT_BUS: [&str; 6] = [
    "lvm-pv-uuid-",
    "dm-",
    "md-",
    "nvme-eui.",
    "nvme-nvme.",
    "nvme-uuid.",
];

/// The path a Pool should list: `wwn-`, else a bus id (`ata-`, `nvme-`,
/// `scsi-`, `virtio-`...), else any link, else the kernel name.
pub fn preferred(links: &[String], name: &str) -> String {
    let base = |l: &String| {
        Path::new(l)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_owned()
    };
    links
        .iter()
        .find(|l| base(l).starts_with("wwn-"))
        .or_else(|| {
            links
                .iter()
                .find(|l| !NOT_BUS.iter().any(|p| base(l).starts_with(p)))
        })
        .or_else(|| links.first())
        .cloned()
        .unwrap_or_else(|| format!("/dev/{name}"))
}

/// Why a pass runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Wake {
    /// A uevent on this disk or one of its partitions.
    Disk(String),
    /// Mount table or another block device changed: cheap facts again.
    Refresh,
    /// A Pool device change: LVM reports again.
    Lvm,
    /// Lost events: read every disk again.
    All,
}

/// What the next pass must read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dirty {
    /// Read every disk and the LVM reports.
    pub all: bool,
    pub lvm: bool,
    /// Kernel names to read again.
    pub disks: BTreeSet<String>,
}

impl Dirty {
    pub fn everything() -> Self {
        Self {
            all: true,
            ..Self::default()
        }
    }

    pub fn add(&mut self, w: Wake) {
        match w {
            Wake::Disk(d) => {
                self.disks.insert(d);
            }
            Wake::Refresh => {}
            Wake::Lvm => self.lvm = true,
            Wake::All => self.all = true,
        }
    }
}

/// One pass: the status and the disks it had no time for.
#[derive(Debug, Clone, PartialEq)]
pub struct Pass {
    pub status: NodeDisksStatus,
    pub unchecked: BTreeSet<String>,
}

/// The status of `disks`, at most [`MAX_ROWS`] of them, saying what is
/// left out.
pub fn status(
    mut disks: Vec<DiskStatus>,
    unchecked: &BTreeSet<String>,
    watch: DiskWatch,
) -> NodeDisksStatus {
    let mut notes = Vec::new();
    if !unchecked.is_empty() {
        let names: Vec<&str> = unchecked.iter().map(String::as_str).collect();
        notes.push(format!(
            "scan stopped after {} s, not checked yet: {}",
            SCAN_MAX.as_secs(),
            names.join(", ")
        ));
    }
    if disks.len() > MAX_ROWS {
        notes.push(format!(
            "{} disks, only the first {MAX_ROWS} listed",
            disks.len()
        ));
        disks.truncate(MAX_ROWS);
    }
    let blank = disks
        .iter()
        .filter(|d| d.state == DeviceState::Blank)
        .count();
    NodeDisksStatus {
        blank: u32::try_from(blank).unwrap_or(u32::MAX),
        message: (!notes.is_empty()).then(|| notes.join("; ")),
        watch: Some(watch),
        disks,
    }
}

/// A new status only when it differs from the written one.
pub fn changed(written: Option<&NodeDisks>, now: &NodeDisksStatus) -> bool {
    written.and_then(|o| o.status.as_ref()) != Some(now)
}

/// The object for `node`, owned by its Node so it goes with the node.
pub fn object(node: &str, node_uid: &str) -> NodeDisks {
    NodeDisks {
        metadata: ObjectMeta {
            name: Some(node.to_owned()),
            owner_references: Some(vec![OwnerReference {
                api_version: "v1".into(),
                kind: "Node".into(),
                name: node.to_owned(),
                uid: node_uid.to_owned(),
                ..OwnerReference::default()
            }]),
            ..ObjectMeta::default()
        },
        spec: NodeDisksSpec {},
        status: None,
    }
}

/// Creates the object if needed and patches its status if it changed;
/// true when it wrote.
pub async fn write(client: &Client, node: &str, now: &NodeDisksStatus) -> Result<bool, String> {
    let api: Api<NodeDisks> = Api::all(client.clone());
    let current = if let Some(o) = api.get_opt(node).await.map_err(|e| e.to_string())? {
        o
    } else {
        let uid = Api::<Node>::all(client.clone())
            .get(node)
            .await
            .map_err(|e| format!("read node {node}: {e}"))?
            .metadata
            .uid
            .ok_or_else(|| format!("node {node} has no uid"))?;
        api.create(&PostParams::default(), &object(node, &uid))
            .await
            .map_err(|e| format!("create: {e}"))?
    };
    if !changed(Some(&current), now) {
        return Ok(false);
    }
    // A merge patch replaces the whole list: rows of unplugged disks go.
    api.patch_status(
        node,
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({ "status": now })),
    )
    .await
    .map_err(|e| format!("patch status: {e}"))?;
    Ok(true)
}

/// Where passes come from; a trait so the loop is tested without disks.
#[async_trait::async_trait]
pub trait Source: Send {
    async fn pass(&mut self, dirty: Dirty) -> Pass;
}

/// Where a status goes; true when it wrote.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, status: &NodeDisksStatus) -> Result<bool, String>;
}

pub struct KubeSink {
    pub client: Client,
    pub node: String,
}

impl std::fmt::Debug for KubeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeSink")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Sink for KubeSink {
    async fn write(&self, status: &NodeDisksStatus) -> Result<bool, String> {
        write(&self.client, &self.node, status).await
    }
}

fn earliest(times: &[Option<Instant>]) -> Option<Instant> {
    times.iter().flatten().min().copied()
}

/// Passes at start, [`DEBOUNCE`] after wakes and every `full_every`;
/// writes each pass, retrying a failed write alone with backoff.
pub async fn run_loop(
    mut src: impl Source,
    sink: impl Sink,
    mut wakes: UnboundedReceiver<Wake>,
    full_every: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut dirty = Dirty::everything();
    let mut scan_at = Some(Instant::now());
    let mut next_full = Instant::now() + full_every;
    let mut pending: Option<NodeDisksStatus> = None;
    let mut retry_at = Instant::now();
    let mut backoff = RETRY_FIRST;
    let mut wakes_open = true;
    loop {
        if scan_at.is_some_and(|t| Instant::now() >= t) {
            scan_at = None;
            let p = src.pass(std::mem::take(&mut dirty)).await;
            if !p.unchecked.is_empty() {
                dirty.disks.extend(p.unchecked);
                scan_at = Some(Instant::now() + RETRY_FIRST);
            }
            pending = Some(p.status);
            retry_at = Instant::now();
        }
        if let Some(s) = &pending
            && Instant::now() >= retry_at
        {
            match sink.write(s).await {
                Ok(wrote) => {
                    if wrote {
                        tracing::info!(blank = s.blank, "NodeDisks updated");
                    }
                    pending = None;
                    backoff = RETRY_FIRST;
                }
                Err(error) => {
                    tracing::warn!(%error, retry = ?backoff, "NodeDisks write failed");
                    retry_at = Instant::now() + backoff;
                    backoff = (backoff * 2).min(RETRY_MAX);
                }
            }
        }
        let wake = earliest(&[scan_at, pending.as_ref().map(|_| retry_at), Some(next_full)])
            .unwrap_or(next_full);
        tokio::select! {
            r = stop.wait_for(|s| *s) => {
                let _ = r;
                return;
            }
            () = tokio::time::sleep_until(wake) => {
                if Instant::now() >= next_full {
                    dirty.all = true;
                    scan_at = Some(Instant::now());
                    next_full = Instant::now() + full_every;
                }
            }
            w = wakes.recv(), if wakes_open => match w {
                Some(w) => {
                    dirty.add(w);
                    let soon = Instant::now() + DEBOUNCE;
                    scan_at = Some(scan_at.map_or(soon, |t| t.min(soon)));
                }
                None => wakes_open = false,
            },
        }
    }
}

/// The agent's inventory task; the first pass waits for the Pool cache.
pub async fn run(
    client: Client,
    scanner: Scanner,
    ready: impl std::future::Future<Output = ()> + Send,
    wakes: UnboundedReceiver<Wake>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    ready.await;
    let udev = scanner.udev_readable();
    tracing::info!(watch = ?scanner.watch(), udev, "NodeDisks inventory started");
    if !udev {
        tracing::warn!(
            "host udev database unreadable: a multipath path not yet in a map may read Blank"
        );
    }
    let full_every = match scanner.watch() {
        DiskWatch::Uevent => FULL_EVERY,
        DiskWatch::Poll => POLL_EVERY,
    };
    let sink = KubeSink {
        client,
        node: scanner.node().to_owned(),
    };
    run_loop(scanner, sink, wakes, full_every, stop).await;
}

#[cfg(test)]
mod tests;
