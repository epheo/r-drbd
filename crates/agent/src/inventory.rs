//! The node's NodeDisks: every whole disk with its state, so the admin
//! picks a Pool's disks from kubectl.
//!
//! Event driven, in its own task so a slow disk never holds up Pools and
//! Volumes. Kernel uevents mark single disks dirty (udev turns a close
//! after write, as by mkfs or pvcreate, into a change uevent); a mount
//! table change, a dm map or a Pool device change refresh the cheap
//! facts of every disk. Only dirty disks, and disks whose cheap facts
//! changed, are read (wipefs) or opened; every disk is at start and
//! hourly. Without a uevent socket, or once it fails, every 5 minutes.

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

pub use events::Watcher;
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
/// A disk udevd was not done with is read again this soon: udevd's end
/// sends no kernel uevent.
pub const RECHECK: Duration = Duration::from_secs(5);
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
    /// The uevent watch ended: poll from now on.
    WatchLost,
}

/// What the next pass must read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dirty {
    /// Read every disk and the LVM reports.
    pub all: bool,
    pub lvm: bool,
    /// Kernel names a uevent named: read again, udev wait restarted.
    pub disks: BTreeSet<String>,
    /// Kernel names the loop brings back: unchecked or unsettled last time.
    pub again: BTreeSet<String>,
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
            Wake::Lvm => self.lvm = true,
            Wake::All => self.all = true,
            Wake::Refresh | Wake::WatchLost => {}
        }
    }
}

/// One pass: the status, the disks it had no time for, and the disks
/// udevd had not settled.
#[derive(Debug, Clone, PartialEq)]
pub struct Pass {
    pub status: NodeDisksStatus,
    pub unchecked: BTreeSet<String>,
    pub unsettled: BTreeSet<String>,
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

/// The object as the API holds it, created if absent.
async fn current(client: &Client, node: &str) -> Result<NodeDisks, String> {
    let api: Api<NodeDisks> = Api::all(client.clone());
    if let Some(o) = api.get_opt(node).await.map_err(|e| e.to_string())? {
        Ok(o)
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
            .map_err(|e| format!("create: {e}"))
    }
}

/// A merge patch replaces the whole list: rows of unplugged disks go.
async fn patch(client: &Client, node: &str, now: &NodeDisksStatus) -> Result<(), String> {
    Api::<NodeDisks>::all(client.clone())
        .patch_status(
            node,
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({ "status": now })),
        )
        .await
        .map(drop)
        .map_err(|e| format!("patch status: {e}"))
}

/// Where passes come from; a trait so the loop is tested without disks.
#[async_trait::async_trait]
pub trait Source: Send {
    async fn pass(&mut self, dirty: Dirty) -> Pass;
    fn set_watch(&mut self, watch: DiskWatch);
}

/// Where a status goes; true when it wrote. The loop calls it only with
/// a status other than the last one it wrote.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, status: &NodeDisksStatus) -> Result<bool, String>;
}

/// Reads the object once, on the first write and after an error; later
/// writes patch straight away.
pub struct KubeSink {
    client: Client,
    node: String,
    seen: std::sync::atomic::AtomicBool,
}

impl KubeSink {
    pub fn new(client: Client, node: String) -> Self {
        Self {
            client,
            node,
            seen: std::sync::atomic::AtomicBool::new(false),
        }
    }
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
        use std::sync::atomic::Ordering;
        let res = async {
            if !self.seen.load(Ordering::SeqCst) {
                let o = current(&self.client, &self.node).await?;
                self.seen.store(true, Ordering::SeqCst);
                if !changed(Some(&o), status) {
                    return Ok(false);
                }
            }
            patch(&self.client, &self.node, status).await?;
            Ok(true)
        }
        .await;
        if res.is_err() {
            self.seen.store(false, Ordering::SeqCst);
        }
        res
    }
}

fn earliest(times: &[Option<Instant>]) -> Option<Instant> {
    times.iter().flatten().min().copied()
}

/// Passes at start, [`DEBOUNCE`] after wakes and every `full_every`;
/// writes a pass whose status differs from the last written, retrying a
/// failed write alone with backoff that wakes never shorten.
pub async fn run_loop(
    mut src: impl Source,
    sink: impl Sink,
    mut wakes: UnboundedReceiver<Wake>,
    mut full_every: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut dirty = Dirty::everything();
    let mut scan_at = Some(Instant::now());
    let mut next_full = Instant::now() + full_every;
    let mut written: Option<NodeDisksStatus> = None;
    let mut pending: Option<NodeDisksStatus> = None;
    let mut retry_at = Instant::now();
    let mut backoff = RETRY_FIRST;
    let mut failing = false;
    let mut wakes_open = true;
    loop {
        if scan_at.is_some_and(|t| Instant::now() >= t) {
            scan_at = None;
            let full = dirty.all;
            let p = src.pass(std::mem::take(&mut dirty)).await;
            if *stop.borrow() {
                return;
            }
            if !p.unchecked.is_empty() {
                dirty.again.extend(p.unchecked);
                scan_at = Some(Instant::now() + RETRY_FIRST);
            }
            if !p.unsettled.is_empty() {
                dirty.again.extend(p.unsettled);
                let soon = Instant::now() + RECHECK;
                scan_at = Some(scan_at.map_or(soon, |t| t.min(soon)));
            }
            // A full pass writes once more: an object deleted by hand comes back.
            if full {
                written = None;
            }
            if written.as_ref() == Some(&p.status) {
                pending = None;
            } else {
                pending = Some(p.status);
                if !failing {
                    retry_at = Instant::now();
                }
            }
        }
        if let Some(s) = &pending
            && Instant::now() >= retry_at
        {
            match sink.write(s).await {
                Ok(wrote) => {
                    if wrote {
                        tracing::info!(blank = s.blank, "NodeDisks updated");
                    }
                    written = pending.take();
                    backoff = RETRY_FIRST;
                    failing = false;
                }
                Err(error) => {
                    tracing::warn!(%error, retry = ?backoff, "NodeDisks write failed");
                    retry_at = Instant::now() + backoff;
                    backoff = (backoff * 2).min(RETRY_MAX);
                    failing = true;
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
                Some(Wake::WatchLost) => {
                    tracing::warn!("uevent watch ended: NodeDisks checks every disk every 5 minutes from now on");
                    src.set_watch(DiskWatch::Poll);
                    full_every = POLL_EVERY;
                    next_full = next_full.min(Instant::now() + POLL_EVERY);
                    let soon = Instant::now() + DEBOUNCE;
                    scan_at = Some(scan_at.map_or(soon, |t| t.min(soon)));
                }
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

/// Drops the wakes queued while the caches load: the first pass reads
/// everything anyway. Returns whether the watch was lost meanwhile.
pub async fn drain_until_ready(
    ready: impl std::future::Future<Output = ()> + Send,
    wakes: &mut UnboundedReceiver<Wake>,
) -> bool {
    let mut ready = std::pin::pin!(ready);
    let mut lost = false;
    loop {
        tokio::select! {
            biased;
            w = wakes.recv(), if !wakes.is_empty() => match w {
                Some(Wake::WatchLost) => lost = true,
                Some(_) => {}
                None => return lost,
            },
            () = &mut ready => return lost,
        }
    }
}

/// The agent's inventory task; the first pass waits for the Pool cache.
pub async fn run(
    client: Client,
    mut scanner: Scanner,
    watcher: Option<Watcher>,
    ready: impl std::future::Future<Output = ()> + Send,
    mut wakes: UnboundedReceiver<Wake>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    // Held until this task ends: dropping it stops the watcher thread.
    let _watcher = watcher;
    if drain_until_ready(ready, &mut wakes).await {
        tracing::warn!("uevent watch ended: NodeDisks checks every disk every 5 minutes");
        scanner.set_watch(DiskWatch::Poll);
    }
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
    let sink = KubeSink::new(client, scanner.node().to_owned());
    run_loop(scanner, sink, wakes, full_every, stop).await;
}

#[cfg(test)]
mod tests;
