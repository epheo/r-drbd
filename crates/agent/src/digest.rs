//! Holder copy digests. Under live buffered writes the copies of one round
//! can differ for good, so each holder hashes its snapshot LV once the
//! Snapshot is Ready, and only matching digests let a restore skip its sync.
//! One hash at a time per node, off the reconcile loop, at idle IO priority.

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use model::snapshot::DIGEST_CHUNK;
use ring::digest::{Context, SHA256, digest};

use crate::Exec;
use crate::drbd::{self, argv};
use crate::exec::{DEFAULT_TIMEOUT, run_ok};
use crate::storage::Lvm;
use crate::watch::Trigger;

/// O_DIRECT needs buffers, offsets and lengths aligned to the logical
/// block size, at most 4 KiB.
const ALIGN: usize = 4096;
/// A failed hash is tried again after this.
const RETRY: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest {
    /// Lowercase hex.
    pub hex: String,
    pub bytes: u64,
}

/// sha256 over the sha256 of each `buf.len()` chunk of `f` over
/// [0, bytes); the last chunk is shorter when `bytes` is not a multiple.
/// Reads stay aligned for O_DIRECT, so `f` must hold `bytes` rounded up
/// to [`ALIGN`].
pub fn root(f: &impl FileExt, bytes: u64, buf: &mut [u8]) -> std::io::Result<String> {
    let chunk = buf.len();
    let mut root = Context::new(&SHA256);
    let mut at = 0u64;
    while at < bytes {
        let len = usize::try_from(bytes - at).map_or(chunk, |l| l.min(chunk));
        let read = len.next_multiple_of(ALIGN).min(chunk);
        fill(f, &mut buf[..read], at)?;
        let data = &buf[..len];
        // Unmapped thin ranges read as zeros without disk IO; skipping
        // their hash makes the cost follow the allocated bytes.
        if len == DIGEST_CHUNK && data.iter().all(|b| *b == 0) {
            root.update(zero_chunk());
        } else {
            root.update(digest(&SHA256, data).as_ref());
        }
        at += len as u64;
    }
    Ok(hex(root.finish().as_ref()))
}

fn zero_chunk() -> &'static [u8] {
    static ZERO: OnceLock<Vec<u8>> = OnceLock::new();
    ZERO.get_or_init(|| digest(&SHA256, &vec![0; DIGEST_CHUNK]).as_ref().to_vec())
}

fn fill(f: &impl FileExt, buf: &mut [u8], at: u64) -> std::io::Result<()> {
    let mut done = 0;
    while done < buf.len() {
        match f.read_at(&mut buf[done..], at + done as u64)? {
            0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            n => done += n,
        }
    }
    Ok(())
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

/// [`root`] of a block device read with O_DIRECT, so the page cache
/// neither serves stale pages nor fills with the image.
fn hash_device(dev: &str, bytes: u64) -> Result<String, String> {
    let f = File::options()
        .read(true)
        .custom_flags(rustix::fs::OFlags::DIRECT.bits().cast_signed())
        .open(dev)
        .map_err(|e| format!("open {dev}: {e}"))?;
    let mut mem = vec![0u8; DIGEST_CHUNK + ALIGN];
    let off = mem.as_ptr().align_offset(ALIGN);
    root(&f, bytes, &mut mem[off..off + DIGEST_CHUNK]).map_err(|e| format!("read {dev}: {e}"))
}

/// Hashes one snapshot LV.
#[async_trait::async_trait]
pub trait Hasher: std::fmt::Debug + Send + Sync {
    async fn digest(&self, vg: &str, lv: &str) -> Result<Digest, String>;
}

/// The snapshot LV on this host: activated for the read only, its range
/// from its own superblock.
pub struct LvHasher {
    lvm: Lvm,
    exec: Arc<dyn Exec>,
}

impl std::fmt::Debug for LvHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LvHasher").finish_non_exhaustive()
    }
}

impl LvHasher {
    /// `exec` must not be the reconcile loop's: a hash takes minutes and
    /// would hold the loop's deadline.
    pub fn new(exec: Arc<dyn Exec>) -> Self {
        Self {
            lvm: Lvm::new(exec.clone()),
            exec,
        }
    }

    async fn read(&self, dev: String) -> Result<Digest, String> {
        let out = run_ok(self.exec.as_ref(), &argv::dump_md(&dev), DEFAULT_TIMEOUT)
            .await
            .map_err(|e| e.to_string())?;
        let bytes = drbd::la_size(&out.stdout)?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (exec, handle) = (self.exec.clone(), tokio::runtime::Handle::current());
        // A thread of its own: the IO priority is per thread, and a pooled
        // one would keep it for other work.
        std::thread::Builder::new()
            .name("digest".into())
            .spawn(move || {
                if let Err(e) = handle.block_on(idle_io(exec.as_ref())) {
                    tracing::warn!(error = %e, "digest runs at the default IO priority");
                }
                let _ = tx.send(hash_device(&dev, bytes));
            })
            .map_err(|e| format!("digest thread: {e}"))?;
        let hex = rx.await.map_err(|_| "digest thread ended".to_owned())??;
        Ok(Digest { hex, bytes })
    }
}

#[async_trait::async_trait]
impl Hasher for LvHasher {
    async fn digest(&self, vg: &str, lv: &str) -> Result<Digest, String> {
        let r = match self.lvm.activate_snapshot(vg, lv).await {
            Ok(()) => self.read(format!("/dev/{vg}/{lv}")).await,
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = self.lvm.deactivate(vg, lv).await {
            tracing::warn!(vg, lv, error = %e, "snapshot LV left active after its digest");
        }
        r
    }
}

/// The calling thread to the idle IO class. The image has no ionice and
/// the agent no unsafe code, so the host's ionice sets it by thread id,
/// which the agent's host PID namespace shares.
pub async fn idle_io(exec: &dyn Exec) -> Result<(), String> {
    let link = std::fs::read_link("/proc/thread-self").map_err(|e| e.to_string())?;
    let tid = link
        .file_name()
        .and_then(|t| t.to_str())
        .ok_or("no thread id")?
        .to_owned();
    let argv: Vec<String> = ["ionice", "-c", "3", "-p", &tid]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    run_ok(exec, &argv, DEFAULT_TIMEOUT)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// One snapshot LV of this node to hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub uid: String,
    pub snapshot: String,
    pub vg: String,
    pub lv: String,
    /// A restore waits for this digest.
    pub first: bool,
}

#[derive(Debug)]
struct Done {
    result: Result<Digest, String>,
    at: tokio::time::Instant,
}

#[derive(Debug, Default)]
struct State {
    running: Option<String>,
    done: HashMap<String, Done>,
}

/// The digest jobs of this node, by Snapshot uid. Without a hasher no job
/// ever starts.
#[derive(Debug, Clone, Default)]
pub struct Digests {
    hasher: Option<Arc<dyn Hasher>>,
    wake: Option<Arc<Trigger>>,
    state: Arc<Mutex<State>>,
}

impl Digests {
    /// `wake` brings the pass that acks a finished digest.
    pub fn new(hasher: Arc<dyn Hasher>, wake: Option<Arc<Trigger>>) -> Self {
        Self {
            hasher: Some(hasher),
            wake,
            state: Arc::default(),
        }
    }

    /// The digest of a finished job.
    pub fn result(&self, uid: &str) -> Option<Digest> {
        let st = self.state.lock().unwrap();
        st.done.get(uid)?.result.as_ref().ok().cloned()
    }

    /// `jobs` are every snapshot this node still has to hash. Forgets the
    /// results of the others, and starts one job when none runs: those a
    /// restore waits for first, a failed one only after [`RETRY`].
    pub fn schedule(&self, mut jobs: Vec<Job>) {
        let Some(hasher) = self.hasher.clone() else {
            return;
        };
        let mut st = self.state.lock().unwrap();
        st.done.retain(|uid, _| jobs.iter().any(|j| j.uid == *uid));
        if st.running.is_some() {
            return;
        }
        jobs.sort_by(|a, b| {
            b.first
                .cmp(&a.first)
                .then_with(|| a.snapshot.cmp(&b.snapshot))
        });
        let Some(job) = jobs.into_iter().find(|j| {
            st.done
                .get(&j.uid)
                .is_none_or(|d| d.result.is_err() && d.at.elapsed() >= RETRY)
        }) else {
            return;
        };
        st.running = Some(job.uid.clone());
        drop(st);
        let (state, wake) = (self.state.clone(), self.wake.clone());
        tokio::spawn(async move {
            tracing::info!(
                snapshot = job.snapshot,
                lv = job.lv,
                "hashing the snapshot LV"
            );
            let result = hasher.digest(&job.vg, &job.lv).await;
            match &result {
                Ok(d) => tracing::info!(
                    snapshot = job.snapshot,
                    digest = d.hex,
                    bytes = d.bytes,
                    "snapshot LV hashed"
                ),
                Err(e) => {
                    tracing::warn!(snapshot = job.snapshot, error = %e, "hashing the snapshot LV failed");
                }
            }
            let mut st = state.lock().unwrap();
            st.running = None;
            st.done.insert(
                job.uid,
                Done {
                    result,
                    at: tokio::time::Instant::now(),
                },
            );
            drop(st);
            if let Some(w) = wake {
                w.fire();
            }
        });
    }
}

#[cfg(test)]
mod tests;
