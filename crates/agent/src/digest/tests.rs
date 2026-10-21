use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

const C: usize = DIGEST_CHUNK;

fn sha(b: &[u8]) -> Vec<u8> {
    digest(&SHA256, b).as_ref().to_vec()
}

fn file_with(bytes: &[u8]) -> File {
    static N: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "r-drbd-digest-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let mut f = File::create(&path).unwrap();
    f.write_all(bytes).unwrap();
    let f = File::open(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    f
}

fn root_of(data: &[u8], bytes: u64) -> String {
    let mut buf = vec![0u8; C];
    root(&file_with(data), bytes, &mut buf).unwrap()
}

#[test]
fn root_hashes_chunk_hashes_in_order() {
    let mut data = vec![0u8; 3 * C];
    data[C + 7] = 1;
    let want = {
        let mut cat = sha(&vec![0u8; C]);
        cat.extend(sha(&data[C..2 * C]));
        cat.extend(sha(&vec![0u8; C]));
        hex(&sha(&cat))
    };
    assert_eq!(root_of(&data, 3 * C as u64), want);
    assert_eq!(want.len(), 64);
}

#[test]
fn a_zero_chunk_counts_as_its_known_hash() {
    assert_eq!(zero_chunk(), sha(&vec![0u8; C]));
    let zeros = vec![0u8; 2 * C];
    assert_eq!(
        root_of(&zeros, 2 * C as u64),
        hex(&sha(&[zero_chunk(), zero_chunk()].concat()))
    );
}

#[test]
fn a_tail_hashes_only_its_own_bytes() {
    // 1000 bytes into the last chunk; past them lies other data, read
    // for alignment and never hashed.
    let bytes = C as u64 + 1000;
    let mut data = vec![0u8; C + ALIGN];
    data[10] = 9;
    data[C + 999] = 5;
    let want = {
        let mut cat = sha(&data[..C]);
        cat.extend(sha(&data[C..C + 1000]));
        hex(&sha(&cat))
    };
    let mut other = data.clone();
    other[C + 1000..].fill(0xff);
    assert_eq!(root_of(&data, bytes), want);
    assert_eq!(root_of(&other, bytes), want);
    // A zero tail is hashed at its own length, not as a full chunk.
    let zeros = vec![0u8; C + ALIGN];
    assert_eq!(
        root_of(&zeros, bytes),
        hex(&sha(&[zero_chunk(), &sha(&[0u8; 1000])].concat()))
    );
}

#[test]
fn one_byte_changes_the_root() {
    let a = vec![0u8; 2 * C];
    let mut b = a.clone();
    b[2 * C - 1] = 1;
    assert_ne!(root_of(&a, 2 * C as u64), root_of(&b, 2 * C as u64));
}

#[test]
fn a_short_device_is_an_error() {
    let mut buf = vec![0u8; C];
    let f = file_with(&vec![0u8; C]);
    let e = root(&f, 2 * C as u64, &mut buf).unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[derive(Debug)]
struct Fake {
    calls: Mutex<Vec<String>>,
    fail: bool,
}

#[async_trait::async_trait]
impl Hasher for Fake {
    async fn digest(&self, _vg: &str, lv: &str) -> Result<Digest, String> {
        self.calls.lock().unwrap().push(lv.to_owned());
        if self.fail {
            return Err("no".into());
        }
        Ok(Digest {
            hex: format!("h-{lv}"),
            bytes: 1,
        })
    }
}

fn job(uid: &str, first: bool) -> Job {
    Job {
        uid: uid.into(),
        snapshot: format!("s-{uid}"),
        vg: "vg".into(),
        lv: format!("lv-{uid}"),
        first,
    }
}

async fn settle(d: &Digests) {
    for _ in 0..100 {
        if d.state.lock().unwrap().running.is_none() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("job still running");
}

#[tokio::test]
async fn one_job_at_a_time_restores_first() {
    let fake = Arc::new(Fake {
        calls: Mutex::default(),
        fail: false,
    });
    let wake = Arc::new(Trigger::default());
    let d = Digests::new(fake.clone(), Some(wake.clone()));
    let jobs = vec![job("a", false), job("b", true)];
    d.schedule(jobs.clone());
    // A second schedule while one runs starts nothing.
    d.schedule(jobs.clone());
    settle(&d).await;
    assert_eq!(*fake.calls.lock().unwrap(), ["lv-b"]);
    assert_eq!(d.result("b").unwrap().hex, "h-lv-b");
    assert!(d.result("a").is_none());
    tokio::time::timeout(Duration::from_secs(1), wake.changed())
        .await
        .expect("a finished job wakes a pass");

    d.schedule(jobs);
    settle(&d).await;
    assert_eq!(*fake.calls.lock().unwrap(), ["lv-b", "lv-a"]);
    // Acked: no longer listed, its result goes.
    d.schedule(vec![job("a", false)]);
    assert!(d.result("b").is_none());
    assert!(d.result("a").is_some());
}

#[tokio::test(start_paused = true)]
async fn a_failed_job_waits_before_its_retry() {
    let fake = Arc::new(Fake {
        calls: Mutex::default(),
        fail: true,
    });
    let d = Digests::new(fake.clone(), None);
    d.schedule(vec![job("a", false)]);
    settle(&d).await;
    assert!(d.result("a").is_none());
    d.schedule(vec![job("a", false)]);
    settle(&d).await;
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
    tokio::time::advance(RETRY).await;
    d.schedule(vec![job("a", false)]);
    settle(&d).await;
    assert_eq!(fake.calls.lock().unwrap().len(), 2);
}

#[test]
fn without_a_hasher_nothing_runs() {
    let d = Digests::default();
    d.schedule(vec![job("a", true)]);
    assert!(d.state.lock().unwrap().running.is_none());
}
