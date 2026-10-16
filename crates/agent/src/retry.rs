//! Short retries for API writes. A connection that died during a network
//! partition fails the first request after it heals; kube-rs dials a new one
//! on the next request, so retrying within seconds beats waiting for the
//! 30 s resync (Volume Ready lagged 44 s behind DRBD in a partition test).

use std::future::Future;
use std::time::Duration;

/// Sleeps between attempts: four tries within about 7 s.
pub const BACKOFF: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

/// Transport failures and server-side errors are worth another try; a
/// rejected request (4xx other than 429) fails the same way again.
pub fn retryable(e: &kube::Error) -> bool {
    match e {
        kube::Error::Api(s) => s.code >= 500 || s.code == 429,
        _ => true,
    }
}

/// Runs `op` until it succeeds, `retry` says no, or `delays` run out.
pub async fn with_backoff<T, E, F, Fut, R>(
    what: &str,
    delays: &[Duration],
    retry: R,
    mut op: F,
) -> Result<T, E>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    R: Fn(&E) -> bool,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < delays.len() && retry(&e) => {
                let wait = delays[attempt];
                tracing::warn!(what, error = %e, ?wait, "API call failed, retrying");
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// One pass worth of writes. Once one exhausts its backoff on a retryable
/// error the API is presumed unreachable and the rest are skipped (None):
/// against a dead connection each try still costs a connect or read
/// timeout. Writes are diffs against the cache, so the next pass rebuilds them.
pub async fn each_with_backoff<'a, I, E, F, Fut, R>(
    items: &'a [I],
    delays: &[Duration],
    retry: R,
    mut op: F,
) -> Vec<Option<Result<(), E>>>
where
    E: std::fmt::Display,
    F: FnMut(&'a I) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    R: Fn(&E) -> bool,
{
    let mut out = Vec::with_capacity(items.len());
    let mut dead = false;
    for item in items {
        if dead {
            out.push(None);
            continue;
        }
        let r = with_backoff("write", delays, &retry, || op(item)).await;
        dead = r.as_ref().is_err_and(&retry);
        out.push(Some(r));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A writer that fails `fails` times, then succeeds.
    struct Flaky {
        fails: u32,
        calls: AtomicU32,
    }

    impl Flaky {
        async fn write(&self) -> Result<(), String> {
            // A real write awaits the network.
            tokio::task::yield_now().await;
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fails {
                Err("connect error: connection refused".into())
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failing_once_is_retried_after_a_second() {
        let w = Flaky {
            fails: 1,
            calls: AtomicU32::new(0),
        };
        let started = tokio::time::Instant::now();
        with_backoff("status", &BACKOFF, |_| true, || w.write())
            .await
            .unwrap();
        assert_eq!(w.calls.load(Ordering::SeqCst), 2);
        assert_eq!(started.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_the_backoff_or_when_not_retryable() {
        let w = Flaky {
            fails: 10,
            calls: AtomicU32::new(0),
        };
        let started = tokio::time::Instant::now();
        assert!(
            with_backoff("status", &BACKOFF, |_| true, || w.write())
                .await
                .is_err()
        );
        assert_eq!(w.calls.load(Ordering::SeqCst), 4);
        assert_eq!(started.elapsed(), Duration::from_secs(7));

        let w = Flaky {
            fails: 10,
            calls: AtomicU32::new(0),
        };
        assert!(
            with_backoff("status", &BACKOFF, |_| false, || w.write())
                .await
                .is_err()
        );
        assert_eq!(w.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dead_connection_costs_one_backoff_per_pass() {
        // Every write fails: the first waits out the backoff, the rest are skipped.
        let w = Flaky {
            fails: 100,
            calls: AtomicU32::new(0),
        };
        let started = tokio::time::Instant::now();
        let r = each_with_backoff(&[1, 2, 3], &BACKOFF, |_| true, |_| w.write()).await;
        assert!(matches!(r[0], Some(Err(_))));
        assert!(r[1].is_none() && r[2].is_none());
        assert_eq!(w.calls.load(Ordering::SeqCst), 4);
        assert_eq!(started.elapsed(), Duration::from_secs(7));

        // A write that recovers on retry keeps the next ones going.
        let w = Flaky {
            fails: 1,
            calls: AtomicU32::new(0),
        };
        let r = each_with_backoff(&[1, 2], &BACKOFF, |_| true, |_| w.write()).await;
        assert!(r.iter().all(|r| matches!(r, Some(Ok(())))));
        assert_eq!(w.calls.load(Ordering::SeqCst), 3);

        // A rejected write is not a dead API: the next one still runs.
        let w = Flaky {
            fails: 1,
            calls: AtomicU32::new(0),
        };
        let r = each_with_backoff(&[1, 2], &BACKOFF, |_| false, |_| w.write()).await;
        assert!(matches!(r[0], Some(Err(_))) && matches!(r[1], Some(Ok(()))));
    }

    #[test]
    fn rejected_requests_are_not_retried() {
        let api = |code| {
            kube::Error::Api(Box::new(kube::core::Status {
                code,
                ..kube::core::Status::default()
            }))
        };
        assert!(!retryable(&api(422)));
        assert!(!retryable(&api(409)));
        assert!(retryable(&api(503)));
        assert!(retryable(&api(429)));
    }
}
