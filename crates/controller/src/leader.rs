//! One active controller, elected with a coordination.k8s.io Lease.

use std::time::{Duration, Instant};

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use kube::api::PostParams;
use kube::{Api, Client};
use model::keys::{CONTROLLER_LEASE_NAME, LEASE_RENEW_SECS, LEASE_STALE_SECS};
use tracing::{debug, info, warn};

use crate::Error;

const RENEW: Duration = Duration::from_secs(LEASE_RENEW_SECS);
/// Bound of one Lease API call, so a hung connection cannot stall renewal.
const CALL: Duration = Duration::from_secs(5);
/// The holder steps down well before contenders see the Lease expire
/// (LEASE_STALE_SECS from their own observation), so two leaders never overlap.
pub const RENEW_DEADLINE: Duration = Duration::from_secs(20);

fn stale_secs() -> i32 {
    i32::try_from(LEASE_STALE_SECS).unwrap_or(i32::MAX)
}

#[derive(Debug, thiserror::Error)]
enum LeaseError {
    #[error("Lease API call timed out after {} s", CALL.as_secs())]
    Timeout,
    #[error(transparent)]
    Kube(#[from] kube::Error),
}

async fn timed<T>(f: impl Future<Output = Result<T, kube::Error>>) -> Result<T, LeaseError> {
    tokio::time::timeout(CALL, f)
        .await
        .map_err(|_| LeaseError::Timeout)?
        .map_err(Into::into)
}

/// Expiry as a contender sees it: measured on this process's clock from
/// the first time it saw the current (holder, renewTime) pair, so clock
/// skew between nodes cannot shorten a holder's term.
#[derive(Debug, Default)]
pub struct Observer {
    seen: Option<(String, Option<MicroTime>, Instant)>,
}

impl Observer {
    pub fn is_free(&mut self, spec: Option<&LeaseSpec>, identity: &str, now: Instant) -> bool {
        let Some(spec) = spec else { return true };
        let holder = match spec.holder_identity.as_deref() {
            None | Some("") => return true,
            Some(h) if h == identity => return true,
            Some(h) => h.to_owned(),
        };
        let renewed = spec.renew_time.clone();
        let since = match &self.seen {
            Some((h, r, at)) if *h == holder && *r == renewed => *at,
            _ => {
                self.seen = Some((holder, renewed, now));
                now
            }
        };
        let duration = spec.lease_duration_seconds.unwrap_or_else(stale_secs);
        let duration = Duration::from_secs(u64::try_from(duration).unwrap_or(0));
        now.duration_since(since) >= duration
    }
}

/// Blocks until this process holds the Lease.
pub async fn acquire(client: &Client, namespace: &str, identity: &str) -> Result<(), Error> {
    let api: Api<Lease> = Api::namespaced(client.clone(), namespace);
    let mut observer = Observer::default();
    loop {
        match try_acquire(&api, identity, &mut observer).await {
            Ok(true) => {
                info!(
                    lease = CONTROLLER_LEASE_NAME,
                    namespace, identity, "lease acquired"
                );
                return Ok(());
            }
            Ok(false) => debug!(identity, "lease held by another controller"),
            Err(e) => warn!(error = %e, "lease acquire attempt failed"),
        }
        tokio::time::sleep(RENEW).await;
    }
}

async fn try_acquire(
    api: &Api<Lease>,
    identity: &str,
    observer: &mut Observer,
) -> Result<bool, LeaseError> {
    let now = Timestamp::now();
    let Some(mut lease) = timed(api.get_opt(CONTROLLER_LEASE_NAME)).await? else {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(CONTROLLER_LEASE_NAME.to_owned()),
                ..ObjectMeta::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(identity.to_owned()),
                lease_duration_seconds: Some(stale_secs()),
                acquire_time: Some(MicroTime(now)),
                renew_time: Some(MicroTime(now)),
                lease_transitions: Some(0),
                ..LeaseSpec::default()
            }),
        };
        return match timed(api.create(&PostParams::default(), &lease)).await {
            Ok(_) => Ok(true),
            Err(LeaseError::Kube(kube::Error::Api(s))) if s.is_already_exists() => Ok(false),
            Err(e) => Err(e),
        };
    };
    if !observer.is_free(lease.spec.as_ref(), identity, Instant::now()) {
        return Ok(false);
    }
    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    if spec.holder_identity.as_deref() != Some(identity) {
        spec.lease_transitions = Some(spec.lease_transitions.unwrap_or(0) + 1);
        spec.acquire_time = Some(MicroTime(now));
    }
    spec.holder_identity = Some(identity.to_owned());
    spec.lease_duration_seconds = Some(stale_secs());
    spec.renew_time = Some(MicroTime(now));
    // replace carries the resourceVersion read above: two contenders
    // cannot both win.
    match timed(api.replace(CONTROLLER_LEASE_NAME, &PostParams::default(), &lease)).await {
        Ok(_) => Ok(true),
        Err(LeaseError::Kube(kube::Error::Api(s))) if s.is_conflict() => Ok(false),
        Err(e) => Err(e),
    }
}

/// Renews every LEASE_RENEW_SECS; returns once the Lease is lost: another
/// holder took it, or no renewal succeeded within RENEW_DEADLINE.
pub async fn hold(client: Client, namespace: &str, identity: &str) -> Error {
    let api: Api<Lease> = Api::namespaced(client, namespace);
    hold_loop(|| renew(&api, identity), RENEW, RENEW_DEADLINE).await
}

/// The deadline is raced against the sleep and the renewal, so a slow or
/// failing renew cannot push the step-down past `last_ok + deadline`.
async fn hold_loop<F, Fut>(mut renew: F, period: Duration, deadline: Duration) -> Error
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, LeaseError>>,
{
    let mut last_ok = tokio::time::Instant::now();
    loop {
        let attempt = async {
            tokio::time::sleep(period).await;
            renew().await
        };
        tokio::select! {
            r = attempt => match r {
                Ok(true) => last_ok = tokio::time::Instant::now(),
                Ok(false) => {
                    warn!("lease taken by another controller");
                    return Error::LostLease(CONTROLLER_LEASE_NAME.to_owned());
                }
                Err(e) => warn!(error = %e, "lease renewal failed"),
            },
            () = tokio::time::sleep_until(last_ok + deadline) => {
                warn!("no lease renewal within the deadline; stepping down");
                return Error::LostLease(CONTROLLER_LEASE_NAME.to_owned());
            }
        }
    }
}

async fn renew(api: &Api<Lease>, identity: &str) -> Result<bool, LeaseError> {
    let mut lease = timed(api.get(CONTROLLER_LEASE_NAME)).await?;
    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    if spec.holder_identity.as_deref() != Some(identity) {
        return Ok(false);
    }
    spec.renew_time = Some(MicroTime(Timestamp::now()));
    timed(api.replace(CONTROLLER_LEASE_NAME, &PostParams::default(), &lease)).await?;
    Ok(true)
}

/// Clears the holder so a successor starts at once instead of waiting for
/// expiry; best effort, a failure only delays the successor.
pub async fn release(client: Client, namespace: &str, identity: &str) {
    let api: Api<Lease> = Api::namespaced(client, namespace);
    let result = async {
        let mut lease = timed(api.get(CONTROLLER_LEASE_NAME)).await?;
        let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
        if spec.holder_identity.as_deref() != Some(identity) {
            return Ok(());
        }
        spec.holder_identity = None;
        timed(api.replace(CONTROLLER_LEASE_NAME, &PostParams::default(), &lease)).await?;
        Ok::<(), LeaseError>(())
    }
    .await;
    match result {
        Ok(()) => info!(identity, "lease released"),
        Err(e) => warn!(error = %e, "lease release failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(holder: &str, renewed: i64) -> LeaseSpec {
        LeaseSpec {
            holder_identity: Some(holder.into()),
            lease_duration_seconds: Some(40),
            renew_time: Some(MicroTime(
                Timestamp::from_second(renewed).expect("timestamp"),
            )),
            ..LeaseSpec::default()
        }
    }

    #[test]
    fn freeness() {
        let t0 = Instant::now();
        let mut o = Observer::default();
        assert!(o.is_free(None, "me", t0));
        assert!(o.is_free(Some(&spec("me", 1)), "me", t0));
        assert!(o.is_free(Some(&LeaseSpec::default()), "me", t0));
        // A renewTime far in the past does not count: only our observation does.
        assert!(!o.is_free(Some(&spec("other", 1)), "me", t0));
        assert!(!o.is_free(Some(&spec("other", 1)), "me", t0 + Duration::from_secs(39)));
        // A renewal restarts the clock.
        assert!(!o.is_free(Some(&spec("other", 2)), "me", t0 + Duration::from_secs(41)));
        assert!(!o.is_free(Some(&spec("other", 2)), "me", t0 + Duration::from_secs(80)));
        assert!(o.is_free(Some(&spec("other", 2)), "me", t0 + Duration::from_secs(81)));
    }

    async fn steps_down_at(renew_outcome: fn(u32) -> Option<bool>) -> Duration {
        let start = tokio::time::Instant::now();
        let mut calls = 0;
        let e = hold_loop(
            || {
                calls += 1;
                let n = calls;
                async move {
                    match renew_outcome(n) {
                        Some(true) => Ok(true),
                        Some(false) => Err(LeaseError::Timeout),
                        // A renew that never answers.
                        None => std::future::pending().await,
                    }
                }
            },
            RENEW,
            RENEW_DEADLINE,
        )
        .await;
        assert!(matches!(e, Error::LostLease(_)));
        start.elapsed()
    }

    #[tokio::test(start_paused = true)]
    async fn renew_deadline_is_hard() {
        // Every renewal fails: down at exactly 20 s, not after a third tick.
        assert_eq!(steps_down_at(|_| Some(false)).await, RENEW_DEADLINE);
        // Renewals hang: the deadline still fires.
        assert_eq!(steps_down_at(|_| None).await, RENEW_DEADLINE);
        // Two good renewals at 10 s and 20 s, then a hang: 20 s after the last.
        assert_eq!(
            steps_down_at(|n| (n <= 2).then_some(true)).await,
            Duration::from_secs(40)
        );
    }
}
