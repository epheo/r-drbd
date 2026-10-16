//! DRBD connections stuck out of Connected, and the TCP probe that tells
//! why: nothing listening, a firewall, or packets dropped.
//!
//! A stray connect to a waiting DRBD listener costs the peer one kernel
//! log line ("Error receiving initial packet") and no state change; once
//! Connected, DRBD closes the listener and a connect is refused. Probes
//! still run only while a connection is stuck, and rarely.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::jiff::Timestamp;
use model::{PeerProbe, ProbeResult};
use tokio::time::Instant;

/// A healthy connect takes under a second and DRBD retries every 10 s
/// (connect-int): two missed retries. With the 30 s pass interval and
/// the probe timeout the admin sees the cause within a minute.
pub const STUCK: Duration = Duration::from_secs(20);
/// Connected this long ends the window; shorter is a flap and keeps it,
/// so a flapping link reports once instead of once per flap.
pub const STABLE: Duration = Duration::from_secs(10);
/// Each probe of a listening peer logs one kernel line there; a firewall
/// fix shows as Connected anyway, so the result rarely needs a refresh.
pub const PROBE_EVERY: Duration = Duration::from_secs(300);
/// Linux resends a SYN after 1 s, then 3 s: one resend is enough on a LAN.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
/// Async sockets, no thread each, all of a pass at once so the pass waits
/// one timeout at most. The cap bounds open sockets; the rest are due
/// again on the next pass.
pub const PROBES_PER_PASS: usize = 256;
/// A result older than this is probed again when a new window opens: a
/// link that recovered and broke again may have a new cause.
pub const CARRY_MAX: Duration = Duration::from_secs(60);

const CONNECTED: &str = "Connected";
/// DRBD stopped trying: split brain or an explicit disconnect.
const STAND_ALONE: &str = "StandAlone";

/// The probe result of a connect attempt, with the OS error of a Failed one.
pub fn classify(r: &io::Result<()>) -> (ProbeResult, Option<String>) {
    use io::ErrorKind as K;
    match r {
        Ok(()) => (ProbeResult::Accepted, None),
        Err(e) => match e.kind() {
            K::ConnectionRefused => (ProbeResult::Refused, None),
            // EPERM and EACCES come from a local output filter.
            K::HostUnreachable | K::NetworkUnreachable | K::PermissionDenied => {
                (ProbeResult::Filtered, None)
            }
            K::TimedOut | K::WouldBlock => (ProbeResult::TimedOut, None),
            _ => (ProbeResult::Failed, Some(e.to_string())),
        },
    }
}

/// Whole seconds: the API stores no more, and a finer value would make
/// every status comparison see a change.
pub fn wall_now() -> Time {
    Time(Timestamp::from_second(Timestamp::now().as_second()).unwrap_or(Timestamp::UNIX_EPOCH))
}

/// A peer's agent Lease as this agent sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerLease {
    pub acquired: Option<Timestamp>,
    pub renewed: Option<Timestamp>,
}

impl PeerLease {
    /// Renewed within the stale bound by this node's clock; skew only
    /// moves when a probe is skipped, the controller judges the rest.
    pub fn fresh(&self) -> bool {
        let stale = i64::try_from(model::keys::LEASE_STALE_SECS).unwrap_or(i64::MAX);
        self.renewed
            .is_some_and(|r| Timestamp::now().as_second() - r.as_second() < stale)
    }
}

/// The peer agent's run, from its Lease acquireTime: a new run means the
/// peer restarted, and an older window or probe says nothing about it.
pub type Epoch = Option<Timestamp>;

#[derive(Debug, Clone)]
struct Conn {
    target: SocketAddr,
    epoch: Epoch,
    down_at: Instant,
    since: Time,
    up_at: Option<Instant>,
}

impl Conn {
    fn stuck(&self, now: Instant) -> bool {
        now.duration_since(self.down_at) >= STUCK
    }
}

#[derive(Debug, Clone)]
struct Probed {
    at: Instant,
    target: SocketAddr,
    epoch: Epoch,
    probe: PeerProbe,
}

/// What one connection looks like now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub state: String,
    pub target: SocketAddr,
    /// The peer reports its resource up and its agent Lease is fresh; a
    /// connection to a peer that is down or still starting is not stuck.
    pub peer_up: bool,
    pub epoch: Epoch,
    /// This node's record from its last status entry, which seeds the
    /// window and the rate limit after an agent restart.
    pub prior: Option<PeerProbe>,
}

/// A probe to run: volume, peer, address.
pub type Due = (String, String, SocketAddr);

fn age(t: &Time) -> Duration {
    let secs = wall_now().0.as_second().saturating_sub(t.0.as_second());
    Duration::from_secs(u64::try_from(secs).unwrap_or(0))
}

fn back(now: Instant, d: Duration) -> Instant {
    now.checked_sub(d).unwrap_or(now)
}

/// Per volume and peer, the window a connection has been out of Connected,
/// and the last probe, which outlives the window: a link that flaps with
/// long Connected spells between drops is probed once per PROBE_EVERY,
/// not once per drop.
#[derive(Debug, Default)]
pub struct Watch {
    conns: HashMap<String, BTreeMap<String, Conn>>,
    probes: HashMap<(String, String), Probed>,
    seeded: HashSet<String>,
}

impl Watch {
    /// The volume's connections as events2 shows them; peers missing
    /// from `seen` are dropped.
    pub fn observe(&mut self, volume: &str, seen: &BTreeMap<String, Seen>, now: Instant) {
        let first = self.seeded.insert(volume.to_owned());
        self.probes
            .retain(|(v, p), _| v != volume || seen.contains_key(p));
        let conns = self.conns.entry(volume.to_owned()).or_default();
        conns.retain(|peer, _| seen.contains_key(peer));
        for (peer, s) in seen {
            let key = (volume.to_owned(), peer.clone());
            if self
                .probes
                .get(&key)
                .is_some_and(|p| p.epoch != s.epoch || p.target != s.target)
            {
                self.probes.remove(&key);
            }
            if s.state == STAND_ALONE || !s.peer_up {
                conns.remove(peer);
                continue;
            }
            if s.state == CONNECTED {
                let done = conns.get_mut(peer).is_some_and(|c| {
                    let up = *c.up_at.get_or_insert(now);
                    now.duration_since(up) >= STABLE
                });
                if done {
                    conns.remove(peer);
                }
                continue;
            }
            let prior = s
                .prior
                .as_ref()
                .filter(|p| first && p.address == s.target.ip().to_string())
                .filter(|p| p.port == s.target.port());
            if let Some(c) = conns.get_mut(peer)
                && c.epoch == s.epoch
            {
                c.up_at = None;
                c.target = s.target;
                continue;
            }
            let (down_at, since) = match prior {
                Some(p) => (back(now, age(&p.since)), p.since.clone()),
                None => (now, wall_now()),
            };
            if prior.is_none()
                && self
                    .probes
                    .get(&key)
                    .is_some_and(|p| now.duration_since(p.at) >= CARRY_MAX)
            {
                self.probes.remove(&key);
            }
            if let Some(p) = prior {
                self.probes.entry(key).or_insert_with(|| Probed {
                    at: back(now, age(&p.probed_at)),
                    target: s.target,
                    epoch: s.epoch,
                    probe: p.clone(),
                });
            }
            conns.insert(
                peer.clone(),
                Conn {
                    target: s.target,
                    epoch: s.epoch,
                    down_at,
                    since,
                    up_at: None,
                },
            );
        }
        if conns.is_empty() {
            self.conns.remove(volume);
        }
    }

    pub fn retain(&mut self, keep: impl Fn(&str) -> bool) {
        self.conns.retain(|v, _| keep(v));
        self.probes.retain(|(v, _), _| keep(v));
        self.seeded.retain(|v| keep(v));
    }

    /// Stuck connections, down right now, never probed or probed long ago;
    /// the least recently probed first, at most PROBES_PER_PASS.
    pub fn due(&self, now: Instant) -> Vec<Due> {
        let last = |v: &String, p: &String| self.probes.get(&(v.clone(), p.clone())).map(|x| x.at);
        let mut due: Vec<Due> = self
            .conns
            .iter()
            .flat_map(|(v, conns)| conns.iter().map(move |(p, c)| (v, p, c)))
            .filter(|(_, _, c)| c.stuck(now) && c.up_at.is_none())
            .filter(|(v, p, _)| {
                self.probes
                    .get(&((*v).clone(), (*p).clone()))
                    .is_none_or(|x| now.duration_since(x.at) >= PROBE_EVERY)
            })
            .map(|(v, p, c)| (v.clone(), p.clone(), c.target))
            .collect();
        due.sort_by_key(|(v, p, _)| last(v, p));
        due.truncate(PROBES_PER_PASS);
        due
    }

    /// Stores a probe result; None when the connection moved on meanwhile.
    pub fn record(
        &mut self,
        volume: &str,
        peer: &str,
        target: SocketAddr,
        result: &io::Result<()>,
        now: Instant,
    ) -> Option<PeerProbe> {
        let c = self
            .conns
            .get(volume)?
            .get(peer)
            .filter(|c| c.target == target)?;
        let (result, error) = classify(result);
        let probe = PeerProbe {
            node: peer.to_owned(),
            address: target.ip().to_string(),
            port: target.port(),
            since: c.since.clone(),
            result,
            probed_at: wall_now(),
            error,
            peer_run: c.epoch.map(Time),
        };
        let epoch = c.epoch;
        self.probes.insert(
            (volume.to_owned(), peer.to_owned()),
            Probed {
                at: now,
                target,
                epoch,
                probe: probe.clone(),
            },
        );
        Some(probe)
    }

    /// The stuck connections of a volume with a probe of this peer run,
    /// by peer name; `since` is the current window's start.
    pub fn reported(&self, volume: &str, now: Instant) -> Vec<PeerProbe> {
        self.conns
            .get(volume)
            .into_iter()
            .flatten()
            .filter(|(_, c)| c.stuck(now))
            .filter_map(|(peer, c)| {
                let p = self.probes.get(&(volume.to_owned(), peer.clone()))?;
                (p.target == c.target && p.epoch == c.epoch).then(|| PeerProbe {
                    since: c.since.clone(),
                    ..p.probe.clone()
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONE: Vec<Due> = Vec::new();

    fn err(kind: io::ErrorKind) -> io::Result<()> {
        Err(io::Error::from(kind))
    }

    #[test]
    fn classification() {
        use io::ErrorKind as K;
        let only = |r: io::Result<()>| classify(&r).0;
        assert_eq!(only(Ok(())), ProbeResult::Accepted);
        assert_eq!(only(err(K::ConnectionRefused)), ProbeResult::Refused);
        assert_eq!(only(err(K::HostUnreachable)), ProbeResult::Filtered);
        assert_eq!(only(err(K::NetworkUnreachable)), ProbeResult::Filtered);
        assert_eq!(only(err(K::PermissionDenied)), ProbeResult::Filtered);
        assert_eq!(only(err(K::TimedOut)), ProbeResult::TimedOut);
        assert_eq!(only(err(K::WouldBlock)), ProbeResult::TimedOut);
        // The errno values the kernel returns, as std maps them.
        assert_eq!(
            only(Err(io::Error::from_raw_os_error(113))),
            ProbeResult::Filtered
        );
        assert_eq!(
            only(Err(io::Error::from_raw_os_error(111))),
            ProbeResult::Refused
        );
        assert_eq!(
            only(Err(io::Error::from_raw_os_error(1))),
            ProbeResult::Filtered
        );
        let (r, e) = classify(&err(K::AddrNotAvailable));
        assert_eq!(r, ProbeResult::Failed);
        assert!(e.is_some());
    }

    fn addr() -> SocketAddr {
        "10.0.0.2:11003".parse().unwrap()
    }

    fn seen(state: &str, peer_up: bool) -> BTreeMap<String, Seen> {
        BTreeMap::from([(
            "n2".to_owned(),
            Seen {
                state: state.to_owned(),
                target: addr(),
                peer_up,
                epoch: None,
                prior: None,
            },
        )])
    }

    #[tokio::test(start_paused = true)]
    async fn stuck_after_the_window_then_probed_rarely() {
        let mut w = Watch::default();
        w.observe("v", &seen("Connecting", true), Instant::now());
        assert_eq!(w.due(Instant::now()), NONE);
        tokio::time::advance(STUCK).await;
        w.observe("v", &seen("Connecting", true), Instant::now());
        let due = w.due(Instant::now());
        assert_eq!(due, vec![("v".to_owned(), "n2".to_owned(), addr())]);
        assert_eq!(
            w.reported("v", Instant::now()),
            Vec::<PeerProbe>::new(),
            "not probed yet"
        );
        let p = w
            .record(
                "v",
                "n2",
                addr(),
                &err(io::ErrorKind::HostUnreachable),
                Instant::now(),
            )
            .unwrap();
        assert_eq!(p.result, ProbeResult::Filtered);
        assert_eq!((p.address.as_str(), p.port), ("10.0.0.2", 11003));
        assert_eq!(w.reported("v", Instant::now()), vec![p]);
        assert_eq!(w.due(Instant::now()), NONE);
        tokio::time::advance(PROBE_EVERY).await;
        assert_eq!(w.due(Instant::now()).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn flaps_keep_the_window_a_stable_connect_ends_it() {
        let mut w = Watch::default();
        w.observe("v", &seen("Connecting", true), Instant::now());
        tokio::time::advance(STUCK).await;
        w.record("v", "n2", addr(), &Ok(()), Instant::now());
        // A short Connected is a flap: still reported, not probed while up.
        w.observe("v", &seen("Connected", true), Instant::now());
        tokio::time::advance(STABLE / 2).await;
        w.observe("v", &seen("Connected", true), Instant::now());
        assert_eq!(w.reported("v", Instant::now()).len(), 1);
        assert_eq!(w.due(Instant::now() + PROBE_EVERY), NONE);
        w.observe("v", &seen("Connecting", true), Instant::now());
        assert_eq!(w.reported("v", Instant::now()).len(), 1);
        w.observe("v", &seen("Connected", true), Instant::now());
        tokio::time::advance(STABLE).await;
        w.observe("v", &seen("Connected", true), Instant::now());
        assert_eq!(w.reported("v", Instant::now()), Vec::<PeerProbe>::new());
        assert!(w.conns.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn standalone_and_peer_down_are_not_stuck() {
        let mut w = Watch::default();
        w.observe("v", &seen("Connecting", true), Instant::now());
        tokio::time::advance(STUCK).await;
        w.observe("v", &seen("StandAlone", true), Instant::now());
        assert_eq!(w.due(Instant::now()), NONE);
        // The window starts when the peer is up, not before.
        w.observe("v", &seen("Connecting", false), Instant::now());
        tokio::time::advance(STUCK).await;
        w.observe("v", &seen("Connecting", true), Instant::now());
        assert_eq!(w.due(Instant::now()), NONE);
        // A connection gone from events2 is forgotten.
        w.observe("v", &BTreeMap::new(), Instant::now());
        assert!(w.conns.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_address_drops_the_old_result() {
        let mut w = Watch::default();
        w.observe("v", &seen("Connecting", true), Instant::now());
        tokio::time::advance(STUCK).await;
        w.record(
            "v",
            "n2",
            addr(),
            &err(io::ErrorKind::TimedOut),
            Instant::now(),
        );
        let mut moved = seen("Connecting", true);
        let other: SocketAddr = "10.0.0.9:11003".parse().unwrap();
        moved.get_mut("n2").unwrap().target = other;
        w.observe("v", &moved, Instant::now());
        assert_eq!(w.reported("v", Instant::now()), Vec::<PeerProbe>::new());
        assert!(
            w.record(
                "v",
                "n2",
                addr(),
                &err(io::ErrorKind::TimedOut),
                Instant::now()
            )
            .is_none(),
            "a result for the old address is dropped"
        );
        assert_eq!(w.due(Instant::now())[0].2, other);
    }

    fn with(state: &str, f: impl FnOnce(&mut Seen)) -> BTreeMap<String, Seen> {
        let mut m = seen(state, true);
        f(m.get_mut("n2").unwrap());
        m
    }

    /// Down 20 s, probed, then Connected long enough to close the window.
    async fn probed_then_closed(w: &mut Watch) {
        w.observe("v", &seen("Connecting", true), Instant::now());
        tokio::time::advance(STUCK).await;
        w.observe("v", &seen("Connecting", true), Instant::now());
        assert_eq!(w.due(Instant::now()).len(), 1);
        w.record(
            "v",
            "n2",
            addr(),
            &err(io::ErrorKind::HostUnreachable),
            Instant::now(),
        );
        w.observe("v", &seen("Connected", true), Instant::now());
        tokio::time::advance(STABLE).await;
        w.observe("v", &seen("Connected", true), Instant::now());
        assert_eq!(w.reported("v", Instant::now()), Vec::<PeerProbe>::new());
    }

    #[tokio::test(start_paused = true)]
    async fn the_last_probe_outlives_the_window() {
        let mut w = Watch::default();
        probed_then_closed(&mut w).await;
        // The next drop reports the earlier result without a new probe.
        w.observe("v", &seen("Connecting", true), Instant::now());
        tokio::time::advance(STUCK).await;
        w.observe("v", &seen("Connecting", true), Instant::now());
        assert_eq!(w.due(Instant::now()), NONE);
        let r = w.reported("v", Instant::now());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].result, ProbeResult::Filtered);
        tokio::time::advance(PROBE_EVERY).await;
        assert_eq!(w.due(Instant::now()).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_peer_run_restarts_the_window() {
        let mut w = Watch::default();
        w.observe("v", &seen("Connecting", true), Instant::now());
        tokio::time::advance(STUCK).await;
        w.record("v", "n2", addr(), &Ok(()), Instant::now());
        assert_eq!(w.reported("v", Instant::now()).len(), 1);
        // The peer agent came back: its Lease has a new acquireTime.
        let next = with("Connecting", |s| s.epoch = Some(Timestamp::UNIX_EPOCH));
        w.observe("v", &next, Instant::now());
        assert_eq!(w.reported("v", Instant::now()), Vec::<PeerProbe>::new());
        assert_eq!(w.due(Instant::now()), NONE);
        tokio::time::advance(STUCK).await;
        w.observe("v", &next, Instant::now());
        assert_eq!(w.due(Instant::now()).len(), 1, "probed again");
    }

    #[tokio::test(start_paused = true)]
    async fn a_restart_resumes_from_the_last_status() {
        let ten = Time(Timestamp::from_second(wall_now().0.as_second() - 100).unwrap());
        let prior = PeerProbe {
            node: "n2".into(),
            address: "10.0.0.2".into(),
            port: 11003,
            since: ten.clone(),
            result: ProbeResult::TimedOut,
            probed_at: ten,
            error: None,
            peer_run: None,
        };
        let mut w = Watch::default();
        let s = with("Connecting", |s| s.prior = Some(prior.clone()));
        w.observe("v", &s, Instant::now());
        // Reported at once with its window start; not probed again yet.
        assert_eq!(w.reported("v", Instant::now()), vec![prior.clone()]);
        assert_eq!(w.due(Instant::now()), NONE);
        tokio::time::advance(PROBE_EVERY.checked_sub(Duration::from_secs(100)).unwrap()).await;
        w.observe("v", &s, Instant::now());
        assert_eq!(w.due(Instant::now()).len(), 1);

        // Connected by now: the old record seeds nothing.
        let mut w = Watch::default();
        w.observe(
            "v",
            &with("Connected", |s| s.prior = Some(prior.clone())),
            Instant::now(),
        );
        w.observe(
            "v",
            &with("Connecting", |s| s.prior = Some(prior)),
            Instant::now(),
        );
        assert_eq!(w.reported("v", Instant::now()), Vec::<PeerProbe>::new());
    }

    #[tokio::test(start_paused = true)]
    async fn an_old_result_is_probed_again_in_a_new_window() {
        let mut w = Watch::default();
        probed_then_closed(&mut w).await;
        tokio::time::advance(CARRY_MAX).await;
        w.observe("v", &seen("Connecting", true), Instant::now());
        tokio::time::advance(STUCK).await;
        w.observe("v", &seen("Connecting", true), Instant::now());
        assert_eq!(w.reported("v", Instant::now()), Vec::<PeerProbe>::new());
        assert_eq!(w.due(Instant::now()).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn probes_per_pass_are_capped_least_recent_first() {
        let mut w = Watch::default();
        for i in 0..300 {
            w.observe(
                &format!("v{i:03}"),
                &seen("Connecting", true),
                Instant::now(),
            );
        }
        tokio::time::advance(STUCK).await;
        let due = w.due(Instant::now());
        assert_eq!(due.len(), PROBES_PER_PASS);
        for (v, p, t) in &due {
            w.record(v, p, *t, &Ok(()), Instant::now());
        }
        let rest = w.due(Instant::now());
        assert_eq!(rest.len(), 300 - PROBES_PER_PASS);
        assert!(rest.iter().all(|d| !due.contains(d)));
    }

    #[tokio::test(start_paused = true)]
    async fn the_record_names_the_peer_run() {
        let mut w = Watch::default();
        let run = Timestamp::from_second(1_700_000_000).unwrap();
        let s = with("Connecting", |s| s.epoch = Some(run));
        w.observe("v", &s, Instant::now());
        tokio::time::advance(STUCK).await;
        let p = w
            .record("v", "n2", addr(), &Ok(()), Instant::now())
            .unwrap();
        assert_eq!(p.peer_run, Some(Time(run)));
    }
}
