//! DRBD connections stuck out of Connected, and the TCP probe that tells
//! why: nothing listening, a firewall, or packets dropped.
//!
//! A stray connect to a waiting DRBD listener costs the peer one kernel
//! log line ("Error receiving initial packet") and no state change; once
//! Connected, DRBD closes the listener and a connect is refused. Probes
//! still run only while a connection is stuck, and rarely.

use std::collections::{BTreeMap, HashMap};
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

#[derive(Debug, Clone)]
struct Conn {
    target: SocketAddr,
    down_at: Instant,
    since: Time,
    up_at: Option<Instant>,
    probed: Option<(Instant, PeerProbe)>,
}

impl Conn {
    fn stuck(&self, now: Instant) -> bool {
        now.duration_since(self.down_at) >= STUCK
    }
}

/// What one connection looks like now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub state: String,
    pub target: SocketAddr,
    /// The peer reports its resource up; a connection to a peer that is
    /// down or still starting is not stuck.
    pub peer_up: bool,
}

/// A probe to run: volume, peer, address.
pub type Due = (String, String, SocketAddr);

/// Per volume and peer, the window a connection has been out of Connected.
#[derive(Debug, Default)]
pub struct Watch {
    conns: HashMap<String, BTreeMap<String, Conn>>,
}

impl Watch {
    /// The volume's connections as events2 shows them; peers missing
    /// from `seen` are dropped.
    pub fn observe(&mut self, volume: &str, seen: &BTreeMap<String, Seen>, now: Instant) {
        let conns = self.conns.entry(volume.to_owned()).or_default();
        conns.retain(|peer, _| seen.contains_key(peer));
        for (peer, s) in seen {
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
            let c = conns.entry(peer.clone()).or_insert_with(|| Conn {
                target: s.target,
                down_at: now,
                since: wall_now(),
                up_at: None,
                probed: None,
            });
            c.up_at = None;
            if c.target != s.target {
                c.target = s.target;
                c.probed = None;
            }
        }
        if conns.is_empty() {
            self.conns.remove(volume);
        }
    }

    pub fn retain(&mut self, keep: impl Fn(&str) -> bool) {
        self.conns.retain(|v, _| keep(v));
    }

    /// Stuck connections, down right now, never probed or probed long ago.
    pub fn due(&self, now: Instant) -> Vec<Due> {
        self.conns
            .iter()
            .flat_map(|(v, conns)| conns.iter().map(move |(p, c)| (v, p, c)))
            .filter(|(_, _, c)| c.stuck(now) && c.up_at.is_none())
            .filter(|(_, _, c)| {
                c.probed
                    .as_ref()
                    .is_none_or(|(at, _)| now.duration_since(*at) >= PROBE_EVERY)
            })
            .map(|(v, p, c)| (v.clone(), p.clone(), c.target))
            .collect()
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
            .get_mut(volume)?
            .get_mut(peer)
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
        };
        c.probed = Some((now, probe.clone()));
        Some(probe)
    }

    /// The stuck and probed connections of a volume, by peer name.
    pub fn reported(&self, volume: &str, now: Instant) -> Vec<PeerProbe> {
        self.conns
            .get(volume)
            .into_iter()
            .flat_map(BTreeMap::values)
            .filter(|c| c.stuck(now))
            .filter_map(|c| c.probed.as_ref().map(|(_, p)| p.clone()))
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
}
