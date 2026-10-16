//! Pure DRBD allocation for a new assignment.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine;
use model::keys::{MINOR_RANGE, NODE_ID_MAX, PORT_RANGE};
use model::{AssignedReplica, Assignment, Role};

/// Minors and per-node ports already taken by existing assignments.
#[derive(Debug, Default, Clone)]
pub struct Used {
    minors: BTreeSet<u32>,
    ports: BTreeMap<String, BTreeSet<u16>>,
}

impl Used {
    pub fn from_assignments<'a>(all: impl IntoIterator<Item = &'a Assignment>) -> Self {
        let mut used = Self::default();
        for a in all {
            used.add(a);
        }
        used
    }

    pub fn add(&mut self, a: &Assignment) {
        if let Some(m) = a.minor {
            self.minors.insert(m);
        }
        for r in &a.replicas {
            if let Some(p) = r.port {
                self.ports.entry(r.node.clone()).or_default().insert(p);
            }
        }
    }

    pub fn lowest_free_minor(&self) -> Option<u32> {
        MINOR_RANGE.into_iter().find(|m| !self.minors.contains(m))
    }

    pub fn lowest_free_port(&self, node: &str) -> Option<u16> {
        let taken = self.ports.get(node);
        PORT_RANGE
            .into_iter()
            .find(|p| taken.is_none_or(|t| !t.contains(p)))
    }
}

/// Random inputs, apart so tests can fix them.
#[derive(Debug, Clone, Copy)]
pub struct Entropy {
    pub secret: [u8; 15],
    pub gi: u64,
}

impl Entropy {
    pub fn random() -> Result<Self, getrandom::Error> {
        let mut secret = [0_u8; 15];
        getrandom::fill(&mut secret)?;
        loop {
            let gi = getrandom::u64()?;
            if gi_hex(gi).is_some() {
                return Ok(Self { secret, gi });
            }
        }
    }
}

/// Day-0 generation id: lowest bit cleared, never 0 or 4, 16 uppercase hex
/// digits. None asks the caller to draw again.
pub fn gi_hex(raw: u64) -> Option<String> {
    let v = raw & !1;
    (v != 0 && v != 4).then(|| format!("{v:016X}"))
}

/// 15 bytes give exactly 20 Base64 characters with no padding.
pub fn secret(bytes: &[u8; 15]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AllocError {
    #[error("no free DRBD minor in {}..={}", MINOR_RANGE.start(), MINOR_RANGE.end())]
    NoMinor,
    #[error("no free DRBD port on node {0}")]
    NoPort(String),
    #[error("more than {} DRBD peers", u32::from(NODE_ID_MAX) + 1)]
    TooManyPeers,
    #[error("generation id draw was 0 or 4")]
    BadGi,
    #[error("no diskful replica to assign")]
    Empty,
}

/// Builds the assignment for freshly placed replicas. One diskful replica
/// is a plain LV: no DRBD fields at all. Otherwise node ids go 0.. in
/// placement order, the tiebreaker last; each replica gets the lowest free
/// port on its node; the first diskful node starts UpToDate.
pub fn allocate(
    diskful: &[placer::Chosen],
    tiebreaker: Option<&str>,
    used: &Used,
    entropy: Entropy,
) -> Result<Assignment, AllocError> {
    let first = diskful.first().ok_or(AllocError::Empty)?;
    if diskful.len() == 1 {
        return Ok(Assignment {
            replicas: vec![AssignedReplica {
                node: first.node.clone(),
                pool: Some(first.pool.clone()),
                node_id: 0,
                port: None,
                role: Role::Diskful,
            }],
            minor: None,
            secret: None,
            gi: None,
            initial_up_to_date_node: None,
        });
    }
    let members = diskful
        .iter()
        .map(|c| (c.node.as_str(), Some(c.pool.as_str()), Role::Diskful))
        .chain(tiebreaker.map(|n| (n, None, Role::Tiebreaker)));
    let mut replicas = Vec::new();
    for (i, (node, pool, role)) in members.enumerate() {
        let node_id = u8::try_from(i)
            .ok()
            .filter(|id| *id <= NODE_ID_MAX)
            .ok_or(AllocError::TooManyPeers)?;
        let port = used
            .lowest_free_port(node)
            .ok_or_else(|| AllocError::NoPort(node.to_owned()))?;
        replicas.push(AssignedReplica {
            node: node.to_owned(),
            pool: pool.map(str::to_owned),
            node_id,
            port: Some(port),
            role,
        });
    }
    Ok(Assignment {
        replicas,
        minor: Some(used.lowest_free_minor().ok_or(AllocError::NoMinor)?),
        secret: Some(secret(&entropy.secret)),
        gi: Some(gi_hex(entropy.gi).ok_or(AllocError::BadGi)?),
        initial_up_to_date_node: Some(first.node.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chosen(node: &str, pool: &str) -> placer::Chosen {
        placer::Chosen {
            node: node.into(),
            pool: pool.into(),
        }
    }

    const E: Entropy = Entropy {
        secret: [7; 15],
        gi: 0xABCD_EF01_2345_6789,
    };

    #[test]
    fn single_replica_has_no_drbd_fields() {
        let a = allocate(&[chosen("n1", "p1")], None, &Used::default(), E).unwrap();
        assert_eq!(a.replicas.len(), 1);
        assert_eq!(a.replicas[0].node_id, 0);
        assert_eq!(a.replicas[0].port, None);
        assert_eq!(a.replicas[0].pool.as_deref(), Some("p1"));
        assert_eq!(a.replicas[0].role, Role::Diskful);
        assert!(a.minor.is_none() && a.secret.is_none() && a.gi.is_none());
        assert!(a.initial_up_to_date_node.is_none());
    }

    #[test]
    fn replicated_with_tiebreaker() {
        let a = allocate(
            &[chosen("n2", "p2"), chosen("n1", "p1")],
            Some("n3"),
            &Used::default(),
            E,
        )
        .unwrap();
        let ids: Vec<_> = a
            .replicas
            .iter()
            .map(|r| (r.node.as_str(), r.node_id, r.port, r.role))
            .collect();
        assert_eq!(
            ids,
            [
                ("n2", 0, Some(7000), Role::Diskful),
                ("n1", 1, Some(7000), Role::Diskful),
                ("n3", 2, Some(7000), Role::Tiebreaker),
            ]
        );
        assert_eq!(a.replicas[2].pool, None);
        assert_eq!(a.minor, Some(1000));
        assert_eq!(a.secret.as_deref().map(str::len), Some(20));
        assert_eq!(a.gi.as_deref(), Some("ABCDEF0123456788"));
        assert_eq!(a.initial_up_to_date_node.as_deref(), Some("n2"));
    }

    #[test]
    fn minor_and_port_reuse_lowest_free() {
        let first = allocate(
            &[chosen("n1", "p1"), chosen("n2", "p2")],
            None,
            &Used::default(),
            E,
        )
        .unwrap();
        let mut used = Used::from_assignments([&first]);
        let second = allocate(&[chosen("n1", "p1"), chosen("n3", "p3")], None, &used, E).unwrap();
        assert_eq!(second.minor, Some(1001));
        assert_eq!(second.replicas[0].port, Some(7001));
        assert_eq!(second.replicas[1].port, Some(7000));
        used.add(&second);
        // The first volume goes away: its minor and n1 port are free again.
        let used = Used::from_assignments([&second]);
        let third = allocate(&[chosen("n1", "p1"), chosen("n2", "p2")], None, &used, E).unwrap();
        assert_eq!(third.minor, Some(1000));
        assert_eq!(third.replicas[0].port, Some(7000));
    }

    #[test]
    fn exhaustion() {
        let mut used = Used::default();
        let full = Assignment {
            replicas: PORT_RANGE
                .map(|p| AssignedReplica {
                    node: "n1".into(),
                    pool: None,
                    node_id: 0,
                    port: Some(p),
                    role: Role::Diskful,
                })
                .collect(),
            minor: None,
            secret: None,
            gi: None,
            initial_up_to_date_node: None,
        };
        used.add(&full);
        assert_eq!(used.lowest_free_port("n1"), None);
        assert_eq!(used.lowest_free_port("n2"), Some(7000));
        let err = allocate(&[chosen("n2", "p"), chosen("n1", "p")], None, &used, E).unwrap_err();
        assert_eq!(err, AllocError::NoPort("n1".into()));
        assert_eq!(allocate(&[], None, &used, E), Err(AllocError::Empty));
    }

    #[test]
    fn gi_rules() {
        assert_eq!(gi_hex(0), None);
        assert_eq!(gi_hex(1), None);
        assert_eq!(gi_hex(4), None);
        assert_eq!(gi_hex(5), None);
        assert_eq!(gi_hex(2).as_deref(), Some("0000000000000002"));
        assert_eq!(gi_hex(u64::MAX).as_deref(), Some("FFFFFFFFFFFFFFFE"));
        for _ in 0..64 {
            let e = Entropy::random().unwrap();
            let g = gi_hex(e.gi).unwrap();
            assert_eq!(g.len(), 16);
            assert!(
                g.chars()
                    .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
            );
            let v = u64::from_str_radix(&g, 16).unwrap();
            assert_eq!(v & 1, 0);
            assert!(v != 0 && v != 4);
        }
    }

    #[test]
    fn secret_is_twenty_chars() {
        let e = Entropy::random().unwrap();
        let s = secret(&e.secret);
        assert_eq!(s.len(), 20);
        assert!(!s.contains('='));
        assert_eq!(secret(&[0; 15]), "AAAAAAAAAAAAAAAAAAAA");
    }
}
