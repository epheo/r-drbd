use std::fmt::Write as _;
use std::net::SocketAddr;

/// One volume per resource in the product.
pub const VOLUME: u32 = 0;

/// drbdadm treats a peer volume without a disk as diskless and turns its
/// bitmap off, so a diskful peer needs a path; this one is never opened
/// (only the local disk is) and its name says so.
pub const PEER_DISK: &str = "/dev/r-drbd/peer-disk-never-opened";

/// First line of every file: `# r-drbd <version> sha256:<hex of the rest>`.
pub const HEADER_PREFIX: &str = "# r-drbd ";
const HASH_TAG: &str = "sha256:";

/// Optional settings the installed drbd-utils may not know; each one left
/// out is reported in [`Rendered::dropped`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Features {
    pub tiebreaker: bool,
    pub discard_granularity: bool,
    pub suspended_primary_outdated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disk<'a> {
    /// This host's backing device.
    Local(&'a str),
    /// A diskful peer.
    Peer,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Host<'a> {
    pub name: &'a str,
    pub node_id: u8,
    pub address: SocketAddr,
    pub disk: Disk<'a>,
    /// False for a diskless client that must not break quorum ties.
    pub tiebreaker: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resource<'a> {
    pub name: &'a str,
    /// Product version for the header.
    pub version: &'a str,
    pub minor: u32,
    pub secret: &'a str,
    pub local: Host<'a>,
    /// Every peer this host connects to.
    pub peers: Vec<Host<'a>>,
    pub two_primaries: bool,
    /// Thin pool chunk size; zero blocks of that size are discarded on a
    /// resync target instead of written.
    pub discard_granularity: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub text: String,
    /// Hex sha256 of everything after the header line.
    pub hash: String,
    /// Settings left out because the tools lack them.
    pub dropped: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    #[error("{what} {value:?} cannot go into a DRBD resource file")]
    Unsafe { what: &'static str, value: String },
    #[error("node id {0} appears twice")]
    DuplicateNodeId(u8),
    #[error("node id {0} is above 31")]
    NodeId(u8),
    #[error("host {0} has the local disk but is a peer")]
    PeerWithLocalDisk(String),
    #[error("shared secret is longer than 64 characters")]
    SecretLength,
}

/// drbdadm's tokenizer reads a quoted string up to the next quote and has
/// no escapes, so a value must not carry one.
fn safe(what: &'static str, value: &str) -> Result<(), RenderError> {
    let bad = value.is_empty()
        || value
            .chars()
            .any(|c| c == '"' || c == '\\' || c.is_control());
    if bad {
        return Err(RenderError::Unsafe {
            what,
            value: value.to_owned(),
        });
    }
    Ok(())
}

fn check(r: &Resource<'_>) -> Result<(), RenderError> {
    safe("resource name", r.name)?;
    safe("shared secret", r.secret)?;
    // DRBD's limit on the secret.
    if r.secret.len() > 64 {
        return Err(RenderError::SecretLength);
    }
    let mut seen = [false; 32];
    for h in std::iter::once(&r.local).chain(&r.peers) {
        safe("host name", h.name)?;
        let slot = seen
            .get_mut(usize::from(h.node_id))
            .ok_or(RenderError::NodeId(h.node_id))?;
        if std::mem::replace(slot, true) {
            return Err(RenderError::DuplicateNodeId(h.node_id));
        }
        if let Disk::Local(p) = h.disk {
            safe("disk path", p)?;
        }
    }
    if let Some(p) = r.peers.iter().find(|p| matches!(p.disk, Disk::Local(_))) {
        return Err(RenderError::PeerWithLocalDisk(p.name.to_owned()));
    }
    Ok(())
}

fn address(a: SocketAddr) -> String {
    match a {
        SocketAddr::V4(v) => format!("ipv4 {}:{}", v.ip(), v.port()),
        SocketAddr::V6(v) => format!("ipv6 [{}]:{}", v.ip(), v.port()),
    }
}

/// Sections in this order: options, net, disk, hosts by node id, one
/// connection per peer by node id.
pub fn render(r: &Resource<'_>, features: Features) -> Result<Rendered, RenderError> {
    check(r)?;
    let mut dropped = Vec::new();
    let mut b = String::new();
    // Writing to a String cannot fail.
    let mut line = |depth: usize, s: &str| {
        let _ = writeln!(b, "{:width$}{s}", "", width = depth * 4);
    };
    line(0, &format!("resource \"{}\" {{", r.name));

    line(1, "options {");
    line(2, "quorum majority;");
    line(2, "on-no-quorum suspend-io;");
    // The man page asks for the same policy as on-no-quorum.
    line(2, "on-no-data-accessible suspend-io;");
    if features.suspended_primary_outdated {
        line(2, "on-suspended-primary-outdated force-secondary;");
    } else {
        dropped.push("on-suspended-primary-outdated");
    }
    line(1, "}");

    line(1, "net {");
    line(2, "protocol C;");
    line(2, "cram-hmac-alg sha256;");
    line(2, &format!("shared-secret \"{}\";", r.secret));
    line(2, "rr-conflict retry-connect;");
    if r.two_primaries {
        line(2, "allow-two-primaries yes;");
    }
    line(1, "}");

    let local_diskful = matches!(r.local.disk, Disk::Local(_));
    if let Some(g) = r
        .discard_granularity
        .filter(|_| local_diskful)
        .and_then(discard_granularity)
    {
        if features.discard_granularity {
            line(1, "disk {");
            line(2, &format!("rs-discard-granularity {g};"));
            line(1, "}");
        } else {
            dropped.push("rs-discard-granularity");
        }
    }

    let mut hosts: Vec<&Host<'_>> = std::iter::once(&r.local).chain(&r.peers).collect();
    hosts.sort_by_key(|h| h.node_id);
    for h in &hosts {
        line(1, &format!("on \"{}\" {{", h.name));
        line(2, &format!("node-id {};", h.node_id));
        line(2, &format!("volume {VOLUME} {{"));
        line(3, &format!("device minor {};", r.minor));
        match h.disk {
            Disk::Local(p) => {
                line(3, &format!("disk \"{p}\";"));
                line(3, "meta-disk internal;");
            }
            Disk::Peer => line(3, &format!("disk \"{PEER_DISK}\";")),
            Disk::None => line(3, "disk none;"),
        }
        // The DRBD default is yes; only the exception is written.
        if !h.tiebreaker {
            if features.tiebreaker {
                line(3, "tiebreaker no;");
            } else if !dropped.contains(&"tiebreaker") {
                dropped.push("tiebreaker");
            }
        }
        line(2, "}");
        line(1, "}");
    }

    let mut peers: Vec<&Host<'_>> = r.peers.iter().collect();
    peers.sort_by_key(|h| h.node_id);
    for p in peers {
        line(1, "connection {");
        for h in [&r.local, p] {
            line(
                2,
                &format!("host \"{}\" address {};", h.name, address(h.address)),
            );
        }
        line(1, "}");
    }
    line(0, "}");

    let hash = sha256_hex(&b);
    Ok(Rendered {
        text: format!("{HEADER_PREFIX}{} {HASH_TAG}{hash}\n{b}", r.version),
        hash,
        dropped,
    })
}

/// DRBD refuses an rs-discard-granularity over 1 MiB, while a thin chunk
/// may reach 1 GiB: the largest 4 KiB multiple at most 1 MiB that divides
/// the chunk, so every discard still covers whole chunks.
pub fn discard_granularity(chunk: u64) -> Option<u64> {
    const MAX: u64 = 1 << 20;
    const STEP: u64 = 4096;
    if chunk <= MAX {
        return Some(chunk);
    }
    (1..=MAX / STEP)
        .rev()
        .map(|k| k * STEP)
        .find(|&d| chunk.is_multiple_of(d))
}

fn sha256_hex(s: &str) -> String {
    let d = ring::digest::digest(&ring::digest::SHA256, s.as_bytes());
    d.as_ref().iter().fold(String::new(), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

/// The hash a file states in its header, only when the rest of the file
/// still matches it; a hand-edited file gives None and is rewritten.
pub fn header_hash(text: &str) -> Option<String> {
    let (header, body) = text.split_once('\n')?;
    let stated = header
        .strip_prefix(HEADER_PREFIX)?
        .split(' ')
        .find_map(|t| t.strip_prefix(HASH_TAG))?;
    let actual = sha256_hex(body);
    (stated == actual).then_some(actual)
}
