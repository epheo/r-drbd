//! DRBD resource file rendering with the fixed option set, and metadata sizing.
//!
//! Pure crate: no IO, no process execution. The agent feeds it one
//! resource's view and writes the returned text.

#![forbid(unsafe_code)]

/// How an option value is written: strings are double quoted, everything
/// else bare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionKind {
    String,
    Boolean,
    Numeric,
    NumericOrSymbol,
    Handler,
}

/// A rendered option line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionLine {
    pub name: String,
    pub value: String,
    pub kind: OptionKind,
}

/// The fixed option set. `resource` goes
/// into the options section, `net` into net, `disk` into every volume's
/// nested disk section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedOptions {
    pub resource: Vec<OptionLine>,
    pub net: Vec<OptionLine>,
    pub disk: Vec<OptionLine>,
}

impl FixedOptions {
    /// protocol C; quorum majority; on-no-quorum suspend-io;
    /// on-no-data-accessible suspend-io; on-suspended-primary-outdated
    /// force-secondary; rr-conflict retry-connect; rs-discard-granularity
    /// from the thin pool chunk size when given. quorum and the three on-*
    /// options are omitted for a single replica (no DRBD at all in the
    /// product); replicas 0 behaves like 1. `two_primaries` (ReadWriteMany
    /// block volumes) adds allow-two-primaries to net.
    pub fn new(replicas: u32, rs_discard_granularity: Option<u64>, two_primaries: bool) -> Self {
        let resource = if replicas > 1 {
            vec![
                opt("on-no-data-accessible", "suspend-io", OptionKind::Handler),
                opt("on-no-quorum", "suspend-io", OptionKind::Handler),
                opt(
                    "on-suspended-primary-outdated",
                    "force-secondary",
                    OptionKind::Handler,
                ),
                opt("quorum", "majority", OptionKind::NumericOrSymbol),
            ]
        } else {
            Vec::new()
        };
        let net = two_primaries
            .then(|| opt("allow-two-primaries", "yes", OptionKind::Boolean))
            .into_iter()
            .chain([
                opt("protocol", "C", OptionKind::Handler),
                opt("rr-conflict", "retry-connect", OptionKind::Handler),
            ])
            .collect();
        let disk = rs_discard_granularity
            .map(|g| {
                opt(
                    "rs-discard-granularity",
                    &g.to_string(),
                    OptionKind::Numeric,
                )
            })
            .into_iter()
            .collect();
        Self {
            resource,
            net,
            disk,
        }
    }
}

fn opt(name: &str, value: &str, kind: OptionKind) -> OptionLine {
    OptionLine {
        name: name.to_owned(),
        value: value.to_owned(),
        kind,
    }
}

const TIEBREAKER_MIN_KERNEL: Version = Version {
    major: 9,
    minor: 3,
    patch: 1,
};
const TIEBREAKER_MIN_UTILS: Version = Version {
    major: 9,
    minor: 34,
    patch: 0,
};

/// The `tiebreaker` line is understood only from kernel module 9.3.1 and
/// drbd-utils 9.34.0 on; below that the agent passes
/// `Tiebreaker::Unsupported`.
pub fn tiebreaker_supported(v: &ToolVersions) -> bool {
    v.kernel >= TIEBREAKER_MIN_KERNEL && v.utils >= TIEBREAKER_MIN_UTILS
}

/// Versions that change what is rendered (tiebreaker line, discard
/// granularity, resync-after on peers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolVersions {
    pub kernel: Version,
    pub utils: Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddrFamily {
    Ipv4,
    Ipv6,
}

/// One `host` line endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub family: AddrFamily,
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskPath {
    /// Local diskful volume: the backing device.
    Backing(String),
    /// `disk none;` (local diskless, or diskless peer).
    None,
    /// Peer diskful volume: the fixed placeholder path.
    PeerPlaceholder,
}

/// What the `tiebreaker` line looks like.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tiebreaker {
    Yes,
    No,
    /// Versions too old: the line is written as a comment naming the minimum
    /// versions.
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeEntry {
    pub number: u32,
    pub disk: DiskPath,
    pub tiebreaker: Tiebreaker,
    /// The fixed disk options; the nested `disk` section exists only when
    /// non-empty.
    pub disk_options: Vec<OptionLine>,
    pub minor: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostEntry {
    pub host: String,
    pub node_id: u8,
    pub volumes: Vec<VolumeEntry>,
}

/// Two host lines: local first, then peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    pub local: Endpoint,
    pub peer: Endpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    pub peer: String,
    pub net_options: Vec<OptionLine>,
    pub endpoints: Endpoints,
}

/// Header comment fields; everything here is ignored by change detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub generator: String,
    pub node_name: String,
    pub hostname: String,
    pub local_time: String,
    pub utc_time: String,
}

/// The complete input of one resource file. Peers and
/// connections must already be sorted by node name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceFile {
    pub name: String,
    pub header: Header,
    pub handlers: Vec<OptionLine>,
    pub options: Vec<OptionLine>,
    pub secret: String,
    pub net: Vec<OptionLine>,
    pub disk: Vec<OptionLine>,
    pub local: HostEntry,
    pub peers: Vec<HostEntry>,
    pub connections: Vec<Connection>,
}

/// Render the file text. Byte stable for equal input.
pub fn render(file: &ResourceFile) -> String {
    let mut w = Writer::default();
    let h = &file.header;
    w.line(
        0,
        &format!(
            "# generated by {}, node {}, host {}, at {} (UTC {})",
            header_text(&h.generator),
            header_text(&h.node_name),
            header_text(&h.hostname),
            header_text(&h.local_time),
            header_text(&h.utc_time)
        ),
    );
    w.blank();
    w.line(0, &format!("resource {}", quote(&file.name)));
    w.open(0);

    if !file.handlers.is_empty() {
        w.blank();
        w.options_section(1, "handlers", &file.handlers);
    }
    if !file.options.is_empty() {
        w.blank();
        w.options_section(1, "options", &file.options);
    }

    w.blank();
    w.line(1, "net");
    w.open(1);
    w.line(2, "cram-hmac-alg     sha1;");
    w.line(2, &format!("shared-secret     {};", quote(&file.secret)));
    for o in &file.net {
        w.option(2, o);
    }
    w.close(1);

    if !file.disk.is_empty() {
        w.blank();
        w.options_section(1, "disk", &file.disk);
    }

    for host in std::iter::once(&file.local).chain(&file.peers) {
        w.blank();
        w.host(host);
    }

    for c in &file.connections {
        w.blank();
        w.connection(c);
    }

    w.close(0);
    w.out
}

/// Without quotes and newlines a header field can never form the
/// `resource "` marker that change detection keys on, nor leave the comment.
fn header_text(v: &str) -> String {
    v.chars()
        .filter(|c| !matches!(c, '"' | '\n' | '\r'))
        .collect()
}

/// Escaped so a stray quote or newline in a name or secret cannot break out
/// of the quoted value.
fn quote(v: &str) -> String {
    let mut q = String::with_capacity(v.len() + 2);
    q.push('"');
    for c in v.chars() {
        match c {
            '"' | '\\' => {
                q.push('\\');
                q.push(c);
            }
            '\n' | '\r' => q.push(' '),
            _ => q.push(c),
        }
    }
    q.push('"');
    q
}

#[derive(Default)]
struct Writer {
    out: String,
}

impl Writer {
    fn line(&mut self, indent: usize, text: &str) {
        // Every value reaching here is quoted, header-cleaned or a fixed
        // token; a newline would still open a way to inject a section.
        debug_assert!(
            !text.contains(['\n', '\r']),
            "newline in resource file token"
        );
        for _ in 0..indent {
            self.out.push_str("    ");
        }
        self.out.push_str(text);
        self.out.push('\n');
    }

    fn blank(&mut self) {
        self.out.push('\n');
    }

    fn open(&mut self, indent: usize) {
        self.line(indent, "{");
    }

    fn close(&mut self, indent: usize) {
        self.line(indent, "}");
    }

    fn option(&mut self, indent: usize, o: &OptionLine) {
        let value = match o.kind {
            OptionKind::String => quote(&o.value),
            OptionKind::Boolean
            | OptionKind::Numeric
            | OptionKind::NumericOrSymbol
            | OptionKind::Handler => o.value.clone(),
        };
        self.line(indent, &format!("{} {value};", o.name));
    }

    fn options_section(&mut self, indent: usize, keyword: &str, opts: &[OptionLine]) {
        self.line(indent, keyword);
        self.open(indent);
        for o in opts {
            self.option(indent + 1, o);
        }
        self.close(indent);
    }

    fn host(&mut self, host: &HostEntry) {
        self.line(1, &format!("on {}", quote(&host.host)));
        self.open(1);
        for v in &host.volumes {
            self.volume(v);
        }
        self.line(2, &format!("node-id    {};", host.node_id));
        self.close(1);
    }

    fn volume(&mut self, v: &VolumeEntry) {
        self.line(2, &format!("volume {}", v.number));
        self.open(2);
        let disk = match &v.disk {
            DiskPath::Backing(path) => path.as_str(),
            DiskPath::None => "none",
            DiskPath::PeerPlaceholder => {
                self.line(
                    3,
                    "# placeholder path: the peer's real backing device is never opened here",
                );
                "/dev/drbd/this/is/not/used"
            }
        };
        self.line(3, &format!("disk        {disk};"));
        match v.tiebreaker {
            Tiebreaker::Yes => self.line(3, "tiebreaker yes;"),
            Tiebreaker::No => self.line(3, "tiebreaker no;"),
            Tiebreaker::Unsupported => self.line(
                3,
                &format!(
                    "# tiebreaker yes; (needs kernel >= {} and drbd-utils >= {})",
                    version_text(TIEBREAKER_MIN_KERNEL),
                    version_text(TIEBREAKER_MIN_UTILS)
                ),
            ),
        }
        if !v.disk_options.is_empty() {
            self.options_section(3, "disk", &v.disk_options);
        }
        self.line(3, "meta-disk   internal;");
        self.line(3, &format!("device      minor {};", v.minor));
        self.close(2);
    }

    fn connection(&mut self, c: &Connection) {
        self.line(1, "connection");
        self.open(1);
        if !c.net_options.is_empty() {
            self.blank();
            self.options_section(2, "net", &c.net_options);
        }
        self.endpoint(&c.endpoints.local);
        self.endpoint(&c.endpoints.peer);
        self.close(1);
    }

    fn endpoint(&mut self, e: &Endpoint) {
        let addr = match e.family {
            AddrFamily::Ipv4 => format!("ipv4 {}:{}", e.address, e.port),
            AddrFamily::Ipv6 => format!("ipv6 [{}]:{}", e.address, e.port),
        };
        self.line(2, &format!("host {} address {addr};", quote(&e.host)));
    }
}

fn version_text(v: Version) -> String {
    format!("{}.{}.{}", v.major, v.minor, v.patch)
}

/// Change detection: compare from the first `resource "` marker.
/// A missing marker on either side counts as different.
pub fn body_differs(existing: &str, new: &str) -> bool {
    const MARKER: &str = "resource \"";
    match (existing.find(MARKER), new.find(MARKER)) {
        (Some(a), Some(b)) => existing[a..] != new[b..],
        _ => true,
    }
}

/// Metadata sizing inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaParams {
    pub peer_slots: u8,
    pub al_stripes: u32,
    pub al_stripe_size_kib: u64,
    pub bitmap_block_kib: u64,
}

impl Default for MetaParams {
    fn default() -> Self {
        Self {
            peer_slots: 7,
            al_stripes: 1,
            al_stripe_size_kib: 32,
            bitmap_block_kib: 4,
        }
    }
}

/// Gross size (net plus internal metadata) in KiB. Worked example:
/// net 1048576 with defaults gives 1048840.
pub fn gross_size_kib(net_kib: u64, p: MetaParams) -> Result<u64, Error> {
    let b = bitmap_block(p)?;
    let al = al_kib(p);
    let peers = u128::from(p.peer_slots);
    let n = u128::from(net_kib);
    if n < 4 {
        return Err(Error::SizeRange(format!(
            "net size {net_kib} KiB below the 4 KiB bitmap minimum"
        )));
    }
    let an = ceil_align(n, 4);
    let mut g = an + al + 4;
    let mut cover = 0;
    let mut bm = 0;
    // Growing the bitmap grows the device it must cover; iterate to a fixpoint.
    while cover < g {
        let pb = ceil_align(g.div_ceil(b * 8), 8);
        cover = pb * b * 8;
        bm = ceil_align((pb * peers).div_ceil(1024), 4);
        g = an + ceil_align(al + 4 + bm, 4);
    }
    let md = ceil_align(bm + al + 4, 4);
    let gross = (ceil_align(n, b) + md).max(MIN_GROSS_KIB);
    if gross > MAX_GROSS_KIB {
        return Err(Error::SizeRange(format!(
            "gross size {gross} KiB above 2^40 KiB"
        )));
    }
    Ok(u64::try_from(gross).expect("bounded by MAX_GROSS_KIB"))
}

const MIN_GROSS_KIB: u128 = 68;
const MAX_GROSS_KIB: u128 = 1 << 40;

fn bitmap_block(p: MetaParams) -> Result<u128, Error> {
    let b = p.bitmap_block_kib;
    if b.is_power_of_two() && (4..=1024).contains(&b) {
        Ok(u128::from(b))
    } else {
        Err(Error::BitmapBlockSize(b))
    }
}

fn al_kib(p: MetaParams) -> u128 {
    ceil_align(
        u128::from(p.al_stripes) * u128::from(p.al_stripe_size_kib),
        4,
    )
}

fn ceil_align(v: u128, a: u128) -> u128 {
    v.div_ceil(a) * a
}

/// Net size from a gross size in KiB (after storage rounding). Worked example:
/// gross 1052672 with defaults gives 1052408.
pub fn net_size_kib(gross_kib: u64, p: MetaParams) -> Result<u64, Error> {
    let b = bitmap_block(p)?;
    let al = al_kib(p);
    let gr = u128::from(gross_kib);
    if gr > MAX_GROSS_KIB {
        return Err(Error::SizeRange(format!(
            "gross size {gross_kib} KiB above 2^40 KiB"
        )));
    }
    let ag = ceil_align(gr, 4);
    let pb = ceil_align(ag.div_ceil(b * 8), 8);
    let bm = ceil_align((pb * u128::from(p.peer_slots)).div_ceil(1024), 4);
    let md = ceil_align(bm + al + 4, 4);
    let floor = gr / 4 * 4;
    match floor.checked_sub(md) {
        Some(net) if net > 0 => Ok(u64::try_from(net).expect("below gross")),
        _ => Err(Error::SizeRange(format!(
            "gross size {gross_kib} KiB leaves no room after {md} KiB of metadata"
        ))),
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("size out of range: {0}")]
    SizeRange(String),
    #[error("bitmap block size must be a power of two in 4..1024 KiB, got {0}")]
    BitmapBlockSize(u64),
}
