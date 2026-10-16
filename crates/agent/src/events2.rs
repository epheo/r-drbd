//! events2 stream model and the tracked DRBD state it feeds.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Exists,
    Create,
    Change,
    Destroy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Object {
    Resource {
        name: String,
    },
    Connection {
        name: String,
        conn_name: String,
        peer_node_id: Option<u8>,
    },
    Device {
        name: String,
        volume: u32,
    },
    PeerDevice {
        name: String,
        conn_name: String,
        volume: u32,
    },
    /// The `exists -` end-of-dump marker.
    EndOfDump,
    Ignored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Line {
    pub action: Action,
    pub object: Object,
    /// key to value, split at the first colon; tokens without a colon dropped.
    pub fields: Vec<(String, String)>,
}

impl Line {
    fn get(&self, key: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("line has an action but no object type: {0}")]
    NoObject(String),
    #[error("missing identity key {key} in: {line}")]
    MissingIdentity { key: &'static str, line: String },
    #[error("bad number for {key}: {value}")]
    Number { key: &'static str, value: String },
}

/// One line; `Ok(None)` for empty or ignored actions.
pub fn parse_line(line: &str) -> Result<Option<Line>, ParseError> {
    let line = line.trim_end_matches(['\n', '\r']);
    let mut tokens = line.split(' ').filter(|t| !t.is_empty());
    let Some(action) = tokens.next() else {
        return Ok(None);
    };
    let action = match action {
        "exists" => Action::Exists,
        "create" => Action::Create,
        "change" => Action::Change,
        "destroy" => Action::Destroy,
        _ => return Ok(None),
    };
    let kind = tokens
        .next()
        .ok_or_else(|| ParseError::NoObject(line.to_owned()))?;
    let fields: Vec<(String, String)> = tokens
        .filter_map(|t| t.split_once(':'))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    let get = |key: &'static str| -> Result<String, ParseError> {
        fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .ok_or_else(|| ParseError::MissingIdentity {
                key,
                line: line.to_owned(),
            })
    };
    let volume = || -> Result<u32, ParseError> {
        let v = get("volume")?;
        v.parse().map_err(|_| ParseError::Number {
            key: "volume",
            value: v,
        })
    };
    let object = match kind {
        "-" => Object::EndOfDump,
        "resource" => Object::Resource { name: get("name")? },
        "connection" => {
            let peer_node_id = match fields.iter().find(|(k, _)| k == "peer-node-id") {
                Some((_, v)) => Some(v.parse().map_err(|_| ParseError::Number {
                    key: "peer-node-id",
                    value: v.clone(),
                })?),
                None => None,
            };
            Object::Connection {
                name: get("name")?,
                conn_name: get("conn-name")?,
                peer_node_id,
            }
        }
        "device" => Object::Device {
            name: get("name")?,
            volume: volume()?,
        },
        "peer-device" => Object::PeerDevice {
            name: get("name")?,
            conn_name: get("conn-name")?,
            volume: volume()?,
        },
        _ => Object::Ignored,
    };
    Ok(Some(Line {
        action,
        object,
        fields,
    }))
}

/// A `call helper` or `response helper` line: no object type after the
/// action, so it parses apart from [`Line`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelperLine {
    pub call: bool,
    pub name: String,
    pub conn_name: Option<String>,
    pub helper: String,
}

/// `call helper name:r0 peer-node-id:1 conn-name:b helper:split-brain`
/// and its `response helper ... status:N` line; None for anything else.
pub fn parse_helper(line: &str) -> Option<HelperLine> {
    let mut tokens = line
        .trim_end_matches(['\n', '\r'])
        .split(' ')
        .filter(|t| !t.is_empty());
    let call = match tokens.next()? {
        "call" => true,
        "response" => false,
        _ => return None,
    };
    if tokens.next()? != "helper" {
        return None;
    }
    let mut name = None;
    let mut conn_name = None;
    let mut helper = None;
    for (k, v) in tokens.filter_map(|t| t.split_once(':')) {
        match k {
            "name" => name = Some(v.to_owned()),
            "conn-name" => conn_name = Some(v.to_owned()),
            "helper" => helper = Some(v.to_owned()),
            _ => {}
        }
    }
    Some(HelperLine {
        call,
        name: name?,
        conn_name,
        helper: helper?,
    })
}

pub const CONNECTION_STATES: &[&str] = &[
    "StandAlone",
    "Disconnecting",
    "Unconnected",
    "Timeout",
    "BrokenPipe",
    "NetworkFailure",
    "ProtocolError",
    "TearDown",
    "Connecting",
    "Connected",
];
pub const DISK_STATES: &[&str] = &[
    "Diskless",
    "Attaching",
    "Detaching",
    "Failed",
    "Negotiating",
    "Inconsistent",
    "Outdated",
    "DUnknown",
    "Consistent",
    "UpToDate",
];
const REPLICATION_STATES: &[&str] = &[
    "Off",
    "Established",
    "StartingSyncS",
    "StartingSyncT",
    "WFBitMapS",
    "WFBitMapT",
    "WFSyncUUID",
    "SyncSource",
    "SyncTarget",
    "PausedSyncS",
    "PausedSyncT",
    "VerifyS",
    "VerifyT",
    "Ahead",
    "Behind",
];

fn label(v: &str, domain: &[&str], other: &str) -> String {
    if domain.contains(&v) { v } else { other }.to_owned()
}

fn yes_no(v: &str) -> Option<bool> {
    match v {
        "yes" => Some(true),
        "no" => Some(false),
        _ => None,
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Resource {
    /// Primary, Secondary or Unknown.
    pub role: Option<String>,
    /// The `suspended` list, e.g. `["user", "quorum"]`; `no` gives an empty list.
    pub suspended: Vec<String>,
    pub may_promote: Option<bool>,
    pub promotion_score: Option<i64>,
    /// Keyed by conn-name (the peer's node name).
    pub connections: BTreeMap<String, Connection>,
    /// Keyed by volume number.
    pub devices: BTreeMap<u32, Device>,
    /// Keyed by (conn-name, volume).
    pub peer_devices: BTreeMap<(String, u32), PeerDevice>,
}

impl Resource {
    pub fn suspended_by_user(&self) -> bool {
        self.suspended.iter().any(|s| s == "user")
    }

    pub fn is_primary(&self) -> bool {
        self.role.as_deref() == Some("Primary")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Connection {
    pub peer_node_id: Option<u8>,
    /// One of [`CONNECTION_STATES`] or Unknown.
    pub state: String,
    pub peer_role: Option<String>,
    /// DRBD called the split-brain helper for this connection; cleared once
    /// it is Connected again. Its StandAlone is then a decision for
    /// spec.resolve, not something an adjust should reconnect.
    pub split_brain: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Device {
    pub minor: Option<u32>,
    /// One of [`DISK_STATES`]; anything else is DUnknown.
    pub disk: Option<String>,
    pub client: Option<bool>,
    /// The device line carries quorum, not the resource line (seen on DRBD 9.3).
    pub quorum: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PeerDevice {
    pub replication: Option<String>,
    pub peer_disk: Option<String>,
    pub peer_client: Option<bool>,
    /// Resync progress 0 to 100; cleared by every update without `done`.
    pub done: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TrackError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("{kind} not tracked: {name}")]
    Unknown { kind: &'static str, name: String },
    /// State is built from the dump alone; a change seen before its end
    /// makes the dump unreliable, so the stream starts over.
    #[error("change before the end of the initial dump")]
    BeforeDump,
}

/// What one applied line changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    /// Resources whose state changed (every tracked one when the dump ends).
    pub resources: Vec<String>,
    /// The end-of-dump marker made the state available.
    pub became_available: bool,
}

/// Consecutive streams ended by a change during the dump before the
/// tracker queues such changes instead: a host whose dump keeps racing
/// changes would otherwise never get state.
pub const QUEUE_AFTER_RESTARTS: u32 = 3;
/// Bound on the queued changes; more ends the stream again.
pub const QUEUE_MAX: usize = 4096;

/// Tracked DRBD state for all resources on this node. Unavailable until the
/// initial dump ends; [`Tracker::reset`] on every stream restart
/// clears everything but the split-brain marks it can carry over.
#[derive(Debug, Default)]
pub struct Tracker {
    available: bool,
    resources: BTreeMap<String, Resource>,
    dirty: BTreeSet<String>,
    /// (resource, connection) marked split brain before the last restart.
    split_brain_before: BTreeSet<(String, String)>,
    /// Streams in a row ended by a change before the dump end.
    before_dump_restarts: u32,
    queued: Vec<Line>,
}

impl Tracker {
    pub fn is_available(&self) -> bool {
        self.available
    }

    pub fn resource(&self, name: &str) -> Option<&Resource> {
        self.resources.get(name)
    }

    pub fn resources(&self) -> &BTreeMap<String, Resource> {
        &self.resources
    }

    /// Resources touched since the last call.
    pub fn take_dirty(&mut self) -> BTreeSet<String> {
        std::mem::take(&mut self.dirty)
    }

    /// The dump shows no split-brain helper call, so a connection marked
    /// before the restart keeps its mark when the new dump still shows it
    /// StandAlone; anything else in the dump drops the mark.
    pub fn reset(&mut self) {
        let names: Vec<String> = self.resources.keys().cloned().collect();
        self.split_brain_before = self
            .resources
            .iter()
            .flat_map(|(r, res)| {
                res.connections
                    .iter()
                    .filter(|(_, c)| c.split_brain)
                    .map(move |(c, _)| (r.clone(), c.clone()))
            })
            .collect();
        self.available = false;
        self.resources.clear();
        self.queued.clear();
        self.dirty.extend(names);
    }

    pub fn before_dump_restarts(&self) -> u32 {
        self.before_dump_restarts
    }

    /// Parse and apply one raw line.
    pub fn feed(&mut self, raw: &str) -> Result<Applied, TrackError> {
        if let Some(h) = parse_helper(raw) {
            return Ok(self.helper(&h));
        }
        match parse_line(raw)? {
            Some(line) => self.apply(&line),
            None => Ok(Applied::default()),
        }
    }

    /// Live events only; a helper for an object not tracked is
    /// dropped (the dump will show its state).
    fn helper(&mut self, h: &HelperLine) -> Applied {
        let hit = h.helper == "split-brain"
            && h.conn_name
                .as_ref()
                .and_then(|c| self.resources.get_mut(&h.name)?.connections.get_mut(c))
                .map(|c| c.split_brain = true)
                .is_some();
        if !hit {
            return Applied::default();
        }
        self.dirty.insert(h.name.clone());
        Applied {
            resources: vec![h.name.clone()],
            became_available: false,
        }
    }

    /// Before the end-of-dump marker only `exists` lines are accepted; a
    /// change ends the stream, or after [`QUEUE_AFTER_RESTARTS`] such ends
    /// in a row waits in a bounded queue for the dump end.
    pub fn apply(&mut self, line: &Line) -> Result<Applied, TrackError> {
        if line.object == Object::EndOfDump {
            if self.available {
                return Ok(Applied::default());
            }
            self.available = true;
            for (r, c) in std::mem::take(&mut self.split_brain_before) {
                if let Some(conn) = self
                    .resources
                    .get_mut(&r)
                    .and_then(|x| x.connections.get_mut(&c))
                    .filter(|x| x.state == "StandAlone")
                {
                    conn.split_brain = true;
                }
            }
            let queued = std::mem::take(&mut self.queued);
            if queued.is_empty() {
                self.before_dump_restarts = 0;
            }
            for l in &queued {
                self.apply_now(l)?;
            }
            return Ok(Applied {
                resources: self.resources.keys().cloned().collect(),
                became_available: true,
            });
        }
        if !self.available && line.action != Action::Exists {
            if self.before_dump_restarts < QUEUE_AFTER_RESTARTS || self.queued.len() >= QUEUE_MAX {
                self.before_dump_restarts = self.before_dump_restarts.saturating_add(1);
                return Err(TrackError::BeforeDump);
            }
            self.queued.push(line.clone());
            return Ok(Applied::default());
        }
        Ok(Applied {
            resources: self.apply_now(line)?.into_iter().collect(),
            became_available: false,
        })
    }

    fn apply_now(&mut self, line: &Line) -> Result<Option<String>, TrackError> {
        let create = matches!(line.action, Action::Exists | Action::Create);
        let name = match &line.object {
            Object::Resource { name } => {
                if line.action == Action::Destroy {
                    self.resources
                        .remove(name)
                        .ok_or_else(|| unknown("resource", name))?;
                } else {
                    let r = self.slot(name, create)?;
                    apply_resource(r, line)?;
                }
                name
            }
            Object::Connection {
                name,
                conn_name,
                peer_node_id,
            } => {
                let r = self.slot(name, false)?;
                apply_connection(r, line, name, conn_name, *peer_node_id, create)?;
                name
            }
            Object::Device { name, volume } => {
                let r = self.slot(name, false)?;
                if line.action == Action::Destroy {
                    r.devices
                        .remove(volume)
                        .ok_or_else(|| unknown("device", &format!("{name}/{volume}")))?;
                    r.peer_devices.retain(|(_, v), _| v != volume);
                } else {
                    let d = match r.devices.get_mut(volume) {
                        Some(d) => d,
                        None if create => r.devices.entry(*volume).or_default(),
                        None => return Err(unknown("device", &format!("{name}/{volume}"))),
                    };
                    apply_device(d, line)?;
                }
                name
            }
            Object::PeerDevice {
                name,
                conn_name,
                volume,
            } => {
                let r = self.slot(name, false)?;
                let key = (conn_name.clone(), *volume);
                if line.action == Action::Destroy {
                    r.peer_devices.remove(&key).ok_or_else(|| {
                        unknown("peer-device", &format!("{name}/{conn_name}/{volume}"))
                    })?;
                } else {
                    if !r.connections.contains_key(conn_name) {
                        return Err(unknown("connection", conn_name));
                    }
                    let p = match r.peer_devices.get_mut(&key) {
                        Some(p) => p,
                        None if create => r.peer_devices.entry(key).or_default(),
                        None => {
                            return Err(unknown(
                                "peer-device",
                                &format!("{name}/{conn_name}/{volume}"),
                            ));
                        }
                    };
                    apply_peer_device(p, line)?;
                }
                name
            }
            Object::EndOfDump | Object::Ignored => return Ok(None),
        };
        self.dirty.insert(name.clone());
        Ok(Some(name.clone()))
    }

    fn slot(&mut self, name: &str, create: bool) -> Result<&mut Resource, TrackError> {
        if create {
            return Ok(self.resources.entry(name.to_owned()).or_default());
        }
        self.resources
            .get_mut(name)
            .ok_or_else(|| unknown("resource", name))
    }
}

fn unknown(kind: &'static str, name: &str) -> TrackError {
    TrackError::Unknown {
        kind,
        name: name.to_owned(),
    }
}

fn apply_connection(
    r: &mut Resource,
    line: &Line,
    name: &str,
    conn_name: &str,
    peer_node_id: Option<u8>,
    create: bool,
) -> Result<(), TrackError> {
    if line.action == Action::Destroy {
        r.connections
            .remove(conn_name)
            .ok_or_else(|| unknown("connection", conn_name))?;
        r.peer_devices.retain(|(c, _), _| c != conn_name);
        return Ok(());
    }
    let c = match r.connections.get_mut(conn_name) {
        Some(c) => c,
        None if create => {
            let id = peer_node_id.ok_or_else(|| ParseError::MissingIdentity {
                key: "peer-node-id",
                line: format!("connection {name} {conn_name}"),
            })?;
            r.connections
                .entry(conn_name.to_owned())
                .or_insert(Connection {
                    peer_node_id: Some(id),
                    state: "Unknown".to_owned(),
                    peer_role: None,
                    split_brain: false,
                })
        }
        None => return Err(unknown("connection", conn_name)),
    };
    if let Some(s) = line.get("connection") {
        c.state = label(s, CONNECTION_STATES, "Unknown");
        if c.state == "Connected" {
            c.split_brain = false;
        }
    }
    if let Some(role) = line.get("role") {
        c.peer_role = Some(role.to_owned());
    }
    Ok(())
}

fn apply_resource(r: &mut Resource, line: &Line) -> Result<(), ParseError> {
    if let Some(role) = line.get("role") {
        r.role = Some(label(role, &["Primary", "Secondary"], "Unknown"));
    }
    if let Some(s) = line.get("suspended") {
        r.suspended = s
            .split(',')
            .filter(|s| !s.is_empty() && *s != "no")
            .map(str::to_owned)
            .collect();
    }
    if let Some(v) = line.get("may_promote") {
        r.may_promote = yes_no(v);
    }
    if let Some(v) = line.get("promotion_score") {
        r.promotion_score = Some(v.parse().map_err(|_| ParseError::Number {
            key: "promotion_score",
            value: v.to_owned(),
        })?);
    }
    Ok(())
}

fn apply_device(d: &mut Device, line: &Line) -> Result<(), ParseError> {
    if let Some(v) = line.get("minor") {
        d.minor = Some(v.parse().map_err(|_| ParseError::Number {
            key: "minor",
            value: v.to_owned(),
        })?);
    }
    if let Some(v) = line.get("disk").or_else(|| line.get("peer-disk")) {
        d.disk = Some(label(v, DISK_STATES, "DUnknown"));
    }
    if let Some(v) = line.get("client").or_else(|| line.get("peer-client")) {
        d.client = yes_no(v);
    }
    if let Some(v) = line.get("quorum") {
        d.quorum = yes_no(v);
    }
    Ok(())
}

fn apply_peer_device(p: &mut PeerDevice, line: &Line) -> Result<(), ParseError> {
    if let Some(v) = line.get("replication") {
        p.replication = Some(label(v, REPLICATION_STATES, "Unknown"));
    }
    if let Some(v) = line.get("peer-disk").or_else(|| line.get("disk")) {
        p.peer_disk = Some(label(v, DISK_STATES, "DUnknown"));
    }
    if let Some(v) = line.get("peer-client").or_else(|| line.get("client")) {
        p.peer_client = yes_no(v);
    }
    p.done = match line.get("done") {
        Some(v) => Some(
            v.parse::<f64>()
                .ok()
                .filter(|f| f.is_finite())
                .ok_or_else(|| ParseError::Number {
                    key: "done",
                    value: v.to_owned(),
                })?
                .clamp(0.0, 100.0),
        ),
        None => None,
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> String {
        v.to_owned()
    }

    #[test]
    fn parses_line_forms() {
        let l = parse_line("exists resource name:r0 role:Secondary suspended:no force-io-failures:no may_promote:no promotion_score:0\n")
            .unwrap()
            .unwrap();
        assert_eq!(l.action, Action::Exists);
        assert_eq!(l.object, Object::Resource { name: s("r0") });
        assert_eq!(l.fields[1], (s("role"), s("Secondary")));

        let l = parse_line(
            "create connection name:r0 peer-node-id:1 conn-name:ghost connection:Connecting role:Unknown",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            l.object,
            Object::Connection {
                name: s("r0"),
                conn_name: s("ghost"),
                peer_node_id: Some(1)
            }
        );

        let l = parse_line("change device name:r0 volume:0 minor:51999 backing_dev:/dev/vg/lv disk:UpToDate client:no quorum:no")
            .unwrap()
            .unwrap();
        assert_eq!(l.action, Action::Change);
        assert_eq!(
            l.object,
            Object::Device {
                name: s("r0"),
                volume: 0
            }
        );
        // Split at the first colon only.
        let l = parse_line(
            "exists path name:r0 peer-node-id:1 conn-name:ghost local:ipv4:10.0.0.1:11000",
        )
        .unwrap()
        .unwrap();
        assert_eq!(l.object, Object::Ignored);
        assert_eq!(l.fields[3], (s("local"), s("ipv4:10.0.0.1:11000")));

        let l =
            parse_line("destroy peer-device name:r0 peer-node-id:1 conn-name:ghost volume:0 junk")
                .unwrap()
                .unwrap();
        assert_eq!(l.action, Action::Destroy);
        assert_eq!(
            l.object,
            Object::PeerDevice {
                name: s("r0"),
                conn_name: s("ghost"),
                volume: 0
            }
        );
        assert_eq!(l.fields.len(), 4, "token without a colon dropped");

        let l = parse_line("exists -").unwrap().unwrap();
        assert_eq!(l.object, Object::EndOfDump);
    }

    #[test]
    fn ignored_and_errors() {
        assert_eq!(parse_line("").unwrap(), None);
        assert_eq!(parse_line("\n").unwrap(), None);
        assert_eq!(
            parse_line("call helper name:r0 volume:0 helper:before-resync-target").unwrap(),
            None
        );
        assert_eq!(
            parse_line("response helper name:r0 helper:x status:0").unwrap(),
            None
        );
        assert!(matches!(parse_line("change"), Err(ParseError::NoObject(_))));
        assert!(matches!(
            parse_line("change device name:r0 minor:3"),
            Err(ParseError::MissingIdentity { key: "volume", .. })
        ));
        assert!(matches!(
            parse_line("change device name:r0 volume:x"),
            Err(ParseError::Number { key: "volume", .. })
        ));
        assert!(matches!(
            parse_line("exists connection conn-name:a"),
            Err(ParseError::MissingIdentity { key: "name", .. })
        ));
    }

    const DUMP: &[&str] = &[
        "exists resource name:r0 role:Secondary suspended:quorum force-io-failures:no may_promote:no promotion_score:0",
        "exists connection name:r0 peer-node-id:1 conn-name:ghost connection:Connecting role:Unknown",
        "exists device name:r0 volume:0 minor:51999 backing_dev:/dev/vg/r0 disk:UpToDate client:no open:no quorum:no",
        "exists peer-device name:r0 peer-node-id:1 conn-name:ghost volume:0 replication:Off peer-disk:DUnknown peer-client:no resync-suspended:no",
        "exists path name:r0 peer-node-id:1 conn-name:ghost local:ipv4:192.0.2.2:11999 peer:ipv4:192.0.2.1:11999 established:no",
    ];

    #[test]
    fn initial_dump_only() {
        let mut t = Tracker::default();
        for l in DUMP {
            t.feed(l).unwrap();
        }
        assert!(!t.is_available());
        let a = t.feed("exists -").unwrap();
        assert!(a.became_available);
        assert_eq!(a.resources, vec![s("r0")]);
        let r = t.resource("r0").unwrap();
        assert_eq!(r.role.as_deref(), Some("Secondary"));
        assert_eq!(r.suspended, vec![s("quorum")]);
        assert_eq!(r.connections["ghost"].peer_node_id, Some(1));
        assert_eq!(r.connections["ghost"].state, "Connecting");
        assert_eq!(r.devices[&0].disk.as_deref(), Some("UpToDate"));
        assert_eq!(r.devices[&0].minor, Some(51999));
        assert_eq!(r.devices[&0].quorum, Some(false));
        assert_eq!(
            r.peer_devices[&(s("ghost"), 0)].replication.as_deref(),
            Some("Off")
        );
        assert_eq!(
            t.take_dirty().into_iter().collect::<Vec<_>>(),
            vec![s("r0")]
        );
        assert!(t.take_dirty().is_empty());
        // A second marker changes nothing.
        assert_eq!(t.feed("exists -").unwrap(), Applied::default());
    }

    #[test]
    fn change_during_the_dump_ends_the_stream_then_queues() {
        let mut t = Tracker::default();
        for n in 1..=QUEUE_AFTER_RESTARTS {
            t.reset();
            t.feed(DUMP[0]).unwrap();
            assert_eq!(
                t.feed("change resource name:r0 role:Primary"),
                Err(TrackError::BeforeDump)
            );
            assert_eq!(t.before_dump_restarts(), n);
            assert_eq!(t.resource("r0").unwrap().role.as_deref(), Some("Secondary"));
        }
        // Racing for the third time in a row: queued and replayed.
        t.reset();
        t.feed(DUMP[0]).unwrap();
        t.feed("change resource name:r0 role:Primary").unwrap();
        assert_eq!(t.resource("r0").unwrap().role.as_deref(), Some("Secondary"));
        t.feed("exists -").unwrap();
        assert_eq!(t.resource("r0").unwrap().role.as_deref(), Some("Primary"));
        assert_eq!(t.before_dump_restarts(), QUEUE_AFTER_RESTARTS);
        // A clean dump starts the count over.
        t.reset();
        t.feed("exists -").unwrap();
        assert_eq!(t.before_dump_restarts(), 0);
    }

    #[test]
    fn split_brain_mark_survives_a_restart_while_standalone() {
        let mut t = Tracker::default();
        for l in DUMP {
            t.feed(l).unwrap();
        }
        t.feed("exists -").unwrap();
        t.feed("call helper name:r0 peer-node-id:1 conn-name:ghost helper:split-brain")
            .unwrap();
        t.feed("change connection name:r0 conn-name:ghost connection:StandAlone")
            .unwrap();
        t.reset();
        t.feed(DUMP[0]).unwrap();
        t.feed("exists connection name:r0 peer-node-id:1 conn-name:ghost connection:StandAlone")
            .unwrap();
        t.feed("exists -").unwrap();
        assert!(t.resource("r0").unwrap().connections["ghost"].split_brain);
        // Connecting in the next dump: the mark is gone.
        t.reset();
        t.feed(DUMP[0]).unwrap();
        t.feed(DUMP[1]).unwrap();
        t.feed("exists -").unwrap();
        assert!(!t.resource("r0").unwrap().connections["ghost"].split_brain);
    }

    #[test]
    fn stream_changes_and_restart() {
        let mut t = Tracker::default();
        for l in DUMP {
            t.feed(l).unwrap();
        }
        t.feed("exists -").unwrap();
        t.feed("change peer-device name:r0 peer-node-id:1 conn-name:ghost volume:0 replication:SyncTarget done:12.5")
            .unwrap();
        assert_eq!(
            t.resource("r0").unwrap().peer_devices[&(s("ghost"), 0)].done,
            Some(12.5)
        );
        // Without --statistics `done` vanishes on the next update.
        t.feed("change peer-device name:r0 peer-node-id:1 conn-name:ghost volume:0 replication:Established")
            .unwrap();
        let pd = &t.resource("r0").unwrap().peer_devices[&(s("ghost"), 0)];
        assert_eq!(
            (pd.replication.as_deref(), pd.done),
            (Some("Established"), None)
        );
        t.feed("change peer-device name:r0 conn-name:ghost volume:0 done:250")
            .unwrap();
        assert_eq!(
            t.resource("r0").unwrap().peer_devices[&(s("ghost"), 0)].done,
            Some(100.0)
        );
        t.feed("change connection name:r0 conn-name:ghost connection:Weird")
            .unwrap();
        assert_eq!(
            t.resource("r0").unwrap().connections["ghost"].state,
            "Unknown"
        );
        t.feed("change device name:r0 volume:0 disk:Bogus").unwrap();
        assert_eq!(
            t.resource("r0").unwrap().devices[&0].disk.as_deref(),
            Some("DUnknown")
        );

        assert!(matches!(
            t.feed("change resource name:other role:Primary"),
            Err(TrackError::Unknown {
                kind: "resource",
                ..
            })
        ));
        assert!(matches!(
            t.feed("change connection name:r0 conn-name:nobody connection:Connected"),
            Err(TrackError::Unknown {
                kind: "connection",
                ..
            })
        ));
        assert!(matches!(
            t.feed("change resource name:r0 promotion_score:abc"),
            Err(TrackError::Parse(ParseError::Number { .. }))
        ));
        assert!(matches!(
            t.feed("change peer-device name:r0 conn-name:ghost volume:0 done:x"),
            Err(TrackError::Parse(ParseError::Number { .. }))
        ));

        t.feed("destroy connection name:r0 peer-node-id:1 conn-name:ghost")
            .unwrap();
        let r = t.resource("r0").unwrap();
        assert!(r.connections.is_empty() && r.peer_devices.is_empty());
        t.feed("destroy device name:r0 volume:0").unwrap();
        t.feed("destroy resource name:r0").unwrap();
        assert!(t.resource("r0").is_none());

        t.feed("create resource name:r1 role:Secondary").unwrap();
        t.take_dirty();
        t.reset();
        assert!(!t.is_available());
        assert!(t.resources().is_empty());
        assert_eq!(
            t.take_dirty().into_iter().collect::<Vec<_>>(),
            vec![s("r1")]
        );
        t.feed("exists -").unwrap();
        assert!(t.is_available());
    }

    #[test]
    fn split_brain_helper_marks_the_connection() {
        assert_eq!(
            parse_helper("call helper name:r0 peer-node-id:1 conn-name:b helper:split-brain"),
            Some(HelperLine {
                call: true,
                name: s("r0"),
                conn_name: Some(s("b")),
                helper: s("split-brain"),
            })
        );
        assert!(
            !parse_helper("response helper name:r0 conn-name:b helper:split-brain status:0")
                .unwrap()
                .call
        );
        assert_eq!(parse_helper("exists resource name:r0"), None);
        assert_eq!(parse_helper("call helper conn-name:b"), None);

        let mut t = Tracker::default();
        for l in DUMP {
            t.feed(l).unwrap();
        }
        t.feed("exists -").unwrap();
        t.take_dirty();
        t.feed("call helper name:r0 peer-node-id:1 conn-name:ghost helper:split-brain")
            .unwrap();
        t.feed(
            "response helper name:r0 peer-node-id:1 conn-name:ghost helper:split-brain status:0",
        )
        .unwrap();
        t.feed("change connection name:r0 conn-name:ghost connection:Disconnecting")
            .unwrap();
        t.feed("change connection name:r0 conn-name:ghost connection:StandAlone")
            .unwrap();
        assert!(t.resource("r0").unwrap().connections["ghost"].split_brain);
        assert_eq!(
            t.take_dirty().into_iter().collect::<Vec<_>>(),
            vec![s("r0")]
        );
        // Unknown objects and other helpers change nothing.
        assert_eq!(
            t.feed("call helper name:zz conn-name:x helper:split-brain")
                .unwrap(),
            Applied::default()
        );
        t.feed("call helper name:r0 volume:0 helper:before-resync-target")
            .unwrap();
        t.feed("change connection name:r0 conn-name:ghost connection:Connected")
            .unwrap();
        assert!(!t.resource("r0").unwrap().connections["ghost"].split_brain);
    }

    #[test]
    fn create_connection_needs_peer_node_id() {
        let mut t = Tracker::default();
        t.feed("exists -").unwrap();
        t.feed("create resource name:r0 role:Secondary").unwrap();
        assert!(matches!(
            t.feed("create connection name:r0 conn-name:b connection:Connecting"),
            Err(TrackError::Parse(ParseError::MissingIdentity {
                key: "peer-node-id",
                ..
            }))
        ));
        t.feed("create resource name:r0 suspended:user,quorum")
            .unwrap();
        assert!(t.resource("r0").unwrap().suspended_by_user());
    }
}
