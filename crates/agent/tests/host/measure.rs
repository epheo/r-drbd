//! Measurements behind the restore of an image into a larger disk. Each
//! test prints its numbers and asserts only what keeps them meaningful;
//! run with --nocapture. Resources, LVs, the throwaway VG and the nft
//! tables are named rw-m*; minors 99980 to 99983, ports 11980 to 11988,
//! loopback addresses only. Resource files go under the scratch
//! directory, outside the include glob of the agent's drbdadm, and reach
//! drbdadm through -c; their options come from the agent's renderer.

#![allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use agent::drbd::argv;
use agent::{Exec, ExecMode};
use serde_json::Value;

use super::{GIB, NO_BACKUP, TEST_DIR, THIN, VG, enabled, host_command, hostname, lvm, sh};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;
/// The test VG's extent.
const EXTENT: u64 = 4 * MIB;
const SLOTS: u8 = model::keys::PEER_SLOTS;
const MINOR0: u32 = 99_980;
const PORT0: u16 = 11_980;
const PREFIX: &str = "rw-m";
const SECRET: &str = "bWVhc3VyZW1lYXN1cmVtZWFzdXJlbWVhc3VyZW1lYQ==";
const NESTED_PV: &str = "rw-m-pv";
const NESTED_VG: &str = "rw-m-nested";

fn dev(lv: &str) -> String {
    format!("/dev/{VG}/{lv}")
}

/// The LV size the agent gives a volume of `net` bytes in a VG of `extent`.
fn gross(net: u64, extent: u64) -> u64 {
    agent::storage::desired_bytes(drbd_config::gross_bytes(net, SLOTS), extent)
}

fn formula(lv_bytes: u64) -> u64 {
    drbd_config::usable_bytes(lv_bytes, SLOTS)
}

fn files_dir() -> String {
    format!("{TEST_DIR}/rw-m")
}

fn stop_file() -> String {
    format!("{TEST_DIR}/rw-m-stop")
}

fn secs(d: Duration) -> String {
    format!("{:.3}s", d.as_secs_f64())
}

fn signed(a: u64, b: u64) -> String {
    if a >= b {
        format!("+{}", a - b)
    } else {
        format!("-{}", b - a)
    }
}

async fn ok(parts: &[&str]) -> String {
    let out = sh(parts).await;
    assert_eq!(out.exit_code, Some(0), "{parts:?}: {}", out.stderr);
    out.stdout
}

/// `argv` with `limit` seconds to run.
async fn run_long(argv: &[String], limit: u64) -> agent::Output {
    let mode = ExecMode::Timeout {
        wait: Duration::from_secs(limit),
        term: Duration::from_secs(5),
        kill: Duration::from_secs(5),
    };
    super::exec().run(argv, mode).await.unwrap()
}

/// A shell line on the host, for pipes, with `limit` seconds to run.
async fn shell(line: &str, limit: u64) -> String {
    let out = run_long(&agent::exec::argv(&["sh", "-c", line]), limit).await;
    assert_eq!(out.exit_code, Some(0), "{line}: {}", out.stderr);
    out.stdout.trim().to_owned()
}

fn first_line(s: &str) -> &str {
    s.lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
}

async fn lv_bytes(lv: &str) -> u64 {
    let target = format!("{VG}/{lv}");
    ok(&[
        "lvs",
        "--noheadings",
        "--units",
        "b",
        "--nosuffix",
        "-o",
        "lv_size",
        &target,
    ])
    .await
    .trim()
    .parse()
    .unwrap()
}

async fn dm_name(lv: &str) -> String {
    let target = format!("{VG}/{lv}");
    let path = ok(&["lvs", "--noheadings", "-o", "lv_dm_path", &target]).await;
    path.trim().rsplit('/').next().unwrap().to_owned()
}

/// (mapped sectors, highest mapped sector) of an active thin LV, from the
/// thin target's status line.
async fn thin_map(lv: &str) -> (u64, Option<u64>) {
    let line = ok(&["dmsetup", "status", &dm_name(lv).await]).await;
    let f: Vec<&str> = line.split_whitespace().collect();
    (f[3].parse().unwrap(), f.get(4).and_then(|h| h.parse().ok()))
}

/// Data chunks in use in the test thin pool.
async fn pool_used() -> u64 {
    let name = format!("{}-tpool", dm_name(THIN).await);
    let line = ok(&["dmsetup", "status", &name]).await;
    let f: Vec<&str> = line.split_whitespace().collect();
    f[5].split('/').next().unwrap().parse().unwrap()
}

async fn write_zeroes_max(lv: &str) -> String {
    let link = format!("/dev/mapper/{}", dm_name(lv).await);
    let real = std::fs::canonicalize(&link).unwrap();
    let dm = real.file_name().unwrap().to_string_lossy().into_owned();
    std::fs::read_to_string(format!("/sys/block/{dm}/queue/write_zeroes_max_bytes"))
        .unwrap_or_default()
        .trim()
        .to_owned()
}

/// The first 16 hex digits of the sha256 of [off, off + len) of `dev`.
async fn sha(dev: &str, off: u64, len: u64) -> String {
    let line = format!(
        "dd if={dev} iflag=direct,skip_bytes,count_bytes bs=16M skip={off} count={len} status=none | sha256sum"
    );
    shell(&line, 900).await.chars().take(16).collect()
}

/// Bytes that are not zero in [off, off + len) of `dev`.
async fn nonzero(dev: &str, off: u64, len: u64) -> u64 {
    let line = format!(
        "dd if={dev} iflag=direct,skip_bytes,count_bytes bs=16M skip={off} count={len} status=none | tr -d '\\000' | wc -c"
    );
    shell(&line, 900).await.parse().unwrap()
}

fn sectors(minor: u32) -> u64 {
    std::fs::read_to_string(format!("/sys/block/drbd{minor}/size"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// The fields of a dump-md the restore reads; bytes, not sectors.
#[derive(Debug, Default, Clone, Copy)]
struct Md {
    la: u64,
    meta_bytes: u64,
    max_peers: u64,
    unclean: bool,
}

fn parse_md(dump: &str) -> Md {
    let num = |key: &str| {
        dump.lines()
            .find_map(|l| l.trim().strip_prefix(key))
            .and_then(|v| v.trim().trim_end_matches(';').trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    Md {
        la: num("la-size-sect ") * 512,
        meta_bytes: num("# md_size_sect ") * 512,
        max_peers: num("max-peers "),
        unclean: dump.contains("unclean_meta_data"),
    }
}

/// The agent's dump-md command on `dev`.
async fn dump(dev: &str) -> (agent::Output, Md) {
    let out = run_long(&argv::dump_md(dev), 600).await;
    let md = parse_md(&out.stdout);
    (out, md)
}

async fn status(res: &str) -> Option<Value> {
    let out = sh(&["drbdsetup", "status", "--json", "--statistics", res]).await;
    if out.exit_code != Some(0) {
        return None;
    }
    serde_json::from_str::<Value>(&out.stdout)
        .ok()?
        .get(0)
        .cloned()
}

fn disk_state(v: Option<&Value>) -> String {
    v.and_then(|v| v["devices"][0]["disk-state"].as_str())
        .unwrap_or("down")
        .to_owned()
}

fn quorum(v: Option<&Value>) -> Option<bool> {
    v?["devices"][0]["quorum"].as_bool()
}

fn conns(v: Option<&Value>) -> Vec<&Value> {
    v.and_then(|v| v["connections"].as_array())
        .map(|a| a.iter().collect())
        .unwrap_or_default()
}

fn peer_devices(v: Option<&Value>) -> Vec<&Value> {
    conns(v)
        .into_iter()
        .flat_map(|c| c["peer_devices"].as_array().into_iter().flatten())
        .collect()
}

/// Exactly `n` connections, all Connected.
fn connected(v: Option<&Value>, n: usize) -> bool {
    let c = conns(v);
    c.len() == n && c.iter().all(|c| c["connection-state"] == "Connected")
}

fn conn_state(v: Option<&Value>, peer: &str) -> String {
    conns(v)
        .iter()
        .find(|c| c["name"] == peer)
        .and_then(|c| c["connection-state"].as_str())
        .unwrap_or("none")
        .to_owned()
}

fn busy(v: Option<&Value>) -> bool {
    peer_devices(v).iter().any(|p| {
        p["replication-state"]
            .as_str()
            .is_some_and(|s| s != "Established" && s != "Off")
    })
}

/// KiB received, KiB sent and KiB out of sync over every peer device.
fn traffic(v: Option<&Value>) -> (u64, u64, u64) {
    let mut t = (0, 0, 0);
    for p in peer_devices(v) {
        t.0 += p["received"].as_u64().unwrap_or(0);
        t.1 += p["sent"].as_u64().unwrap_or(0);
        t.2 += p["out-of-sync"].as_u64().unwrap_or(0);
    }
    t
}

fn brief(v: Option<&Value>) -> String {
    let Some(v) = v else {
        return "down".to_owned();
    };
    let d = &v["devices"][0];
    let mut s = format!(
        "{} disk {} quorum {} suspended {} (quorum {})",
        v["role"].as_str().unwrap_or("?"),
        d["disk-state"].as_str().unwrap_or("?"),
        d["quorum"],
        v["suspended"],
        v["suspended-quorum"]
    );
    for c in v["connections"].as_array().into_iter().flatten() {
        let p = &c["peer_devices"][0];
        let _ = write!(
            s,
            "; {} {} {} {}",
            c["name"].as_str().unwrap_or("?"),
            c["connection-state"].as_str().unwrap_or("?"),
            p["peer-disk-state"].as_str().unwrap_or("-"),
            p["replication-state"].as_str().unwrap_or("-")
        );
    }
    s
}

/// The quorum a replica entry reports: events2 through the agent's tracker.
async fn entry_quorum(res: &str) -> Option<bool> {
    let out = sh(&["drbdsetup", "events2", "--now", res]).await;
    let mut t = agent::events2::Tracker::default();
    for l in out.stdout.lines() {
        let _ = t.feed(l);
    }
    t.resource(res)?.devices.get(&0)?.quorum
}

/// Polls the status of `res` until `done` holds or `limit` seconds pass;
/// whether it held, and the last statuses.
async fn poll(
    res: &[&str],
    limit: u64,
    done: impl Fn(&[Option<Value>]) -> bool,
) -> (bool, Vec<Option<Value>>) {
    let start = Instant::now();
    loop {
        let mut v = Vec::new();
        for r in res {
            v.push(status(r).await);
        }
        if done(&v) {
            return (true, v);
        }
        if start.elapsed() >= Duration::from_secs(limit) {
            return (false, v);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for(
    res: &[&str],
    limit: u64,
    what: &str,
    done: impl Fn(&[Option<Value>]) -> bool,
) -> Vec<Option<Value>> {
    let (held, v) = poll(res, limit, done).await;
    assert!(
        held,
        "timed out after {limit}s: {what}: {:?}",
        v.iter().map(Option::as_ref).map(brief).collect::<Vec<_>>()
    );
    v
}

#[derive(Clone, Copy)]
struct Env {
    features: drbd_config::Features,
    chunk: u64,
}

impl Env {
    async fn probe() -> Self {
        let exec = super::exec();
        let features = agent::node::probe(&exec)
            .await
            .drbd()
            .expect("DRBD tools and module on the host");
        let rows = lvm().lvs(VG).await.unwrap();
        let chunk = agent::storage::pool_capacity(&rows, THIN)
            .unwrap()
            .chunk_bytes;
        Self { features, chunk }
    }
}

#[derive(Clone)]
struct Node {
    res: String,
    minor: u32,
    /// The backing device; none for a diskless tiebreaker.
    disk: Option<String>,
}

fn node(res: &str, minor: u32, disk: Option<String>) -> Node {
    Node {
        res: res.to_owned(),
        minor,
        disk,
    }
}

/// The members of one volume, each its own resource on 127.0.0.<i + 1> as
/// in drbd.rs, but each connection on its own pair of ports: one kernel
/// refuses two paths to one remote address and port, and three members
/// must connect in a full mesh.
struct Rig {
    env: Env,
    nodes: Vec<Node>,
}

impl Rig {
    fn new(env: Env, nodes: Vec<Node>) -> Self {
        let rig = Self { env, nodes };
        std::fs::create_dir_all(files_dir()).unwrap();
        for i in 0..rig.nodes.len() {
            std::fs::write(rig.file(i), rig.text(i)).unwrap();
        }
        rig
    }

    fn ip(i: usize) -> IpAddr {
        format!("127.0.0.{}", i + 1).parse().unwrap()
    }

    fn port(i: usize, j: usize) -> u16 {
        PORT0 + u16::try_from(3 * i + j).unwrap()
    }

    fn file(&self, i: usize) -> String {
        format!("{}/{}.res", files_dir(), self.nodes[i].res)
    }

    fn res(&self, i: usize) -> &str {
        &self.nodes[i].res
    }

    fn names(&self) -> Vec<&str> {
        self.nodes.iter().map(|n| n.res.as_str()).collect()
    }

    fn vol(&self, i: usize) -> String {
        format!("{}/{}", self.nodes[i].res, drbd_config::VOLUME)
    }

    fn text(&self, me: usize) -> String {
        use drbd_config::{Disk, Host, Resource};
        let n = self.nodes.len();
        let local = hostname();
        let names: Vec<String> = (0..n)
            .map(|j| {
                if j == me {
                    local.clone()
                } else {
                    format!("m{j}")
                }
            })
            .collect();
        let host = |j: usize, disk| Host {
            name: &names[j],
            node_id: u8::try_from(j).unwrap(),
            address: SocketAddr::new(Self::ip(j), Self::port(j, j)),
            disk,
            tiebreaker: true,
        };
        let local_disk = match &self.nodes[me].disk {
            Some(p) => Disk::Local(p),
            None => Disk::None,
        };
        let r = Resource {
            name: &self.nodes[me].res,
            version: "measure",
            minor: self.nodes[me].minor,
            secret: SECRET,
            local: host(me, local_disk),
            peers: (0..n)
                .filter(|&j| j != me)
                .map(|j| {
                    let disk = if self.nodes[j].disk.is_some() {
                        Disk::Peer
                    } else {
                        Disk::None
                    };
                    host(j, disk)
                })
                .collect(),
            two_primaries: false,
            discard_granularity: Some(self.env.chunk),
        };
        let text = drbd_config::render(&r, self.env.features).unwrap().text;
        let cut = text
            .find("    connection {")
            .unwrap_or_else(|| text.rfind("}\n").unwrap());
        let mut out = format!("global {{ usage-count no; }}\n{}", &text[..cut]);
        for j in (0..n).filter(|&j| j != me) {
            let _ = write!(
                out,
                "    connection {{\n        host \"{}\" address ipv4 {}:{};\n        host \"{}\" address ipv4 {}:{};\n    }}\n",
                names[me],
                Self::ip(me),
                Self::port(me, j),
                names[j],
                Self::ip(j),
                Self::port(j, me)
            );
        }
        out.push_str("}\n");
        out
    }

    async fn adm(&self, i: usize, args: &[&str]) -> agent::Output {
        let file = self.file(i);
        let mut cmd = vec!["drbdadm", "-c", &file];
        cmd.extend_from_slice(args);
        sh(&cmd).await
    }

    async fn adm_ok(&self, i: usize, args: &[&str]) {
        let out = self.adm(i, args).await;
        assert_eq!(
            out.exit_code,
            Some(0),
            "{} {args:?}: {}",
            self.res(i),
            out.stderr
        );
    }

    /// As the agent runs it: --force, a slot for each possible peer.
    async fn create_md(&self, i: usize) {
        let peers = format!("--max-peers={SLOTS}");
        self.adm_ok(i, &["create-md", "--force", &peers, &self.vol(i)])
            .await;
    }

    async fn up(&self, i: usize) {
        self.adm_ok(i, &["up", self.res(i)]).await;
    }

    async fn down(&self, i: usize) {
        ok(&["drbdsetup", "down", self.res(i)]).await;
    }

    async fn new_uuid(&self, i: usize, flag: &str) -> agent::Output {
        self.adm(i, &["new-current-uuid", flag, &self.vol(i)]).await
    }

    /// create-md on every disk, every member up and connected, then the
    /// first data on member 0 without a sync.
    async fn start(&self) {
        let n = self.nodes.len();
        for i in 0..n {
            if self.nodes[i].disk.is_some() {
                self.create_md(i).await;
            }
        }
        for i in 0..n {
            self.up(i).await;
        }
        let names = self.names();
        wait_for(&names, 30, "all connected", |v| {
            v.iter().all(|s| connected(s.as_ref(), n - 1))
        })
        .await;
        let out = self.new_uuid(0, "--clear-bitmap").await;
        assert_eq!(out.exit_code, Some(0), "{}", out.stderr);
        wait_for(&names, 30, "every disk UpToDate", |v| {
            v.iter()
                .zip(&self.nodes)
                .all(|(s, nd)| nd.disk.is_none() || disk_state(s.as_ref()) == "UpToDate")
        })
        .await;
    }
}

fn nested_config() -> String {
    let d = dev(NESTED_PV);
    format!(
        "devices {{ use_devicesfile = 0 scan_lvs = 1 global_filter = [ \"a|.*|\" ] filter = [ \"a|^{d}$|\", \"r|.*|\" ] }} {NO_BACKUP}"
    )
}

/// Every rw-m* resource down, the throwaway VG gone, every rw-m* LV
/// removed; at the start and at the end of each test, also on panic.
struct Scrub;

impl Scrub {
    fn new() -> Self {
        scrub();
        std::fs::create_dir_all(files_dir()).unwrap();
        Self
    }
}

impl Drop for Scrub {
    fn drop(&mut self) {
        scrub();
    }
}

fn scrub() {
    for t in CUT_TABLES {
        Cut::remove(t);
    }
    let _ = std::fs::write(stop_file(), "");
    std::thread::sleep(Duration::from_secs(1));
    let _ = std::fs::remove_file(stop_file());
    let drbdsetup = |args: &[&str]| std::process::Command::new("drbdsetup").args(args).output();
    if let Ok(out) = drbdsetup(&["status", "--json"]) {
        let v: Value = serde_json::from_slice(&out.stdout).unwrap_or_default();
        for r in v.as_array().into_iter().flatten() {
            let Some(name) = r["name"].as_str().filter(|n| n.starts_with(PREFIX)) else {
                continue;
            };
            for d in r["devices"].as_array().into_iter().flatten() {
                if let Some(minor) = d["minor"].as_u64() {
                    let _ = drbdsetup(&["resume-io", &minor.to_string()]);
                }
            }
            let _ = drbdsetup(&["down", name]);
        }
    }
    let cfg = nested_config();
    let _ = host_command("vgremove")
        .args(["--config", &cfg, "-y", "-f", NESTED_VG])
        .output();
    let _ = host_command("pvremove")
        .args(["--config", &cfg, "-y", &dev(NESTED_PV)])
        .output();
    for _ in 0..3 {
        let names: Vec<String> = host_command("lvs")
            .args(["--noheadings", "-o", "lv_name", VG])
            .output()
            .map(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .lines()
                    .map(|l| l.trim().to_owned())
                    .filter(|n| n.starts_with(PREFIX))
                    .collect()
            })
            .unwrap_or_default();
        if names.is_empty() {
            break;
        }
        for n in names {
            let _ = host_command("lvremove")
                .args(["-f", &format!("{VG}/{n}")])
                .output();
        }
    }
    let _ = std::fs::remove_dir_all(files_dir());
}

/// create-md for every peer slot on `disk`, an attach with no peer and
/// the first data, down: the device size the kernel showed, the dump of
/// what DRBD wrote, the dump's size in bytes and its run time.
async fn attach_alone(env: Env, disk: &str) -> (u64, Md, usize, Duration) {
    let rig = Rig::new(env, vec![node("rw-ma", MINOR0, Some(disk.to_owned()))]);
    rig.start().await;
    let size = sectors(MINOR0) * 512;
    rig.down(0).await;
    let (out, md) = dump(disk).await;
    assert_eq!(out.exit_code, Some(0), "{}", out.stderr);
    assert_eq!(agent::drbd::la_size(&out.stdout), Ok(md.la));
    (size, md, out.stdout.len(), out.duration)
}

/// la_size after create-md and an attach, against the agent's
/// usable-size formula, over image sizes and VG extents.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn measure_la_size_sweep() {
    if !enabled() {
        return;
    }
    let _scrub = Scrub::new();
    let env = Env::probe().await;
    let l = lvm();
    let sizes: [(&str, u64); 13] = [
        ("100MiB+12345", 100 * MIB + 12_345),
        ("1GiB", GIB),
        ("1GiB+1", GIB + 1),
        ("1GiB+4KiB", GIB + 4 * KIB),
        ("2GiB", 2 * GIB),
        ("3GiB+7MiB", 3 * GIB + 7 * MIB),
        ("3.5GiB", 7 * GIB / 2),
        ("5GiB-4KiB", 5 * GIB - 4 * KIB),
        ("10GiB", 10 * GIB),
        ("17GiB+4KiB", 17 * GIB + 4 * KIB),
        ("40GiB", 40 * GIB),
        ("100GiB", 100 * GIB),
        ("1TiB", 1024 * GIB),
    ];
    // Attached alone, no first data: what the superblock records.
    let l0 = gross(GIB, EXTENT);
    l.create(VG, THIN, "rw-m-sz", l0, &[]).await.unwrap();
    let rig = Rig::new(env, vec![node("rw-ma", MINOR0, Some(dev("rw-m-sz")))]);
    rig.create_md(0).await;
    rig.up(0).await;
    let shown = sectors(MINOR0) * 512;
    rig.down(0).await;
    let (out, md) = dump(&dev("rw-m-sz")).await;
    println!(
        "attached alone, Inconsistent, then down: device {shown}, dump la {} (exit {:?}, parser {:?})",
        md.la,
        out.exit_code,
        agent::drbd::la_size(&out.stdout)
    );
    l.remove(VG, "rw-m-sz").await.unwrap();

    let mut seen: BTreeMap<u64, (u64, Md, usize, Duration)> = BTreeMap::new();
    let mut rows: Vec<(&str, u64, u64, u64)> = Vec::new();
    // An LV whose size is a multiple of the test VG's extent is the same
    // device in a VG of that larger extent.
    for extent in [EXTENT, 32 * MIB, 128 * MIB] {
        for &(name, s) in &sizes {
            let l0 = gross(s, extent);
            if let Entry::Vacant(e) = seen.entry(l0) {
                l.create(VG, THIN, "rw-m-sz", l0, &[]).await.unwrap();
                e.insert(attach_alone(env, &dev("rw-m-sz")).await);
                l.remove(VG, "rw-m-sz").await.unwrap();
            }
            rows.push((name, s, extent, l0));
        }
    }
    // Extents below the test VG's: a throwaway VG on a thin LV of it.
    l.create(VG, THIN, NESTED_PV, 48 * GIB, &[]).await.unwrap();
    let cfg = nested_config();
    ok(&["pvcreate", "--config", &cfg, &dev(NESTED_PV)]).await;
    let nested_lv = format!("/dev/mapper/{}-rw--m--sz", NESTED_VG.replace('-', "--"));
    for (extent, flag) in [(MIB, "1m"), (4 * KIB, "4k")] {
        let out = sh(&[
            "vgcreate",
            "--config",
            &cfg,
            "-s",
            flag,
            NESTED_VG,
            &dev(NESTED_PV),
        ])
        .await;
        if out.exit_code != Some(0) {
            println!("vgcreate -s {flag} refused: {}", out.stderr.trim());
            continue;
        }
        for &(name, s) in sizes.iter().filter(|(_, s)| *s <= 40 * GIB) {
            let l0 = gross(s, extent);
            if let Entry::Vacant(e) = seen.entry(l0) {
                let size = format!("{l0}b");
                ok(&[
                    "lvcreate", "--config", &cfg, "-y", "-Wn", "-Zn", "-L", &size, "-n", "rw-m-sz",
                    NESTED_VG,
                ])
                .await;
                let real: u64 = ok(&["blockdev", "--getsize64", &nested_lv])
                    .await
                    .trim()
                    .parse()
                    .unwrap();
                assert_eq!(real, l0, "the throwaway VG's LV size");
                e.insert(attach_alone(env, &nested_lv).await);
                let target = format!("{NESTED_VG}/rw-m-sz");
                ok(&["lvremove", "--config", &cfg, "-y", &target]).await;
            }
            rows.push((name, s, extent, l0));
        }
        ok(&["vgremove", "--config", &cfg, "-y", NESTED_VG]).await;
    }
    ok(&["pvremove", "--config", &cfg, "-y", &dev(NESTED_PV)]).await;
    l.remove(VG, NESTED_PV).await.unwrap();

    println!(
        "image, extent: L0 | la (dump) | device (sysfs) | formula-la | md | L0-la-md | C2 C3 | dump bytes, time"
    );
    let mut differ = 0;
    let mut fails = 0;
    for &(name, s, extent, l0) in &rows {
        let (size, md, bytes, took) = seen[&l0];
        let f = formula(l0);
        let (c2, c3) = (s <= md.la, md.la + md.meta_bytes <= l0);
        differ += usize::from(f != md.la);
        fails += usize::from(!(c2 && c3));
        println!(
            "{name}, {} KiB: {l0} | {} | {size} | {} | {} | {} | {c2} {c3} | {bytes}, {}",
            extent / KIB,
            md.la,
            signed(f, md.la),
            md.meta_bytes,
            signed(l0, md.la + md.meta_bytes),
            secs(took)
        );
        assert_eq!(size, md.la, "the dump's la_size is the device size");
        assert_eq!(md.max_peers, u64::from(SLOTS));
    }
    println!(
        "MetadataSizeDiffers would fire on {differ} of {} (image, extent) pairs ({} LV sizes); C2 or C3 fails on {fails}",
        rows.len(),
        seen.len()
    );

    // Two replicas one extent apart, as holders whose VGs round otherwise:
    // the device takes the smaller usable size, and each superblock says so.
    let s = GIB + 4 * KIB;
    let la0 = gross(s, EXTENT);
    let pair = [("rw-ma", la0), ("rw-mb", la0 + EXTENT)];
    for (lv, bytes) in pair {
        l.create(VG, THIN, lv, bytes, &[]).await.unwrap();
    }
    let rig = Rig::new(
        env,
        vec![
            node("rw-ma", MINOR0, Some(dev("rw-ma"))),
            node("rw-mb", MINOR0 + 1, Some(dev("rw-mb"))),
        ],
    );
    rig.start().await;
    let shown = [sectors(MINOR0) * 512, sectors(MINOR0 + 1) * 512];
    rig.down(0).await;
    rig.down(1).await;
    for (i, (lv, bytes)) in pair.into_iter().enumerate() {
        let (_, md) = dump(&dev(lv)).await;
        println!(
            "two holders one extent apart, {lv}: L0 {bytes} formula {} device {} la {} md {} fires {}",
            formula(bytes),
            shown[i],
            md.la,
            md.meta_bytes,
            md.la != formula(bytes)
        );
    }
}

/// A writer on a Primary device, O_DIRECT and buffered, until the stop
/// file appears; returns its loop count.
fn writer(minor: u32) -> tokio::task::JoinHandle<String> {
    let _ = std::fs::remove_file(stop_file());
    let stop = stop_file();
    let script = format!(
        "i=0; while [ ! -e {stop} ]; do i=$((i+1)); \
         dd if=/dev/urandom of=/dev/drbd{minor} bs=128K count=1 seek=$((i % 64)) oflag=direct conv=notrunc status=none; \
         dd if=/dev/urandom of=/dev/drbd{minor} bs=1M count=8 seek=$((128 + i % 32 * 8)) conv=notrunc status=none; done; echo $i"
    );
    tokio::spawn(async move { shell(&script, 600).await })
}

async fn stop(w: tokio::task::JoinHandle<String>) -> String {
    std::fs::write(stop_file(), "").unwrap();
    let loops = w.await.unwrap();
    let _ = std::fs::remove_file(stop_file());
    loops
}

/// dump-md --force on thin snapshots of a live, Primary, written-to
/// device: the LV's content and the pool's allocation before and after.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn measure_dump_md_writes_nothing() {
    if !enabled() {
        return;
    }
    let _scrub = Scrub::new();
    let env = Env::probe().await;
    let l = lvm();
    let l0 = gross(GIB, EXTENT);
    for lv in ["rw-ma", "rw-mb"] {
        l.create(VG, THIN, lv, l0, &[]).await.unwrap();
    }
    let rig = Rig::new(
        env,
        vec![
            node("rw-ma", MINOR0, Some(dev("rw-ma"))),
            node("rw-mb", MINOR0 + 1, Some(dev("rw-mb"))),
        ],
    );
    rig.start().await;
    ok(&["drbdsetup", "primary", "rw-ma"]).await;
    let w = writer(MINOR0);
    tokio::time::sleep(Duration::from_secs(3)).await;
    // No suspend: a crash-consistent copy taken mid-write, writable.
    l.restore(VG, "rw-ma", "rw-ma-live", &[]).await.unwrap();
    // A holder copy as a round takes it.
    for m in [MINOR0, MINOR0 + 1] {
        let out = run_long(&argv::suspend_io(m), 60).await;
        assert_eq!(out.exit_code, Some(0), "{}", out.stderr);
    }
    for lv in ["rw-ma", "rw-mb"] {
        l.snapshot(VG, lv, &format!("{lv}+img"), &[]).await.unwrap();
    }
    for m in [MINOR0, MINOR0 + 1] {
        ok(&["drbdsetup", "resume-io", &m.to_string()]).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let loops = stop(w).await;
    let al = status("rw-ma").await;
    ok(&["drbdsetup", "secondary", "rw-ma"]).await;
    println!("writer loops {loops}; source a: {}", brief(al.as_ref()));
    l.restore(VG, "rw-ma+img", "rw-mx", &[]).await.unwrap();
    let img = format!("{VG}/rw-ma+img");
    ok(&["lvchange", "-ay", "-K", &img]).await;
    let img_before = sha(&dev("rw-ma+img"), 0, l0).await;
    for (what, lv) in [
        ("writable snapshot of the live LV", "rw-ma-live"),
        ("replica cut from a holder copy", "rw-mx"),
    ] {
        let d = dev(lv);
        let before = (pool_used().await, thin_map(lv).await, sha(&d, 0, l0).await);
        let (out1, md) = dump(&d).await;
        let (out2, _) = dump(&d).await;
        let after = (pool_used().await, thin_map(lv).await, sha(&d, 0, l0).await);
        println!(
            "{what} {lv}: dump-md --force exit {:?} {:?}, unclean marker {}, la {} md {}; stderr {:?}",
            out1.exit_code,
            out2.exit_code,
            md.unclean,
            md.la,
            md.meta_bytes,
            first_line(&out1.stderr)
        );
        println!(
            "  before (pool chunks, mapped, sha) {before:?}\n  after            {after:?}\n  unchanged: content {} pool {} mapped {}",
            before.2 == after.2,
            before.0 == after.0,
            before.1 == after.1
        );
        let m = argv::dump_md(&d);
        let plain: Vec<&str> = m
            .iter()
            .map(String::as_str)
            .filter(|a| *a != "--force")
            .collect();
        let out = sh(&plain).await;
        let again = (pool_used().await, sha(&d, 0, l0).await);
        println!(
            "  without --force: exit {:?} stderr {:?} stdout bytes {}; then (pool chunks, sha) {again:?}",
            out.exit_code,
            first_line(&out.stderr),
            out.stdout.len()
        );
    }
    let img_after = sha(&dev("rw-ma+img"), 0, l0).await;
    println!(
        "holder copy {img_before} -> {img_after}: unchanged {}",
        img_before == img_after
    );
    ok(&["lvchange", "-an", &img]).await;
}

/// new-current-uuid with --clear-bitmap and with --force-resync on a
/// two-node resource with its peer away, under the agent's quorum options;
/// then what the peer does when it comes back.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn measure_new_current_uuid_without_quorum() {
    if !enabled() {
        return;
    }
    let _scrub = Scrub::new();
    let env = Env::probe().await;
    let l = lvm();
    let l0 = gross(64 * MIB, EXTENT);
    for lv in ["rw-ma", "rw-mb"] {
        l.create(VG, THIN, lv, l0, &[]).await.unwrap();
    }
    let rig = Rig::new(
        env,
        vec![
            node("rw-ma", MINOR0, Some(dev("rw-ma"))),
            node("rw-mb", MINOR0 + 1, Some(dev("rw-mb"))),
        ],
    );
    println!(
        "--- rw-ma.res\n{}",
        std::fs::read_to_string(rig.file(0)).unwrap()
    );
    let both = ["rw-ma", "rw-mb"];
    for flag in ["--clear-bitmap", "--force-resync"] {
        for lost in [Lost::Never, Lost::Down, Lost::Cut] {
            rig.create_md(0).await;
            rig.create_md(1).await;
            rig.up(0).await;
            let mut cut = None;
            if lost != Lost::Never {
                rig.up(1).await;
                wait_for(&both, 30, "connected", |v| {
                    v.iter().all(|s| connected(s.as_ref(), 1))
                })
                .await;
                cut = lose(&rig, &[1], lost).await;
                wait_for(&["rw-ma"], 60, "peer gone", |v| {
                    conn_state(v[0].as_ref(), "m1") != "Connected"
                })
                .await;
            }
            wait_for(&["rw-ma"], 30, "attached", |v| {
                disk_state(v[0].as_ref()) == "Inconsistent"
            })
            .await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            let before = status("rw-ma").await;
            let out = rig.new_uuid(0, flag).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let after = status("rw-ma").await;
            println!(
                "peer {lost:?}, {flag}: exit {:?} stderr {:?}\n  before: {}\n  after:  {}",
                out.exit_code,
                out.stderr.trim(),
                brief(before.as_ref()),
                brief(after.as_ref())
            );
            // The peer comes back.
            let mark = kernel_mark().await;
            match cut.take() {
                Some(c) => drop(c),
                None => rig.up(1).await,
            }
            let start = Instant::now();
            let (settled, v) = poll(&both, 60, |v| {
                v.iter()
                    .all(|s| connected(s.as_ref(), 1) && !busy(s.as_ref()))
            })
            .await;
            println!(
                "  peer back, settled {settled} after {}: a {} | b {} | b (received, sent, oos) KiB {:?}\n{}",
                secs(start.elapsed()),
                brief(v[0].as_ref()),
                brief(v[1].as_ref()),
                traffic(v[1].as_ref()),
                kernel_since(&mark, "rw-m[ab]").await
            );
            rig.down(0).await;
            rig.down(1).await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lost {
    /// Never up.
    Never,
    /// drbdsetup down: its peers hear that it leaves.
    Down,
    /// Its packets dropped, as a crash or a cut link: its peers hear nothing.
    Cut,
}

const CUT_TABLES: [&str; 2] = ["rw-m-cut", "rw-m-cut2"];

/// Drops every packet to and from the members' addresses on the rig's
/// ports, in a table of its own, loopback addresses only; deleted on drop.
struct Cut {
    table: &'static str,
}

impl Cut {
    fn members(members: &[usize]) -> Self {
        Self::named(CUT_TABLES[0], members)
    }

    fn named(table: &'static str, members: &[usize]) -> Self {
        use std::io::Write;
        Self::remove(table);
        let ports = format!("{}-{}", PORT0, PORT0 + 8);
        let mut rules = format!(
            "table inet {table} {{\n chain input {{\n  type filter hook input priority -300; policy accept;\n"
        );
        for &i in members {
            let ip = Rig::ip(i);
            for (addr, port) in [
                ("saddr", "dport"),
                ("saddr", "sport"),
                ("daddr", "dport"),
                ("daddr", "sport"),
            ] {
                let _ = writeln!(rules, "  ip {addr} {ip} tcp {port} {ports} drop");
            }
        }
        rules.push_str(" }\n}\n");
        let mut child = host_command("nft")
            .args(["-f", "-"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .expect("nft");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(rules.as_bytes())
            .unwrap();
        assert!(child.wait().unwrap().success(), "nft -f");
        Self { table }
    }

    fn remove(table: &str) {
        let _ = host_command("nft")
            .args(["delete", "table", "inet", table])
            .stderr(std::process::Stdio::null())
            .status();
    }
}

impl Drop for Cut {
    fn drop(&mut self) {
        Self::remove(self.table);
    }
}

/// Takes `members` away as `how` says; a cut lasts as long as the value.
async fn lose(rig: &Rig, members: &[usize], how: Lost) -> Option<Cut> {
    match how {
        Lost::Never => None,
        Lost::Down => {
            for &i in members {
                rig.down(i).await;
            }
            None
        }
        Lost::Cut => Some(Cut::members(members)),
    }
}

/// The timestamp of the last kernel log line: the ring buffer is full, so
/// a line count does not move.
async fn kernel_mark() -> String {
    shell(
        "dmesg | tail -n 1 | sed 's/^\\[ *\\([0-9.]*\\)\\].*/\\1/'",
        30,
    )
    .await
}

/// Kernel log lines after `mark` that match `pattern` and tell how a
/// handshake ended.
async fn kernel_since(mark: &str, pattern: &str) -> String {
    shell(
        &format!(
            "dmesg | awk -v m={mark} '{{ t = substr($0, 2, index($0, \"]\") - 2) + 0; if (t > m) print }}' | grep -E '{pattern}' | grep -E 'uuid_compare|full sync|Began resync|Resync done|Split-Brain|Unrelated|bits:' || true"
        ),
        30,
    )
    .await
}

fn locks() -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir("/var/run/drbd/lock")
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let len = e.metadata().map_or(0, |m| m.len());
            format!("{name} ({len} B)")
        })
        .filter(|n| n.contains("-9998") || n.contains("-10000"))
        .collect();
    v.sort();
    v
}

/// drbdmeta with a minor: one no resource uses, one an attached resource
/// uses, one a diskless resource uses; then concurrent runs on one minor.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn measure_drbdmeta_minor() {
    if !enabled() {
        return;
    }
    let _scrub = Scrub::new();
    let env = Env::probe().await;
    let l = lvm();
    println!("lock files before: {:?}", locks());
    // X: metadata written by an attach, as on an image copy.
    let lx = gross(GIB, EXTENT);
    l.create(VG, THIN, "rw-mx", lx, &[]).await.unwrap();
    let (_, mdx, _, _) = attach_alone(env, &dev("rw-mx")).await;
    // A attached on MINOR0 with another size; C diskless on MINOR0 + 2.
    let la_lv = gross(256 * MIB, EXTENT);
    l.create(VG, THIN, "rw-ma", la_lv, &[]).await.unwrap();
    let a = Rig::new(env, vec![node("rw-ma", MINOR0, Some(dev("rw-ma")))]);
    a.create_md(0).await;
    a.up(0).await;
    let la_a = sectors(MINOR0) * 512;
    let c = Rig::new(env, vec![node("rw-mc", MINOR0 + 2, None)]);
    c.up(0).await;
    println!(
        "X la {} (L0 {lx}); A attached on {MINOR0}, device {la_a}; C diskless on {}: {}",
        mdx.la,
        MINOR0 + 2,
        brief(status("rw-mc").await.as_ref())
    );
    let x = dev("rw-mx");
    for (what, minor) in [
        ("no resource", agent::drbd::UNUSED_MINOR),
        ("attached resource", MINOR0),
        ("diskless resource", MINOR0 + 2),
    ] {
        for force in [true, false] {
            let m = minor.to_string();
            let mut cmd = vec!["drbdmeta"];
            if force {
                cmd.push("--force");
            }
            cmd.extend([m.as_str(), "v09", &x, "internal", "dump-md"]);
            let out = sh(&cmd).await;
            let md = parse_md(&out.stdout);
            println!(
                "minor {minor} ({what}), force {force}: exit {:?}, la {} (X {}), stdout bytes {}, stderr {:?}",
                out.exit_code,
                md.la,
                mdx.la,
                out.stdout.len(),
                out.stderr.trim()
            );
        }
    }
    let state = (status("rw-ma").await, status("rw-mc").await);
    println!(
        "after: A {} | C {}",
        brief(state.0.as_ref()),
        brief(state.1.as_ref())
    );
    let sb = sh(&[
        "drbdmeta",
        "--force",
        &agent::drbd::UNUSED_MINOR.to_string(),
        "v09",
        &x,
        "internal",
        "dump-superblock",
        "--output-format=json",
    ])
    .await;
    let head: String = sb.stdout.lines().take(30).collect::<Vec<_>>().join("\n");
    println!(
        "dump-superblock json of X (exit {:?}, stderr {:?}), head:\n{head}",
        sb.exit_code,
        sb.stderr.trim()
    );
    println!("lock files after: {:?}", locks());
    a.down(0).await;
    c.down(0).await;

    // The per-minor lock file held by this process, by each lock kind.
    let lock = format!("/var/run/drbd/lock/drbd-147-{}", agent::drbd::UNUSED_MINOR);
    for kind in ["flock", "fcntl"] {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock)
            .unwrap();
        let op = rustix::fs::FlockOperation::NonBlockingLockExclusive;
        let taken = if kind == "flock" {
            rustix::fs::flock(&f, op)
        } else {
            rustix::fs::fcntl_lock(&f, op)
        };
        let mode = ExecMode::Timeout {
            wait: Duration::from_secs(10),
            term: Duration::from_secs(2),
            kill: Duration::from_secs(2),
        };
        let t = Instant::now();
        let what = match super::exec().run(&argv::dump_md(&x), mode).await {
            Ok(o) => format!(
                "exit {:?}, la {}, stderr {:?}",
                o.exit_code,
                parse_md(&o.stdout).la,
                o.stderr.trim()
            ),
            Err(e) => e.to_string(),
        };
        println!(
            "lock file held by {kind} ({taken:?}): drbdmeta after {}: {what}",
            secs(t.elapsed())
        );
    }

    // Concurrent dumps of two 40 GiB LVs: bitmaps of 7.5 MiB make each
    // run long enough to overlap.
    let big = gross(40 * GIB, EXTENT);
    let mut devs = Vec::new();
    for (i, lv) in ["rw-my", "rw-mz"].into_iter().enumerate() {
        l.create(VG, THIN, lv, big, &[]).await.unwrap();
        let rig = Rig::new(env, vec![node(lv, MINOR0 + 3, Some(dev(lv)))]);
        rig.create_md(0).await;
        if i == 0 {
            rig.up(0).await;
            rig.down(0).await;
        }
        devs.push(dev(lv));
    }
    let (out, md) = dump(&devs[0]).await;
    println!(
        "dump-md of a 40 GiB LV: exit {:?}, {} bytes of output in {}, la {}",
        out.exit_code,
        out.stdout.len(),
        secs(out.duration),
        md.la
    );
    let same = agent::drbd::UNUSED_MINOR;
    for minors in [[same; 4], [same, same + 1, same + 2, same + 3]] {
        let mut failed = 0;
        let mut first = None;
        let rounds = 10;
        let start = Instant::now();
        for _ in 0..rounds {
            let runs =
                minors.iter().enumerate().map(|(k, m)| {
                    let d = devs[k % 2].clone();
                    let m = m.to_string();
                    async move {
                        sh(&["drbdmeta", "--force", &m, "v09", &d, "internal", "dump-md"]).await
                    }
                });
            for out in futures::future::join_all(runs).await {
                if out.exit_code != Some(0) || !out.stdout.contains("la-size-sect") {
                    failed += 1;
                    first.get_or_insert_with(|| {
                        format!("exit {:?}: {}", out.exit_code, out.stderr.trim())
                    });
                }
            }
        }
        println!(
            "4 concurrent dump-md on minors {minors:?}, {rounds} rounds in {}: {failed} of {} failed; first failure {first:?}",
            secs(start.elapsed()),
            rounds * 4
        );
    }
    println!("lock files at the end: {:?}", locks());
}

/// The quorum each member reports, through DRBD and through the agent's
/// events2 tracker, with two diskful replicas and a diskless tiebreaker,
/// before and after the first data, under each partition.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn measure_quorum_before_first_data() {
    if !enabled() {
        return;
    }
    let _scrub = Scrub::new();
    let env = Env::probe().await;
    let l = lvm();
    let l0 = gross(64 * MIB, EXTENT);
    for lv in ["rw-ma", "rw-mb"] {
        l.create(VG, THIN, lv, l0, &[]).await.unwrap();
    }
    let rig = Rig::new(
        env,
        vec![
            node("rw-ma", MINOR0, Some(dev("rw-ma"))),
            node("rw-mb", MINOR0 + 1, Some(dev("rw-mb"))),
            node("rw-mc", MINOR0 + 2, None),
        ],
    );
    println!(
        "--- rw-mc.res\n{}",
        std::fs::read_to_string(rig.file(2)).unwrap()
    );
    let all = ["rw-ma", "rw-mb", "rw-mc"];
    let mesh = |v: &[Option<Value>]| v.iter().all(|s| connected(s.as_ref(), 2));
    rig.create_md(0).await;
    rig.create_md(1).await;
    // b never up: a node down since before the volume existed.
    rig.up(0).await;
    rig.up(2).await;
    wait_for(&["rw-ma", "rw-mc"], 30, "a and c connected", |v| {
        v.iter().all(|s| {
            conn_state(s.as_ref(), "m0") == "Connected"
                || conn_state(s.as_ref(), "m2") == "Connected"
        })
    })
    .await;
    report("every disk Inconsistent, b never up", &all).await;
    // The start the agent would run after its wait, were quorum no gate.
    let out = rig.new_uuid(0, "--force-resync").await;
    println!(
        "b never up, new-current-uuid --force-resync on a: exit {:?} stderr {:?}",
        out.exit_code,
        out.stderr.trim()
    );
    report("b never up, a started as the source", &all).await;
    let mark = kernel_mark().await;
    rig.up(1).await;
    let start = Instant::now();
    let (settled, _) = poll(&all, 60, |v| {
        v.iter()
            .all(|s| connected(s.as_ref(), 2) && !busy(s.as_ref()))
    })
    .await;
    println!(
        "b up at last: settled {settled} after {}\n{}",
        secs(start.elapsed()),
        kernel_since(&mark, "rw-mb").await
    );
    report("b up at last", &all).await;
    // A fresh volume again for the partitions below.
    for i in 0..3 {
        rig.down(i).await;
    }
    rig.create_md(0).await;
    rig.create_md(1).await;
    for i in 0..3 {
        rig.up(i).await;
    }
    wait_for(&all, 30, "full mesh", mesh).await;
    report("all connected, every disk Inconsistent", &all).await;
    for first_data in [false, true] {
        if first_data {
            let out = rig.new_uuid(0, "--clear-bitmap").await;
            assert_eq!(out.exit_code, Some(0), "{}", out.stderr);
            wait_for(&all, 30, "UpToDate", |v| {
                disk_state(v[0].as_ref()) == "UpToDate" && disk_state(v[1].as_ref()) == "UpToDate"
            })
            .await;
            report("after the first data, all connected", &all).await;
        }
        let disks = if first_data {
            "UpToDate"
        } else {
            "Inconsistent"
        };
        let settled = |v: &[Option<Value>]| mesh(v) && v.iter().all(|s| !busy(s.as_ref()));
        for how in [Lost::Down, Lost::Cut] {
            let tag = format!("{disks}, {how:?}");
            let mut cuts = Vec::new();
            cuts.extend(lose(&rig, &[1], how).await);
            wait_for(&["rw-ma", "rw-mc"], 60, "b gone", |v| {
                v.iter()
                    .all(|s| conn_state(s.as_ref(), "m1") != "Connected")
            })
            .await;
            report(&format!("{tag}: b lost"), &all).await;
            if cuts.drain(..).count() == 0 {
                rig.up(1).await;
            }
            wait_for(&all, 90, "full mesh again", settled).await;
            cuts.extend(lose(&rig, &[2], how).await);
            wait_for(&["rw-ma", "rw-mb"], 60, "c gone", |v| {
                v.iter()
                    .all(|s| conn_state(s.as_ref(), "m2") != "Connected")
            })
            .await;
            report(&format!("{tag}: tiebreaker lost"), &all).await;
            if how == Lost::Cut {
                cuts.push(Cut::named("rw-m-cut2", &[1]));
            } else {
                rig.down(1).await;
            }
            wait_for(&["rw-ma"], 60, "a alone", |v| {
                conn_state(v[0].as_ref(), "m1") != "Connected"
            })
            .await;
            report(&format!("{tag}: b and the tiebreaker lost"), &all).await;
            if cuts.drain(..).count() == 0 {
                rig.up(1).await;
                rig.up(2).await;
            }
            wait_for(&all, 90, "full mesh again", settled).await;
        }
    }
}

async fn report(state: &str, res: &[&str]) {
    tokio::time::sleep(Duration::from_secs(2)).await;
    for r in res {
        let v = status(r).await;
        println!(
            "{state}: {r} quorum {:?}, entry {:?} | {}",
            quorum(v.as_ref()),
            entry_quorum(r).await,
            brief(v.as_ref())
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum Zero {
    /// fallocate(ZERO_RANGE | KEEP_SIZE) on the block device: what safe
    /// Rust can issue.
    Fallocate,
    /// BLKZEROOUT, through blkdiscard --zeroout.
    Blkzeroout,
    /// A discard: the control.
    Discard,
}

async fn zero(how: Zero, dev: &str, off: u64, len: u64) {
    use rustix::fs::FallocateFlags;
    let (o, n) = (off.to_string(), len.to_string());
    match how {
        Zero::Fallocate => {
            let f = std::fs::OpenOptions::new().write(true).open(dev).unwrap();
            rustix::fs::fallocate(
                &f,
                FallocateFlags::ZERO_RANGE | FallocateFlags::KEEP_SIZE,
                off,
                len,
            )
            .unwrap();
        }
        Zero::Blkzeroout => {
            ok(&[
                "blkdiscard",
                "-f",
                "--zeroout",
                "--offset",
                &o,
                "--length",
                &n,
                dev,
            ])
            .await;
        }
        Zero::Discard => {
            ok(&["blkdiscard", "-f", "--offset", &o, "--length", &n, dev]).await;
        }
    }
}

fn fdatasync(dev: &str) -> Duration {
    let t = Instant::now();
    std::fs::OpenOptions::new()
        .write(true)
        .open(dev)
        .unwrap()
        .sync_data()
        .unwrap();
    t.elapsed()
}

/// Zeroing the dead tail [la_size, L0) of replica LVs cut from a holder
/// copy, after the lvextend: what reads back, what the pool allocates, and
/// whether the copy and its origin change.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn measure_zero_tail() {
    if !enabled() {
        return;
    }
    let _scrub = Scrub::new();
    let env = Env::probe().await;
    let l = lvm();
    let s = GIB;
    let l0 = gross(s, EXTENT);
    l.create(VG, THIN, "rw-ma", l0, &[]).await.unwrap();
    let rig = Rig::new(env, vec![node("rw-ma", MINOR0, Some(dev("rw-ma")))]);
    rig.create_md(0).await;
    rig.up(0).await;
    ok(&["drbdsetup", "primary", "--force", "rw-ma"]).await;
    let la = sectors(MINOR0) * 512;
    // Data up to the device end: the chunk that holds la_size is part
    // data, part metadata.
    shell(
        &format!(
            "dd if=/dev/urandom of=/dev/drbd{MINOR0} bs=4M count={la} iflag=count_bytes,fullblock oflag=direct status=none"
        ),
        600,
    )
    .await;
    ok(&["drbdsetup", "secondary", "rw-ma"]).await;
    rig.down(0).await;
    l.snapshot(VG, "rw-ma", "rw-ma+img", &[]).await.unwrap();
    let img_lv = format!("{VG}/rw-ma+img");
    ok(&["lvchange", "-ay", "-K", &img_lv]).await;
    let img = dev("rw-ma+img");
    let (_, md) = dump(&img).await;
    assert_eq!(md.la, la);
    let img_all = sha(&img, 0, l0).await;
    let src_all = sha(&dev("rw-ma"), 0, l0).await;
    let img_data = sha(&img, 0, la).await;
    let g = gross(4 * GIB, EXTENT);
    let chunk = env.chunk;
    let tail_chunks = (l0 - la / chunk * chunk).div_ceil(chunk);
    println!(
        "image {s}, L0 {l0}, la {la}, md {}, tail {} bytes; chunk {chunk}: {tail_chunks} chunks meet the tail, la at {} into its chunk; G {g}",
        md.meta_bytes,
        l0 - la,
        la % chunk
    );
    for (lv, how) in [
        ("rw-mx", Zero::Fallocate),
        ("rw-my", Zero::Blkzeroout),
        ("rw-mz", Zero::Discard),
    ] {
        l.restore(VG, "rw-ma+img", lv, &[]).await.unwrap();
        l.extend(VG, lv, g).await.unwrap();
        let d = dev(lv);
        let wz = write_zeroes_max(lv).await;
        let used0 = pool_used().await;
        let map0 = thin_map(lv).await;
        let nz0 = nonzero(&d, la, l0 - la).await;
        let t = Instant::now();
        zero(how, &d, la, l0 - la).await;
        let took = t.elapsed();
        let synced = fdatasync(&d);
        let used1 = pool_used().await;
        let map1 = thin_map(lv).await;
        let nz1 = nonzero(&d, la, l0 - la).await;
        let data = sha(&d, 0, la).await == img_data;
        let past = nonzero(&d, l0, g - l0).await;
        println!(
            "{how:?} on {lv}: write_zeroes_max_bytes {wz}; tail non-zero bytes {nz0} -> {nz1}; [0, la) equal to the copy {data}; [L0, G) non-zero {past}; pool chunks {} ({tail_chunks} meet the tail); mapped sectors, highest {map0:?} -> {map1:?}; zero {} fdatasync {}",
            signed(used1, used0),
            secs(took),
            secs(synced)
        );
    }
    let img_end = sha(&img, 0, l0).await;
    let src_end = sha(&dev("rw-ma"), 0, l0).await;
    println!(
        "holder copy {img_all} -> {img_end} unchanged {}; its origin {src_all} -> {src_end} unchanged {}",
        img_all == img_end,
        src_all == src_end
    );
    ok(&["lvchange", "-an", &img_lv]).await;
}

/// The restore at full size by hand: a 2 GiB image, snapshotted on two
/// holders, into a 40 GiB disk on two replicas, each cut from its own
/// holder copy; then Ready, resync bytes, verify and content.
#[tokio::test]
#[ignore = "needs root, DRBD 9 and the test VG"]
async fn measure_restore_into_larger_disk() {
    if !enabled() {
        return;
    }
    let _scrub = Scrub::new();
    let env = Env::probe().await;
    let l = lvm();
    let s = 2 * GIB;
    let size = 40 * GIB;
    let l_src = gross(s, EXTENT);
    for lv in ["rw-ma", "rw-mb"] {
        l.create(VG, THIN, lv, l_src, &[]).await.unwrap();
    }
    let src = Rig::new(
        env,
        vec![
            node("rw-ma", MINOR0, Some(dev("rw-ma"))),
            node("rw-mb", MINOR0 + 1, Some(dev("rw-mb"))),
        ],
    );
    src.start().await;
    let la = sectors(MINOR0) * 512;
    ok(&["drbdsetup", "primary", "rw-ma"]).await;
    let d = format!("/dev/drbd{MINOR0}");
    shell(
        &format!(
            "dd if=/dev/urandom of={d} bs=4M count=64 oflag=direct status=none && \
             dd if=/dev/urandom of={d} bs=4M count=16 seek=200 oflag=direct status=none && \
             dd if=/dev/urandom of={d} bs=4096 count=256 seek={} oflag=direct status=none",
            la / 4096 - 256
        ),
        300,
    )
    .await;
    ok(&["drbdsetup", "secondary", "rw-ma"]).await;
    for lv in ["rw-ma", "rw-mb"] {
        l.snapshot(VG, lv, &format!("{lv}+img"), &[]).await.unwrap();
    }
    src.down(0).await;
    src.down(1).await;
    let mut img_sha = Vec::new();
    for lv in ["rw-ma+img", "rw-mb+img"] {
        ok(&["lvchange", "-ay", "-K", &format!("{VG}/{lv}")]).await;
        img_sha.push(sha(&dev(lv), 0, la).await);
    }
    println!("image {s}: holder LVs {l_src}, la {la}; holder copies over [0, la): {img_sha:?}");

    let g = gross(size, EXTENT);
    let used_start = pool_used().await;
    let t_all = Instant::now();
    let replicas = [
        ("rw-mx", "rw-ma+img", Zero::Fallocate),
        ("rw-my", "rw-mb+img", Zero::Blkzeroout),
    ];
    let mut zeroed_from = Vec::new();
    for (lv, img, how) in replicas {
        let mut steps: Vec<(&str, Duration)> = Vec::new();
        let from = format!("r-drbd.from.uid-{lv}.uid-img");
        let seed = format!("r-drbd.seed.uid-{lv}.uid-img");
        let t = Instant::now();
        l.restore(VG, img, lv, &["r-drbd.new", &from, &seed])
            .await
            .unwrap();
        steps.push(("1 lvcreate", t.elapsed()));
        let l0 = lv_bytes(lv).await;
        let t = Instant::now();
        let (out, md) = dump(&dev(lv)).await;
        steps.push(("2 dump-md", t.elapsed()));
        let c1 = out.exit_code == Some(0) && lv_bytes(lv).await == l0;
        let checks = [c1, s <= md.la, md.la + md.meta_bytes <= l0, l0 <= g];
        assert!(checks.iter().all(|c| *c), "{lv}: C1 to C4 {checks:?}");
        let zeroing = format!("r-drbd.zeroing.uid-{lv}.{}", md.la);
        let t = Instant::now();
        l.change_tags(VG, lv, &[&zeroing], &[]).await.unwrap();
        steps.push(("3 tag zeroing", t.elapsed()));
        let t = Instant::now();
        l.extend(VG, lv, g).await.unwrap();
        steps.push(("4 lvextend", t.elapsed()));
        let t = Instant::now();
        zero(how, &dev(lv), md.la, l0 - md.la).await;
        steps.push(("5 zero", t.elapsed()));
        steps.push(("6 fdatasync", fdatasync(&dev(lv))));
        let zeroed = format!("r-drbd.zeroed.uid-{lv}.{}", md.la);
        let t = Instant::now();
        l.change_tags(VG, lv, &[&zeroed], &[&zeroing])
            .await
            .unwrap();
        steps.push(("7 tag zeroed", t.elapsed()));
        zeroed_from.push(md.la);
        println!(
            "{lv} from {img} ({how:?}): L0 {l0} la {} md {} G {g}; C1 to C4 {checks:?}; {}",
            md.la,
            md.meta_bytes,
            steps
                .iter()
                .map(|(n, d)| format!("{n} {}", secs(*d)))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    assert_eq!(zeroed_from[0], zeroed_from[1], "equal zeroedFrom");
    let rig = Rig::new(
        env,
        vec![
            node("rw-mx", MINOR0 + 2, Some(dev("rw-mx"))),
            node("rw-my", MINOR0 + 3, Some(dev("rw-my"))),
        ],
    );
    let both = ["rw-mx", "rw-my"];
    let t = Instant::now();
    rig.create_md(0).await;
    rig.create_md(1).await;
    let t_md = t.elapsed();
    let t = Instant::now();
    rig.up(0).await;
    rig.up(1).await;
    wait_for(&both, 60, "connected, Inconsistent", |v| {
        v.iter()
            .all(|s| connected(s.as_ref(), 1) && disk_state(s.as_ref()) == "Inconsistent")
    })
    .await;
    let t_up = t.elapsed();
    let t = Instant::now();
    let out = rig.new_uuid(0, "--clear-bitmap").await;
    assert_eq!(out.exit_code, Some(0), "{}", out.stderr);
    let v = wait_for(&both, 60, "UpToDate", |v| {
        v.iter()
            .all(|s| disk_state(s.as_ref()) == "UpToDate" && !busy(s.as_ref()))
    })
    .await;
    let t_ready = t.elapsed();
    let total = t_all.elapsed();
    let dev_sizes = [sectors(MINOR0 + 2) * 512, sectors(MINOR0 + 3) * 512];
    println!(
        "8 create-md x2 {}; up and connected {}; new-current-uuid to both UpToDate {}; from the first lvcreate {}",
        secs(t_md),
        secs(t_up),
        secs(t_ready),
        secs(total)
    );
    println!(
        "device sizes {dev_sizes:?} (asked {size}); (received, sent, oos) KiB x {:?} y {:?}\n  x {}\n  y {}",
        traffic(v[0].as_ref()),
        traffic(v[1].as_ref()),
        brief(v[0].as_ref()),
        brief(v[1].as_ref())
    );
    let used_ready = pool_used().await;

    // Online verify of the whole 40 GiB, rate limits raised for it.
    for (res, peer) in [("rw-mx", "1"), ("rw-my", "0")] {
        ok(&["drbdsetup", "net-options", res, peer, "--verify-alg=sha256"]).await;
        ok(&[
            "drbdsetup",
            "peer-device-options",
            res,
            peer,
            "0",
            "--c-max-rate=4194304",
            "--resync-rate=4194304",
        ])
        .await;
    }
    let t = Instant::now();
    ok(&["drbdsetup", "verify", "rw-mx", "1", "0"]).await;
    let v = wait_for(&both, 1500, "verify ended", |v| {
        t.elapsed() > Duration::from_secs(1) && v.iter().all(|s| !busy(s.as_ref()))
    })
    .await;
    let t_verify = t.elapsed();
    let log = shell(
        "dmesg | grep -E 'rw-mx.*(Online verify|verify found|out of sync)' | tail -n 3 || true",
        30,
    )
    .await;
    println!(
        "verify {}: oos KiB x {} y {}; kernel log:\n{log}",
        secs(t_verify),
        traffic(v[0].as_ref()).2,
        traffic(v[1].as_ref()).2
    );

    for (i, (lv, _, _)) in replicas.iter().enumerate() {
        let d = dev(lv);
        let data = sha(&d, 0, la).await;
        let t = Instant::now();
        let past = nonzero(&d, la, dev_sizes[i] - la).await;
        println!(
            "{lv}: [0, la) {data} equal to the image {}; [la, device end) non-zero bytes {past} (read in {}); mapped sectors, highest {:?}",
            data == img_sha[0],
            secs(t.elapsed()),
            thin_map(lv).await
        );
    }
    println!(
        "pool chunks of {} KiB: +{} at Ready for both replicas, +{} after verify",
        env.chunk / KIB,
        used_ready.saturating_sub(used_start),
        pool_used().await.saturating_sub(used_start)
    );
    rig.down(0).await;
    rig.down(1).await;
    for lv in ["rw-ma+img", "rw-mb+img"] {
        ok(&["lvchange", "-an", &format!("{VG}/{lv}")]).await;
    }
}
