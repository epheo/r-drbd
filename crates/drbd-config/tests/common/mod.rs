#![allow(dead_code, unreachable_pub)]

use drbd_config::{
    AddrFamily, Connection, DiskPath, Endpoint, Endpoints, FixedOptions, Header, HostEntry,
    ResourceFile, Tiebreaker, VolumeEntry,
};

pub fn header() -> Header {
    Header {
        generator: "r-drbd test".into(),
        node_name: "node-a".into(),
        hostname: "node-a".into(),
        local_time: "2026-10-15 10:00:00".into(),
        utc_time: "2026-10-15 08:00:00".into(),
    }
}

pub fn endpoint(host: &str, address: &str) -> Endpoint {
    Endpoint {
        host: host.into(),
        family: AddrFamily::Ipv4,
        address: address.into(),
        port: 7000,
    }
}

pub fn volume(disk: DiskPath, fixed: &FixedOptions) -> VolumeEntry {
    VolumeEntry {
        number: 0,
        disk,
        tiebreaker: Tiebreaker::Yes,
        disk_options: fixed.disk.clone(),
        minor: 1000,
    }
}

pub fn host(name: &str, node_id: u8, disk: DiskPath, fixed: &FixedOptions) -> HostEntry {
    HostEntry {
        host: name.into(),
        node_id,
        volumes: vec![volume(disk, fixed)],
    }
}

/// Reference resource: node-a local, node-b diskful peer, node-c tiebreaker.
pub fn sample(fixed: &FixedOptions) -> ResourceFile {
    let a = endpoint("node-a", "10.0.0.11");
    let connection = |peer: &str, address: &str| Connection {
        peer: peer.into(),
        net_options: fixed.net.clone(),
        endpoints: Endpoints {
            local: a.clone(),
            peer: endpoint(peer, address),
        },
    };
    ResourceFile {
        name: "pvc-0a1b".into(),
        header: header(),
        handlers: Vec::new(),
        options: fixed.resource.clone(),
        secret: "q2XlS0v3Yp9cJr7nAa1F".into(),
        net: fixed.net.clone(),
        disk: Vec::new(),
        local: host(
            "node-a",
            0,
            DiskPath::Backing("/dev/thinpool/pvc-0a1b_00000".into()),
            fixed,
        ),
        peers: vec![
            host("node-b", 1, DiskPath::PeerPlaceholder, fixed),
            host("node-c", 2, DiskPath::None, fixed),
        ],
        connections: vec![
            connection("node-b", "10.0.0.12"),
            connection("node-c", "10.0.0.13"),
        ],
    }
}

pub fn body(text: &str) -> &str {
    &text[text.find("resource \"").expect("marker present")..]
}
