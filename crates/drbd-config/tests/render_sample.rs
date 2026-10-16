mod common;

use drbd_config::{Connection, DiskPath, Endpoints, FixedOptions, ResourceFile, render};

#[test]
fn render_sample() {
    let expected = include_str!("fixtures/sample.res");
    let got = render(&common::sample(&FixedOptions::new(2, None, false)));
    assert_eq!(common::body(&got), common::body(expected));
}

#[test]
fn two_primaries_only_for_read_write_many() {
    let plain =
        common::body(&render(&common::sample(&FixedOptions::new(2, None, false)))).to_owned();
    assert!(!plain.contains("allow-two-primaries"));
    let rwx = render(&common::sample(&FixedOptions::new(2, None, true)));
    let rwx = common::body(&rwx);
    // Resource net and every connection's net, sorted before protocol.
    assert_eq!(rwx.matches("allow-two-primaries yes;\n").count(), 3);
    assert!(rwx.contains("\n        allow-two-primaries yes;\n        protocol C;"));
    assert!(rwx.contains("\n            allow-two-primaries yes;\n            protocol C;"));
    let without: Vec<&str> = rwx
        .lines()
        .filter(|l| l.trim() != "allow-two-primaries yes;")
        .collect();
    assert_eq!(without, plain.lines().collect::<Vec<_>>());
}

#[test]
fn header_is_one_comment_line_then_blank() {
    let mut file = common::sample(&FixedOptions::new(2, None, false));
    file.header.hostname = "evil\nresource \"x\"".into();
    let got = render(&file);
    let mut lines = got.lines();
    assert!(lines.next().unwrap().starts_with('#'));
    assert_eq!(lines.next(), Some(""));
    assert_eq!(lines.next(), Some("resource \"pvc-0a1b\""));
    assert_eq!(got.matches("resource \"").count(), 1);
}

#[test]
fn node_c_view() {
    let fixed = FixedOptions::new(2, None, false);
    let c = common::endpoint("node-c", "10.0.0.13");
    let connection = |peer: &str, address: &str| Connection {
        peer: peer.into(),
        net_options: fixed.net.clone(),
        endpoints: Endpoints {
            local: c.clone(),
            peer: common::endpoint(peer, address),
        },
    };
    let file = ResourceFile {
        local: common::host("node-c", 2, DiskPath::None, &fixed),
        peers: vec![
            common::host("node-a", 0, DiskPath::PeerPlaceholder, &fixed),
            common::host("node-b", 1, DiskPath::PeerPlaceholder, &fixed),
        ],
        connections: vec![
            connection("node-a", "10.0.0.11"),
            connection("node-b", "10.0.0.12"),
        ],
        ..common::sample(&fixed)
    };
    let expected = r#"resource "pvc-0a1b"
{

    options
    {
        on-no-data-accessible suspend-io;
        on-no-quorum suspend-io;
        on-suspended-primary-outdated force-secondary;
        quorum majority;
    }

    net
    {
        cram-hmac-alg     sha1;
        shared-secret     "q2XlS0v3Yp9cJr7nAa1F";
        protocol C;
        rr-conflict retry-connect;
    }

    on "node-c"
    {
        volume 0
        {
            disk        none;
            tiebreaker yes;
            meta-disk   internal;
            device      minor 1000;
        }
        node-id    2;
    }

    on "node-a"
    {
        volume 0
        {
            # placeholder path: the peer's real backing device is never opened here
            disk        /dev/drbd/this/is/not/used;
            tiebreaker yes;
            meta-disk   internal;
            device      minor 1000;
        }
        node-id    0;
    }

    on "node-b"
    {
        volume 0
        {
            # placeholder path: the peer's real backing device is never opened here
            disk        /dev/drbd/this/is/not/used;
            tiebreaker yes;
            meta-disk   internal;
            device      minor 1000;
        }
        node-id    1;
    }

    connection
    {

        net
        {
            protocol C;
            rr-conflict retry-connect;
        }
        host "node-c" address ipv4 10.0.0.13:7000;
        host "node-a" address ipv4 10.0.0.11:7000;
    }

    connection
    {

        net
        {
            protocol C;
            rr-conflict retry-connect;
        }
        host "node-c" address ipv4 10.0.0.13:7000;
        host "node-b" address ipv4 10.0.0.12:7000;
    }
}
"#;
    assert_eq!(common::body(&render(&file)), expected);
}
