use drbd_config::{
    Disk, Features, Host, PEER_DISK, RenderError, Resource, discard_granularity, render,
    same_content,
};

const ALL: Features = Features {
    tiebreaker: true,
    discard_granularity: true,
    suspended_primary_outdated: true,
};

fn host(name: &str, id: u8, addr: &str, disk: Disk<'static>) -> Host<'static> {
    Host {
        name: Box::leak(name.to_owned().into_boxed_str()),
        node_id: id,
        address: addr.parse().unwrap(),
        disk,
        tiebreaker: true,
    }
}

fn resource() -> Resource<'static> {
    Resource {
        name: "pvc-1",
        version: "0.1.0",
        minor: 50000,
        secret: "c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0MTI=",
        local: host("n1", 0, "10.0.0.1:11000", Disk::Local("/dev/vg/pvc-1")),
        peers: vec![
            Host {
                tiebreaker: false,
                ..host("n4", 3, "[fd00::4]:11002", Disk::None)
            },
            host("n2", 1, "10.0.0.2:11000", Disk::Peer),
            host("n3", 2, "10.0.0.3:11001", Disk::None),
        ],
        two_primaries: false,
        discard_granularity: Some(65536),
    }
}

const BODY: &str = r#"resource "pvc-1" {
    options {
        quorum majority;
        on-no-quorum suspend-io;
        on-no-data-accessible suspend-io;
        on-suspended-primary-outdated force-secondary;
    }
    net {
        protocol C;
        cram-hmac-alg sha256;
        shared-secret "c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0MTI=";
        rr-conflict retry-connect;
    }
    disk {
        rs-discard-granularity 65536;
    }
    on "n1" {
        node-id 0;
        volume 0 {
            device minor 50000;
            disk "/dev/vg/pvc-1";
            meta-disk internal;
        }
    }
    on "n2" {
        node-id 1;
        volume 0 {
            device minor 50000;
            disk "/dev/r-drbd/peer-disk-never-opened";
        }
    }
    on "n3" {
        node-id 2;
        volume 0 {
            device minor 50000;
            disk none;
        }
    }
    on "n4" {
        node-id 3;
        volume 0 {
            device minor 50000;
            disk none;
            tiebreaker no;
        }
    }
    connection {
        host "n1" address ipv4 10.0.0.1:11000;
        host "n2" address ipv4 10.0.0.2:11000;
    }
    connection {
        host "n1" address ipv4 10.0.0.1:11000;
        host "n3" address ipv4 10.0.0.3:11001;
    }
    connection {
        host "n1" address ipv4 10.0.0.1:11000;
        host "n4" address ipv6 [fd00::4]:11002;
    }
}
"#;

#[test]
fn full_file() {
    let r = render(&resource(), ALL).unwrap();
    let (header, body) = r.text.split_once('\n').unwrap();
    assert_eq!(body, BODY);
    assert_eq!(header, "# r-drbd 0.1.0");
    assert_eq!(r.dropped, Vec::<&str>::new());
    assert!(BODY.contains(PEER_DISK));
}

#[test]
fn content_ignores_the_version_and_catches_edits() {
    let a = render(&resource(), ALL).unwrap();
    let b = render(
        &Resource {
            version: "9.9.9",
            ..resource()
        },
        ALL,
    )
    .unwrap();
    assert_ne!(a.text, b.text);
    assert!(same_content(&a.text, &b.text));
    let edited = a.text.replace("11001", "11009");
    assert!(!same_content(&edited, &a.text));
    let unheaded = a.text.split_once('\n').unwrap().1;
    assert!(!same_content(unheaded, &a.text));
    assert!(!same_content("", &a.text));
}

#[test]
fn unsupported_settings_are_left_out_and_reported() {
    let r = render(&resource(), Features::default()).unwrap();
    assert!(!r.text.contains("tiebreaker"));
    assert!(!r.text.contains("rs-discard-granularity"));
    assert!(!r.text.contains("on-suspended-primary-outdated"));
    assert!(!r.text.contains("    disk {"));
    assert_eq!(
        r.dropped,
        [
            "on-suspended-primary-outdated",
            "rs-discard-granularity",
            "tiebreaker"
        ]
    );
}

#[test]
fn diskless_local_and_two_primaries() {
    let mut r = resource();
    r.local = Host {
        tiebreaker: false,
        ..host("n4", 3, "[fd00::4]:11002", Disk::None)
    };
    r.peers = vec![host("n1", 0, "10.0.0.1:11000", Disk::Peer)];
    r.two_primaries = true;
    let text = render(&r, ALL).unwrap().text;
    // No local disk: no discard setting.
    assert!(!text.contains("rs-discard-granularity"));
    assert!(
        text.contains(
            "        rr-conflict retry-connect;\n        allow-two-primaries yes;\n    }\n"
        )
    );
    assert!(text.contains(
        "    connection {\n        host \"n4\" address ipv6 [fd00::4]:11002;\n        host \"n1\" address ipv4 10.0.0.1:11000;\n    }\n"
    ));
    assert_eq!(text.matches("connection {").count(), 1);
}

#[test]
fn refuses_what_drbdadm_would_misread() {
    let mut r = resource();
    r.name = "a\"b";
    assert!(matches!(render(&r, ALL), Err(RenderError::Unsafe { .. })));
    let mut r = resource();
    r.local.disk = Disk::Local("/dev/x\ny");
    assert!(matches!(render(&r, ALL), Err(RenderError::Unsafe { .. })));
    let mut r = resource();
    r.peers[0].node_id = 0;
    assert_eq!(render(&r, ALL), Err(RenderError::DuplicateNodeId(0)));
    let mut r = resource();
    r.peers[0].node_id = 32;
    assert_eq!(render(&r, ALL), Err(RenderError::NodeId(32)));
    let mut r = resource();
    r.peers[1].disk = Disk::Local("/dev/vg/x");
    assert_eq!(
        render(&r, ALL),
        Err(RenderError::PeerWithLocalDisk("n2".into()))
    );
    let mut r = resource();
    let long = "x".repeat(65);
    r.secret = &long;
    assert_eq!(render(&r, ALL), Err(RenderError::SecretLength));
}

#[test]
fn discard_granularity_stays_within_drbd_limit() {
    const K: u64 = 1024;
    const M: u64 = K * K;
    assert_eq!(discard_granularity(64 * K), Some(64 * K));
    assert_eq!(discard_granularity(M), Some(M));
    assert_eq!(discard_granularity(2 * M), Some(M));
    assert_eq!(discard_granularity(1024 * M), Some(M));
    assert_eq!(discard_granularity(3 * 512 * K), Some(768 * K));

    let mut r = resource();
    r.discard_granularity = Some(2 * M);
    let text = render(&r, ALL).unwrap().text;
    assert!(text.contains("rs-discard-granularity 1048576;"), "{text}");
}
