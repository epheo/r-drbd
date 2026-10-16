mod common;

use drbd_config::{DiskPath, FixedOptions, ResourceFile, render};

#[test]
fn render_local_free() {
    let fixed = FixedOptions::new(1, None, false);
    assert_eq!(fixed.resource, Vec::new());
    let file = ResourceFile {
        peers: Vec::new(),
        connections: Vec::new(),
        local: common::host(
            "node-a",
            0,
            DiskPath::Backing("/dev/vg/pvc-1".into()),
            &fixed,
        ),
        ..common::sample(&fixed)
    };
    let expected = r#"resource "pvc-0a1b"
{

    net
    {
        cram-hmac-alg     sha1;
        shared-secret     "q2XlS0v3Yp9cJr7nAa1F";
        protocol C;
        rr-conflict retry-connect;
    }

    on "node-a"
    {
        volume 0
        {
            disk        /dev/vg/pvc-1;
            tiebreaker yes;
            meta-disk   internal;
            device      minor 1000;
        }
        node-id    0;
    }
}
"#;
    let got = render(&file);
    assert_eq!(common::body(&got), expected);
    assert!(!got.contains("quorum"));
    assert!(!got.contains("connection"));
    assert!(!got.contains("options"));
}
