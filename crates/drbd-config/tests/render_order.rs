mod common;

use drbd_config::{FixedOptions, OptionKind, OptionLine, render};

fn sorted(opts: &[OptionLine]) -> bool {
    opts.windows(2).all(|w| w[0].name < w[1].name)
}

#[test]
fn fixed_options_sorted_by_name() {
    for replicas in [1, 2, 3] {
        for two_primaries in [false, true] {
            let f = FixedOptions::new(replicas, Some(65536), two_primaries);
            assert!(sorted(&f.resource), "{f:?}");
            assert!(sorted(&f.net), "{f:?}");
            assert!(sorted(&f.disk), "{f:?}");
        }
    }
    let f = FixedOptions::new(3, Some(65536), false);
    let names: Vec<_> = f.resource.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "on-no-data-accessible",
            "on-no-quorum",
            "on-suspended-primary-outdated",
            "quorum"
        ]
    );
    let disk: Vec<_> = f
        .disk
        .iter()
        .map(|o| (o.name.as_str(), o.value.as_str()))
        .collect();
    assert_eq!(disk, [("rs-discard-granularity", "65536")]);
    assert_eq!(FixedOptions::new(3, None, false).disk, Vec::new());
}

#[test]
fn render_order() {
    let fixed = FixedOptions::new(3, Some(65536), false);
    let mut file = common::sample(&fixed);
    file.handlers = vec![OptionLine {
        name: "quorum-lost".into(),
        value: "/bin/true".into(),
        kind: OptionKind::String,
    }];
    file.disk = vec![OptionLine {
        name: "al-extents".into(),
        value: "6007".into(),
        kind: OptionKind::Numeric,
    }];
    let got = render(&file);
    let expected = include_str!("fixtures/render_order.res");
    assert_eq!(common::body(&got), expected);
}

#[test]
fn connection_without_net_options() {
    let fixed = FixedOptions::new(2, None, false);
    let mut file = common::sample(&fixed);
    file.connections.truncate(1);
    file.connections[0].net_options.clear();
    let got = render(&file);
    let expected_tail = r#"    connection
    {
        host "node-a" address ipv4 10.0.0.11:7000;
        host "node-b" address ipv4 10.0.0.12:7000;
    }
}
"#;
    assert!(got.ends_with(expected_tail), "got:\n{got}");
}

#[test]
fn ipv6_host_line() {
    let fixed = FixedOptions::new(2, None, false);
    let mut file = common::sample(&fixed);
    file.connections.truncate(1);
    let peer = &mut file.connections[0].endpoints.peer;
    peer.family = drbd_config::AddrFamily::Ipv6;
    peer.address = "fd00::12".into();
    peer.port = 7001;
    let got = render(&file);
    assert!(got.contains("        host \"node-b\" address ipv6 [fd00::12]:7001;\n"));
}
