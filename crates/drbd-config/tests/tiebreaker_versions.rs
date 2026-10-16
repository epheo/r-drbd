mod common;

use drbd_config::{
    DiskPath, FixedOptions, Tiebreaker, ToolVersions, Version, render, tiebreaker_supported,
};

fn v(major: u32, minor: u32, patch: u32) -> Version {
    Version {
        major,
        minor,
        patch,
    }
}

fn tools(kernel: Version, utils: Version) -> ToolVersions {
    ToolVersions { kernel, utils }
}

#[test]
fn support_rule() {
    assert!(tiebreaker_supported(&tools(v(9, 3, 1), v(9, 34, 0))));
    assert!(tiebreaker_supported(&tools(v(9, 4, 0), v(10, 0, 0))));
    assert!(!tiebreaker_supported(&tools(v(9, 3, 0), v(9, 34, 0))));
    assert!(!tiebreaker_supported(&tools(v(9, 2, 9), v(9, 40, 0))));
    assert!(!tiebreaker_supported(&tools(v(9, 3, 1), v(9, 33, 9))));
    assert!(!tiebreaker_supported(&tools(v(8, 4, 11), v(9, 0, 0))));
}

fn rendered_volume(t: Tiebreaker) -> String {
    let fixed = FixedOptions::new(2, None, false);
    let mut file = common::sample(&fixed);
    file.peers.clear();
    file.connections.clear();
    file.local = common::host("node-a", 0, DiskPath::None, &fixed);
    file.local.volumes[0].tiebreaker = t;
    render(&file)
}

#[test]
fn tiebreaker_versions() {
    let yes = rendered_volume(Tiebreaker::Yes);
    assert!(yes.contains("            disk        none;\n            tiebreaker yes;\n"));
    let no = rendered_volume(Tiebreaker::No);
    assert!(no.contains("            disk        none;\n            tiebreaker no;\n"));

    let old = rendered_volume(Tiebreaker::Unsupported);
    assert!(old.contains(
        "            disk        none;\n            # tiebreaker yes; (needs kernel >= 9.3.1 and drbd-utils >= 9.34.0)\n            meta-disk   internal;\n"
    ), "{old}");
    assert!(!old.contains("\n            tiebreaker"));
}
