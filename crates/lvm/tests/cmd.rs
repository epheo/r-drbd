use lvm::cmd;

const C: &str = r#"devices { scan_lvs = 0 filter = [ "r|^/dev/drbd|" ] }"#;

fn v(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|&s| s.to_owned()).collect()
}

#[test]
fn config_is_one_static_value() {
    assert_eq!(cmd::CONFIG, C);
    assert_eq!(
        format!("devices {{ {} }}", lvm::device_rules!()),
        cmd::CONFIG
    );
}

#[test]
fn reports() {
    let head = |p: &str| {
        v(&[
            p,
            "--config",
            C,
            "--reportformat",
            "json",
            "--units",
            "b",
            "--nosuffix",
            "--binary",
            "-o",
        ])
    };
    let mut want = head("lvs");
    want.extend(v(&[
        "lv_name,lv_path,lv_size,segtype,pool_lv,lv_active_locally,lv_device_open,zero,data_percent,chunk_size",
        "vg1",
    ]));
    assert_eq!(cmd::lvs("vg1"), want);
    let mut want = head("lvs");
    want.extend(v(&["lv_name,lv_tags", "vg1"]));
    assert_eq!(cmd::lv_tags("vg1"), want);
    let mut want = head("vgs");
    want.extend(v(&["vg_name,vg_extent_size", "vg1"]));
    assert_eq!(cmd::vgs("vg1"), want);
}

#[test]
fn creates_put_options_before_names() {
    assert_eq!(
        cmd::create_thin("vg1", "thin", "pvc-1", 1 << 30, &["a", "b"]),
        v(&[
            "lvcreate",
            "--config",
            C,
            "--thinpool",
            "thin",
            "--virtualsize",
            "1073741824b",
            "--name",
            "pvc-1",
            "--addtag",
            "a",
            "--addtag",
            "b",
            "vg1",
        ])
    );
    assert_eq!(
        cmd::create_thin("vg1", "thin", "pvc-1", 4096, &[]),
        v(&[
            "lvcreate",
            "--config",
            C,
            "--thinpool",
            "thin",
            "--virtualsize",
            "4096b",
            "--name",
            "pvc-1",
            "vg1",
        ])
    );
    assert_eq!(
        cmd::snapshot("vg1", "pvc-1", "pvc-1+s"),
        v(&[
            "lvcreate",
            "--config",
            C,
            "--snapshot",
            "--setactivationskip",
            "y",
            "--permission",
            "r",
            "--name",
            "pvc-1+s",
            "vg1/pvc-1",
        ])
    );
    assert_eq!(
        cmd::restore("vg1", "pvc-1+s", "pvc-2", &["t"]),
        v(&[
            "lvcreate",
            "--config",
            C,
            "--snapshot",
            "--setactivationskip",
            "n",
            "--name",
            "pvc-2",
            "--addtag",
            "t",
            "vg1/pvc-1+s",
        ])
    );
}

#[test]
fn changes_put_options_before_names() {
    assert_eq!(
        cmd::extend("vg1", "pvc-1", 2 << 30),
        v(&[
            "lvextend",
            "--config",
            C,
            "--size",
            "2147483648b",
            "vg1/pvc-1"
        ])
    );
    assert_eq!(
        cmd::remove("vg1", "pvc-1"),
        v(&["lvremove", "--config", C, "--yes", "vg1/pvc-1"])
    );
    assert_eq!(
        cmd::activate("vg1", "pvc-1"),
        v(&["lvchange", "--config", C, "--activate", "y", "vg1/pvc-1"])
    );
    assert_eq!(
        cmd::deactivate("vg1", "pvc-1"),
        v(&["lvchange", "--config", C, "--activate", "n", "vg1/pvc-1"])
    );
    assert_eq!(
        cmd::change_tags("vg1", "pvc-1", &["a"], &["b", "c"]),
        v(&[
            "lvchange",
            "--config",
            C,
            "--addtag",
            "a",
            "--deltag",
            "b",
            "--deltag",
            "c",
            "vg1/pvc-1",
        ])
    );
    assert_eq!(
        cmd::enable_zeroing("vg1", "thin"),
        v(&["lvchange", "--config", C, "--zero", "y", "vg1/thin"])
    );
}

#[test]
fn device_size() {
    assert_eq!(
        cmd::device_size("/dev/drbd1"),
        v(&["blockdev", "--getsize64", "/dev/drbd1"])
    );
    assert_eq!(cmd::parse_device_size("2147483648\n"), Some(2_147_483_648));
    assert_eq!(cmd::parse_device_size("x"), None);
}

#[test]
fn host_probe() {
    assert_eq!(
        cmd::parse_version(
            "  LVM version:     2.03.41(2)-RHEL10 (2026-05-15)\n  Library version: 1.02.215\n"
        ),
        Some((2, 3, 41))
    );
    assert_eq!(
        cmd::parse_version("  LVM version:     2.02.187(2) (2020)\n"),
        Some((2, 2, 187))
    );
    assert_eq!(cmd::parse_version("garbage"), None);
    assert!((2, 2, 187) < cmd::MIN_VERSION && (2, 3, 41) > cmd::MIN_VERSION);
    assert_eq!(
        cmd::host_setting("devices/scan_lvs"),
        v(&["lvmconfig", "--typeconfig", "full", "devices/scan_lvs"])
    );
    assert_eq!(
        cmd::parse_host_setting("scan_lvs=0\n").as_deref(),
        Some("0")
    );
    assert_eq!(
        cmd::parse_host_setting("global_filter=[\"a|.*|\"]\n").as_deref(),
        Some("[\"a|.*|\"]")
    );
    assert_eq!(
        cmd::parse_host_setting("  Configuration node devices/x not found\n"),
        None
    );
}
