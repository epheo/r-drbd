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
        "lv_name,lv_path,lv_size,segtype,pool_lv,lv_active_locally,lv_device_open,zero,data_percent,chunk_size,metadata_percent,lv_health_status",
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
        cmd::activate_snapshot("vg1", "snap-1"),
        v(&[
            "lvchange",
            "--config",
            C,
            "--activate",
            "y",
            "--ignoreactivationskip",
            "vg1/snap-1"
        ])
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

#[test]
fn devices_mode_reports() {
    let head = |p: &str, fields: &str| {
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
            fields,
        ])
    };
    assert_eq!(
        cmd::vgs_all(),
        head(
            "vgs",
            "vg_name,vg_extent_size,vg_size,vg_free,vg_tags,vg_missing_pv_count"
        )
    );
    assert_eq!(cmd::pvs_all(), head("pvs", "pv_name,vg_name"));
    let mut want = head("lvs", "lv_name,segtype,lv_size,lv_metadata_size,chunk_size");
    want.push("vg1".into());
    assert_eq!(cmd::thin_pools("vg1"), want);
}

#[test]
fn devices_mode_changes_never_force() {
    let devs = v(&["/dev/disk/by-id/a", "/dev/disk/by-id/b"]);
    let all = [
        (
            cmd::vgcreate("vg1", "r-drbd.pool.p1", &devs),
            v(&[
                "vgcreate",
                "--config",
                C,
                "--addtag",
                "r-drbd.pool.p1",
                "vg1",
                "/dev/disk/by-id/a",
                "/dev/disk/by-id/b",
            ]),
        ),
        (
            cmd::vgextend("vg1", &devs[1..]),
            v(&["vgextend", "--config", C, "vg1", "/dev/disk/by-id/b"]),
        ),
        (
            cmd::create_thin_pool("vg1", "thin", 65_536, 71_303_168),
            v(&[
                "lvcreate",
                "--config",
                C,
                "--type",
                "thin-pool",
                "--extents",
                "100%FREE",
                "--chunksize",
                "65536b",
                "--poolmetadatasize",
                "71303168b",
                "--zero",
                "y",
                "--name",
                "thin",
                "vg1",
            ]),
        ),
        (
            cmd::extend_pool_metadata("vg1", "thin", 75_497_472),
            v(&[
                "lvextend",
                "--config",
                C,
                "--poolmetadatasize",
                "75497472b",
                "vg1/thin",
            ]),
        ),
        (
            cmd::extend_pool_data("vg1", "thin"),
            v(&[
                "lvextend",
                "--config",
                C,
                "--extents",
                "+100%FREE",
                "vg1/thin",
            ]),
        ),
        (
            cmd::extend_pool_data_to("vg1", "thin", 1 << 40),
            v(&[
                "lvextend",
                "--config",
                C,
                "--size",
                "1099511627776b",
                "vg1/thin",
            ]),
        ),
        (
            cmd::remove_inactive("vg1", "thin"),
            v(&["lvremove", "--config", C, "vg1/thin"]),
        ),
        (cmd::vgremove("vg1"), v(&["vgremove", "--config", C, "vg1"])),
        (
            cmd::pvremove(&devs),
            v(&[
                "pvremove",
                "--config",
                C,
                "/dev/disk/by-id/a",
                "/dev/disk/by-id/b",
            ]),
        ),
    ];
    for (got, want) in all {
        assert_eq!(got, want);
        for forced in ["--yes", "-y", "-f", "-ff", "--force"] {
            assert!(!got.iter().any(|a| a == forced), "{got:?}");
        }
    }
}
