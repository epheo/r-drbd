use lvm::PvFilter;
use lvm::argv;

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|&a| a.to_owned()).collect()
}

fn f() -> PvFilter {
    PvFilter(Some(
        "devices { filter=['a|/dev/sdb|','r|.*|'] }".to_owned(),
    ))
}

const F: &str = "devices { filter=['a|/dev/sdb|','r|.*|'] }";
const LVS_FIELDS: &str = "lv_name,lv_path,lv_size,vg_name,pool_lv,data_percent,lv_attr,metadata_percent,chunk_size,stripes,origin";
const VGS_FIELDS: &str = "vg_name,vg_extent_size,vg_size,vg_free,lv_name,lv_size,data_percent";

#[test]
fn pvdisplay_for_vg() {
    assert_eq!(
        argv::pvdisplay_for_vg("vg0"),
        s(&[
            "pvdisplay",
            "--config",
            r#"devices { ignore_suspended_devices=1 filter=["r|^/dev/drbd.*|"] }"#,
            "--columns",
            "-o",
            "pv_name",
            "-S",
            "vg_name=vg0",
            "--noheadings",
            "--nosuffix",
        ])
    );
}

#[test]
fn lvs_units_after_noheadings() {
    let vgs = s(&["vg0", "vg1"]);
    let tail = [
        "-o",
        LVS_FIELDS,
        "--separator",
        ";",
        "--noheadings",
        "--units",
        "k",
        "--nosuffix",
        "vg0",
        "vg1",
    ];
    let mut plain = vec!["lvs"];
    plain.extend(tail);
    assert_eq!(argv::lvs(&PvFilter(None), &vgs), s(&plain));
    let mut filtered = vec!["lvs", "--config", F];
    filtered.extend(tail);
    assert_eq!(argv::lvs(&f(), &vgs), s(&filtered));
}

#[test]
fn vgs_units_before_noheadings() {
    let vgs = s(&["vg0"]);
    let tail = [
        "-o",
        VGS_FIELDS,
        "--separator",
        ";",
        "--units",
        "k",
        "--noheadings",
        "--nosuffix",
        "vg0",
    ];
    let mut plain = vec!["vgs"];
    plain.extend(tail);
    assert_eq!(argv::vgs(&PvFilter(None), &vgs), s(&plain));
    let mut filtered = vec!["vgs", "--config", F];
    filtered.extend(tail);
    assert_eq!(argv::vgs(&f(), &vgs), s(&filtered));
}

#[test]
fn lvs_without_vgs_has_no_trailing_args() {
    let v = argv::lvs(&PvFilter(None), &[]);
    assert_eq!(v.last().map(String::as_str), Some("--nosuffix"));
}

#[test]
fn lvcreate_thin() {
    assert_eq!(
        argv::lvcreate_thin(&PvFilter(None), "vg0", "tp0", "pvc-1", 1_052_672),
        s(&[
            "lvcreate",
            "--virtualsize",
            "1052672k",
            "vg0",
            "--thinpool",
            "tp0",
            "--name",
            "pvc-1",
        ])
    );
    assert_eq!(
        argv::lvcreate_thin(&f(), "vg0", "tp0", "pvc-1", 4096),
        s(&[
            "lvcreate",
            "--config",
            F,
            "--virtualsize",
            "4096k",
            "vg0",
            "--thinpool",
            "tp0",
            "--name",
            "pvc-1",
        ])
    );
}

#[test]
fn lvresize() {
    assert_eq!(
        argv::lvresize(&PvFilter(None), "vg0", "pvc-1", 8192),
        s(&["lvresize", "--size", "8192k", "vg0/pvc-1", "-f"])
    );
    assert_eq!(
        argv::lvresize(&f(), "vg0", "pvc-1", 8192),
        s(&[
            "lvresize",
            "--config",
            F,
            "--size",
            "8192k",
            "vg0/pvc-1",
            "-f"
        ])
    );
}

#[test]
fn lvremove() {
    assert_eq!(
        argv::lvremove(&PvFilter(None), "vg0", "pvc-1"),
        s(&["lvremove", "-f", "vg0/pvc-1"])
    );
    assert_eq!(
        argv::lvremove(&f(), "vg0", "pvc-1"),
        s(&["lvremove", "--config", F, "-f", "vg0/pvc-1"])
    );
}

#[test]
fn activate() {
    assert_eq!(
        argv::activate(&PvFilter(None), "vg0", "pvc-1"),
        s(&["lvchange", "-ay", "-K", "-y", "vg0/pvc-1"])
    );
    assert_eq!(
        argv::activate(&f(), "vg0", "pvc-1"),
        s(&["lvchange", "--config", F, "-ay", "-K", "-y", "vg0/pvc-1"])
    );
}

#[test]
fn deactivate() {
    assert_eq!(
        argv::deactivate(&PvFilter(None), "vg0", "pvc-1"),
        s(&["lvchange", "-an", "vg0/pvc-1"])
    );
    assert_eq!(
        argv::deactivate(&f(), "vg0", "pvc-1"),
        s(&["lvchange", "--config", F, "-an", "vg0/pvc-1"])
    );
}

#[test]
fn thinpool_zero() {
    assert_eq!(
        argv::thinpool_zero(&PvFilter(None), "vg0", "tp0"),
        s(&["lvchange", "--zero", "y", "vg0/tp0"])
    );
    assert_eq!(
        argv::thinpool_zero(&f(), "vg0", "tp0"),
        s(&["lvchange", "--config", F, "--zero", "y", "vg0/tp0"])
    );
}

#[test]
fn snapshot_thin() {
    let tail = [
        "--snapshot",
        "--setactivationskip",
        "y",
        "--ignoreactivationskip",
        "--activate",
        "y",
        "--name",
        "vg0/pvc-1_snap1",
        "vg0/pvc-1",
        "-pr",
    ];
    let mut plain = vec!["lvcreate"];
    plain.extend(tail);
    assert_eq!(
        argv::snapshot_thin(&PvFilter(None), "vg0", "pvc-1", "pvc-1_snap1"),
        s(&plain)
    );
    let mut filtered = vec!["lvcreate", "--config", F];
    filtered.extend(tail);
    assert_eq!(
        argv::snapshot_thin(&f(), "vg0", "pvc-1", "pvc-1_snap1"),
        s(&filtered)
    );
}

#[test]
fn restore_thin() {
    assert_eq!(
        argv::restore_thin(&PvFilter(None), "vg0", "pvc-1_snap1", "pvc-2"),
        s(&[
            "lvcreate",
            "--snapshot",
            "--name",
            "pvc-2",
            "vg0/pvc-1_snap1"
        ])
    );
    assert_eq!(
        argv::restore_thin(&f(), "vg0", "pvc-1_snap1", "pvc-2"),
        s(&[
            "lvcreate",
            "--config",
            F,
            "--snapshot",
            "--name",
            "pvc-2",
            "vg0/pvc-1_snap1",
        ])
    );
}

/// The tag rides on lvcreate itself: no moment exists with the LV untagged.
#[test]
fn tagged_creates() {
    assert_eq!(
        argv::lvcreate_thin_tagged(&f(), "vg0", "tp0", "pvc-1", 4096, Some("r-drbd.new")),
        s(&[
            "lvcreate",
            "--config",
            F,
            "--addtag",
            "r-drbd.new",
            "--virtualsize",
            "4096k",
            "vg0",
            "--thinpool",
            "tp0",
            "--name",
            "pvc-1",
        ])
    );
    assert_eq!(
        argv::restore_thin_tagged(&f(), "vg0", "pvc-1_snap1", "pvc-2", Some("r-drbd.new")),
        s(&[
            "lvcreate",
            "--config",
            F,
            "--addtag",
            "r-drbd.new",
            "--snapshot",
            "--name",
            "pvc-2",
            "vg0/pvc-1_snap1",
        ])
    );
    assert_eq!(
        argv::lvcreate_thin_tagged(&f(), "vg0", "tp0", "pvc-1", 4096, None),
        argv::lvcreate_thin(&f(), "vg0", "tp0", "pvc-1", 4096)
    );
}

#[test]
fn other_commands() {
    assert_eq!(
        argv::blockdev_getsize64("/dev/vg0/pvc-1"),
        s(&["blockdev", "--getsize64", "/dev/vg0/pvc-1"])
    );
    assert_eq!(
        argv::blockdev_setrw("/dev/vg0/pvc-1"),
        s(&["blockdev", "--setrw", "/dev/vg0/pvc-1"])
    );
    assert_eq!(
        argv::wipefs("/dev/vg0/pvc-1"),
        s(&["wipefs", "-a", "-f", "/dev/vg0/pvc-1"])
    );
}
