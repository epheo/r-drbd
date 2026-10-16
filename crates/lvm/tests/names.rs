use lvm::names::{NAME_MAX, check_new_lv, snapshot_lv, valid_lvm_name, volume_lv};

#[test]
fn lv_names() {
    assert_eq!(volume_lv("pvc-1"), "pvc-1");
    assert_eq!(snapshot_lv("pvc-1", "snapshot-2"), "pvc-1+snapshot-2");
    let longest = snapshot_lv(&"a".repeat(NAME_MAX), &"b".repeat(NAME_MAX));
    assert_eq!(longest.len(), 127);
    assert!(valid_lvm_name(&longest));
}

#[test]
fn lvm_charset() {
    for ok in ["thin", "a+b_c.d-e", "0", "...", &"a".repeat(127)] {
        assert!(valid_lvm_name(ok), "{ok}");
    }
    for bad in [
        "",
        "-a",
        ".",
        "..",
        "a/b",
        "a b",
        "caf\u{e9}",
        &"a".repeat(128),
    ] {
        assert!(!valid_lvm_name(bad), "{bad}");
    }
}

#[test]
fn checks_lvcreate_would_fail() {
    // CSI names: a snapshot name may start with "snapshot" after the separator.
    let csi_snap = snapshot_lv(
        "pvc-0a1b2c3d-4e5f-6789-abcd-ef0123456789",
        "snapshot-0a1b2c3d-4e5f-6789-abcd-ef0123456789",
    );
    assert_eq!(check_new_lv("ab-cdef-ghi", &csi_snap), Ok(()));
    assert_eq!(
        check_new_lv("vg", "snapshot-1"),
        Err("LVM reserves LV names starting with snapshot: snapshot-1".into())
    );
    assert_eq!(
        check_new_lv("vg", "pvmove0"),
        Err("LVM reserves LV names starting with pvmove: pvmove0".into())
    );
    assert_eq!(
        check_new_lv("vg", "a_tmeta"),
        Err("LVM reserves LV names containing _tmeta: a_tmeta".into())
    );
    assert!(check_new_lv("vg", "-a").is_err());
    assert!(check_new_lv("-vg", "a").is_err());
}

#[test]
fn device_mapper_length() {
    // Measured on a host: in a VG of 11 bytes with two dashes (13 once
    // doubled) a dash-free LV name of 113 bytes is the longest lvcreate takes.
    let vg = "ab-cdef-ghi";
    assert_eq!(check_new_lv(vg, &"x".repeat(113)), Ok(()));
    assert_eq!(
        check_new_lv(vg, &"x".repeat(114)),
        Err(format!(
            "LV name {vg}/{} too long: its device-mapper name takes 128 bytes, the limit is 127",
            "x".repeat(114)
        ))
    );
    // Each dash counts twice.
    assert!(check_new_lv(vg, &format!("{}-x", "x".repeat(110))).is_ok());
    assert!(check_new_lv(vg, &format!("{}-x", "x".repeat(111))).is_err());
}
