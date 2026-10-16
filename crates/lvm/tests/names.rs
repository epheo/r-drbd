use lvm::names::{snapshot_lv, valid, valid_lvm_name, volume_lv};

#[test]
fn lvm_names() {
    let long = "a".repeat(127);
    let too_long = "a".repeat(128);
    for (n, want) in [
        ("thin", true),
        ("nvme-vg-2tb", true),
        ("r-drbd-test", true),
        ("a+b_c.d-e", true),
        (".a", true),
        ("...", true),
        ("_", true),
        ("9", true),
        (long.as_str(), true),
        ("", false),
        ("-foo", false),
        (".", false),
        ("..", false),
        ("a/b", false),
        ("a b", false),
        ("vg;rm", false),
        ("caf\u{e9}", false),
        (too_long.as_str(), false),
    ] {
        assert_eq!(valid_lvm_name(n), want, "{n:?}");
    }
}

#[test]
fn lv_names() {
    assert_eq!(volume_lv("pvc-1234"), "pvc-1234");
    assert_eq!(snapshot_lv("pvc-1234", "snap1"), "pvc-1234_snap1");
}

#[test]
fn valid_names() {
    for n in ["ab", "_a", "pvc-1234", "A_b-9", &"a".repeat(48)] {
        assert!(valid(n), "{n}");
    }
}

#[test]
fn invalid_names() {
    for n in [
        "",
        "a",
        "__",
        "_-1",
        "1a",
        "-a",
        "a.meta",
        "a b",
        "pvc/1",
        "caf\u{e9}",
        &"a".repeat(49),
    ] {
        assert!(!valid(n), "{n}");
    }
}
