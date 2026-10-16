use lvm::filter::rejects_drbd;

#[test]
fn first_match_decides() {
    assert_eq!(rejects_drbd(r#"["a|.*|"]"#), Some(false));
    assert_eq!(rejects_drbd(r#"["r|^/dev/drbd|", "a|.*|"]"#), Some(true));
    assert_eq!(rejects_drbd(r#"["r|/dev/drbd.*|"]"#), Some(true));
    assert_eq!(rejects_drbd(r#"["r#drbd#"]"#), Some(true), "any delimiter");
    // An accept before the reject wins.
    assert_eq!(rejects_drbd(r#"["a|.*|", "r|^/dev/drbd|"]"#), Some(false));
    // Character classes are not evaluated.
    assert_eq!(rejects_drbd(r#"["r|^/dev/drbd[0-9]|"]"#), None);
    // One name rejected, the other undecided: rejected.
    assert_eq!(rejects_drbd(r#"["r|^/dev/drbd0$|"]"#), Some(true));
    // One name accepted keeps the device.
    assert_eq!(
        rejects_drbd(r#"["a|by-res|", "r|^/dev/drbd|"]"#),
        Some(false)
    );
    assert_eq!(
        rejects_drbd(r#"["r|^/dev/sd|"]"#),
        Some(false),
        "no name decided"
    );
    assert_eq!(rejects_drbd("[]"), Some(false));
    assert_eq!(rejects_drbd(r#"["x|y|"]"#), None);
}
