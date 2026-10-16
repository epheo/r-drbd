use lvm::PvFilter;

#[test]
fn empty_pv_list_gives_none() {
    assert_eq!(PvFilter::from_pvs(&[]), PvFilter(None));
}

#[test]
fn two_pvs_match_spec_example() {
    let pvs = vec!["/dev/sdb".to_owned(), "/dev/sdc".to_owned()];
    assert_eq!(
        PvFilter::from_pvs(&pvs),
        PvFilter(Some(
            "devices { filter=['a|/dev/sdb|','a|/dev/sdc|','r|.*|'] }".to_owned()
        ))
    );
}
