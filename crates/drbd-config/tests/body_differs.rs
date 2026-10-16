mod common;

use drbd_config::{FixedOptions, render};

#[test]
fn body_differs() {
    let fixed = FixedOptions::new(2, None, false);
    let file = common::sample(&fixed);
    let first = render(&file);

    let mut later = file.clone();
    later.header.local_time = "2027-01-01 00:00:00".into();
    later.header.generator = "r-drbd 9.9.9".into();
    let second = render(&later);
    assert_ne!(first, second);
    assert!(!drbd_config::body_differs(&first, &second));

    let mut changed = file.clone();
    changed.secret = "another-secret-value".into();
    assert!(drbd_config::body_differs(&first, &render(&changed)));

    let no_marker = "# truncated file\n";
    assert!(drbd_config::body_differs(no_marker, &first));
    assert!(drbd_config::body_differs(&first, no_marker));
    assert!(drbd_config::body_differs("", ""));
}

#[test]
fn header_with_marker_text_is_ignored() {
    let file = common::sample(&FixedOptions::new(2, None, false));
    let first = render(&file);
    let mut later = file.clone();
    later.header.hostname = "resource \"x\"".into();
    later.header.generator = "gen\nresource \"y\"\n{".into();
    let second = render(&later);
    assert_ne!(first, second);
    assert!(!drbd_config::body_differs(&first, &second));
    assert!(!drbd_config::body_differs(&second, &first));
}
