mod common;

use drbd_config::{FixedOptions, OptionKind, OptionLine, render};

fn line(name: &str, value: &str, kind: OptionKind) -> OptionLine {
    OptionLine {
        name: name.into(),
        value: value.into(),
        kind,
    }
}

#[test]
fn quoting() {
    let mut file = common::sample(&FixedOptions::new(2, None, false));
    file.net = vec![
        line("verify-alg", "crct10dif", OptionKind::String),
        line("max-buffers", "8000", OptionKind::Numeric),
        line("allow-two-primaries", "no", OptionKind::Boolean),
        line("ping-timeout", "default", OptionKind::NumericOrSymbol),
        line("after-sb-0pri", "discard-zero-changes", OptionKind::Handler),
    ];
    let got = render(&file);
    for expected in [
        "        verify-alg \"crct10dif\";\n",
        "        max-buffers 8000;\n",
        "        allow-two-primaries no;\n",
        "        ping-timeout default;\n",
        "        after-sb-0pri discard-zero-changes;\n",
        "        shared-secret     \"q2XlS0v3Yp9cJr7nAa1F\";\n",
    ] {
        assert!(got.contains(expected), "missing {expected:?} in\n{got}");
    }
}

#[test]
fn string_values_cannot_break_out() {
    let mut file = common::sample(&FixedOptions::new(2, None, false));
    file.net = vec![line("verify-alg", "a\"b\\c\nd", OptionKind::String)];
    let got = render(&file);
    assert!(
        got.contains("        verify-alg \"a\\\"b\\\\c d\";\n"),
        "{got}"
    );
}
