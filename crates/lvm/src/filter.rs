//! Whether an lvm.conf filter value rejects DRBD devices, evaluated the way
//! lvm.conf(5) describes it: per device name the first matching pattern
//! decides; the device is rejected when at least one name is decided and
//! every decided name is rejected.

/// Names a DRBD device has on every host.
const DRBD_NAMES: [&str; 2] = ["/dev/drbd0", "/dev/drbd/by-res/r/0"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tok {
    Lit(u8),
    Any,
    AnyRun,
}

/// A pattern: anchors and tokens. `None` for regex syntax beyond literals,
/// `.`, `.*`, `^` and `$`: no regex engine here, so no guess either.
fn compile(re: &str) -> Option<(bool, bool, Vec<Tok>)> {
    let (start, re) = re.strip_prefix('^').map_or((false, re), |r| (true, r));
    let (end, re) = re.strip_suffix('$').map_or((false, re), |r| (true, r));
    let b = re.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'.' if b.get(i + 1) == Some(&b'*') => {
                toks.push(Tok::AnyRun);
                i += 1;
            }
            b'.' => toks.push(Tok::Any),
            b'\\' | b'[' | b']' | b'(' | b')' | b'{' | b'}' | b'+' | b'?' | b'|' | b'*' | b'^'
            | b'$' => return None,
            c => toks.push(Tok::Lit(c)),
        }
        i += 1;
    }
    Some((start, end, toks))
}

fn match_here(toks: &[Tok], s: &[u8], end: bool) -> bool {
    match toks.split_first() {
        None => !end || s.is_empty(),
        Some((Tok::AnyRun, rest)) => (0..=s.len()).any(|k| match_here(rest, &s[k..], end)),
        Some((t, rest)) => s.split_first().is_some_and(|(c, tail)| {
            (*t == Tok::Any || *t == Tok::Lit(*c)) && match_here(rest, tail, end)
        }),
    }
}

fn matches(re: &(bool, bool, Vec<Tok>), name: &str) -> bool {
    let (start, end, toks) = re;
    let s = name.as_bytes();
    if *start {
        match_here(toks, s, *end)
    } else {
        (0..=s.len()).any(|k| match_here(toks, &s[k..], *end))
    }
}

/// `Some(true)` when the filter rejects DRBD devices, `None` when a pattern
/// uses syntax this check does not evaluate.
/// `value` is lvmconfig's form: `["r|^/dev/drbd|", "a|.*|"]`.
pub fn rejects_drbd(value: &str) -> Option<bool> {
    let mut rules = Vec::new();
    // Quoted items sit at the odd positions between quotes.
    for item in value.split('"').skip(1).step_by(2) {
        let mut chars = item.chars();
        let accept = match chars.next()? {
            'a' => true,
            'r' => false,
            _ => return None,
        };
        let delim = chars.next()?;
        let body = chars.as_str();
        let re = &body[..body.rfind(delim)?];
        rules.push((accept, compile(re)?));
    }
    let decided: Vec<bool> = DRBD_NAMES
        .iter()
        .filter_map(|n| rules.iter().find(|(_, re)| matches(re, n)).map(|(a, _)| *a))
        .collect();
    Some(!decided.is_empty() && decided.iter().all(|accept| !accept))
}
