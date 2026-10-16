//! Tracked files must not name private notes, lab details or the projects r-drbd came from.
//! Patterns are assembled at run time so this file does not match itself.

use std::path::Path;
use std::process::Command;

/// The ignore file names the private dirs; Cargo.lock hashes match short patterns.
const SKIP: &[&str] = &[".gitignore", "Cargo.lock"];

fn patterns() -> Vec<String> {
    vec![
        ["docs", "/internal"].concat(),
        ["lin", "stor"].concat(),
        ["stor", "nas"].concat(),
        ["satel", "lite"].concat(),
        ["pira", "eus"].concat(),
        // Lab details live in the ignored lab/ dir.
        ["d", "a2"].concat(),
        ["v", "m3"].concat(),
        ["172", ".16."].concat(),
        ["/var", "/home"].concat(),
        ["scratch", "pad"].concat(),
    ]
}

/// An ADR number: the tag, a dash, a digit.
fn has_adr_id(line: &str) -> bool {
    let tag = ["adr", "-"].concat();
    line.match_indices(&tag)
        .any(|(i, _)| line[i + tag.len()..].starts_with(|c: char| c.is_ascii_digit()))
}

#[test]
fn tracked_files_stay_public() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let Ok(out) = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z"])
        .output()
    else {
        eprintln!("SKIPPED: git not available");
        return;
    };
    if !out.status.success() {
        eprintln!("SKIPPED: not a git checkout");
        return;
    }
    let pats = patterns();
    let mut hits = Vec::new();
    for path in String::from_utf8_lossy(&out.stdout).split('\0') {
        if path.is_empty() || SKIP.contains(&path) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(root.join(path)) else {
            continue;
        };
        for (n, line) in text.lines().enumerate() {
            let lower = line.to_lowercase();
            if pats.iter().any(|p| lower.contains(p.as_str())) || has_adr_id(&lower) {
                hits.push(format!("{path}:{}: {line}", n + 1));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "private, lab or legacy names in tracked files:\n{}",
        hits.join("\n")
    );
}
