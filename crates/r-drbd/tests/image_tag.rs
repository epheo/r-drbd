//! Every place that names the pinned image build must name the same one:
//! the two pod manifests and the kustomization. A partial bump would run
//! the controller and the agents on different builds.

use std::path::Path;

const IMAGE: &str = "ghcr.io/epheo/r-drbd:";

/// Files that carry the pin. deploy/dev follows `latest` on purpose and
/// the workflows produce the tags, so neither is listed.
const PINNED: &[&str] = &[
    "deploy/base/20-controller.yaml",
    "deploy/base/30-agent.yaml",
    "deploy/base/kustomization.yaml",
];

fn tags_in(text: &str) -> Vec<String> {
    let mut tags = Vec::new();
    for line in text.lines() {
        for (i, _) in line.match_indices(IMAGE) {
            let rest = &line[i + IMAGE.len()..];
            let tag: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_'))
                .collect();
            tags.push(tag);
        }
        // kustomize names the tag apart from the image.
        if let Some(t) = line.trim().strip_prefix("newTag:") {
            tags.push(t.trim().to_owned());
        }
    }
    tags
}

#[test]
fn every_pin_names_the_same_build() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut seen: Vec<(String, String)> = Vec::new();
    for file in PINNED {
        let text =
            std::fs::read_to_string(root.join(file)).unwrap_or_else(|e| panic!("{file}: {e}"));
        let tags = tags_in(&text);
        assert!(!tags.is_empty(), "{file} names no pinned image");
        for t in tags {
            seen.push((file.to_string(), t));
        }
    }
    let first = &seen[0].1;
    assert!(
        first.starts_with("sha-") || first.starts_with('v'),
        "the pin must be a sha- or v tag, not {first}"
    );
    let off: Vec<_> = seen.iter().filter(|(_, t)| t != first).collect();
    assert!(off.is_empty(), "pins disagree with {first}: {off:?}");
}
