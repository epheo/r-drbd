//! deploy/base/crds.yaml is the output of `r-drbd crds` (JSON, which is
//! YAML), committed so kustomize installs the CRDs with the manifests and
//! schema changes show in review.

use std::path::Path;
use std::process::Command;

#[test]
fn committed_crds_match_the_model() {
    let out = Command::new(env!("CARGO_BIN_EXE_r-drbd"))
        .arg("crds")
        .output()
        .expect("run r-drbd crds");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let generated = String::from_utf8(out.stdout).unwrap();
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/base/crds.yaml");
    let committed = std::fs::read_to_string(&file).unwrap_or_default();
    assert!(
        committed == generated,
        "deploy/base/crds.yaml is stale; regenerate it:\n  cargo run -p r-drbd -- crds > deploy/base/crds.yaml"
    );
}
