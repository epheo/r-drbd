//! The snapshot round bounds hold together.

use model::snapshot::{CLOCK_MARGIN, RESUME_AFTER, SUSPEND_BOUND, TAKE_WINDOW};

#[test]
fn no_node_resumes_before_every_take() {
    assert!(RESUME_AFTER > TAKE_WINDOW + SUSPEND_BOUND + CLOCK_MARGIN);
}

/// What an older version stored, phases this one removed included.
const OLD: &str = r#"{
  "apiVersion": "r-drbd.io/v1alpha1", "kind": "Snapshot",
  "metadata": { "name": "old" },
  "spec": { "volume": "vol" },
  "status": {
    "phase": "Resuming",
    "acks": [
      { "node": "n1", "phase": "Resuming", "pool": "p1", "ok": true },
      { "node": "n2", "phase": "Suspended", "ok": true },
      { "node": "n3", "phase": "Taken", "ok": true }
    ]
  }
}"#;

#[test]
fn a_removed_phase_decodes_as_unknown() {
    use model::{Snapshot, SnapshotPhase};
    let s: Snapshot = serde_json::from_str(OLD).expect("decodes");
    let st = s.status.unwrap();
    assert_eq!(st.phase, Some(SnapshotPhase::Unknown));
    assert!(st.acks.iter().all(|a| a.phase == SnapshotPhase::Unknown));
    // Never written back.
    assert!(serde_json::to_string(&SnapshotPhase::Unknown).is_err());
    assert_eq!(
        serde_json::to_string(&SnapshotPhase::Taking).unwrap(),
        "\"Taking\""
    );
}

#[test]
fn unknown_is_not_in_the_schema() {
    let crd = serde_json::to_string(&<model::Snapshot as kube::CustomResourceExt>::crd()).unwrap();
    assert!(!crd.contains("\"Unknown\""), "{crd}");
}
