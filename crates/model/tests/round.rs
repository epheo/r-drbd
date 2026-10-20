//! The snapshot round bounds hold together.

use model::snapshot::{CLOCK_MARGIN, RESUME_AFTER, SUSPEND_BOUND, TAKE_WINDOW};

#[test]
fn no_node_resumes_before_every_take() {
    assert!(RESUME_AFTER > TAKE_WINDOW + SUSPEND_BOUND + CLOCK_MARGIN);
}
