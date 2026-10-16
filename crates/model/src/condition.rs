//! metav1.Condition helpers; condition types of the three kinds.

pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

pub mod types {
    pub const READY: &str = "Ready";
    pub const DEGRADED: &str = "Degraded";
    pub const QUORUM_LOST: &str = "QuorumLost";
    pub const SPLIT_BRAIN: &str = "SplitBrain";
    pub const AVAILABLE: &str = "Available";
}

pub const TRUE: &str = "True";
pub const FALSE: &str = "False";
pub const UNKNOWN: &str = "Unknown";

/// Build a condition at `now`; `observed_generation` is the spec generation it describes.
pub fn new(
    type_: &str,
    status: &str,
    reason: &str,
    message: impl Into<String>,
    observed_generation: Option<i64>,
    now: Time,
) -> Condition {
    Condition {
        type_: type_.to_owned(),
        status: status.to_owned(),
        reason: reason.to_owned(),
        message: message.into(),
        observed_generation,
        last_transition_time: now,
    }
}

/// Replace the condition of the same type. The transition time only moves
/// when the status changes, as kubectl and consumers expect.
pub fn set(conditions: &mut Vec<Condition>, mut cond: Condition) {
    if let Some(existing) = conditions.iter_mut().find(|c| c.type_ == cond.type_) {
        if existing.status == cond.status {
            cond.last_transition_time = existing.last_transition_time.clone();
        }
        *existing = cond;
    } else {
        conditions.push(cond);
    }
}

pub fn get<'a>(conditions: &'a [Condition], type_: &str) -> Option<&'a Condition> {
    conditions.iter().find(|c| c.type_ == type_)
}

pub fn is_true(conditions: &[Condition], type_: &str) -> bool {
    get(conditions, type_).is_some_and(|c| c.status == TRUE)
}
