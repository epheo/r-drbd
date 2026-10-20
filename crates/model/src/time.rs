//! Wall-clock time as the API stores it.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::jiff::Timestamp;

/// Now, in whole seconds: a metav1.Time keeps no more on the wire, and a
/// finer value never equals its stored copy, so every compare-before-write
/// would see a change.
pub fn now() -> Time {
    Time(Timestamp::from_second(Timestamp::now().as_second()).unwrap_or(Timestamp::UNIX_EPOCH))
}
