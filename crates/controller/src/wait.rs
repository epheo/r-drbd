//! Bounded waits on store state for the CSI calls.

use std::time::Duration;

use crate::store::Stores;

/// Re-evaluates `check` on every store change until it yields a value or
/// `timeout` passes (None).
pub async fn until<T>(
    stores: &Stores,
    timeout: Duration,
    mut check: impl FnMut(&Stores) -> Option<T>,
) -> Option<T> {
    let mut rx = stores.subscribe();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        rx.borrow_and_update();
        if let Some(v) = check(stores) {
            return Some(v);
        }
        match tokio::time::timeout_at(deadline, rx.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return check(stores),
        }
    }
}
