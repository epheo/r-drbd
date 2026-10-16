mod common;

use common::{GIB, pool, request};
use placer::{Outcome, Reason, fits, place, promise, room};

// common::pool: total 100 GiB, overcommit 100, reserve 5, committed 0.

#[test]
fn empty_pool_offers_all_but_the_reserve() {
    let p = pool("p", "n1", 100 * GIB);
    assert_eq!(room(&p), 95 * GIB);
    assert!(fits(&p, 95 * GIB));
    assert!(!fits(&p, 95 * GIB + 1));
}

#[test]
fn committed_sizes_bind_without_overcommit() {
    // Nothing written yet: free is full, yet the promises cap the room.
    let mut p = pool("p", "n1", 100 * GIB);
    p.committed_bytes = 90 * GIB;
    assert_eq!(room(&p), 5 * GIB);
    assert!(fits(&p, 5 * GIB));
    assert!(!fits(&p, 5 * GIB + 1));
}

#[test]
fn actual_free_binds_when_snapshots_hold_blocks() {
    let mut p = pool("p", "n1", 20 * GIB);
    p.committed_bytes = 10 * GIB;
    assert_eq!(room(&p), 15 * GIB);
    assert!(!fits(&p, 15 * GIB + 1));
}

#[test]
fn free_at_or_below_the_reserve_takes_nothing() {
    let mut p = pool("p", "n1", 5 * GIB);
    assert_eq!(room(&p), 0);
    assert!(!fits(&p, 1));
    p.free_bytes = 5 * GIB + 1;
    assert!(fits(&p, 1));
    assert!(!fits(&p, 2));
}

#[test]
fn reserve_rounds_up() {
    // 5 percent of 101 bytes is 5.05: the reserve is 6.
    let mut p = pool("p", "n1", 101);
    p.total_bytes = 101;
    assert_eq!(room(&p), 95);
}

#[test]
fn overcommit_raises_only_the_promise_bound() {
    let mut p = pool("p", "n1", 100 * GIB);
    p.overcommit_percent = 300;
    p.committed_bytes = 200 * GIB;
    // promise 285 - 200 = 85, actual 95.
    assert_eq!(room(&p), 85 * GIB);
    p.committed_bytes = 0;
    // promise 285, actual 95: a volume must still be writable in full today.
    assert_eq!(room(&p), 95 * GIB);
}

#[test]
fn committed_above_a_lowered_policy_takes_nothing() {
    let mut p = pool("p", "n1", 100 * GIB);
    p.committed_bytes = 200 * GIB;
    assert_eq!(room(&p), 0);
    p.committed_bytes = u64::MAX;
    assert!(!fits(&p, 1));
}

#[test]
fn no_reserve_no_overcommit_is_plain_thick_accounting() {
    let mut p = pool("p", "n1", 60 * GIB);
    p.reserve_percent = 0;
    p.committed_bytes = 30 * GIB;
    assert_eq!(room(&p), 60 * GIB);
    p.committed_bytes = 50 * GIB;
    assert_eq!(room(&p), 50 * GIB);
}

#[test]
fn zero_capacity_never_fits() {
    let mut p = pool("p", "n1", u64::MAX);
    p.total_bytes = 0;
    assert_eq!(room(&p), 0);
    assert!(!fits(&p, 0));
}

#[test]
fn widest_inputs_do_not_overflow() {
    let mut p = pool("p", "n1", u64::MAX);
    p.total_bytes = u64::MAX;
    p.overcommit_percent = 1000;
    p.reserve_percent = 0;
    assert_eq!(room(&p), u64::MAX);
    assert!(fits(&p, u64::MAX));
    p.reserve_percent = 50;
    assert_eq!(room(&p), u64::MAX - u64::MAX.div_ceil(2));
}

#[test]
fn promise_alone_ignores_free() {
    let mut p = pool("p", "n1", 0);
    p.committed_bytes = 90 * GIB;
    assert_eq!(promise(&p), 5 * GIB);
    assert_eq!(room(&p), 0);
}

#[test]
fn a_pool_holding_the_data_is_not_gated() {
    // A 60 GiB volume and its snapshot fill a 100 GiB pool: a 60 GiB
    // restore fails both bounds, yet it can only go there.
    let mut p = pool("p", "n1", 0);
    p.committed_bytes = 60 * GIB;
    let mut req = request(1);
    req.size_bytes = 60 * GIB;
    req.pool = Some("p".into());
    req.node = Some("n1".into());
    assert!(matches!(
        place(&req, &[p.clone()]),
        Outcome::Insufficient { rejected, .. } if rejected[0].reason == Reason::NoCapacity
    ));
    req.holds_data = true;
    assert!(matches!(place(&req, &[p.clone()]), Outcome::Placed { .. }));
    // Only a pinned pool can hold the data; the flag alone exempts nothing.
    req.pool = None;
    assert!(matches!(place(&req, &[p]), Outcome::Insufficient { .. }));
}
