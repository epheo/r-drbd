mod common;

use common::{GIB, pool};
use placer::{THIN_OVERSUBSCRIPTION_RATIO, fits};

#[test]
fn free_side_boundary() {
    let mut p = pool("p", "n1", GIB);
    p.total_bytes = 100 * GIB;
    assert!(fits(&p, 20 * GIB));
    assert!(!fits(&p, 20 * GIB + 1));
}

#[test]
fn total_side_boundary_with_reserved() {
    let mut p = pool("p", "n1", 10 * GIB);
    p.total_bytes = 10 * GIB;
    p.reserved_bytes = 150 * GIB;
    assert!(fits(&p, 50 * GIB));
    assert!(!fits(&p, 50 * GIB + 1));
}

#[test]
fn reserved_beyond_ratio_never_fits() {
    let mut p = pool("p", "n1", 10 * GIB);
    p.total_bytes = 10 * GIB;
    p.reserved_bytes = 10 * GIB * THIN_OVERSUBSCRIPTION_RATIO;
    assert!(!fits(&p, 1));
    p.reserved_bytes = u64::MAX;
    assert!(!fits(&p, 0));
}

#[test]
fn thin_minimum_free_space() {
    // total 100: minimum is ceil(100 / 10) = 10; free 0 gives estimate 0, free 1 gives 20.
    let mut p = pool("p", "n1", 0);
    p.total_bytes = 100;
    p.free_bytes = 0;
    assert!(!fits(&p, 0));
    p.free_bytes = 1;
    assert!(fits(&p, 0));
    assert!(fits(&p, 20));
    assert!(!fits(&p, 21));
}

#[test]
fn thin_minimum_binds_while_size_fits() {
    // total 100, minimum 10; reserved pins the estimate at 9 then 10 for a size of 5.
    let mut p = pool("p", "n1", 100);
    p.total_bytes = 100;
    p.reserved_bytes = 100 * 20 - 9;
    assert!(!fits(&p, 5));
    p.reserved_bytes = 100 * 20 - 10;
    assert!(fits(&p, 5));
}

#[test]
fn thin_minimum_rounds_up() {
    // ceil(101 / 10) = 11; reserved drives the estimate to 10 then 11.
    let mut p = pool("p", "n1", 101);
    p.total_bytes = 101;
    p.reserved_bytes = 101 * 20 - 10;
    assert!(!fits(&p, 0));
    p.reserved_bytes = 101 * 20 - 11;
    assert!(fits(&p, 0));
}

#[test]
fn zero_capacity_never_fits() {
    let mut p = pool("p", "n1", 0);
    p.total_bytes = 0;
    assert!(!fits(&p, 0));
    p.free_bytes = u64::MAX;
    assert!(!fits(&p, 0));
}

#[test]
fn saturates_instead_of_overflowing() {
    let mut p = pool("p", "n1", u64::MAX);
    p.total_bytes = u64::MAX;
    assert!(fits(&p, u64::MAX));
    p.reserved_bytes = 1;
    assert!(!fits(&p, u64::MAX));
    assert!(fits(&p, u64::MAX - 1));
}
