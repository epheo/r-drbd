mod common;

use common::{GIB, pool, request};
use placer::{Chosen, Insufficient, Reason, Rejected, place};

fn c(node: &str, pool: &str) -> Chosen {
    Chosen {
        node: node.to_string(),
        pool: pool.to_string(),
    }
}

fn rej(node: &str, pool: &str, reason: Reason) -> Rejected {
    Rejected {
        node: node.to_string(),
        pool: pool.to_string(),
        reason,
    }
}

#[test]
fn one_replica() {
    let pools = [pool("a", "n1", 10 * GIB), pool("b", "n2", 20 * GIB)];
    assert_eq!(place(&request(1), &pools), Ok(vec![c("n2", "b")]));
}

#[test]
fn two_replicas_on_three_nodes() {
    let pools = [
        pool("a", "n1", 10 * GIB),
        pool("b", "n2", 20 * GIB),
        pool("c", "n3", 30 * GIB),
    ];
    assert_eq!(
        place(&request(2), &pools),
        Ok(vec![c("n3", "c"), c("n2", "b")])
    );
}

#[test]
fn insufficient_nodes_gives_no_partial_result() {
    let mut full = pool("c", "n3", 0);
    full.free_bytes = 0;
    let pools = [pool("a", "n1", 10 * GIB), pool("b", "n1", 20 * GIB), full];
    assert_eq!(
        place(&request(2), &pools),
        Err(Insufficient {
            missing: 1,
            rejected: vec![rej("n3", "c", Reason::NoCapacity)],
        })
    );
}

#[test]
fn no_pools_is_insufficient_with_no_reasons() {
    assert_eq!(
        place(&request(1), &[]),
        Err(Insufficient {
            missing: 1,
            rejected: Vec::new()
        })
    );
}

#[test]
fn size_too_large_is_no_capacity() {
    let mut req = request(1);
    req.size_bytes = u64::MAX;
    assert_eq!(
        place(&req, &[pool("a", "n1", 10 * GIB)]),
        Err(Insufficient {
            missing: 1,
            rejected: vec![rej("n1", "a", Reason::NoCapacity)],
        })
    );
}

#[test]
fn zero_replicas_places_nothing() {
    assert_eq!(
        place(&request(0), &[pool("a", "n1", 10 * GIB)]),
        Ok(Vec::new())
    );
}

#[test]
fn huge_replicas_is_insufficient_without_abort() {
    let pools = [pool("a", "n1", 10 * GIB), pool("b", "n2", 20 * GIB)];
    assert_eq!(
        place(&request(u32::MAX), &pools),
        Err(Insufficient {
            missing: u32::MAX - 2,
            rejected: Vec::new(),
        })
    );
}

#[test]
fn four_replicas() {
    let pools = [
        pool("a", "n1", 50 * GIB),
        pool("b", "n2", 40 * GIB),
        pool("c", "n3", 30 * GIB),
        pool("d", "n4", 20 * GIB),
        pool("e", "n5", 10 * GIB),
    ];
    assert_eq!(
        place(&request(4), &pools),
        Ok(vec![c("n1", "a"), c("n2", "b"), c("n3", "c"), c("n4", "d")])
    );
}

#[test]
fn shortage_of_distinct_nodes_reports_missing_without_rejections() {
    let pools = [pool("a", "n1", 20 * GIB), pool("b", "n1", 10 * GIB)];
    assert_eq!(
        place(&request(2), &pools),
        Err(Insufficient {
            missing: 1,
            rejected: Vec::new(),
        })
    );
}
