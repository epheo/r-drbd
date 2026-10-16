mod common;

use common::{GIB, names, pool, request};
use placer::{Chosen, Outcome, Reason, Rejected, place};

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
fn one_replica_has_no_tiebreaker() {
    let pools = [pool("a", "n1", 10 * GIB), pool("b", "n2", 20 * GIB)];
    assert_eq!(
        place(&request(1), &pools),
        Outcome::Placed {
            diskful: vec![c("n2", "b")],
            tiebreaker: None,
        }
    );
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
        Outcome::Placed {
            diskful: vec![c("n3", "c"), c("n2", "b")],
            tiebreaker: Some("n1".to_string()),
        }
    );
}

#[test]
fn two_replicas_with_one_existing() {
    let mut req = request(2);
    // n1 sorts first, so a tiebreaker on n3 proves the existing node is excluded.
    req.existing = names(&["n1"]);
    let pools = [
        pool("a", "n1", 30 * GIB),
        pool("b", "n2", 20 * GIB),
        pool("c", "n3", 10 * GIB),
    ];
    assert_eq!(
        place(&req, &pools),
        Outcome::Placed {
            diskful: vec![c("n2", "b")],
            tiebreaker: Some("n3".to_string()),
        }
    );
}

#[test]
fn three_replicas_have_no_tiebreaker() {
    let pools = [
        pool("a", "n1", 10 * GIB),
        pool("b", "n2", 20 * GIB),
        pool("c", "n3", 30 * GIB),
        pool("d", "n4", 5 * GIB),
    ];
    match place(&request(3), &pools) {
        Outcome::Placed { tiebreaker, .. } => assert_eq!(tiebreaker, None),
        other => panic!("{other:?}"),
    }
}

#[test]
fn two_replicas_on_two_nodes_have_no_tiebreaker_candidate() {
    let pools = [pool("a", "n1", 10 * GIB), pool("b", "n2", 20 * GIB)];
    assert_eq!(
        place(&request(2), &pools),
        Outcome::Placed {
            diskful: vec![c("n2", "b"), c("n1", "a")],
            tiebreaker: None,
        }
    );
}

#[test]
fn tiebreaker_comes_from_ready_nodes_of_any_tier() {
    let mut master = pool("m", "master", 0);
    master.tier = "slow".to_string();
    let mut down = pool("d", "down", 0);
    down.node_ready = false;
    let pools = [
        pool("a", "n1", 10 * GIB),
        pool("b", "n2", 20 * GIB),
        pool("c", "n3", 5 * GIB),
        down,
        master,
    ];
    match place(&request(2), &pools) {
        Outcome::Placed { tiebreaker, .. } => {
            assert_eq!(tiebreaker, Some("master".to_string()));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn insufficient_nodes_gives_no_partial_result() {
    let mut full = pool("c", "n3", 0);
    full.free_bytes = 0;
    let pools = [pool("a", "n1", 10 * GIB), pool("b", "n1", 20 * GIB), full];
    assert_eq!(
        place(&request(2), &pools),
        Outcome::Insufficient {
            missing: 1,
            rejected: vec![rej("n3", "c", Reason::NoCapacity)],
        }
    );
}

#[test]
fn no_pools_is_insufficient_with_no_reasons() {
    assert_eq!(
        place(&request(1), &[]),
        Outcome::Insufficient {
            missing: 1,
            rejected: Vec::new()
        }
    );
}

#[test]
fn existing_satisfying_replicas_is_already_placed() {
    let mut req = request(2);
    req.existing = names(&["n1", "n2"]);
    assert_eq!(place(&req, &[]), Outcome::AlreadyPlaced);
    req.existing = names(&["n1", "n2", "n3"]);
    assert_eq!(place(&req, &[]), Outcome::AlreadyPlaced);
}

#[test]
fn size_too_large_is_no_capacity() {
    let mut req = request(1);
    req.size_bytes = u64::MAX;
    assert_eq!(
        place(&req, &[pool("a", "n1", 10 * GIB)]),
        Outcome::Insufficient {
            missing: 1,
            rejected: vec![rej("n1", "a", Reason::NoCapacity)],
        }
    );
}

#[test]
fn zero_replicas_is_already_placed() {
    assert_eq!(
        place(&request(0), &[pool("a", "n1", 10 * GIB)]),
        Outcome::AlreadyPlaced
    );
}

#[test]
fn huge_replicas_is_insufficient_without_abort() {
    let pools = [pool("a", "n1", 10 * GIB), pool("b", "n2", 20 * GIB)];
    assert_eq!(
        place(&request(u32::MAX), &pools),
        Outcome::Insufficient {
            missing: u32::MAX - 2,
            rejected: Vec::new(),
        }
    );
}

#[test]
fn duplicate_existing_counts_once() {
    let pools = [
        pool("a", "n1", 30 * GIB),
        pool("b", "n2", 20 * GIB),
        pool("c", "n3", 10 * GIB),
    ];
    let mut req = request(2);
    req.existing = names(&["n1", "n1"]);
    assert_eq!(
        place(&req, &pools),
        Outcome::Placed {
            diskful: vec![c("n2", "b")],
            tiebreaker: Some("n3".to_string()),
        }
    );
    req.replicas = 3;
    assert_eq!(
        place(&req, &pools),
        Outcome::Placed {
            diskful: vec![c("n2", "b"), c("n3", "c")],
            tiebreaker: None,
        }
    );
}

#[test]
fn four_replicas_get_a_tiebreaker() {
    let pools = [
        pool("a", "n1", 50 * GIB),
        pool("b", "n2", 40 * GIB),
        pool("c", "n3", 30 * GIB),
        pool("d", "n4", 20 * GIB),
        pool("e", "n5", 10 * GIB),
    ];
    assert_eq!(
        place(&request(4), &pools),
        Outcome::Placed {
            diskful: vec![c("n1", "a"), c("n2", "b"), c("n3", "c"), c("n4", "d")],
            tiebreaker: Some("n5".to_string()),
        }
    );
}

#[test]
fn existing_node_absent_from_pools_still_counts() {
    // The existing node has no pool view (for example it is down), yet it
    // makes the count even and must not become the tiebreaker.
    let mut req = request(2);
    req.existing = names(&["n0"]);
    let pools = [pool("a", "n1", 20 * GIB), pool("b", "n2", 10 * GIB)];
    assert_eq!(
        place(&req, &pools),
        Outcome::Placed {
            diskful: vec![c("n1", "a")],
            tiebreaker: Some("n2".to_string()),
        }
    );
}

#[test]
fn shortage_of_distinct_nodes_reports_missing_without_rejections() {
    let pools = [pool("a", "n1", 20 * GIB), pool("b", "n1", 10 * GIB)];
    assert_eq!(
        place(&request(2), &pools),
        Outcome::Insufficient {
            missing: 1,
            rejected: Vec::new(),
        }
    );
}
