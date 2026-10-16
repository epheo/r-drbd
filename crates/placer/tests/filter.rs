mod common;

use common::{GIB, names, pool, request};
use placer::{Outcome, PoolView, Reason, Request, place};

fn reasons(out: Outcome) -> Vec<(String, Reason)> {
    match out {
        Outcome::Insufficient { rejected, .. } => {
            rejected.into_iter().map(|r| (r.pool, r.reason)).collect()
        }
        other => panic!("expected Insufficient, got {other:?}"),
    }
}

fn only_reason(req: &Request, pool: PoolView) -> Reason {
    let got = reasons(place(req, &[pool]));
    assert_eq!(got.len(), 1);
    got[0].1
}

#[test]
fn every_reason_in_order() {
    // Each step fixes the first violation only, so the reported reason proves
    // it outranks every later one still violated.
    let mut req = request(3);
    req.pool = Some("p".to_string());
    req.node = Some("n1".to_string());
    req.existing = names(&["n2"]);

    let mut p = pool("q", "n2", 0);
    p.node_ready = false;
    p.available = false;
    p.tier = "slow".to_string();
    assert_eq!(only_reason(&req, p.clone()), Reason::NodeNotReady);
    p.node_ready = true;
    assert_eq!(only_reason(&req, p.clone()), Reason::PoolUnavailable);
    p.available = true;
    assert_eq!(only_reason(&req, p.clone()), Reason::WrongTier);
    p.tier = "fast".to_string();
    assert_eq!(only_reason(&req, p.clone()), Reason::NotThePinnedPool);
    p.name = "p".to_string();
    assert_eq!(only_reason(&req, p.clone()), Reason::NotThePinnedNode);
    // A node pin and an existing replica on another node cannot both apply
    // to one pool, so the last two reasons are checked without the node pin.
    req.node = None;
    assert_eq!(only_reason(&req, p.clone()), Reason::NodeAlreadyHolds);
    p.node = "n3".to_string();
    assert_eq!(only_reason(&req, p.clone()), Reason::NoCapacity);
    p.free_bytes = 50 * GIB;
    req.replicas = 2;
    assert!(matches!(place(&req, &[p]), Outcome::Placed { .. }));
}

#[test]
fn reasons_name_each_rejected_pool_once_in_input_order() {
    let mut req = request(3);
    req.tier = "fast".to_string();
    let mut a = pool("a", "n1", 50 * GIB);
    a.tier = "slow".to_string();
    let b = pool("b", "n2", 50 * GIB);
    let mut c = pool("c", "n3", 0);
    c.node_ready = false;
    let got = reasons(place(&req, &[a, b, c]));
    assert_eq!(
        got,
        vec![
            ("a".to_string(), Reason::WrongTier),
            ("c".to_string(), Reason::NodeNotReady),
        ]
    );
}

#[test]
fn tier_comparison_is_case_sensitive() {
    let req = request(1);
    let mut p = pool("p", "n1", 50 * GIB);
    p.tier = "Fast".to_string();
    assert_eq!(
        reasons(place(&req, &[p])),
        vec![("p".to_string(), Reason::WrongTier)]
    );
}

#[test]
fn node_pin() {
    let mut req = request(1);
    req.node = Some("n2".to_string());
    let pools = [pool("a", "n1", 90 * GIB), pool("b", "n2", 10 * GIB)];
    match place(&req, &pools) {
        Outcome::Placed { diskful, .. } => {
            assert_eq!(diskful.len(), 1);
            assert_eq!(diskful[0].node, "n2");
            assert_eq!(diskful[0].pool, "b");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn pool_pin() {
    let mut req = request(1);
    req.pool = Some("b".to_string());
    let pools = [pool("a", "n1", 90 * GIB), pool("b", "n1", 10 * GIB)];
    match place(&req, &pools) {
        Outcome::Placed { diskful, .. } => {
            assert_eq!(diskful[0].pool, "b");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn node_already_holding_is_skipped() {
    let mut req = request(2);
    req.existing = names(&["n1"]);
    let pools = [pool("a", "n1", 90 * GIB), pool("b", "n2", 10 * GIB)];
    match place(&req, &pools) {
        Outcome::Placed { diskful, .. } => {
            assert_eq!(diskful.len(), 1);
            assert_eq!(diskful[0].node, "n2");
        }
        other => panic!("{other:?}"),
    }
}
