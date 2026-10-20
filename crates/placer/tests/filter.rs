mod common;

use common::{GIB, pool, request};
use placer::{Chosen, Insufficient, PoolView, Reason, Request, place};

fn reasons(out: Result<Vec<Chosen>, Insufficient>) -> Vec<(String, Reason)> {
    out.expect_err("insufficient")
        .rejected
        .into_iter()
        .map(|r| (r.pool, r.reason))
        .collect()
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

    let mut p = pool("q", "n2", 0);
    p.live = false;
    p.available = false;
    p.tier = "slow".to_string();
    assert_eq!(only_reason(&req, p.clone()), Reason::NodeUnreachable);
    p.live = true;
    assert_eq!(only_reason(&req, p.clone()), Reason::PoolUnavailable);
    p.available = true;
    assert_eq!(only_reason(&req, p.clone()), Reason::WrongTier);
    p.tier = "fast".to_string();
    assert_eq!(only_reason(&req, p.clone()), Reason::NotThePinnedPool);
    p.name = "p".to_string();
    p.node = "n1".to_string();
    assert_eq!(only_reason(&req, p.clone()), Reason::NoCapacity);
    p.free_bytes = 50 * GIB;
    req.replicas = 1;
    assert!(place(&req, &[p]).is_ok());
}

#[test]
fn reasons_name_each_rejected_pool_once_in_input_order() {
    let mut req = request(3);
    req.tier = "fast".to_string();
    let mut a = pool("a", "n1", 50 * GIB);
    a.tier = "slow".to_string();
    let b = pool("b", "n2", 50 * GIB);
    let mut c = pool("c", "n3", 0);
    c.live = false;
    let got = reasons(place(&req, &[a, b, c]));
    assert_eq!(
        got,
        vec![
            ("a".to_string(), Reason::WrongTier),
            ("c".to_string(), Reason::NodeUnreachable),
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
    let diskful = place(&req, &pools).unwrap();
    assert_eq!(diskful.len(), 1);
    assert_eq!(diskful[0].node, "n2");
    assert_eq!(diskful[0].pool, "b");
}

#[test]
fn node_pin_holds_the_first_of_several_replicas() {
    let mut req = request(2);
    req.node = Some("n3".to_string());
    let pools = [
        pool("a", "n1", 90 * GIB),
        pool("b", "n2", 50 * GIB),
        pool("c", "n3", 10 * GIB),
    ];
    let nodes: Vec<String> = place(&req, &pools)
        .unwrap()
        .into_iter()
        .map(|c| c.node)
        .collect();
    assert_eq!(nodes, ["n3", "n1"]);
}

#[test]
fn node_pin_without_room_there_is_insufficient() {
    let mut req = request(2);
    req.node = Some("n3".to_string());
    let pools = [
        pool("a", "n1", 90 * GIB),
        pool("b", "n2", 50 * GIB),
        pool("c", "n3", 0),
    ];
    let got = place(&req, &pools).unwrap_err();
    assert_eq!(got.missing, 1);
    assert_eq!(reasons(Err(got)), [("c".to_string(), Reason::NoCapacity)]);
    // No pool at all on the pinned node: the same, nothing to name.
    let got = place(&req, &pools[..2]).unwrap_err();
    assert_eq!((got.missing, got.rejected.len()), (1, 0));
    // n1 could hold the other replica; with no pool at all both are missing.
    assert_eq!(place(&req, &pools[..1]).unwrap_err().missing, 1);
    assert_eq!(place(&req, &[]).unwrap_err().missing, 2);
}

#[test]
fn pool_pin() {
    let mut req = request(1);
    req.pool = Some("b".to_string());
    let pools = [pool("a", "n1", 90 * GIB), pool("b", "n1", 10 * GIB)];
    assert_eq!(place(&req, &pools).unwrap()[0].pool, "b");
}
