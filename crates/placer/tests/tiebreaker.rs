mod common;

use common::{names, pool};
use placer::{PoolView, tiebreaker};

fn with(name: &str, node: &str, tier: &str) -> PoolView {
    let mut p = pool(name, node, 0);
    p.tier = tier.to_string();
    p
}

fn idle(_: &str) -> usize {
    0
}

#[test]
fn prefers_node_without_pool_of_the_tier() {
    let pools = [
        with("a", "n1", "fast"),
        with("b", "n2", "fast"),
        with("c", "n3", "fast"),
        with("m", "z-master", "slow"),
    ];
    let busy = |n: &str| if n == "z-master" { 99 } else { 0 };
    let got = tiebreaker(
        &names(&["n1", "n2"]),
        &names(&["n1", "n2", "n3", "z-master"]),
        &pools,
        "fast",
        &busy,
    );
    assert_eq!(got.as_deref(), Some("z-master"));
}

#[test]
fn node_without_any_pool_counts_as_no_pool_of_the_tier() {
    let pools = [with("a", "n1", "fast"), with("c", "n3", "fast")];
    let got = tiebreaker(
        &names(&["n1", "n2"]),
        &names(&["n3", "master"]),
        &pools,
        "fast",
        &idle,
    );
    assert_eq!(got.as_deref(), Some("master"));
}

#[test]
fn then_lowest_load() {
    // Pool-less nodes all tie on the tier: the load spreads them.
    let load = |n: &str| if n == "m1" { 3 } else { 1 };
    let got = tiebreaker(
        &names(&["n1", "n2"]),
        &names(&["m1", "m2"]),
        &[],
        "fast",
        &load,
    );
    assert_eq!(got.as_deref(), Some("m2"));
}

#[test]
fn then_name() {
    let pools = [with("a", "n4", "fast"), with("b", "n3", "fast")];
    let got = tiebreaker(
        &names(&["n1", "n2"]),
        &names(&["n4", "n3"]),
        &pools,
        "fast",
        &idle,
    );
    assert_eq!(got.as_deref(), Some("n3"));
}

#[test]
fn none_when_all_live_nodes_hold_a_replica() {
    let pools = [with("a", "n1", "fast"), with("b", "n2", "fast")];
    let got = tiebreaker(
        &names(&["n1", "n2"]),
        &names(&["n1", "n2"]),
        &pools,
        "fast",
        &idle,
    );
    assert_eq!(got, None);
    assert_eq!(
        tiebreaker(&names(&["n1"]), &[], &pools, "fast", &idle),
        None
    );
}
