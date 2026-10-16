mod common;

use common::{names, pool};
use placer::{PoolView, tiebreaker};

fn with(name: &str, node: &str, tier: &str, volumes: u32) -> PoolView {
    let mut p = pool(name, node, 0);
    p.tier = tier.to_string();
    p.volume_count = volumes;
    p
}

#[test]
fn prefers_node_without_pool_of_the_tier() {
    let pools = [
        with("a", "n1", "fast", 0),
        with("b", "n2", "fast", 0),
        with("c", "n3", "fast", 0),
        with("m", "z-master", "slow", 99),
    ];
    let got = tiebreaker(
        &names(&["n1", "n2"]),
        &names(&["n1", "n2", "n3", "z-master"]),
        &pools,
        "fast",
    );
    assert_eq!(got.as_deref(), Some("z-master"));
}

#[test]
fn node_without_any_pool_counts_as_no_pool_of_the_tier() {
    let pools = [with("a", "n1", "fast", 0), with("c", "n3", "fast", 0)];
    let got = tiebreaker(
        &names(&["n1", "n2"]),
        &names(&["n3", "master"]),
        &pools,
        "fast",
    );
    assert_eq!(got.as_deref(), Some("master"));
}

#[test]
fn then_fewest_volumes_summed_over_pools() {
    let pools = [
        with("a", "n3", "fast", 2),
        with("b", "n3", "fast", 2),
        with("c", "n4", "fast", 3),
    ];
    let got = tiebreaker(&names(&["n1", "n2"]), &names(&["n3", "n4"]), &pools, "fast");
    assert_eq!(got.as_deref(), Some("n4"));
}

#[test]
fn then_name() {
    let pools = [with("a", "n4", "fast", 1), with("b", "n3", "fast", 1)];
    let got = tiebreaker(&names(&["n1", "n2"]), &names(&["n4", "n3"]), &pools, "fast");
    assert_eq!(got.as_deref(), Some("n3"));
}

#[test]
fn none_when_all_ready_nodes_hold_a_replica() {
    let pools = [with("a", "n1", "fast", 0), with("b", "n2", "fast", 0)];
    let got = tiebreaker(&names(&["n1", "n2"]), &names(&["n1", "n2"]), &pools, "fast");
    assert_eq!(got, None);
    assert_eq!(tiebreaker(&names(&["n1"]), &[], &pools, "fast"), None);
}

#[test]
fn volume_sum_does_not_overflow() {
    let pools = [
        with("a", "n3", "fast", u32::MAX),
        with("b", "n3", "fast", u32::MAX),
        with("c", "n4", "fast", u32::MAX),
    ];
    let got = tiebreaker(&names(&["n1", "n2"]), &names(&["n3", "n4"]), &pools, "fast");
    assert_eq!(got.as_deref(), Some("n4"));
}
