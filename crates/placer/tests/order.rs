mod common;

use common::{GIB, pool, request};
use placer::{Chosen, Insufficient, place};

fn chosen(out: Result<Vec<Chosen>, Insufficient>) -> Vec<(String, String)> {
    out.expect("placed")
        .into_iter()
        .map(|Chosen { node, pool }| (node, pool))
        .collect()
}

fn pair(node: &str, pool: &str) -> (String, String) {
    (node.to_string(), pool.to_string())
}

#[test]
fn room_descending() {
    let pools = [
        pool("a", "n1", 10 * GIB),
        pool("b", "n2", 30 * GIB),
        pool("c", "n3", 20 * GIB),
    ];
    assert_eq!(
        chosen(place(&request(3), &pools)),
        vec![pair("n2", "b"), pair("n3", "c"), pair("n1", "a")]
    );
}

#[test]
fn equal_room_then_node_name() {
    let pools = [
        pool("a", "n3", 10 * GIB),
        pool("b", "n1", 10 * GIB),
        pool("c", "n2", 10 * GIB),
    ];
    assert_eq!(
        chosen(place(&request(3), &pools)),
        vec![pair("n1", "b"), pair("n2", "c"), pair("n3", "a")]
    );
}

#[test]
fn equal_room_same_node_then_pool_name() {
    let pools = [
        pool("z", "n1", 10 * GIB),
        pool("m", "n1", 10 * GIB),
        pool("q", "n2", 6 * GIB),
    ];
    assert_eq!(
        chosen(place(&request(2), &pools)),
        vec![pair("n1", "m"), pair("n2", "q")]
    );
}

#[test]
fn committed_bytes_count_not_only_free_bytes() {
    // a: room 15 GiB under its promise; b: room 89 GiB.
    let mut a = pool("a", "n1", 95 * GIB);
    a.committed_bytes = 80 * GIB;
    let b = pool("b", "n2", 94 * GIB);
    let mut req = request(1);
    req.size_bytes = 10 * GIB;
    assert_eq!(chosen(place(&req, &[a, b])), vec![pair("n2", "b")]);
}

#[test]
fn one_pool_per_node_even_when_its_second_pool_beats_others() {
    let pools = [
        pool("a", "n1", 50 * GIB),
        pool("b", "n1", 40 * GIB),
        pool("c", "n2", 10 * GIB),
    ];
    assert_eq!(
        chosen(place(&request(2), &pools)),
        vec![pair("n1", "a"), pair("n2", "c")]
    );
}

#[test]
fn deterministic_across_input_permutations() {
    let base = vec![
        pool("a", "n1", 10 * GIB),
        pool("b", "n2", 10 * GIB),
        pool("c", "n3", 20 * GIB),
        pool("d", "n2", 20 * GIB),
        pool("e", "n4", 10 * GIB),
    ];
    let expected = chosen(place(&request(3), &base));
    assert_eq!(
        expected,
        vec![pair("n2", "d"), pair("n3", "c"), pair("n1", "a")]
    );
    let mut pools = base;
    for shift in 0..pools.len() {
        pools.rotate_left(1);
        assert_eq!(
            chosen(place(&request(3), &pools)),
            expected,
            "shift {shift}"
        );
        pools.reverse();
        assert_eq!(
            chosen(place(&request(3), &pools)),
            expected,
            "reversed {shift}"
        );
    }
}
