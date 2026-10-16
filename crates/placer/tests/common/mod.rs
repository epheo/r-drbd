#![allow(dead_code)]

use placer::{PoolView, Request};

pub(crate) const GIB: u64 = 1 << 30;

pub(crate) fn pool(name: &str, node: &str, free: u64) -> PoolView {
    PoolView {
        name: name.to_string(),
        node: node.to_string(),
        tier: "fast".to_string(),
        node_ready: true,
        available: true,
        total_bytes: 100 * GIB,
        free_bytes: free,
        committed_bytes: 0,
        overcommit_percent: 100,
        reserve_percent: 5,
        volume_count: 0,
    }
}

pub(crate) fn request(replicas: u32) -> Request {
    Request {
        size_bytes: GIB,
        replicas,
        tier: "fast".to_string(),
        pool: None,
        node: None,
        existing: Vec::new(),
        holds_data: false,
    }
}

pub(crate) fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(ToString::to_string).collect()
}
