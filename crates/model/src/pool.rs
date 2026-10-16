//! Pool: one thin pool on one node.

use kube::{CustomResource, KubeSchema};
use serde::{Deserialize, Serialize};

use crate::condition::Condition;
use crate::keys::LVM_NAME_PATTERN;

/// Location fields are immutable: a pool is a fixed place on a fixed node.
/// The capacity policy may change at any time; it only gates new placements.
#[derive(CustomResource, KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[kube(
    group = "r-drbd.io",
    version = "v1alpha1",
    kind = "Pool",
    plural = "pools",
    status = "PoolStatus",
    derive = "PartialEq",
    printcolumn = r#"{"name":"Node","type":"string","jsonPath":".spec.node"}"#,
    printcolumn = r#"{"name":"Tier","type":"string","jsonPath":".spec.tier"}"#,
    printcolumn = r#"{"name":"VG","type":"string","jsonPath":".spec.vg"}"#,
    printcolumn = r#"{"name":"Thin pool","type":"string","jsonPath":".spec.thinPool"}"#,
    printcolumn = r#"{"name":"Free","type":"integer","jsonPath":".status.freeBytes"}"#,
    printcolumn = r#"{"name":"Committed","type":"integer","jsonPath":".status.committedBytes"}"#,
    printcolumn = r#"{"name":"Placeable","type":"integer","jsonPath":".status.placeableBytes"}"#,
    printcolumn = r#"{"name":"Available","type":"string","jsonPath":".status.conditions[?(@.type==\"Available\")].status"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PoolSpec {
    #[schemars(length(min = 1, max = 253))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("node is immutable"))]
    pub node: String,
    #[schemars(length(min = 1, max = 253))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("tier is immutable"))]
    pub tier: String,
    /// LVM names only (keys::LVM_NAME_PATTERN): anything else would reach
    /// lvm commands as an option or a path.
    #[schemars(length(min = 1, max = 127), regex(pattern = LVM_NAME_PATTERN))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("vg is immutable"))]
    #[x_kube(validation = Rule::new("self != '.' && self != '..'").message("vg is not a name"))]
    pub vg: String,
    #[schemars(length(min = 1, max = 127), regex(pattern = LVM_NAME_PATTERN))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("thinPool is immutable"))]
    #[x_kube(validation = Rule::new("self != '.' && self != '..'").message("thinPool is not a name"))]
    pub thin_pool: String,
    /// Committed volume sizes may reach this percent of the pool size less
    /// the reserve. 100 never promises more than the pool holds.
    // Below 100 would do the reserve's job a second time.
    // Above 1000 the bound no longer limits anything, and a stray zero would
    // switch the guard off unnoticed.
    #[serde(default = "default_overcommit_percent")]
    #[schemars(range(min = 100, max = 1000))]
    pub overcommit_percent: u32,
    /// Percent of the pool size that placement never plans to use.
    // Above 50 the pool is mostly unusable: a typo, not a policy.
    #[serde(default = "default_reserve_percent")]
    #[schemars(range(max = 50))]
    pub reserve_percent: u32,
}

/// A full thin pool stalls or fails writes on every volume in it, so
/// overcommit is an opt-in.
pub const DEFAULT_OVERCOMMIT_PERCENT: u32 = 100;

/// Room the committed sizes do not cover: snapshot copy-on-write, DRBD
/// metadata above the net size, and time to grow the pool after an alert.
/// A percent, not bytes, so it stays right after the admin extends the pool.
pub const DEFAULT_RESERVE_PERCENT: u32 = 5;

fn default_overcommit_percent() -> u32 {
    DEFAULT_OVERCOMMIT_PERCENT
}

fn default_reserve_percent() -> u32 {
    DEFAULT_RESERVE_PERCENT
}

/// Capacity and Available come from the agent; `volumes`, `committedBytes`
/// and `placeableBytes` from the controller.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PoolStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_bytes: Option<u64>,
    /// Thin pool chunk size; feeds rs-discard-granularity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_bytes: Option<u64>,
    /// Sum of the sizes of the Volumes with a diskful replica here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_bytes: Option<u64>,
    /// Largest new volume placement accepts here now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeable_bytes: Option<u64>,
    /// Names of Volumes with a diskful replica here; the finalizer waits on it.
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Set)]
    pub volumes: Vec<String>,
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["type".into()]))]
    pub conditions: Vec<Condition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}
