//! Pool: one thin pool on one node.

use kube::{CustomResource, KubeSchema};
use serde::{Deserialize, Serialize};

use crate::condition::Condition;
use crate::keys::LVM_NAME_PATTERN;

/// Spec fields are immutable: a pool is a fixed place on a fixed node.
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
}

/// Capacity and Available come from the agent; `volumes` from the controller.
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
