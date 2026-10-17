//! Pool: one thin pool on one node.

use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::{CustomResource, KubeSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::condition::Condition;
use crate::keys::{DEVICE_PATTERN, LVM_NAME_PATTERN};

/// Location fields are immutable: a pool is a fixed place on a fixed node.
/// The capacity policy may change at any time; it only gates new placements.
#[derive(CustomResource, KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq)]
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
// Rules on the spec, not the fields: a field rule sees neither the other
// fields nor whether the field was absent before.
#[x_kube(validation = Rule::new("has(self.devices) == has(oldSelf.devices)").message("devices mode is fixed at creation"))]
#[x_kube(validation = Rule::new("!has(self.devices) || !has(oldSelf.devices) || oldSelf.devices.all(d, d in self.devices)").message("devices is append only: removing a disk is not supported"))]
#[x_kube(validation = Rule::new("!has(self.chunkSize) || has(self.devices)").message("chunkSize needs devices"))]
#[x_kube(validation = Rule::new("!has(self.reclaimPolicy) || has(self.devices)").message("reclaimPolicy needs devices"))]
#[x_kube(validation = Rule::new("has(self.chunkSize) == has(oldSelf.chunkSize) && (!has(self.chunkSize) || string(self.chunkSize) == string(oldSelf.chunkSize))").message("chunkSize is immutable"))]
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
    /// Blank whole disks the agent builds `vg` and `thinPool` from.
    /// Absent: `vg` and `thinPool` already exist on the node.
    /// by-id paths only: kernel names like sdb change between boots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 64), inner(length(max = 300), regex(pattern = DEVICE_PATTERN)))]
    #[x_kube(merge_strategy = ListMerge::Set)]
    pub devices: Option<Vec<String>>,
    /// Thin pool chunk size, devices mode only, fixed at creation.
    /// Absent: 64Ki, set by the agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[x_kube(validation = Rule::new(CHUNK_SIZE_RULE).message("chunkSize must be a power of two from 64Ki to 1Gi"))]
    pub chunk_size: Option<Quantity>,
    /// What deleting the Pool does to the VG, devices mode only.
    /// Absent: Retain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reclaim_policy: Option<ReclaimPolicy>,
}

/// LVM's chunk range, as exact byte values: CEL has no bit operations.
const CHUNK_SIZE_RULE: &str = "isQuantity(string(self)) && quantity(string(self)).isInteger() && quantity(string(self)).asInteger() in [65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912, 1073741824]";

/// Chunk size when `chunkSize` is absent: small random writes, fstrim and
/// snapshots all work in chunks.
pub const DEFAULT_CHUNK_BYTES: u64 = 64 << 10;
pub const MIN_CHUNK_BYTES: u64 = 64 << 10;
pub const MAX_CHUNK_BYTES: u64 = 1 << 30;

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReclaimPolicy {
    /// The VG and thin pool stay; a new Pool of the same name adopts them.
    #[default]
    Retain,
    /// The agent removes the thin pool, the VG and the PV labels once the
    /// VG holds no other LV.
    Delete,
}

impl PoolSpec {
    pub fn reclaim(&self) -> ReclaimPolicy {
        self.reclaim_policy.unwrap_or_default()
    }
}

/// What the agent found on one listed device.
#[derive(
    JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord,
)]
pub enum DeviceState {
    /// The path does not resolve.
    Missing,
    /// A partition, dm, md, loop, drbd, zram, ram, nbd or optical device, or read-only.
    NotADisk,
    /// A PV of this Pool's VG.
    Member,
    /// Held: mounted, holders, or the exclusive open failed.
    Busy,
    /// A signature or a partition table; the message names it.
    NotEmpty,
    /// Usable: whole disk, no signature, not held.
    Blank,
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceStatus {
    pub path: String,
    pub state: DeviceState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
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

/// Capacity, Available, Released and devices come from the agent; `volumes`, `committedBytes`
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
    /// One row per `spec.devices` path, from the agent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["path".into()]))]
    pub devices: Vec<DeviceStatus>,
}
