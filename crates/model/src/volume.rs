//! Volume: one block device, local or DRBD replicated.
//!
//! Name = CSI volume id (pvc-<uuid>), which is also the LV name and the
//! DRBD resource name on every node; the controller rejects names outside
//! keys::NAME_PATTERN (a CEL rule on metadata.name blows the cost budget).

use std::collections::BTreeMap;

use kube::{CustomResource, KubeSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::condition::Condition;

#[derive(CustomResource, KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[kube(
    group = "r-drbd.io",
    version = "v1alpha1",
    kind = "Volume",
    plural = "volumes",
    status = "VolumeStatus",
    derive = "PartialEq",
    printcolumn = r#"{"name":"Size","type":"integer","jsonPath":".spec.sizeBytes"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Tier","type":"string","jsonPath":".spec.tier"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Nodes","type":"string","jsonPath":".status.nodes"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
#[x_kube(validation = Rule::new("!has(oldSelf.assignment) || has(self.assignment)").message("assignment cannot be removed"))]
#[x_kube(validation = Rule::new("self.accessMode != 'ReadWriteMany' || self.replicas >= 2").message("ReadWriteMany needs replicas >= 2"))]
pub struct VolumeSpec {
    /// Usable size as the CSI caller sees it; the LV is larger by the DRBD metadata.
    #[schemars(range(min = 1))]
    #[x_kube(validation = Rule::new("self >= oldSelf").message("sizeBytes cannot shrink"))]
    pub size_bytes: u64,
    /// 1 is a plain LV with no DRBD; 2 or 3 are DRBD with quorum and, for 2, a tiebreaker.
    #[serde(default = "default_replicas")]
    #[schemars(range(min = 1, max = 3))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("replicas is immutable"))]
    pub replicas: u32,
    #[schemars(length(min = 1, max = 63))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("tier is immutable"))]
    pub tier: String,
    /// Placement pins, read once at assignment time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 253))]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 253))]
    pub node: Option<String>,
    /// ReadWriteMany lets more than one node open the device at once (two
    /// DRBD Primaries, block mode only, replicas 2 or 3); the default allows
    /// one attachment at a time.
    #[serde(default)]
    #[x_kube(validation = Rule::new("self == oldSelf").message("accessMode is immutable"))]
    pub access_mode: AccessMode,
    /// Nodes that must see the device (ControllerPublish). A node without a
    /// replica gets a diskless one appended to the assignment.
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Set)]
    pub attachments: Vec<String>,
    /// Nodes whose replica must be placed elsewhere (a dead or retired
    /// node). The controller honours one node at a time, only once
    /// status.initializedAt is set, no resolve is pending and another diskful
    /// replica is UpToDate; a node listed here never receives a diskless
    /// entry either. A node leaves the list once it holds no entry and no
    /// attachment. The replaced node keeps its LV as an orphan.
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Set)]
    pub evict: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[x_kube(validation = Rule::new("self == oldSelf").message("source is immutable"))]
    pub source: Option<VolumeSource>,
    /// Split-brain resolution: every diskful replica except the
    /// survivor is discarded and resynced from it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolve: Option<Resolve>,
    /// Written once by the controller; only diskless entries come and go
    /// with attachments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment: Option<Assignment>,
}

fn default_replicas() -> u32 {
    1
}

/// CSI access modes collapse to two: every SINGLE_NODE_* mode is
/// ReadWriteOnce, every MULTI_NODE_* mode is ReadWriteMany.
#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AccessMode {
    #[default]
    ReadWriteOnce,
    ReadWriteMany,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSource {
    /// Name of a Ready Snapshot; the new volume is a writable thin snapshot of it.
    #[schemars(length(min = 1, max = 253))]
    pub snapshot: String,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Resolve {
    /// Node whose data wins.
    #[schemars(length(min = 1, max = 253))]
    pub survivor: String,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    #[schemars(length(max = 32))]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["node".into()]))]
    pub replicas: Vec<AssignedReplica>,
    /// DRBD minor, cluster wide unique, same on every node; absent for replicas 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[x_kube(validation = Rule::new("self == oldSelf").message("minor is immutable"))]
    pub minor: Option<u32>,
    /// Shared secret, 32 random bytes in Base64; absent for replicas 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("secret is immutable"))]
    pub secret: Option<String>,
    /// The node that makes the first data current: it starts the volume
    /// without a sync, or, for a restore, is the sync source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 253))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("initialUpToDateNode is immutable"))]
    pub initial_up_to_date_node: Option<String>,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AssignedReplica {
    #[schemars(length(min = 1, max = 253))]
    pub node: String,
    /// Pool holding the LV; absent for diskless and tiebreaker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 253))]
    pub pool: Option<String>,
    /// DRBD node id, 0 to 31, unique within the volume.
    #[schemars(range(max = 31))]
    pub node_id: u8,
    /// DRBD listen port on this node; absent for replicas 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    pub role: Role,
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Diskful,
    Diskless,
    Tiebreaker,
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VolumePhase {
    /// No assignment yet.
    #[default]
    Pending,
    /// Assigned; waiting for every replica to be present and in sync.
    Provisioning,
    Ready,
    /// Usable, but a replica is missing, out of date or unknown.
    Degraded,
    /// Placement impossible with the current pools; retried on pool changes.
    Failed,
    Deleting,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeStatus {
    #[serde(default)]
    pub phase: VolumePhase,
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["type".into()]))]
    pub conditions: Vec<Condition>,
    /// Net size every replica currently provides (after a resize, the old size until all grew).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_bytes: Option<u64>,
    /// Node where DRBD is Primary, or where the LV is open for replicas 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,
    /// One entry per node, written by that node's agent.
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["node".into()]))]
    pub replicas: Vec<ReplicaStatus>,
    /// Comma separated diskful nodes, for kubectl.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodes: Option<String>,
    /// Set by the controller the first time the volume is Ready (every
    /// diskful replica UpToDate and connected), which is before any consumer
    /// could write. From then on a replica created on any node (a lost LV, a
    /// node from spec.evict) gets just-created DRBD metadata and a full sync
    /// from its peers; the initial-node start-up never runs again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialized_at: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
    /// File system the node plugin created (or inherited from a snapshot),
    /// written before the first mount. An XFS signature without this
    /// marker is adopted, never formatted, unless it still carries the
    /// mkfs-time label, which proves a format that was cut short.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 16))]
    pub file_system: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// What one agent sees of its replica. `observed_generation` is the spec
/// generation the agent last applied; the controller gates resize and
/// delete on it.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStatus {
    pub node: String,
    /// LV exists and, for DRBD, the resource is configured.
    #[serde(default)]
    pub present: bool,
    /// DRBD disk state (UpToDate, Inconsistent, Diskless, ...); "UpToDate" for replicas 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_state: Option<String>,
    /// Whole percent while SyncTarget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_percent: Option<u32>,
    /// Peer node name to connection state.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub peers: BTreeMap<String, String>,
    /// Primary or opened.
    #[serde(default)]
    pub in_use: bool,
    /// DRBD quorum as this replica sees it; absent for replicas 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quorum: Option<bool>,
    /// /dev/drbd<minor>, or the LV path for replicas 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_path: Option<String>,
    /// LV size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated_bytes: Option<u64>,
    /// Net size of the device as seen on this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}
