//! Snapshot: a thin snapshot of a Volume on every diskful node.
//!
//! The controller drives the rounds through `status.phase`; each agent acts
//! on the phase for its node and records an ack. Suspending and Resuming
//! only exist for DRBD volumes; a replicas 1 volume goes straight to Taking.

use kube::{CustomResource, KubeSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::condition::Condition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

#[derive(CustomResource, KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[kube(
    group = "r-drbd.io",
    version = "v1alpha1",
    kind = "Snapshot",
    plural = "snapshots",
    status = "SnapshotStatus",
    derive = "PartialEq",
    printcolumn = r#"{"name":"Volume","type":"string","jsonPath":".spec.volume"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Size","type":"integer","jsonPath":".status.sizeBytes"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSpec {
    #[schemars(length(min = 1, max = 253))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("volume is immutable"))]
    pub volume: String,
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SnapshotPhase {
    /// Waiting for the volume to be Ready with every diskful replica UpToDate.
    #[default]
    Pending,
    /// Agents suspend DRBD IO.
    Suspending,
    Suspended,
    /// Agents create the thin snapshot LV; 2 minute bound.
    Taking,
    Taken,
    /// Agents resume IO.
    Resuming,
    Ready,
    Failed,
    /// Agents remove the snapshot LVs.
    Deleting,
}

#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStatus {
    #[serde(default)]
    pub phase: SnapshotPhase,
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["type".into()]))]
    pub conditions: Vec<Condition>,
    /// Volume size at the time of the snapshot; restores start from it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// The volume's fileSystem marker at Taking; a restored volume inherits it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 16))]
    pub file_system: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<Time>,
    /// When the current phase began; the round bounds (Suspending, Taking)
    /// count from it and survive a controller restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_since: Option<Time>,
    /// One entry per diskful node, written by that node's agent.
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["node".into()]))]
    pub acks: Vec<SnapshotAck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// The last phase a node completed, and whether it succeeded. `pool` is
/// where the snapshot LV lives, so restore and delete still find it after
/// the Volume is gone.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotAck {
    #[schemars(length(min = 1, max = 253))]
    pub node: String,
    pub phase: SnapshotPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 253))]
    pub pool: Option<String>,
    #[serde(default)]
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
