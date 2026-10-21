//! Snapshot: a thin snapshot of a Volume on every diskful node.
//!
//! The controller drives the rounds through `status.phase`; each agent acts
//! on the phase for its node and records an ack. Suspending only exists for
//! DRBD volumes; a replicas 1 volume goes straight to Taking. Each agent
//! resumes IO on its own once the round is over or its own deadline passed.

use std::time::Duration;

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
// has() on both sides: a field-level `self == oldSelf` never runs on an
// add or a removal.
#[x_kube(validation = Rule::new("has(self.volumeUid) == has(oldSelf.volumeUid) && (!has(self.volumeUid) || self.volumeUid == oldSelf.volumeUid)").message("volumeUid is immutable"))]
pub struct SnapshotSpec {
    #[schemars(length(min = 1, max = 253))]
    #[x_kube(validation = Rule::new("self == oldSelf").message("volume is immutable"))]
    pub volume: String,
    /// The source Volume's uid, read live at CreateSnapshot. A Volume of
    /// that name with another uid, or a Snapshot without one, binds nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 36))]
    pub volume_uid: Option<String>,
}

impl Snapshot {
    /// This Snapshot is of `vol`: same name and the recorded uid.
    pub fn is_of(&self, vol: &crate::Volume) -> bool {
        self.spec.volume == kube::ResourceExt::name_any(vol)
            && self.spec.volume_uid.is_some()
            && self.spec.volume_uid == vol.metadata.uid
    }
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SnapshotPhase {
    /// Waiting for the volume to be Ready, in sync and connected everywhere,
    /// every agent reachable, and no other round on the volume.
    #[default]
    Pending,
    /// Every replica suspends DRBD IO; SUSPEND_BOUND.
    Suspending,
    /// Diskful replicas create the thin snapshot LV; TAKE_BOUND.
    Taking,
    Ready,
    Failed,
    /// Agents remove the snapshot LVs.
    Deleting,
    /// Any other value: one an older or newer version wrote, still stored.
    /// It decodes so one object never stops a watch; no round runs on it
    /// and it is never written back.
    #[serde(other, skip_serializing)]
    #[schemars(skip)]
    Unknown,
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
    /// The source Volume's replicas, written on the way into Taking: the
    /// source is often gone by the time a restore asks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_replicas: Option<u32>,
    /// When the current phase began; the round bounds (Suspending, Taking)
    /// count from it and survive a controller restart. Agents never read
    /// it: a wall clock of another node is no input to their deadlines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_since: Option<Time>,
    /// One entry per replica of the assignment, diskless ones included,
    /// written by that node's agent.
    #[serde(default)]
    #[x_kube(merge_strategy = ListMerge::Map(vec!["node".into()]))]
    pub acks: Vec<SnapshotAck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

// Each agent measures its own suspend on its own monotonic clock. The
// suspends of one round lie within SUSPEND_BOUND of each other plus skew,
// and each take comes at most TAKE_WINDOW after its node's suspend, so
// RESUME_AFTER > TAKE_WINDOW + SUSPEND_BOUND + CLOCK_MARGIN keeps every
// node frozen until every take of the round is done.

/// Controller: Suspending fails unless every replica acked within this of
/// phaseSince.
pub const SUSPEND_BOUND: Duration = Duration::from_secs(30);
/// Controller: Taking fails unless every diskful replica took within this
/// of phaseSince.
pub const TAKE_BOUND: Duration = Duration::from_secs(120);
/// Agent: takes only while its own suspend for the round is younger.
pub const TAKE_WINDOW: Duration = Duration::from_secs(120);
/// Agent: resumes once its own suspend is this old, whatever the round.
pub const RESUME_AFTER: Duration = Duration::from_secs(240);
/// Clock skew between controller runs that read one phaseSince.
pub const CLOCK_MARGIN: Duration = Duration::from_secs(30);

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
