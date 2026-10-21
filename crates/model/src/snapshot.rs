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

    /// Its status was lost, whole or in part: the finalizer without a
    /// phase (the controller writes the first status before its
    /// finalizer), or Ready without the size every Ready has.
    pub fn status_lost(&self) -> bool {
        let st = self.status.as_ref();
        match st.and_then(|st| st.phase) {
            None => kube::ResourceExt::finalizers(self)
                .iter()
                .any(|f| f == crate::keys::FINALIZER_SNAPSHOT),
            Some(SnapshotPhase::Ready) => st.is_some_and(|st| st.size_bytes.is_none()),
            Some(_) => false,
        }
    }
}

#[derive(JsonSchema, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotPhase {
    /// Waiting for the volume to be Ready, in sync and connected everywhere,
    /// every agent reachable, and no other round on the volume.
    Pending,
    /// Every replica suspends DRBD IO; SUSPEND_BOUND.
    Suspending,
    /// Diskful replicas create the thin snapshot LV; TAKE_BOUND.
    Taking,
    Ready,
    Failed,
    /// Agents remove the snapshot LVs.
    Deleting,
    /// The status was lost: every node with a Pool searches its Pools for
    /// this Snapshot's LV by its uid tag. Ready or Lost follows, never a
    /// round.
    Discovering,
    /// The status was lost and no copy was found, or the copies' tags
    /// disagree. Never restored, never taken again; the copies stay.
    Lost,
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
    /// No default: absent reads as absent, never as Pending. The controller
    /// writes the first status before its finalizer, so the finalizer
    /// without a phase means a lost status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<SnapshotPhase>,
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
    /// Every holder's digest matches, written once all holders acked one;
    /// absent while pending. Only true lets a restore start without a sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copies_identical: Option<bool>,
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
/// Holder digests: the unit each holder hashes its snapshot LV in.
pub const DIGEST_CHUNK: usize = 4 << 20;
/// Agent: how long a restore's first data waits for pending digests
/// before it starts as the sync source.
pub const DIGEST_WAIT: Duration = Duration::from_secs(120);
/// Controller: a discovery still waiting for some node after this long
/// says so in its Ready condition, and keeps waiting.
pub const DISCOVERY_SILENT: Duration = Duration::from_secs(600);

/// The last phase a node completed, and whether it succeeded.
/// `takenPool`, `lvBytes` and `allocatedBytes` come from the successful
/// Taking ack and every later ack copies them unchanged, as it does
/// `digest` and `digestBytes` once they exist: the replica may move to
/// another Pool, the snapshot LV never does.
#[derive(KubeSchema, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotAck {
    #[schemars(length(min = 1, max = 253))]
    pub node: String,
    pub phase: SnapshotPhase,
    /// The Pool holding this node's snapshot LV; restores, holder lookups
    /// and deletes read it, never the replica's current Pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 253))]
    pub taken_pool: Option<String>,
    /// Size of the snapshot LV.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lv_bytes: Option<u64>,
    /// Thin data mapped by the snapshot LV at the take.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated_bytes: Option<u64>,
    /// When LVM created the snapshot LV; createdAt is the earliest holder's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lv_created_at: Option<Time>,
    /// Discovering: whether the node found the snapshot LV by its uid tag.
    /// A found ack is a holder ack from then on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found: Option<bool>,
    /// Discovering: every Pool of the node, searched. The answer counts
    /// only while these are the node's Pools.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub searched_pools: Vec<String>,
    /// Discovering: the phaseSince of the discovery answered. An answer
    /// to an earlier discovery, written from a lagging cache after the
    /// acks were cleared, counts for nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_since: Option<Time>,
    /// The size the snapshot LV's tag records, as the take wrote it or a
    /// discovery found it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag_size_bytes: Option<u64>,
    /// The source replicas the snapshot LV's tag records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag_source_replicas: Option<u32>,
    /// Hex sha256 over the sha256 of each 4 MiB chunk of the snapshot LV
    /// over [0, digestBytes), written once after Ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub digest: Option<String>,
    /// The source's DRBD device size, read from the snapshot LV's own
    /// superblock: the range the digest covers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest_bytes: Option<u64>,
    #[serde(default)]
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl SnapshotAck {
    /// A failed ack of `phase` that records nothing else yet.
    pub fn new(node: impl Into<String>, phase: SnapshotPhase) -> Self {
        Self {
            node: node.into(),
            phase,
            taken_pool: None,
            lv_bytes: None,
            allocated_bytes: None,
            lv_created_at: None,
            found: None,
            searched_pools: Vec::new(),
            phase_since: None,
            tag_size_bytes: None,
            tag_source_replicas: None,
            digest: None,
            digest_bytes: None,
            ok: false,
            error: None,
        }
    }

    /// Names a holder copy: takenPool and the tag values. A holder ack
    /// without them was written before snapshot LV tags, and nothing
    /// proves its LV is this Snapshot's copy.
    pub fn holder_copy(&self) -> bool {
        self.taken_pool.is_some()
            && self.tag_size_bytes.is_some()
            && self.tag_source_replicas.is_some()
    }
}
