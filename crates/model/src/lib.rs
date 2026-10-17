//! The r-drbd.io v1alpha1 API: Pool, Volume, Snapshot and NodeDisks.
//!
//! Spec is desired state, status is observed state. Agents write only
//! their own entries of list fields with server-side apply, which is why
//! every per-node list is a list-map keyed by `node`.

#![forbid(unsafe_code)]
// KubeSchema derives a private shadow struct per type; rustc reports its
// `pub` as unreachable at the span of the real struct.
#![allow(unreachable_pub)]

pub mod condition;
pub mod keys;
pub mod nodedisks;
pub mod pool;
pub mod snapshot;
pub mod volume;

pub use condition::Condition;
pub use nodedisks::{DiskStatus, DiskWatch, NodeDisks, NodeDisksSpec, NodeDisksStatus};
pub use pool::{
    DEFAULT_OVERCOMMIT_PERCENT, DEFAULT_RESERVE_PERCENT, DeviceState, DeviceStatus, Pool, PoolSpec,
    PoolStatus, ReclaimPolicy,
};
pub use snapshot::{Snapshot, SnapshotAck, SnapshotPhase, SnapshotSpec, SnapshotStatus};
pub use volume::{
    AccessMode, AssignedReplica, Assignment, PeerProbe, ProbeResult, ReplicaStatus, Resolve, Role,
    Volume, VolumePhase, VolumeSource, VolumeSpec, VolumeStatus,
};

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::CustomResourceExt;

pub const GROUP: &str = "r-drbd.io";
pub const VERSION: &str = "v1alpha1";

/// All CRDs, in install order.
pub fn crds() -> Vec<CustomResourceDefinition> {
    vec![
        Pool::crd(),
        Volume::crd(),
        Snapshot::crd(),
        NodeDisks::crd(),
    ]
}
