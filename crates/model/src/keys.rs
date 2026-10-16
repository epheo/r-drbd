//! Names shared by the controller, the agent, the CSI plugins and their clients.

use std::ops::RangeInclusive;

/// CSI driver name, StorageClass provisioner and topology key prefix.
pub const CSI_DRIVER: &str = "r-drbd.io";
/// CSI topology key; its value is the Kubernetes node name.
pub const TOPOLOGY_KEY: &str = "r-drbd.io/node";

/// PVC annotations that pin placement.
pub const ANNOTATION_PIN_NODE: &str = "r-drbd.io/node";
pub const ANNOTATION_PIN_POOL: &str = "r-drbd.io/pool";

/// StorageClass parameters.
pub const PARAM_TIER: &str = "tier";
pub const PARAM_REPLICAS: &str = "replicas";

/// Node annotations written by the agent.
pub const ANNOTATION_DRBD_KERNEL: &str = "r-drbd.io/drbd-kernel";
pub const ANNOTATION_DRBD_UTILS: &str = "r-drbd.io/drbd-utils";
pub const ANNOTATION_LVM: &str = "r-drbd.io/lvm";
/// Agent heartbeat: a coordination.k8s.io Lease in the product namespace,
/// renewed every LEASE_RENEW_SECS; older than LEASE_STALE_SECS means that
/// node's replicas are Unknown. Leases keep the churn off the Node objects.
pub fn agent_lease_name(node: &str) -> String {
    format!("r-drbd-agent-{node}")
}
pub const CONTROLLER_LEASE_NAME: &str = "r-drbd-controller";
pub const LEASE_RENEW_SECS: u64 = 10;
pub const LEASE_STALE_SECS: u64 = 40;
/// Namespace of the Leases and the pods; env POD_NAMESPACE overrides.
pub const DEFAULT_NAMESPACE: &str = "r-drbd-system";

pub const FINALIZER_POOL: &str = "r-drbd.io/pool";
pub const FINALIZER_VOLUME: &str = "r-drbd.io/volume";
pub const FINALIZER_SNAPSHOT: &str = "r-drbd.io/snapshot";

pub const CONTROLLER_FIELD_MANAGER: &str = "r-drbd-controller";
/// Shared by every node plugin for the volume-level fileSystem marker.
pub const FS_FIELD_MANAGER: &str = "r-drbd-fs";
/// The agent writes it as a replica's lastError; the controller matches on it with starts_with.
pub const LV_LOST: &str = "LV missing after it was reported present; not recreated so the loss stays visible, delete the Volume to start over";
/// Per-node field manager so server-side apply keeps each agent's entries apart.
pub fn agent_field_manager(node: &str) -> String {
    format!("r-drbd-agent-{node}")
}

/// Fixed DRBD allocation ranges.
pub const PORT_RANGE: RangeInclusive<u16> = 7000..=7999;
/// Minors start at 1000 so hand-made /dev/drbd0.. devices never collide.
pub const MINOR_RANGE: RangeInclusive<u32> = 1000..=49_999;
pub const NODE_ID_MAX: u8 = 31;
pub const PEER_SLOTS: u8 = 7;

/// Resource file directory on the host, included from /etc/drbd.d.
pub const RES_DIR: &str = "/var/lib/r-drbd";

/// Volume and Snapshot names become LV and DRBD resource names on the host:
/// a letter or underscore first, then letters, digits, underscore or dash,
/// 2 to 48 characters.
pub const NAME_PATTERN: &str = "^[A-Za-z_][A-Za-z0-9_-]{1,47}$";
/// LVM volume group and logical volume names (Pool.spec.vg, thinPool):
/// letters, digits, `+ _ . -`, never a leading dash, 1 to 127 characters;
/// `.` and `..` are rejected by a CEL rule. Enforced again by the agent.
pub const LVM_NAME_PATTERN: &str = "^[A-Za-z0-9+_.][A-Za-z0-9+_.-]{0,126}$";
