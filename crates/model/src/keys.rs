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

/// DRBD listen ports, one per replica on each node. Outside the kubelet
/// ports (10250 to 10259) and the NodePort range (30000 to 32767), and
/// clear of the 7000s other DRBD stacks use by default.
pub const PORT_RANGE: RangeInclusive<u16> = 11000..=11999;
/// DRBD minors, cluster wide. Above the low numbers other DRBD stacks or
/// hand-made resources take on the same host.
pub const MINOR_RANGE: RangeInclusive<u32> = 50_000..=99_999;
pub const NODE_ID_MAX: u8 = 31;
/// Bitmap slots in each replica's metadata, one per diskful peer it
/// tracks; diskless peers take none. replicas is at most 3, and an evict
/// writes the new replica into the assignment while the evicted one's
/// slot is still held: 3 slots at least. The survivor frees that slot with
/// forget-peer (before its next adjust, or by the slot sweep on a later
/// pass when it was down or forget-peer failed); 3 more slots let that
/// many stale ones wait for the sweep. A slot costs 32 MiB per TiB of
/// volume, so 6 cost 0.02 percent.
pub const PEER_SLOTS: u8 = 6;

/// Resource file directory on the host, included from /etc/drbd.d.
pub const RES_DIR: &str = "/var/lib/r-drbd";

/// Volume and Snapshot names become LV names, and a Volume name the DRBD
/// resource name. Charset: what Kubernetes object names, LVM and DRBD all
/// take (lowercase letters, digits, `-` and `.`, alphanumeric at both ends).
/// Length: a snapshot LV is `<volume>+<snapshot>` and LVM names stop at
/// 127 bytes, so each name gets 63.
/// The agent still refuses an LV name LVM reserves (a Volume named
/// `snapshot...` or `pvmove...`) or one too long for its VG.
pub const NAME_PATTERN: &str = "^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$";
/// LVM volume group and logical volume names (Pool.spec.vg, thinPool):
/// letters, digits, `+ _ . -`, never a leading dash, 1 to 127 characters;
/// `.` and `..` are rejected by a CEL rule. Enforced again by the agent.
pub const LVM_NAME_PATTERN: &str = "^[A-Za-z0-9+_.][A-Za-z0-9+_.-]{0,126}$";
