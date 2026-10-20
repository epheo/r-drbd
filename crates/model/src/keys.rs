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
/// Label on every agent Lease, value "true", so agents watch only those.
pub const AGENT_LEASE_LABEL: &str = "r-drbd.io/agent-lease";
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
/// The API caps a manager at 128 bytes and node names reach 253: a long name
/// keeps its first 96 bytes plus a hash of the whole, so names that differ
/// only past byte 96 stay apart. A name that fits is used as is, so the
/// managers of existing entries never change.
pub fn agent_field_manager(node: &str) -> String {
    const PREFIX: &str = "r-drbd-agent-";
    const MAX: usize = 128;
    const KEEP: usize = 96;
    if PREFIX.len() + node.len() <= MAX {
        return format!("{PREFIX}{node}");
    }
    let mut cut = KEEP;
    while !node.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{PREFIX}{}-{:016x}", &node[..cut], fnv1a(node.as_bytes()))
}

/// FNV-1a 64: fixed forever, since a manager name must not change between builds.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
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
/// Pool.spec.devices items: stable names only, one level under by-id.
pub const DEVICE_PATTERN: &str = "^/dev/disk/by-id/[^/]+$";
/// VG tag of a Pool in devices mode: the Pool name, not its uid, so a
/// Pool recreated after a cluster rebuild finds its VG again.
pub fn pool_vg_tag(pool: &str) -> String {
    format!("r-drbd.pool.{pool}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_manager_fits_the_api_limit() {
        assert_eq!(agent_field_manager("n1"), "r-drbd-agent-n1");
        let fits = "a".repeat(115);
        assert_eq!(agent_field_manager(&fits), format!("r-drbd-agent-{fits}"));

        let long = "a".repeat(253);
        let m = agent_field_manager(&long);
        assert!(m.len() <= 128, "{}", m.len());
        assert!(m.starts_with(&format!("r-drbd-agent-{}-", "a".repeat(96))));
        assert_eq!(m, agent_field_manager(&long), "stable");
        let other = format!("{}b", "a".repeat(252));
        assert_ne!(m, agent_field_manager(&other), "differs past the cut");
        assert_eq!(
            agent_field_manager(&"a".repeat(116)).len(),
            13 + 96 + 1 + 16
        );
    }
}
