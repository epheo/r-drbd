# Reference

`kubectl explain pool.spec`, `volume.spec`, `volume.status` and `snapshot.status` give the same field descriptions from the installed CRDs.

## Pool

Group `r-drbd.io/v1alpha1`, cluster scoped.

| Field | Type | Notes |
|---|---|---|
| `spec.node` | string | Immutable |
| `spec.tier` | string | Immutable |
| `spec.vg` | string | LVM name, immutable |
| `spec.thinPool` | string | LVM name, immutable |
| `spec.overcommitPercent` | integer | 100 to 1000, default 100, mutable |
| `spec.reservePercent` | integer | 0 to 50, default 5, mutable |
| `status.totalBytes` | integer | Thin pool data size |
| `status.freeBytes` | integer | Thin pool free space |
| `status.chunkBytes` | integer | Thin pool chunk size |
| `status.committedBytes` | integer | Sum of the sizes of the volumes placed here |
| `status.placeableBytes` | integer | Largest new volume placement accepts here now, rounded down to MiB, 0 when placement skips the Pool (see [Pools](pools.md#capacity)) |
| `status.volumes` | []string | Volumes with a diskful replica here |
| `status.conditions` | []Condition | `Available` |

Finalizer `r-drbd.io/pool` holds deletion while `status.volumes` is not empty.

## Volume

Group `r-drbd.io/v1alpha1`, cluster scoped.
Created by the CSI driver; the name is the PV name.
Names must match `^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$` and must not start with `snapshot` or `pvmove` (LVM reserves them); others end Failed with reason `InvalidName`.
The pattern is what Kubernetes, LVM and DRBD names all accept; 63 bytes lets `<volume>+<snapshot>` fit LVM's 127-byte LV name.

Spec:

| Field | Type | Notes |
|---|---|---|
| `sizeBytes` | integer | Usable size; cannot shrink |
| `replicas` | 1 to 3 | Default 1; immutable |
| `tier` | string | Immutable |
| `pool` | string | Pin every diskful replica to this Pool |
| `node` | string | Pin one diskful replica to this node |
| `accessMode` | ReadWriteOnce, ReadWriteMany | Immutable; ReadWriteMany needs replicas 2 or 3 |
| `attachments` | []string | Nodes the device is published to; managed by the CSI driver |
| `evict` | []string | Nodes whose replica must move; see [Operations](operations.md) |
| `source.snapshot` | string | Snapshot to restore from; immutable |
| `resolve.survivor` | string | Split brain resolution; cleared when done |
| `assignment` | object | Written by the controller: replicas with node, pool, node id, port and role (diskful, diskless, tiebreaker), DRBD minor and secret. Do not edit |

Status:

| Field | Notes |
|---|---|
| `phase` | Pending, Provisioning, Ready, Degraded, Failed, Deleting |
| `conditions` | `Ready`, `Degraded`, `QuorumLost`, `SplitBrain` |
| `usableBytes` | Size every replica provides |
| `primary` | Node where the device is open |
| `nodes` | Comma separated diskful nodes |
| `initializedAt` | First time the volume was Ready |
| `fileSystem` | File system the node plugin created or inherited |
| `replicas[]` | `node`, `present`, `diskState`, `syncPercent`, `peers`, `inUse`, `quorum`, `devicePath`, `allocatedBytes`, `sizeBytes`, `observedGeneration`, `lastError` |

Finalizer `r-drbd.io/volume` holds deletion until every replica is removed.

## Snapshot

Group `r-drbd.io/v1alpha1`, cluster scoped.
Created by the CSI driver; the name is the CSI snapshot name (`snapshot-<uuid>`).
Names follow the Volume pattern.
The snapshot LV `<volume>+<snapshot>` must also fit the device-mapper name limit on every node that holds the volume: VG name, `-`, LV name, every `-` counted twice, 127 bytes at most.
With CSI names (`pvc-<uuid>`, `snapshot-<uuid>`) the LV part takes 96 bytes, so the VG name may take 30 (a VG name with dashes counts them twice).
A snapshot over the limit ends Failed before any IO is suspended.

| Field | Notes |
|---|---|
| `spec.volume` | Source Volume; immutable |
| `status.phase` | Pending, Suspending, Suspended, Taking, Taken, Resuming, Ready, Failed, Deleting |
| `status.sizeBytes` | Volume size at snapshot time |
| `status.fileSystem` | Inherited by restores |
| `status.createdAt` | When the snapshot was taken |
| `status.phaseSince` | When the current phase began |
| `status.acks[]` | Per node: `node`, `phase`, `pool`, `ok`, `error` |
| `status.lastError` | Why it failed |

Finalizer `r-drbd.io/snapshot` holds deletion until every snapshot LV is removed.

## Condition reasons

Volume `Ready`:

| Reason | Meaning |
|---|---|
| `AllReplicasUpToDate` | Ready |
| `Placing`, `Waiting`, `Provisioning` | Not ready yet |
| `Insufficient` | No Pool of the tier has room on enough nodes |
| `AllocationFailed` | No free DRBD minor, port or node id |
| `InvalidName` | The Volume name is not a valid LV name |
| `SnapshotNotReady` | Restore waits for its snapshot |
| `SnapshotFailed` | Restore source has no node holding it |
| `UnsupportedRestore` | Replica count differs from the snapshot's node count |
| `RestoreNodeConflict` | Pins point away from the snapshot |
| `InvalidSurvivor` | `spec.resolve.survivor` holds no diskful replica |
| `Deleting` | Being removed |

Volume `Degraded`: `ReplicaMissing`, `ReplicaNotUpToDate`, `ReplicaDisconnected`, `NodeUnreachable`, `Resizing`.

Volume `QuorumLost`: `NoQuorum`, `Quorate`.

Volume `SplitBrain`: `SplitBrain`, `NoSplitBrain`, `ResolveWithoutSplitBrain`.

Pool `Available`: `ThinPoolReady`, `ThinPoolUnavailable`, `ZeroingDisabled`, `InvalidName`, `LvmUnsupported` (host LVM older than 2.03.03).

Volume Events: `Evicted`, `EvictionBlocked`, `SplitBrainResolved`.

## CSI driver

Name `r-drbd.io`.

Controller capabilities: create and delete volume, publish and unpublish, expand, create and delete snapshot.
Node capabilities: stage and unstage, expand, volume stats.

Not supported: clone from a PVC, ListVolumes, ListSnapshots, GetCapacity.

StorageClass parameters: `tier` (required), `replicas` (1, 2 or 3, default 1).
PVC annotations: `r-drbd.io/node`, `r-drbd.io/pool`.
Topology key: `r-drbd.io/node`, on local volumes only.

## Command line

    r-drbd controller [--csi-endpoint URL] [--namespace NS] [--identity ID]
    r-drbd agent --node NAME [--csi-endpoint URL] [--namespace NS] [--res-dir DIR]
    r-drbd crds

| Flag | Env | Default |
|---|---|---|
| `--csi-endpoint` | `CSI_ENDPOINT` | `unix:///csi/csi.sock` |
| `--namespace` | `POD_NAMESPACE` | `r-drbd-system` |
| `--identity` (controller) | `POD_NAME` | hostname |
| `--node` (agent) | `NODE_NAME` | required |
| `--res-dir` (agent) | `R_DRBD_RES_DIR` | `/var/lib/r-drbd` |

Other environment:

| Env | Meaning |
|---|---|
| `R_DRBD_HOST_NS` | Agent: namespace directory of the host (`/proc/1/ns`) to run host tools in |
| `RUST_LOG` | Log filter, e.g. `info`, `debug` |

`r-drbd crds` prints the CRDs as a JSON List for `kubectl apply -f -`.

## Cluster objects

| Object | Purpose |
|---|---|
| Lease `r-drbd-controller` | Controller leader election |
| Lease `r-drbd-agent-<node>` | Agent heartbeat, renewed every 10 s, stale after 40 s |
| Node annotations `r-drbd.io/drbd-kernel`, `r-drbd.io/drbd-utils`, `r-drbd.io/lvm` | Tool versions seen by the agent |

## Host

| Item | Value |
|---|---|
| DRBD ports | 11000 to 11999 |
| DRBD minors | 50000 to 99999 |
| DRBD resource name, LV name | Volume name |
| Snapshot LV name | `<volume>+<snapshot>` |
| Resource files | `/var/lib/r-drbd/<volume>.res` |
| Include file | `/etc/drbd.d/r-drbd.res` |
| CSI socket | `/var/lib/kubelet/plugins/r-drbd.io/csi.sock` |
