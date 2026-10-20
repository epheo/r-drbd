# Reference

`kubectl explain pool.spec`, `volume.spec`, `volume.status`, `snapshot.status` and `nodedisks.status` give the same field descriptions from the installed CRDs.

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
| `spec.devices` | []string | `/dev/disk/by-id/<name>` paths, at most 64, no duplicates; append only; present or absent from creation on |
| `spec.chunkSize` | quantity | Power of two, 64Ki to 1Gi; devices only; immutable; absent means 64Ki |
| `spec.reclaimPolicy` | string | `Retain` or `Delete`; devices only; mutable; absent means `Retain` |
| `status.totalBytes` | integer | Thin pool data size |
| `status.freeBytes` | integer | Thin pool free space |
| `status.chunkBytes` | integer | Thin pool chunk size |
| `status.committedBytes` | integer | Sum of the sizes of the volumes placed here |
| `status.placeableBytes` | integer | Largest new volume placement accepts here now, rounded down to MiB, 0 when placement skips the Pool (see [Pools](pools.md#capacity)) |
| `status.volumes` | []string | Volumes with a diskful replica here |
| `status.devices[]` | object | Devices mode, per listed path: `path`, `state`, `sizeBytes`, `model`, `serial`, `message` |
| `status.conditions` | []Condition | `Available`; `Released` under `reclaimPolicy: Delete` once deleted |
| `status.observedGeneration` | integer | Spec generation the agent last applied |

Device `state`: `Member`, `Blank`, `NotEmpty`, `Busy`, `NotADisk`, `Missing` (see [Pools](pools.md#what-the-agent-checks)).

Finalizer `r-drbd.io/pool` holds deletion while `status.volumes` is not empty, and under `reclaimPolicy: Delete` until `Released` is True.

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
| `conditions` | `Ready`, `Degraded`, `QuorumLost`, `SplitBrain`, `PeerUnreachable`, `AttachmentFailed` |
| `usableBytes` | Smallest size the diskful replicas on reachable nodes provide |
| `primary` | Node where the device is open |
| `nodes` | Comma separated diskful nodes |
| `initializedAt` | First time the volume was Ready |
| `fileSystem` | File system the node plugin created or inherited |
| `attachmentErrors[]` | Attached nodes without a diskless replica: `node`, `reason` (NoPort, TooManyPeers), `message`; an entry goes once the node has a replica or is unpublished |
| `observedGeneration` | Spec generation the controller last wrote status for |
| `replicas[]` | `node`, `present`, `diskState`, `syncPercent`, `peers`, `inUse`, `quorum`, `devicePath`, `allocatedBytes`, `sizeBytes`, `observedGeneration`, `lastError`, `unreachablePeers` |
| `replicas[].unreachablePeers[]` | Stuck DRBD connections from this node: `node`, `address`, `port`, `since`, `result` (Refused, Filtered, TimedOut, Accepted, Failed), `probedAt`, `error`, `peerRun` (the peer agent run probed) |

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
| `status.phase` | Pending, Suspending, Taking, Ready, Failed, Deleting |
| `status.sizeBytes` | Volume size at snapshot time |
| `status.fileSystem` | Inherited by restores |
| `status.createdAt` | When the snapshot was taken |
| `status.phaseSince` | When the current phase began |
| `status.acks[]` | Per node: `node`, `phase`, `pool`, `ok`, `error` |
| `status.lastError` | Why it waits in Pending, or why it failed |
| `status.conditions` | `Ready` |
| `status.observedGeneration` | Spec generation the controller last wrote status for |

Finalizer `r-drbd.io/snapshot` holds deletion until every snapshot LV is removed.

## NodeDisks

Group `r-drbd.io/v1alpha1`, cluster scoped, read-only, short name `nd`.
One per node, named after the node, written by its agent, owned by the Node object, so it goes with the node.
It claims nothing: a Pool uses only the disks it lists.

| Field | Type | Notes |
|---|---|---|
| `status.blank` | integer | Disks in state `Blank` |
| `status.watch` | string | `uevent`: changes the kernel or the mount table reports show within seconds, every disk is checked again hourly; `poll`: no uevent socket, or it failed since, every disk every 5 minutes |
| `status.message` | string | Set when the list is incomplete: more than 512 disks, or disks the last check had no time for |
| `status.disks[]` | object | Every whole disk of the node; no partition, loop, RAM, zram, dm, md, DRBD, nbd, RBD, ZFS volume, null block, floppy, optical device, eMMC boot area or hidden NVMe multipath path |
| `status.disks[].path` | string | The path to list in a Pool: the `wwn-` by-id link, else the bus one (`ata-`, `nvme-`, `scsi-`, `virtio-`); `/dev/<name>` without any by-id link |
| `status.disks[].name` | string | Kernel name, as `lsblk` shows it today |
| `status.disks[].links` | []string | Every by-id link of the disk |
| `status.disks[].state` | string | As in a Pool's `status.devices` (see [Pools](pools.md#what-the-agent-checks)) |
| `status.disks[].pool` | string | For `Member`: the Pool whose VG holds the disk |
| `status.disks[].sizeBytes`, `model`, `serial` | | From sysfs |
| `status.disks[].message` | string | What makes the disk not `Blank` |

`Member` and the Pool it names:
- a VG tagged `r-drbd.pool.<pool>` belongs to that Pool on this node;
- an untagged VG belongs to the Pool on this node without `devices` whose `vg` it is; with several such Pools, the first by name, the others in the message;
- a VG tagged for a Pool that does not exist on this node, or for two Pools, gives `NotEmpty` saying so;
- a PV of any other VG is `NotEmpty`, naming the VG, even while its LVs hold the disk.

Other differences from a Pool's checks:
- `wipefs` and the exclusive open run only on a disk every other check finds blank, so a disk in use is never read or opened;
- a disk the kernel announced but udev has not processed yet is `Busy`, "udev not settled", and read again a few seconds later; after a minute without a udev entry the other checks decide.

When the agent reads:
- every disk at start and hourly (every 5 minutes under `watch: poll`);
- one disk when the kernel reports a change on it: added, removed, or written and closed (mkfs, wipefs, pvcreate, partitioning); a writer that never closes the disk, or an exclusive claimer leaving no trace in sysfs, shows at the hourly check;
- the cheap facts of every disk (sysfs, mounts) when the mount table or any block device changes, and the signatures of a disk whose facts changed;
- LVM reports at start, hourly, after a Pool's disks change, and when a disk gains or loses an LVM label.
So an idle disk, spun down or not, is read at start, hourly and on its own changes only.
It writes the object only when something changed.

`kubectl get nodedisks` prints the node and its count of Blank disks.

## Condition reasons

Volume `Ready`:

| Reason | Meaning |
|---|---|
| `AllReplicasUpToDate` | Ready |
| `Resizing` | Still growing: `usableBytes` is below `sizeBytes`; Ready True once the volume was Ready before, False while a restore grows before its first Ready |
| `Placing`, `Waiting` | Not ready yet |
| `Insufficient` | No Pool of the tier has room on enough nodes |
| `AllocationFailed` | No free DRBD minor, port or node id |
| `InvalidName` | The Volume name is not a valid LV name |
| `SnapshotNotReady` | Restore waits for its snapshot |
| `SnapshotFailed` | Restore source has no node holding it |
| `UnsupportedRestore` | Replica count differs from the snapshot's node count |
| `RestoreNodeConflict` | Pins point away from the snapshot |
| `InvalidSurvivor` | `spec.resolve.survivor` holds no diskful replica |
| `ResolveWithoutSplitBrain` | `spec.resolve` is set but there is no split brain; the status of `Ready` is kept; remove `spec.resolve` |
| `Deleting` | Being removed |

Volume `Degraded`: `ReplicaMissing`, `ReplicaNotUpToDate`, `ReplicaDisconnected`, `NodeUnreachable`; False: `AllReplicasUpToDate`, or `Provisioning` before the first Ready.

Volume `QuorumLost`: `NoQuorum`, `Quorate`.

Volume `SplitBrain`: `SplitBrain`, `NoSplitBrain`.

Volume `AttachmentFailed`: True with reason `AttachmentFailed` while `attachmentErrors` is not empty, the message naming each node and cause; False with reason `Attached`.

Volume `PeerUnreachable`:

| Reason | Meaning |
|---|---|
| `PeersReachable` | No stuck connection between running replicas |
| `Firewall` | The probe was rejected (ICMP prohibited or unreachable); open TCP 11000 to 11999 between nodes |
| `Timeout` | The probe got no answer: packets dropped |
| `NotListening` | The probe was refused: nothing listens at that address and port, or a firewall answers with a TCP reset |
| `Unknown` | The port answers but DRBD does not connect, or the probe failed |

Snapshot `Ready`: True with reason `Ready`; False with the phase as reason (`Pending`, `Suspending`, `Taking`, ..., `Failed`, `Deleting`) and `lastError` as message.

Pool `Available`: `ThinPoolReady`, `ThinPoolUnavailable`, `ZeroingDisabled`, `InvalidName`, `LvmUnsupported` (host LVM older than 2.03.03); devices mode also `DeviceNotEmpty`, `DeviceBusy`, `DeviceMissing`, `NotADisk`, `VolumeGroupNotOwned`, `Building` (a build step failed, named in the message; a failed growth step keeps the Pool Available and puts the error in the message).

Pool `Released`: `Removed` (True: thin pool, VG and PV labels removed), `RemoveBlocked` (False: something else is in the VG, the VG is not the Pool's, or it misses a PV; nothing removed), `RemoveFailed` (False: an LVM command failed, retried on the next pass).

Pool Events: `DeviceNotUsable` (Warning: an appended path is not blank and stays out of the pool), `PoolNotGrown` (Warning: a growth step failed, or the metadata maximum leaves VG space out of the pool).

Volume Events: `Evicted`, `EvictionBlocked`, `SplitBrainResolved`, `PeerUnreachable` (Warning), `PeersReachable`, `AttachmentFailed` (Warning, once per distinct message), `StatusApplyFailed` (Warning, once per error kind until an apply succeeds), `DuplicateAssignment` (Warning on each holder of a minor or port held twice; never rewritten).

## CSI driver

Name `r-drbd.io`.

Controller capabilities: create and delete volume, publish and unpublish, expand, create and delete snapshot, capacity.
Node capabilities: stage and unstage, expand, volume stats.

Not supported: clone from a PVC, ListVolumes, ListSnapshots.

GetCapacity answers per node and class from the Pools' `placeableBytes`, without an LVM call; see [Volumes](volumes.md#scheduling).

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
| `--identity` (controller) | `POD_NAME` | hostname; a random suffix is added per process |
| `--node` (agent) | `NODE_NAME` | required |
| `--res-dir` (agent) | `R_DRBD_RES_DIR` | `/var/lib/r-drbd` |

Other environment:

| Env | Meaning |
|---|---|
| `R_DRBD_HOST_NS` | Agent: namespace directory of the host (`/proc/1/ns`) to run host tools in |
| `RUST_LOG` | Log filter, e.g. `info`, `debug` |

`r-drbd crds` prints the CRDs as a JSON List for `kubectl apply -f -`; `deploy/base/crds.yaml` holds the same output.

## Cluster objects

| Object | Purpose |
|---|---|
| Lease `r-drbd-controller` | Controller leader election |
| Lease `r-drbd-agent-<node>` | Agent heartbeat, renewed every 10 s, stale after 40 s |
| CSIStorageCapacity, in the controller's namespace | Capacity per node and StorageClass for the scheduler; owned by the controller Deployment |
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
