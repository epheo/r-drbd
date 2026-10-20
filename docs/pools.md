# Pools

A Pool is one LVM thin pool on one node.
It comes in two modes, fixed when the Pool is created:

- Existing VG: you make the volume group and the thin pool; r-drbd never creates or removes them.
- Devices: you list blank whole disks; the agent builds the VG and the thin pool, grows them when you add disks, and removes them on deletion if you ask (see [Building from blank disks](#building-from-blank-disks)).

## Declaring a Pool

    apiVersion: r-drbd.io/v1alpha1
    kind: Pool
    metadata:
      name: node1-nvme
    spec:
      node: node1
      tier: nvme
      vg: data
      thinPool: thin

| Field | Meaning |
|---|---|
| `node` | Kubernetes node name |
| `tier` | Label a StorageClass selects by |
| `vg` | Volume group on that node |
| `thinPool` | Thin pool LV inside `vg` |
| `overcommitPercent` | Capacity policy, default 100 (see [Capacity](#capacity)) |
| `reservePercent` | Capacity policy, default 5 (see [Capacity](#capacity)) |
| `devices` | Blank disks to build from (see [Building from blank disks](#building-from-blank-disks)) |
| `chunkSize` | Thin pool chunk size, devices only |
| `reclaimPolicy` | `Retain` or `Delete`, devices only (see [Deleting a Pool](#deleting-a-pool)) |

`node`, `tier`, `vg` and `thinPool` are immutable: a Pool is a fixed place on a fixed node.
To change one, create a new Pool and delete the old one once it is empty.
The capacity fields may change at any time; they only gate new placements.

`vg` and `thinPool` must be valid LVM names.
The API server rejects anything else, and the agent checks again.

A node may hold several Pools, of the same tier or not.
Name Pools however you like; `<node>-<tier>` reads well.

## What the agent does with it

- Reads capacity from `lvs` and `vgs` and reports it on change.
- Turns on zeroing for the thin pool if it is off.
  Without it, a new thin LV can expose blocks of deleted ones.
- Creates and removes only LVs inside this thin pool.
  The rest of the volume group, other thin pools included, is never touched, so you can share a VG with other tools.
  In devices mode the VG is the Pool's alone.

## Building from blank disks

    apiVersion: r-drbd.io/v1alpha1
    kind: Pool
    metadata:
      name: node1-nvme
    spec:
      node: node1
      tier: nvme
      vg: r-drbd-nvme
      thinPool: thin
      devices:
        - /dev/disk/by-id/nvme-Example_SSD_2TB_S7KH0000000001
        - /dev/disk/by-id/nvme-Example_SSD_2TB_S7KH0000000002
      reclaimPolicy: Retain

- `devices`: up to 64 paths under `/dev/disk/by-id/`.
  by-id names come from the disk's serial or WWN and survive reboots; `sdb` or `nvme1n1` do not.
- `vg` and `thinPool` still name what the agent creates, so `kubectl get pools` and the host show the same names.
- `devices` can only grow: append a path to add a disk.
  Removing a disk is not supported, and a Pool cannot switch modes.
- `chunkSize`: a power of two from 64Ki to 1Gi, fixed at creation; default 64Ki.
  The chunk is the unit of copy-on-write after a snapshot and the smallest space a discard frees, so small random writes, fstrim and frequent snapshots want it small.
  A tier for large sequential data (backups, media) can set a bigger one.
- `reclaimPolicy`: what deleting the Pool does to the VG; default `Retain`.

### What the agent checks

Before any change, on every pass, each listed path gets one state in `status.devices`:

| State | Meaning |
|---|---|
| `Member` | A PV of this Pool's VG |
| `Blank` | Whole disk, no signature, no partition, not held, exclusive open succeeds |
| `NotEmpty` | A signature or a partition table; the message names it, with its offset |
| `Busy` | Mounted, held by another device (dm, md), a multipath path or not yet settled by udev (up to a minute), or the exclusive open failed, did not return within 10 s, or an earlier one has not returned yet |
| `NotADisk` | A partition, dm, md, loop, drbd, zram, RAM, nbd, RBD, ZFS volume, null block, floppy or optical device, an eMMC boot area, read-only, or size 0 (no medium) |
| `Missing` | The path does not resolve |

When several apply, the first in this order wins: Missing, NotADisk, Member, Busy, NotEmpty, Blank.
A second by-id name of a disk already listed (its `wwn-` and its `nvme-` link) is `NotADisk`, naming the first.
Each row also shows the disk's size, model and serial, so you can tell which disk is which.

How:
- the by-id link is resolved, and sysfs gives partitions, holders, read-only flag, size, model and serial;
- the host's udev database says whether the disk is a multipath path (`DM_MULTIPATH_DEVICE_PATH`, decided or still being decided), not ready (`SYSTEMD_READY=0`), or not processed by udev yet;
- `wipefs --no-act --json` lists signatures, partition tables included; it never writes, and it is not run on a `Busy` disk;
- the agent opens the disk exclusively and closes it at once: the kernel refuses while anything holds it; an open that does not return within 10 s leaves the disk `Busy` and the agent moves on.

r-drbd never wipes a disk.
To reuse a disk with old data, wipe it yourself first (`wipefs -a`), deliberately.

### Building

1. VG absent: every path must be `Blank`.
   One bad path stops the whole Pool, Available False with the reason of the worst row; nothing is created.
2. `vgcreate` with the tag `r-drbd.pool.<pool name>`.
   It runs without `--yes` or `--force`, so LVM's own refusal of a disk with a signature is a second guard.
   With `use_devicesfile = 1` and no devices file, vgcreate would create one with only the new disks, and every other PV of the host, the root VG included, would drop out of LVM's view.
   So on a host that has PVs the agent refuses (reason `Building`, naming them); add them to a devices file first, `vgimportdevices <vg>` per VG or `lvmdevices --adddev` for a PV without one.
   Not `--all`: without the file, LVM also sees PVs inside volumes attached through DRBD, and the import would hand them to the host.
   A host without any PV needs nothing: the file vgcreate makes then lists exactly the host's PVs.
3. The thin pool takes all the space, with metadata sized as below and zeroing on.
4. From there the Pool works like any other.

Every step is checked again right before it runs, and is safe to repeat: an agent restart between two steps resumes from what is on the disk.
A crash inside vgcreate itself can leave PV labels without a VG; those disks then read `NotEmpty` (`LVM2_member`).
Run `pvremove` on them and the build goes on.

A VG of that name that exists without the tag is never adopted or touched: Available False, reason `VolumeGroupNotOwned`.
A VG you made by hand belongs in a Pool without `devices`.
The tag is the Pool name, so a Pool recreated under the same name finds its retained VG again.

### Growing

Append a `Blank` disk to `devices`.
The agent runs `vgextend`, grows the thin pool metadata to the size below for the new data size, then grows the data into all free space.
Metadata comes first: a pool whose data outgrows its metadata fails as a whole.
Volumes keep running throughout.

An appended path that is not `Blank` is left out: the Pool keeps its capacity and stays Available, the row says why, and a Warning Event `DeviceNotUsable` on the Pool says it once.
A growth step that fails also leaves the Pool serving at its current size: the `Available` message holds the error, a Warning Event `PoolNotGrown` says it once, and the next pass tries again.

### Metadata size

    metadata = data / chunk * 64 B + 64 MiB, rounded up to the VG extent

- 64 B per chunk is about twice what a fully mapped pool needs, for B-tree slack and snapshots that diverge over time.
- 64 MiB covers per-volume overhead for about 10000 thin LVs.
- LVM caps thin pool metadata near 15.81 GiB.
  When the rule exceeds it at creation, the chunk doubles until it fits; `status.chunkBytes` shows the chunk used.
  At 64 KiB that happens above about 15.7 TiB of data.
- The chunk never changes after creation, so growth stops at what capped metadata covers by the rule: about 15.7 TiB at 64 KiB, twice that per doubling of the chunk.
  The rest of the VG stays out of the thin pool; the `Available` message and a `PoolNotGrown` Event say how much.
  Plan large Pools with a larger `chunkSize`.
- Examples at 64 KiB: 100 GiB of data gets 164 MiB, 15 TiB about 15 GiB.

### Failures

A disk of the VG that disappears makes the VG partial: Available False, reason `DeviceMissing`.
DRBD detaches the replicas on that node on IO errors, and their Volumes turn Degraded.
There is no RAID inside a node: one disk failure loses this node's replicas, and the other nodes keep the data.
Replacing a disk means a new Pool: move the replicas off with `spec.evict` (see [Operations](operations.md)), then delete the Pool.
Under `reclaimPolicy: Delete` a VG with a missing PV is never removed, so `Released` stays False: switch the Pool to `Retain` first, then clean up the VG and the remaining disks by hand.

### Finding disks

Each agent lists its node's whole disks in a NodeDisks object named after the node:

    kubectl get nodedisks
    kubectl get nodedisks node1 -o yaml

Each row has the `path` to list in a Pool, every by-id link, the kernel name, size, model, serial and the state the checks above give.
List only `Blank` disks in a Pool; `Member` rows name the Pool that holds them.
Changes show within seconds for changes the kernel or the mount table reports: a disk plugged or removed, a signature written and the disk closed (mkfs, wipefs, pvcreate, partitioning), a mount or an unmount.
A writer that never closes the disk, or an exclusive claimer leaving no trace in sysfs, shows at the hourly check of every disk.
Without a uevent socket, or once it fails, the agent checks every disk every 5 minutes; `status.watch` says which.
See [Reference](reference.md#nodedisks) for the fields.

While the agent is not running there, from any machine with cluster access:

    kubectl debug node/node1 -it --profile=sysadmin --image=busybox -- ls -l /host/dev/disk/by-id

Pick the link of the whole disk (no `-part` suffix); the target (`../../nvme1n1`) tells you which kernel name it has today.

## Status

`kubectl get pools` shows node, tier, VG, thin pool, free, committed and placeable bytes, and availability.

| Field | Meaning |
|---|---|
| `totalBytes`, `freeBytes` | Thin pool data size and free space |
| `chunkBytes` | Thin pool chunk size; DRBD resync discards are aligned to it, capped at 1 MiB |
| `committedBytes` | Sum of the sizes of the volumes with a diskful replica here |
| `placeableBytes` | Largest new volume placement accepts here now, rounded down to MiB; 0 when the Pool is not Available, its node is not Ready or its agent is down, or it is being deleted |
| `volumes` | Volumes with a diskful replica here |
| `devices` | Devices mode: one row per listed path with `state`, `sizeBytes`, `model`, `serial` and `message` |
| `conditions` | `Available`; `Released` while a Pool under `reclaimPolicy: Delete` is deleted |

`Available` reasons:

| Reason | Status | Meaning |
|---|---|---|
| `ThinPoolReady` | True | The thin pool is usable |
| `ThinPoolUnavailable` | False | The VG or thin pool is missing or unreadable, or LVM flags its health, or its metadata is over 80 percent used; the message says which. In the last two cases capacity stays reported and existing volumes keep running, but no new volume is placed there |
| `ZeroingDisabled` | False | Zeroing is off and turning it on failed |
| `InvalidName` | False | `vg` or `thinPool` is not a valid LVM name |
| `DeviceNotEmpty` | False | Devices mode: a path holds a signature or partitions |
| `DeviceBusy` | False | Devices mode: a path is mounted or held |
| `DeviceMissing` | False | Devices mode: a path does not resolve before the build, or a disk of the VG is gone |
| `NotADisk` | False | Devices mode: a path is not a whole disk |
| `VolumeGroupNotOwned` | False | Devices mode: a VG of that name exists without the Pool's tag |
| `Building` | False | Devices mode: a build or growth step failed; the message names the step and the LVM error |

New volumes are placed only on Available Pools on Ready nodes.

## Capacity

A thin volume takes pool space only as it is written.
A full thin pool stops writes on every volume in it, so placement keeps a margin by default.

Placement counts, per Pool:

- `total`: thin pool data size (`totalBytes`);
- `free`: data space not yet used (`freeBytes`), snapshots included;
- `committed`: sum of the sizes of the volumes placed here (`committedBytes`).

The rule:

    reserve   = total * reservePercent / 100, rounded up
    promise   = (total - reserve) * overcommitPercent / 100 - committed
    actual    = free - reserve
    placeable = min(promise, actual), at least 0

A volume fits when its size is at most `placeable`.

- `promise` keeps what the volumes may claim once fully written within the pool, less the reserve, times the overcommit.
- `actual` keeps a new volume writable in full today: snapshots and overcommitted volumes use space that `committed` does not count.
- The reserve is never planned for: it absorbs snapshot copy-on-write and DRBD metadata, and gives you time to grow the pool.

| Field | Default | Range | Why the default |
|---|---|---|---|
| `overcommitPercent` | 100 | 100 to 1000 | No volume is promised space the pool does not have |
| `reservePercent` | 5 | 0 to 50 | Room for snapshots and metadata, and time to react |

Set `overcommitPercent` above 100 only if you watch `freeBytes` and can grow the pool (`lvextend`) before it fills.
Snapshots still use space beyond `committed`; keep watching `freeBytes` when you take them.
Lowering the policy below what is committed leaves existing volumes alone; the Pool just takes no new ones.

Restores: a replica that restores from a snapshot copy in this Pool is never refused for capacity.
It already holds the data and can go nowhere else; it still counts in `committed` afterwards.
Any other new replica, empty and filled by a full sync, is gated like a new volume.

Resizes: only the growth is checked, against `promise` of every Pool holding a diskful replica.
A resize writes no data, so `free` does not matter.
A Pool whose `committed` already exceeds its policy refuses any growth.
The CSI expand call then fails with `ResourceExhausted`, naming the Pool.

Evictions use the same rule: a target without room makes `EvictionBlocked` name the capacity policy.

### Upgrading from a release without these fields

Earlier releases let committed sizes reach many times the pool size.
Pools filled past the new default show `placeableBytes` 0 and take no new volume or evicted replica.
To keep placing on such a Pool, raise `overcommitPercent` (up to 1000) and lower it again once you have grown the pool.

## Deleting a Pool

A Pool is held by a finalizer while `status.volumes` is not empty.
Empty it first, and the Pool goes:
- Replicated volumes: move the replica off the node with `spec.evict` (see [Operations](operations.md)).
- Local volumes: copy the data elsewhere, then delete them; a single replica cannot be moved, and restores stay on the snapshot's node.

What happens to the VG:

- Existing VG mode, or `reclaimPolicy: Retain`: nothing.
  The VG and thin pool stay; in devices mode they keep their tag, and a new Pool of the same name adopts them with their data.
- `reclaimPolicy: Delete`: once no volume is left, the agent removes the thin pool, the VG and the PV labels, and only then sets `Released` True; the finalizer waits for it.
  The disks are blank again afterwards.
  Any other LV in the VG stops it: `Released` False names the LV and nothing is removed.

If the node is gone for good, set `reclaimPolicy: Retain` and the Pool goes at once.
