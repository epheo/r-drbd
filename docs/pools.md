# Pools

A Pool is one existing LVM thin pool on one node.
r-drbd never creates or removes the volume group or the thin pool; you do.

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

## Status

`kubectl get pools` shows node, tier, VG, thin pool, free, committed and placeable bytes, and availability.

| Field | Meaning |
|---|---|
| `totalBytes`, `freeBytes` | Thin pool data size and free space |
| `chunkBytes` | Thin pool chunk size; DRBD discards are aligned to it |
| `committedBytes` | Sum of the sizes of the volumes with a diskful replica here |
| `placeableBytes` | Largest new volume placement accepts here now, rounded down to MiB; 0 when the Pool is not Available, its node is not Ready, or it is being deleted |
| `volumes` | Volumes with a diskful replica here |
| `conditions` | `Available` |

`Available` reasons:

| Reason | Status | Meaning |
|---|---|---|
| `ThinPoolReady` | True | The thin pool is usable |
| `ThinPoolUnavailable` | False | The VG or thin pool is missing or unreadable; the message holds the LVM error |
| `ZeroingDisabled` | False | Zeroing is off and turning it on failed |
| `InvalidName` | False | `vg` or `thinPool` is not a valid LVM name |

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
