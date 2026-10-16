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

All fields are immutable: a Pool is a fixed place on a fixed node.
To change one, create a new Pool and delete the old one once it is empty.

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

`kubectl get pools` shows node, tier, VG, thin pool, free bytes and availability.

| Field | Meaning |
|---|---|
| `totalBytes`, `freeBytes` | Thin pool data size and free space |
| `chunkBytes` | Thin pool chunk size; DRBD discards are aligned to it |
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

Thin pools are oversubscribed: placement accepts volumes until their total size reaches 20 times the pool size.
Watch `freeBytes` and grow the thin pool (`lvextend`) before it fills; a full thin pool stops writes on every volume in it.

## Deleting a Pool

A Pool is held by a finalizer while `status.volumes` is not empty.
Empty it first, and the Pool goes:
- Replicated volumes: move the replica off the node with `spec.evict` (see [Operations](operations.md)).
- Local volumes: copy the data elsewhere, then delete them; a single replica cannot be moved, and restores stay on the snapshot's node.
