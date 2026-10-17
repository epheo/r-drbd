# r-drbd documentation

r-drbd is replicated block storage for Kubernetes on DRBD 9 and thin LVM.
You declare thin pools as Pool objects, and claim volumes through a StorageClass.

## Guides

- [Concepts](concepts.md): the objects, the two roles, how a volume is placed.
- [Install](install.md): host requirements, manifests, first Pool and StorageClass.
- [Pools](pools.md): declaring thin pools or building them from blank disks, tiers, capacity, removal.
- [Disks](disks.md): what runs on a disk, the limits, and every corner case of building from disks and of NodeDisks.
- [Volumes](volumes.md): StorageClass parameters, placement pins, access modes, resize.
- [Snapshots](snapshots.md): taking snapshots and restoring them.
- [Operations](operations.md): reading status, node loss, split brain, moving replicas.
- [Data safety](data-safety.md): what r-drbd never does to your data.

## Reference

- [API and CLI reference](reference.md): CRD fields, conditions, CSI behaviour, flags, host paths.
