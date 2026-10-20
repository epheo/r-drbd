# Snapshots

r-drbd implements CSI snapshots with LVM thin snapshots.
Install the external-snapshotter CRDs and snapshot controller first.

## VolumeSnapshotClass

`deploy/base` ships one:

    apiVersion: snapshot.storage.k8s.io/v1
    kind: VolumeSnapshotClass
    metadata:
      name: r-drbd
    driver: r-drbd.io
    deletionPolicy: Delete

## Taking a snapshot

    apiVersion: snapshot.storage.k8s.io/v1
    kind: VolumeSnapshot
    metadata:
      name: demo-snap
    spec:
      volumeSnapshotClassName: r-drbd
      source:
        persistentVolumeClaimName: demo

The CSI driver creates a Snapshot object; `kubectl get snapshots` shows its volume, phase and size.

A snapshot waits until the volume is Ready with every diskful replica UpToDate.
Then, for a replicated volume:

1. Every replica suspends IO.
2. Every diskful node creates a thin snapshot of its LV.
3. Every replica resumes IO.

All copies are taken at the same point in the write stream, so the snapshot is crash consistent on every node.
IO is frozen for the duration of step 2.
A local volume skips the suspend and resume steps.

Bounds:

- Suspending must finish within 30 s.
- Taking must finish within 2 minutes.
- Past a bound, IO is resumed and the snapshot ends Failed with the reason.

A failed take is a failure: the Snapshot never turns Ready without a backing LV on every node.

## Restoring

Create a PVC with the snapshot as its data source:

    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: demo-restored
    spec:
      storageClassName: r-drbd-replicated
      dataSource:
        apiGroup: snapshot.storage.k8s.io
        kind: VolumeSnapshot
        name: demo-snap
      accessModes: [ReadWriteOnce]
      resources:
        requests:
          storage: 1Gi

The new volume is a writable thin snapshot of the snapshot LV, so a restore is instant and shares blocks.
It always lands in the Pools that hold the snapshot.

Rules:

- A local restore (`replicas: 1`) uses one of the nodes holding the snapshot.
- A replicated restore needs `replicas` equal to the number of nodes holding the snapshot.
  Every node holding the snapshot restores it from its own copy, so the restore is instant, shares blocks on every node and needs no sync.
  The copies are identical because the snapshot was taken with IO suspended on every replica.
- A replicated restore larger than its snapshot starts at the snapshot's size with identical copies, then grows online.
  The new area syncs between the replicas; zero blocks travel as discards, so it costs little network and no thin pool space.
  During that sync the other replica is Inconsistent on the new area only, and the volume is not Ready yet.
  The claim is usable once the volume is Ready at the requested size.
- A replica that could not restore its copy is copied in full from another one.
- The requested size may be larger than the snapshot; it may not be smaller.
- Pins that point away from the snapshot's nodes or Pools fail the restore with reason `RestoreNodeConflict`.
- The claim must still fit the scheduler's capacity check (see [Scheduling](volumes.md#scheduling)), although placement never charges a restore against capacity.

The restored volume keeps the source's file system; it is never formatted again.

## Cloning

Cloning a PVC directly is not supported.
Snapshot it, then restore the snapshot.

## Deleting

Deleting the VolumeSnapshot (deletion policy Delete) deletes the Snapshot, and each node removes its snapshot LV.
A Snapshot that a restore still reads from stays until the restored volume is first Ready; a node holding a copy for that restore keeps it until then.
