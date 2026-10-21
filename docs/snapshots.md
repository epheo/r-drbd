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

A snapshot of a replicated volume waits in Pending, saying why in `lastError`, until:

- the volume is Ready, at its requested size, with no resize in flight;
- every replica's agent is reachable;
- every diskful replica is UpToDate, and every diskful or attached replica is connected to every diskful replica;
- every diskful replica applied the current spec;
- no other snapshot of the same volume is suspending or taking.
- at most one replica is Primary: no snapshot runs during a two-Primary live migration.

A replica whose agent is down holds every snapshot of its volume; evict or detach it to take one.

Then:

1. Every replica suspends IO, diskless and tiebreaker included, within 30 s; the Primary acks once none of its writes is still on its way to a disk or a peer.
2. Every diskful node creates a thin snapshot of its LV, and every other replica confirms its IO is still suspended, within 2 minutes.
3. The snapshot is Ready, or Failed with the reason; every replica resumes IO on its own when it sees the round end.

Each node's copy is crash-consistent.
The copies of one snapshot can still differ: a page rewritten while it is written out can reach each node's disk in another version.
Snapshots are crash-consistent: no file system is frozen or flushed first, so data still in a page cache is not in the snapshot, as after a power loss.
For application consistency, quiesce the application (for example with a pre-snapshot hook) before taking the VolumeSnapshot.
A local volume skips the suspend: it waits only for the volume to be Ready and for no other snapshot of it to run.

Each node keeps its own deadlines, on its own clock:

- it takes its copy only within 2 minutes of its own suspend, and never after an agent restart or a reboot;
- it resumes IO 4 minutes after its own suspend, even when the controller or the API server is gone; the snapshot then fails unless every copy was already taken.

So IO stays frozen about 4 minutes at most.
A node that is no longer suspended when it should be (a reboot, a resume on its deadline) fails the snapshot.
A replica attached during the round never suspended, so it fails the snapshot too.
While a round runs, volume expansion is refused (the call is retried) and no replica grows.

A failed take is a failure: the Snapshot never turns Ready without a backing LV on every node.
A snapshot of a volume that is being deleted, or that stays missing for 30 s, fails.

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

It always lands in the Pools that hold the snapshot.

Rules:

- A local restore (`replicas: 1`) uses one of the nodes holding the snapshot, preferring one whose agent is alive; the new volume is a writable thin snapshot of the snapshot LV, so it is instant and shares blocks.
- A replicated restore needs `replicas` equal to the number of nodes holding the snapshot.
  The first node restores its copy, a thin snapshot; every other replica starts empty and is copied in full from it.
  The volume is Ready, and its claim usable, once that sync ends.
- A holder whose node is down, whose agent is stale or whose Pool is not Available keeps the restore Pending, reason `Insufficient`, naming it.
- A Ready snapshot whose acks lost their `takenPool` keeps the restore Pending, reason `SnapshotNotReady`, until each holder finds its LV again and acks it.
- A replicated restore larger than its snapshot starts at the snapshot's size, then grows online; the new area syncs, zero blocks as discards.
  The claim is usable once the volume is Ready at the requested size.
- A full thin pool for an empty replica stays an error.
  The first replica must hold its copy: without it the restore does not start.
- The requested size may be larger than the snapshot; it may not be smaller.
- Pins that point away from the snapshot's nodes or Pools fail the restore with reason `RestoreNodeConflict`.
- Placement charges every replica at the volume's full size: a holding Pool without that room keeps the restore Pending, reason `Insufficient`.
  The claim must also fit the scheduler's capacity check (see [Scheduling](volumes.md#scheduling)).

The restored volume keeps the source's file system; it is never formatted again.

## Cloning

Cloning a PVC directly is not supported.
Snapshot it, then restore the snapshot.

## Deleting

Deleting the VolumeSnapshot (deletion policy Delete) deletes the Snapshot, and each node removes its snapshot LV.
Each node removes it from the Pool it was taken in (`takenPool`), even after its replica moved to another Pool.
A node without that record searches the thin pools of its Pools for the LV; an LV of that name outside them is an error, never removed.
A Snapshot waits for the restores from it that are not yet initialized: it stays until each restored volume is first Ready, and a node holding a copy for such a restore keeps it until then.
A restore that cannot start says why in its Ready condition, for example "LV <vg>/<lv> is not this restore's copy of snapshot <name>; delete the claim and restore again".
Deleting a stuck claim removes its LVs and releases the Snapshot.
