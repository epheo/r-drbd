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

A replicated restore starts without a sync, so every copy of one snapshot must hold the same bytes.
A snapshot of a replicated volume therefore waits in Pending, saying why in `lastError`, until:

- the volume is Ready, at its requested size, with no resize in flight;
- every replica's agent is reachable;
- every diskful replica is UpToDate, and every diskful or attached replica is connected to every diskful replica;
- every diskful replica applied the current spec;
- no other snapshot of the same volume is suspending or taking.
- at most one replica is Primary: no snapshot runs during a two-Primary live migration.

A replica whose agent is down holds every snapshot of its volume; evict or detach it to take one.

Then:

1. Every replica suspends IO, diskless and tiebreaker included, within 30 s: the Primary first, and it acks once none of its writes is still on its way to a disk; the others suspend after that ack.
2. Every diskful node creates a thin snapshot of its LV, and every other replica confirms its IO is still suspended, within 2 minutes.
3. The snapshot is Ready, or Failed with the reason; every replica resumes IO on its own when it sees the round end.

All copies are taken at the same point in the write stream on every node.
Snapshots are crash-consistent: no file system is frozen or flushed first, so data still in a page cache is not in the snapshot, as after a power loss.
For application consistency, quiesce the application (for example with a pre-snapshot hook) before taking the VolumeSnapshot.
A local volume skips the suspend: it waits only for the volume to be Ready and for no other snapshot of it to run.

Each node keeps its own deadlines, on its own clock:

- it takes its copy only within 2 minutes of its own suspend, and never after an agent restart or a reboot;
- it resumes IO 4 minutes after its own suspend, even when the controller or the API server is gone; the snapshot then fails unless every copy was already taken.

So IO stays frozen about 4 minutes at most, and no node resumes before every copy of a Ready snapshot is taken.
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

The new volume is a writable thin snapshot of the snapshot LV, so a restore is instant and shares blocks.
It always lands in the Pools that hold the snapshot.

Rules:

- A local restore (`replicas: 1`) uses one of the nodes holding the snapshot, preferring one whose agent is alive.
- A replicated restore needs `replicas` equal to the number of nodes holding the snapshot.
  Every node holding the snapshot restores it from its own copy, so the restore is instant, shares blocks on every node and needs no sync.
  The copies are identical because the snapshot was taken with IO suspended on every replica.
  A snapshot taken by an older r-drbd version restores from the first node's copy only; the other replicas full-sync from it.
- A holder whose node is down, whose agent is stale or whose Pool is not Available keeps the restore Pending, reason `Insufficient`, naming it.
- A replicated restore larger than its snapshot starts at the snapshot's size with identical copies, then grows online.
  The new area syncs between the replicas; zero blocks travel as discards, so it costs little network and no thin pool space.
  During that sync the other replica is Inconsistent on the new area only, and the volume is not Ready yet.
  The claim is usable once the volume is Ready at the requested size.
- A replica that cannot restore from its own copy, for any reason (the Snapshot, its snapshot LV or the lvcreate), starts empty and is copied in full from the first one; a full thin pool stays an error.
  The first replica must hold its copy: without it the restore does not start.
- The requested size may be larger than the snapshot; it may not be smaller.
- Pins that point away from the snapshot's nodes or Pools fail the restore with reason `RestoreNodeConflict`.
- Placement charges only the growth beyond the snapshot: each holding Pool's capacity policy must still allow it, or the restore stays Pending, reason `Insufficient`.
  The claim must also fit the scheduler's capacity check (see [Scheduling](volumes.md#scheduling)).

The restored volume keeps the source's file system; it is never formatted again.

## Cloning

Cloning a PVC directly is not supported.
Snapshot it, then restore the snapshot.

## Deleting

Deleting the VolumeSnapshot (deletion policy Delete) deletes the Snapshot, and each node removes its snapshot LV.
A Snapshot waits for the restores from it that are not yet initialized: it stays until each restored volume is first Ready, and a node holding a copy for such a restore keeps it until then.
A restore that cannot start says why in its Ready condition, for example "LV <vg>/<lv> is not this restore's copy of snapshot <name>; delete the claim and restore again".
Deleting a stuck claim removes its LVs and releases the Snapshot.
