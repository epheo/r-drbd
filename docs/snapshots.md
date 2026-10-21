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
A CreateSnapshot retry under the same name replaces a Failed Snapshot with a new attempt, answers not ready while a lost status is being rebuilt, and refuses a Lost Snapshot with FailedPrecondition: take a new snapshot under another name (see [Operations](operations.md#a-lost-snapshot-status)).

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
   The lvcreate tags the snapshot LV `r-drbd.snap.<snapshot uid>.<size bytes>.<source replicas>`.
3. The snapshot is Ready, or Failed with the reason; every replica resumes IO on its own when it sees the round end.
   `createdAt` is the creation time of the earliest copy.

Each node's copy is crash-consistent.
The copies of one snapshot can still differ: a page rewritten while it is written out can reach each node's disk in another version.
So once Ready, each node holding a copy of a replicated snapshot hashes it in the background, one snapshot at a time per node, at idle IO priority, and acks the digest:
a sha256 over 4 MiB chunks, up to the DRBD device size the copy's own metadata records.
Once every holder acked, the controller writes `status.copiesIdentical`: true when all digests match; false otherwise, with a Warning Event `ImageCopiesDiffer` naming each holder's digest.
Snapshots are crash-consistent: no file system is frozen or flushed first, so data still in a page cache is not in the snapshot, as after a power loss.
For application consistency, quiesce the application (for example with a pre-snapshot hook) before taking the VolumeSnapshot.
A local volume skips the suspend: it waits only for the volume to be Ready and for no other snapshot of it to run.

Each node keeps its own deadlines, on its own clock:

- it takes its copy only within 2 minutes of its own suspend, and never after an agent restart or a reboot;
- it resumes IO 4 minutes after its own suspend, even when the controller or the API server is gone; the snapshot then fails unless every copy was already taken.

So IO stays frozen about 4 minutes at most.
A node resumes before its deadline only when the Snapshot whose round froze its IO reaches a phase past the round; one whose phase vanished keeps IO frozen up to the deadline, so every copy of a round is taken inside one freeze.
A node that is no longer suspended when it should be (a reboot, a resume on its deadline) fails the snapshot.
A replica attached during the round never suspended, so it fails the snapshot too.
While a round runs, volume expansion is refused (the call is retried) and no replica grows.

A failed take is a failure: the Snapshot never turns Ready without a backing LV on every node.
A snapshot of a volume that is being deleted, or that stays missing for 30 s, fails.
On a Failed Snapshot every node with a Pool removes an LV tagged with its uid, whether or not it took part.
A Failed Snapshot is deleted one hour after it failed.

After a failed round the volume's next snapshot waits in Pending 30 s, doubling per failure in a row up to 10 minutes; a Ready snapshot resets the wait.
A node that never answered a failed round holds the next one until its agent renews its Lease.

An LV of the snapshot LV's name that does not carry this Snapshot's uid (an LV left by an older Snapshot of the same name, say) is never adopted: the round fails before that node suspends IO, the LV stays untouched, and a Warning Event `SnapshotLvConflict` names it.
A retry under the same name fails the same way while that LV is there.

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

- A copy is restored only when its LV carries the Snapshot's uid tag: a snapshot taken before these tags existed is no longer restorable, and its restore fails saying so (CreateVolume answers FailedPrecondition; a restore already placed fails with reason `UnsupportedRestore`).
  A restored LV never keeps the snapshot's tag.
- A local restore (`replicas: 1`) uses one of the nodes holding the snapshot, preferring one whose agent is alive; the new volume is a writable thin snapshot of the snapshot LV, so it is instant and shares blocks.
- A replicated restore needs `replicas` equal to the number of nodes holding the snapshot.
  With `copiesIdentical` true every replica restores its own copy, a thin snapshot, and nothing syncs.
  With it false, the first node restores its copy and every other replica starts empty and is copied in full from it.
  While the digests are pending each replica restores its copy and the first node waits for them up to 2 minutes; past that it is the sync source and the others are copied in full.
  The volume is Ready, and its claim usable, once any sync ends.
- A holder whose node is down, whose agent is stale or whose Pool is not Available keeps the restore Pending, reason `Insufficient`, naming it.
- A Ready snapshot with fewer holders than its source had replicas (an ack lost) has every node without an ack search for its LV by the uid tag and ack a copy it finds.
- A snapshot rebuilding a lost status (phase `Discovering`) is waited for: CreateVolume writes nothing until its size is known again, and the replicas of a restore already placed wait for their copies rather than start empty.
  A Lost one is refused: CreateVolume answers FailedPrecondition, and a restore already placed fails with reason `SnapshotLost`.
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
Each node removes it from the Pool it was taken in (`takenPool`), even after its replica moved to another Pool: the LV of that name with this Snapshot's uid tag, or with no snap tag (taken before tags existed), never one with a snap tag of another uid or one that does not parse.
A node without that record searches the thin pools of its Pools for the LV with this Snapshot's uid tag; such an LV outside them is an error, never removed.
A Snapshot rebuilding a lost status finishes that first; then, Ready or Lost, it removes the copies its discovery found.
A Snapshot waits for the restores from it that are not yet initialized: it stays until each restored volume is first Ready, and a node holding a copy for such a restore keeps it until then.
A restore that cannot start says why in its Ready condition, for example "LV <vg>/<lv> is not this restore's copy of snapshot <name>; delete the claim and restore again".
Deleting a stuck claim removes its LVs and releases the Snapshot.
