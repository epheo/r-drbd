# Concepts

## One binary, two roles

- `r-drbd controller`: a Deployment with one replica.
  It places volumes, allocates DRBD parameters, drives snapshots and serves the CSI controller plugin.
- `r-drbd agent`: a DaemonSet on every node.
  It creates thin LVs and DRBD resources on its node, reports their state and serves the CSI node plugin.

The Kubernetes API server is the only control path.
There is no REST API, no database and no operator.
Everything you can see or change is a Kubernetes object.

## Objects

All three kinds are cluster scoped, in group `r-drbd.io/v1alpha1`.

- **Pool**: one LVM thin pool on one node, labelled with a tier.
- **Volume**: one block device, local or replicated.
  The CSI driver creates one per PersistentVolume; its name is the PV name (`pvc-<uuid>`).
- **Snapshot**: a crash-consistent thin snapshot of a Volume.
  The CSI driver creates one per VolumeSnapshotContent.

Nodes are the Kubernetes Node objects.
A node's DRBD address is its InternalIP.

## Local and replicated volumes

- `replicas: 1`: a plain thin LV on one node, no DRBD.
  The PV gets node affinity, so pods follow the data.
- `replicas: 2` or `3`: a DRBD resource with one diskful replica per node, synchronous (protocol C).
  Any node in the cluster can attach it; nodes without a replica get a diskless one.

Replicated volumes use DRBD quorum.
With 2 replicas, a third node holds a diskless tiebreaker so a single failure keeps quorum.
That third node needs DRBD but no Pool.

## Tiers

A tier is a free-form label on a Pool, such as `nvme` or `hdd`.
A StorageClass asks for a tier; the controller places each replica on a different node with a Pool of that tier.
A node may hold several Pools of the same tier.

## Placement

- Each diskful replica goes to a distinct node.
- Candidates are Available Pools of the requested tier on Ready nodes.
- A Pool accepts a volume only while its capacity policy allows it; by default, never more than the pool holds less a 5 percent reserve (see [Pools](pools.md#capacity)).
- Among the candidates, the most free space wins.
- A PVC can pin a node or a Pool with annotations (see [Volumes](volumes.md)).
- A restore always lands where the snapshot lives.

Placement happens once, when the Volume is first assigned.
r-drbd never moves a replica on its own; you ask for it with `spec.evict` (see [Operations](operations.md)).

## Status

Every object reports typed, watchable status.

- Pool: capacity, chunk size, the Volumes using it, an `Available` condition.
- Volume: phase, primary node, usable size, one entry per replica (disk state, sync progress, peers, quorum, device path, last error) and conditions `Ready`, `Degraded`, `QuorumLost`, `SplitBrain`, `PeerUnreachable`.
- Snapshot: phase, size, and one acknowledgement per node.

Each agent writes only its own replica entries.
An agent heartbeat is a Lease `r-drbd-agent-<node>`, renewed every 10 s.
After 40 s without renewal, that node's entries count as unknown and the Volume turns Degraded with reason `NodeUnreachable`.

## Fixed choices

r-drbd has no per-volume tuning.

- Thin LVM only.
- XFS for file system volumes; raw block otherwise.
- DRBD protocol C, quorum majority, IO suspended while quorum is lost, internal metadata.
- DRBD ports 11000 to 11999, minors 50000 to 99999.
