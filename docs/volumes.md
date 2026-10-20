# Volumes

You normally never write a Volume yourself.
A PVC on an r-drbd StorageClass makes the CSI driver create one, named after the PV.

## StorageClass

    apiVersion: storage.k8s.io/v1
    kind: StorageClass
    metadata:
      name: nvme-replicated
    provisioner: r-drbd.io
    parameters:
      tier: nvme
      replicas: "2"
    reclaimPolicy: Delete
    volumeBindingMode: WaitForFirstConsumer
    allowVolumeExpansion: true

Parameters:

| Parameter | Required | Values | Meaning |
|---|---|---|---|
| `tier` | yes | any Pool tier | Pools the replicas may use |
| `replicas` | no, default 1 | 1, 2, 3 | Diskful copies, each on its own node |

No other parameter exists; r-drbd has no per-volume tuning.

Use `WaitForFirstConsumer` for local volumes, so the volume lands on the node the pod is scheduled to.
Replicated volumes work with either binding mode.

## Pinning a PVC

Two PVC annotations override placement:

    metadata:
      annotations:
        r-drbd.io/node: node2
        r-drbd.io/pool: node2-nvme

- `r-drbd.io/node`: one of the diskful replicas goes to this node.
- `r-drbd.io/pool`: every diskful replica goes to this Pool; only sensible with `replicas: 1`.

The CSI driver copies the annotations into the Volume (`spec.node`, `spec.pool`) once, at creation.
Placement reads them, and so does every `spec.evict` move: a replica pinned to the evicted node does not move.
The default manifests run csi-provisioner with `--extra-create-metadata`, which pins need.

## Volume modes

- `volumeMode: Filesystem`: XFS, formatted on first use.
- `volumeMode: Block`: the raw device, `/dev/drbd<minor>` or the LV for a local volume.

## Access modes

- `ReadWriteOnce` and `ReadWriteOncePod`: one node at a time.
  The device moves between nodes as pods move; DRBD promotes automatically on the node that opens it.
- `ReadWriteMany`: raw block only, `replicas` 2 or 3.
  Several nodes can open the device at once (DRBD dual Primary).
  The application must coordinate its writes; live migration of KubeVirt VMs is the typical user.

Refused:
- `ReadWriteMany` with `Filesystem` mode: XFS is not a shared file system.
- `ReadWriteMany` with `replicas: 1`: dual Primary needs DRBD.

## Topology

- Local volumes are pinned to their node: the PV carries node affinity on `r-drbd.io/node`.
- Replicated volumes have no topology restriction: a node without a replica attaches a diskless one over the network.

## Scheduling

Under `WaitForFirstConsumer` the scheduler sees each node's capacity per StorageClass, refreshed every minute.
For `replicas: 1`, a node's capacity is the largest `placeableBytes` among its Pools of the tier.
A node without such a Pool has none, so it is not chosen for a local claim.
For `replicas` 2 or 3, every node sees the same number, the largest volume placement can spread over that many nodes, since any node can attach it.
Only local claims are kept off nodes without a Pool.
A claim no Pool can hold stays Pending; the pod's events carry the scheduler's message, `node(s) did not have enough free storage`.
A restore claim must fit the current capacity too: the scheduler checks every claim, while placement never charges a restore against capacity, so a restore on a nearly full Pool can stay Pending although placement would accept it.
A pinned local claim (`r-drbd.io/pool`, `r-drbd.io/node`) lands on the pinned node whatever capacity says; if the scheduler picks another node, the PV node affinity then sends the pod to the pinned node.
If capacity changes between two refreshes, CreateVolume refuses the node and the scheduler picks again.

## Resize

Increase the PVC request:

    kubectl patch pvc demo -p '{"spec":{"resources":{"requests":{"storage":"20Gi"}}}}'

Each agent grows its LV, then DRBD grows once all replicas are larger, then XFS grows on the node using it.
Expansion works online.
Shrinking is refused.

`status.usableBytes` keeps the old size until every diskful replica on a reachable node reports the new one.
Meanwhile the Volume stays Ready, with `Ready` reason `Resizing`.
A volume that was never Ready (a restore larger than its snapshot) stays Provisioning below its size instead, `Ready` False with the same reason.

## Lifecycle

`kubectl get volumes` shows size, replicas, tier, phase and diskful nodes.

| Phase | Meaning |
|---|---|
| Pending | Not placed yet; reason `Insufficient` while no Pool has room, retried when Pools change |
| Provisioning | Placed; replicas being created or synced for the first time |
| Ready | Every diskful replica and the tiebreaker present; diskful replicas up to date and connected |
| Degraded | Usable, but a replica is missing, out of date or on an unreachable node |
| Failed | Invalid name, no free DRBD minor, port or node id, or a restore that cannot proceed; the `Ready` condition says why |
| Deleting | Agents are tearing down |

CSI calls wait for Ready up to 30 s, then return an error; the CSI sidecars retry.

## Deleting

Deleting the PVC (reclaim policy Delete) deletes the Volume.
Each agent takes DRBD down and removes the LV.
The Volume's finalizer stays until every replica reports it gone.

A replica on an unreachable node keeps the Volume in Deleting until that node comes back.
If it never will, delete its Node object: replicas on a node that no longer exists count as removed.
