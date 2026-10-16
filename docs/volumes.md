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

Pins are read once, when the volume is placed.
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

## Resize

Increase the PVC request:

    kubectl patch pvc demo -p '{"spec":{"resources":{"requests":{"storage":"20Gi"}}}}'

Each agent grows its LV, then DRBD grows once all replicas are larger, then XFS grows on the node using it.
Expansion works online.
Shrinking is refused.

`status.usableBytes` keeps the old size until every replica reports the new one.

## Lifecycle

`kubectl get volumes` shows size, replicas, tier, phase and diskful nodes.

| Phase | Meaning |
|---|---|
| Pending | Not placed yet |
| Provisioning | Placed; replicas being created or synced for the first time |
| Ready | Every replica present, up to date and connected |
| Degraded | Usable, but a replica is missing, out of date or on an unreachable node |
| Failed | Cannot be placed or restored as asked; the `Ready` condition says why |
| Deleting | Agents are tearing down |

A Failed volume for lack of capacity is retried when Pools change.

CSI calls wait for Ready up to 30 s, then return an error; the CSI sidecars retry.

## Deleting

Deleting the PVC (reclaim policy Delete) deletes the Volume.
Each agent takes DRBD down and removes the LV.
The Volume's finalizer stays until every replica reports it gone.

A replica on an unreachable node keeps the Volume in Deleting until that node comes back.
If it never will, delete its Node object: replicas on a node that no longer exists count as removed.
