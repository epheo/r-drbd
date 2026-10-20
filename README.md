# r-drbd

Replicated block storage for Kubernetes, built on DRBD 9 and thin LVM.
Kubernetes CRDs are the API; one static binary runs as the controller and as a per-node agent, and ships its own CSI driver.

## Features

- Local volumes (1 replica) and synchronously replicated volumes (2 or 3 replicas, DRBD protocol C, quorum with a tiebreaker).
- CSI driver `r-drbd.io`: provisioning, attach, XFS or raw block, online expansion, snapshots and restore.
- ReadWriteMany for raw block volumes with 2 or 3 replicas.
- Pools are per-node objects grouped by tier; placement by tier, or pinned to a node or pool with PVC annotations.
- Typed, watchable status: primary node, per-replica disk state, sync progress and peers, with Ready, Degraded, QuorumLost and SplitBrain conditions.
- Explicit repair actions on the Volume: `spec.resolve` for split brain, `spec.evict` to move a replica off a dead or retired node.
- Safe by default: an existing file system is never reformatted, a lost LV is never silently recreated empty, LVs outside its thin pool are never touched, a disk with a signature is never written, every wait is bounded.
- No operator, no REST API, no database: the Kubernetes API server is the only control path.
- Small footprint: a static binary on a scratch image; about 10 MiB of memory per process at idle.

## Requirements

- Kubernetes, OpenShift or MicroShift; the external-snapshotter CRDs and snapshot controller for snapshots.
- On every storage node: the DRBD 9 kernel module, lvm2, xfsprogs, and a thin pool in a VG or blank disks the agent builds one from. drbd-utils comes in the image.
- TCP ports 11000 to 11999 open between nodes for DRBD.
- 3 nodes for replicated volumes: DRBD quorum needs a third vote, which can come from a node without storage.
- Graceful node shutdown on in kubelet (`shutdownGracePeriod`, `shutdownGracePeriodCriticalPods`), so pods release their volumes before a node goes down.
- The agent runs privileged with hostPID: it executes the host's lvm, xfsprogs and mount in the host mount namespace.

## Quick start

Install the CRDs and the controller and agent:

    podman run --rm ghcr.io/epheo/r-drbd:sha-6765eba crds | kubectl apply -f -
    kubectl apply -k deploy/base

Declare a thin pool on a node (or list blank disks in `devices` and let the agent build it, see docs/pools.md):

    apiVersion: r-drbd.io/v1alpha1
    kind: Pool
    metadata:
      name: node1-nvme
    spec:
      node: node1
      tier: nvme
      vg: data
      thinPool: thin

Use one of the default StorageClasses, `r-drbd-local` (1 replica) or `r-drbd-replicated` (2 replicas), or write your own:

    apiVersion: storage.k8s.io/v1
    kind: StorageClass
    metadata:
      name: fast-replicated
    provisioner: r-drbd.io
    parameters:
      tier: nvme
      replicas: "2"
    volumeBindingMode: WaitForFirstConsumer
    allowVolumeExpansion: true

Then check state with `kubectl get pools,volumes,snapshots`.
See deploy/README.md for the manifests.

## Documentation

See [docs/](docs/README.md): concepts, install, pools, volumes, snapshots, operations, data safety and reference.

## License

Apache-2.0.
