# Install

## Host requirements

On every node that runs the agent (by default, every node):

- DRBD 9 kernel module loaded.
  drbd-utils is not needed on the host: the image carries its own.
  Recommended: module option `usermode_helper=disabled`.
  r-drbd sets no DRBD handlers, so the kernel's calls to `/sbin/drbdadm` do nothing for it, and they fail on hosts without drbd-utils.

On every node that stores data, also:

- lvm2 2.03.03 or newer, and either a thin pool in a volume group, or blank disks the agent builds one from.
  Older LVM does not know `scan_lvs`; the agent then marks its Pools Available False, reason `LvmUnsupported`.
- xfsprogs.
- The host's own LVM must not scan DRBD devices or LVs. Either:
  - a devices file (`use_devicesfile = 1`, the default on recent distributions) that lists only the host's real PVs; or
  - in lvm.conf, `global_filter` rejecting `/dev/drbd` and `scan_lvs = 0`.

  Why: a volume can hold a guest's own LVM. Without these rules, udev's pvscan autoactivation can activate that guest VG through `/dev/drbdN` on the host.
  That holds the DRBD device open and runs the guest VG twice, on the host and in the guest.
  The agent's own commands never scan them, whatever the host config; it logs a warning, once the node has a Pool, when the host's rules miss this or cannot be verified.

Cluster:

- TCP ports 11000 to 11999 open between nodes for DRBD.
- For replicated volumes, 3 nodes: DRBD quorum needs a third vote.
  The third node can be a diskless tiebreaker with no Pool.
- For snapshots, the external-snapshotter CRDs and snapshot controller.

Node shutdown:

- Graceful node shutdown must be on in kubelet, so pods stop and release their volumes before the node goes down.
  In the KubeletConfiguration:

      shutdownGracePeriod: 90s
      shutdownGracePeriodCriticalPods: 30s

  On MicroShift, the same two lines under `kubelet:` in a file in /etc/microshift/config.d/, then restart microshift:

      kubelet:
        shutdownGracePeriod: 90s
        shutdownGracePeriodCriticalPods: 30s

  The critical share is the agent's window to release what kubelet could not; keep it at 30 s or more.
  Without it, a node that shuts down with a volume in use can leave the other replicas Outdated until it returns.
  The agent warns, and records a `GracefulShutdownOff` Event on the Node, while kubelet holds no shutdown delay lock; it checks again every hour.
- If the host has drbd-utils installed, its `drbd-graceful-down.service` and `drbd-graceful-disconnect.service` are not needed.
  Removing drbd-utils from the host, or masking both units, avoids the cases below.
  With graceful node shutdown on, their shutdown hooks find no Primary in use and do no harm.
  With it off, the disconnect unit cuts an in-use Primary as the shutdown starts, before the agent can act, and its peers turn Outdated.
  On hosts whose network is run by systemd-networkd or netplan, a network restart can make the disconnect unit cut an in-use Primary and outdate its peers; a NetworkManager restart does not.
  The agent warns once per start, with a `HostDrbdUnits` Event on the Node, when they are loaded.

## How the agent uses the host

The image holds the static r-drbd binary and a static drbd-utils, with that drbd-utils' GPLv2 source under `/usr/share/src/drbd-utils`.
The agent runs privileged with hostPID and hostNetwork.
drbdadm, drbdsetup and drbdmeta run from the image, with the image's own `/etc/drbd.conf`: it includes the host's `/etc/drbd.d/*.res`, and nothing else from the host's DRBD configuration.
Every other command enters the host's IPC and mount namespaces and runs the host's own lvm, xfsprogs and mount.
The agent reports the versions as Node annotations:
`r-drbd.io/drbd-kernel`, `r-drbd.io/drbd-utils` (the image's), `r-drbd.io/lvm` (the host's).

Host paths it writes:

- `/var/lib/r-drbd/<volume>.res`: DRBD resource files.
- `/etc/drbd.d/r-drbd.res`: the include for that directory.
- `/var/lib/drbd`, `/run/drbd`: drbd-utils state and locks, shared with any host drbd-utils.
- `/var/lib/kubelet/plugins/r-drbd.io/`: the CSI socket.

## Install the manifests

Install the CRDs, printed by the binary itself:

    podman run --rm ghcr.io/epheo/r-drbd:latest crds | kubectl apply -f -

Install the controller, the agent, RBAC, the CSIDriver and default StorageClasses:

    kubectl apply -k deploy/base

Everything runs in namespace `r-drbd-system`.

OpenShift and MicroShift: `deploy/base/00-namespace-rbac.yaml` already binds the privileged SCC to the agent's service account.
On plain Kubernetes that binding is harmless: its role does not exist.

Air-gapped clusters: the manifests pull `ghcr.io/epheo/r-drbd` and the CSI sidecars from `registry.k8s.io`.
Mirror them and patch the images with a kustomize overlay.

Check that it runs:

    kubectl -n r-drbd-system get pods
    kubectl get csidriver r-drbd.io

## Declare a Pool

On each storage node, create a thin pool if you have none:

    lvcreate --type thin-pool -L 500G -n thin data

Then declare it:

    apiVersion: r-drbd.io/v1alpha1
    kind: Pool
    metadata:
      name: node1-nvme
    spec:
      node: node1
      tier: nvme
      vg: data
      thinPool: thin

Or let the agent build the VG and thin pool from blank disks:

    spec:
      node: node1
      tier: nvme
      vg: r-drbd-nvme
      thinPool: thin
      devices:
        - /dev/disk/by-id/nvme-Example_SSD_2TB_S7KH0000000001

Wait for it to be available:

    kubectl get pools

See [Pools](pools.md) for details, and for how to find by-id paths.

## Use a StorageClass

`deploy/base` ships two classes, both for tier `nvme`:

- `r-drbd-local`: 1 replica.
- `r-drbd-replicated`: 2 replicas.

And one VolumeSnapshotClass, `r-drbd`.

Write your own for other tiers; see [Volumes](volumes.md).

## Try it

    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: demo
    spec:
      storageClassName: r-drbd-replicated
      accessModes: [ReadWriteOnce]
      resources:
        requests:
          storage: 1Gi

The classes use `WaitForFirstConsumer`, so the volume is created when a pod uses the claim.
Then:

    kubectl get volumes

## Uninstall

1. Delete every PVC and VolumeSnapshot backed by r-drbd, and wait for the Volumes and Snapshots to go.
2. Delete the Pools.
3. `kubectl delete -k deploy/base`, then delete the CRDs.

Deleting the agent first leaves Volumes stuck on their finalizers: the agent is what removes the LVs.
