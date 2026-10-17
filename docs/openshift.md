# OpenShift

r-drbd runs on OpenShift as on any Kubernetes, with three differences.
RHCOS ships no DRBD module and is not meant to take packages, so Kernel Module Management (KMM) builds the module for each node's kernel and loads it.
Privileged workloads need a SecurityContextConstraints binding, which `deploy/base` carries.
Graceful node shutdown is off until a KubeletConfig turns it on.

This page walks through an install on OpenShift 4.14 or newer with KMM 2.x.
The pieces live in `deploy/openshift`.
r-drbd's own test suite has not run on an OpenShift cluster yet.
The module build was tested against a CentOS Stream 9 kernel outside KMM, not yet with the Driver Toolkit.

## Before you start

- cluster-admin on the cluster.
- Storage nodes with blank disks, or a volume group with a thin pool. RHCOS has lvm2 and xfsprogs.
- Three nodes for replicated volumes; one may be a diskless tiebreaker. A single node cluster serves local volumes only.
- TCP 11000 to 11999 open between nodes. RHCOS puts no firewall between nodes; on a cloud, open them in the node network's security group.
- Egress from the cluster, or the mirror steps below. KMM's build pulls the DRBD source from pkg.linbit.com, may call spaas.drbd.io, and takes its base images and `patch` from registry.access.redhat.com and cdn-ubi.redhat.com; the manifests pull from ghcr.io and registry.k8s.io.
- Nothing for snapshots: OpenShift ships the snapshot CRDs and controller.

## 1. The DRBD kernel module

KMM builds the module with the Driver Toolkit of the running release, for the exact kernel of each node, and loads it.
After a cluster upgrade it builds again for the new kernel.

Install the operator from OperatorHub, or from the CLI:

    oc create namespace openshift-kmm
    oc apply -f - <<EOF
    apiVersion: operators.coreos.com/v1
    kind: OperatorGroup
    metadata:
      name: kernel-module-management
      namespace: openshift-kmm
    ---
    apiVersion: operators.coreos.com/v1alpha1
    kind: Subscription
    metadata:
      name: kernel-module-management
      namespace: openshift-kmm
    spec:
      channel: stable
      name: kernel-module-management
      source: redhat-operators
      sourceNamespace: openshift-marketplace
    EOF
    until oc -n openshift-kmm get deployment kmm-operator-controller >/dev/null 2>&1; do sleep 5; done
    oc -n openshift-kmm rollout status deployment/kmm-operator-controller

On OKD the source is `community-operators`.

Apply the Module and its Dockerfile:

    oc apply -k deploy/openshift/kmm

What it holds:

- `Dockerfile`: builds DRBD 9.3.4 from the LINBIT source tarball, with its checksum pinned, in the Driver Toolkit, and installs `drbd` and `drbd_transport_tcp` under `/opt/lib/modules/<kernel>/` with `depmod`.
  In-tree modules drbd needs (`libcrc32c` on RHEL 9 kernels) resolve through a `host` link to the node's own modules.
- `module.yaml`: a Module for every Linux node.
  It names `drbd_transport_tcp`, so `drbd` loads as its dependency; the kernel cannot autoload the transport from an image the host's modprobe never sees.
  Images go to the internal registry as `openshift-kmm/drbd-kmod:9.3.4-<kernel>`.

A build takes a few minutes per kernel version.
Watch it, then the load:

    oc -n openshift-kmm get builds,pods
    oc -n openshift-kmm describe module drbd
    oc -n openshift-kmm get module drbd -o jsonpath='{.status.moduleLoader}{"\n"}'

`availableNumber` equals `desiredNumber` when every selected node has the module.
After r-drbd is installed, each node also reports the module version:

    oc get nodes -o custom-columns='NAME:.metadata.name,DRBD:.metadata.annotations.r-drbd\.io/drbd-kernel'

### Variants

No internal registry (`managementState: Removed`, common on bare metal): point `containerImage` at a registry you can push to, and add to the Module an `imageRepoSecret` naming a `kubernetes.io/dockerconfigjson` Secret in `openshift-kmm` that can push and pull there.

Build outside the cluster, for an air gap or to keep the artefact under review:

    RELEASE=$(oc get clusterversion version -o jsonpath='{.status.desired.image}')
    DTK=$(oc adm release info --image-for=driver-toolkit "$RELEASE")
    KVER=$(oc get node <node> -o jsonpath='{.status.nodeInfo.kernelVersion}')
    oc get secret -n openshift-config pull-secret -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d > pull-secret.json
    podman build --authfile pull-secret.json \
      --build-arg DTK_AUTO="$DTK" --build-arg KERNEL_FULL_VERSION="$KVER" \
      -t <registry>/drbd-kmod:9.3.4-"$KVER" deploy/openshift/kmm
    podman push <registry>/drbd-kmod:9.3.4-"$KVER"

Then set `containerImage` to that name and drop the `build` section.
One image per kernel version in the cluster.

Secure Boot: the node loads only signed modules.
KMM signs in the cluster with a `sign` section on the kernel mapping: the key and certificate in Secrets, `filesToSign` naming both `.ko` files, and the certificate enrolled in the MOK database of every node.
An unsigned module fails to load with `Required key not available` in the worker pod's log.

Another DRBD version: change `DRBD_VERSION` and `DRBD_SHA256` in the Dockerfile, and the version in the image tag of `module.yaml`.
KMM builds only when the tag is absent, so a Dockerfile change alone does nothing.

## 2. Node settings

    oc apply -k deploy/openshift/node

Two settings per pool, worker and master, in one rollout: the Machine Config Operator reboots each node of the pool once.
Do this before the nodes hold data, or during a maintenance window.
A custom pool needs copies with its own labels, `pools.operator.machineconfiguration.openshift.io/<pool>` on the KubeletConfig and `machineconfiguration.openshift.io/role: <pool>` on the MachineConfig.

Graceful node shutdown, a KubeletConfig: without it, a node that shuts down with a volume in use leaves the other replicas Outdated until it returns; see [Install](install.md).
The agent records a `GracefulShutdownOff` Event on the Node while it is off.
A pool that already has a KubeletConfig takes the two fields in that object instead: each KubeletConfig renders a whole kubelet configuration, and the newest wins.

An LVM devices file, a MachineConfig unit: RHCOS runs LVM with `use_devicesfile = 1` but no devices file, so its LVM scans every block device, DRBD devices included, and a guest's LVM inside a volume would be activated on the host.
On its first boot the unit imports the node's own PVs into a devices file, or creates an empty one, before the container runtime starts, so no DRBD device is ever scanned by the host.
LVM maintains the file from then on, and a Pool's build adds its disks to it.
The agent warns at start on a node whose LVM still scans DRBD devices.

Wait for the pools, then check one node:

    oc get machineconfigpool
    oc get --raw /api/v1/nodes/<node>/proxy/configz | jq .kubeletconfig.shutdownGracePeriod
    oc debug node/<node> -- chroot /host lvmdevices

## 3. r-drbd

As in [Install](install.md):

    podman run --rm ghcr.io/epheo/r-drbd:latest crds | oc apply -f -
    oc apply -k deploy/base

The namespace is labelled `pod-security.kubernetes.io/enforce: privileged`, and the agent's service account is bound to the `privileged` SCC by the manifests.
The agent runs on every node; it needs the module there.
An agent that starts before the module serves the node without replication, records a `DrbdModuleMissing` Event on the Node, and restarts by itself within ten seconds of the module being loaded.
So the order between KMM and r-drbd does not matter, and a node whose module arrives minutes after a reboot heals on its own.

RHCOS has no `drbdadm`, so the agent sets the module's `usermode_helper` to `disabled`; nothing to configure.

Check:

    oc -n r-drbd-system get pods
    oc get csidriver r-drbd.io
    oc get nodes -o custom-columns='NAME:.metadata.name,DRBD:.metadata.annotations.r-drbd\.io/drbd-kernel,LVM:.metadata.annotations.r-drbd\.io/lvm'

## 4. Pools

List each node's disks and take the `path` of `Blank` ones:

    oc get nodedisks
    oc get nodedisks <node> -o yaml

    apiVersion: r-drbd.io/v1alpha1
    kind: Pool
    metadata:
      name: worker-0-nvme
    spec:
      node: worker-0
      tier: nvme
      vg: r-drbd-nvme
      thinPool: thin
      devices:
        - /dev/disk/by-id/nvme-Example_SSD_2TB_S7KH0000000001

    oc get pools

LVM on RHCOS: the root file system is not on LVM, so a fresh node's devices file from step 2 is empty, and the build adds the Pool's disks to it.
A node that skipped step 2 and already has PVs is refused with the PVs named; add them to a devices file first, one VG at a time:

    oc debug node/<node> -- chroot /host vgimportdevices <vg>

Never `vgimportdevices --all` on such a node: with no devices file its LVM also sees PVs inside volumes attached through DRBD, and the import would hand them to the host.

LVM Storage (LVMS) on the same node: its disks show `NotEmpty` in `NodeDisks`, and r-drbd never touches a volume group without its own tag.
Give each product its own disks.

What runs on a disk and every corner case: [Disks](disks.md); the rest of the Pool: [Pools](pools.md).

## 5. StorageClasses and a first volume

`deploy/base` ships `r-drbd-local` and `r-drbd-replicated` for tier `nvme`, and the VolumeSnapshotClass `r-drbd`.

    oc apply -f - <<EOF
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
    EOF

The classes bind on first use; start a pod with the claim, then:

    oc get volumes

## Upgrades

OpenShift upgrade: have KMM build for the target kernel first, or each node comes back from its reboot and waits for the build.
Its agent then runs without replication for those minutes and restarts once the module is there; volumes with a replica on that node are degraded meanwhile.

    TARGET=quay.io/openshift-release-dev/ocp-release:<version>-x86_64
    DTK=$(oc adm release info --image-for=driver-toolkit "$TARGET")
    podman run --rm --authfile pull-secret.json "$DTK" cat /etc/driver-toolkit-release.json

Take `KERNEL_VERSION` from that file, then:

    apiVersion: kmm.sigs.x-k8s.io/v1beta2
    kind: PreflightValidationOCP
    metadata:
      name: drbd-<version>
    spec:
      kernelVersion: <KERNEL_VERSION>
      dtkImage: <DTK>
      pushBuiltImage: true

Wait for `status.modules` to show `verificationStatus: Success` for `drbd`, delete the object, then upgrade.
A KMM too old for `v1beta2` takes a `releaseImage` in the `v1beta1` object instead of `kernelVersion` and `dtkImage`.

r-drbd upgrade: [Operations](operations.md).

## Uninstall

Follow the order in [Install](install.md): claims and snapshots, then Pools, then `deploy/base` and the CRDs.
Then `oc delete -k deploy/openshift/kmm`: KMM unloads the module on each node; a node still holding a DRBD resource keeps it until it reboots.
The node settings can stay; deleting them reboots the pools again, and the devices file stays either way.

## Corner cases

| Case | What happens | What to do |
|---|---|---|
| The build logs `Successfully connected to SPAAS` | The release has no compat patch for this kernel; LINBIT's service generated it | Nothing; it is the normal path for a new kernel |
| The build fails reaching spaas.drbd.io or pkg.linbit.com | No egress from build pods | Allow it, or build outside the cluster |
| The build fails to pull the toolkit | The cluster pull secret cannot reach the release registry (disconnected cluster) | Mirror the Driver Toolkit, or build outside |
| The build cannot push | No internal registry | Use an external registry and `imageRepoSecret` |
| `Required key not available` in a worker pod | Secure Boot, unsigned module | Sign it |
| A node runs the real-time or 64k-pages kernel | KMM builds a second image for that kernel; the toolkit carries its headers | Nothing |
| `availableNumber` stays below `desiredNumber` | A node's build or load failed | `oc -n openshift-kmm describe module drbd`; the Events name the kernel |
| The agent started before the module | It serves the node without replication, records `DrbdModuleMissing`, and restarts by itself within ten seconds of the load | Nothing |
| A node reboots during the node settings rollout while it holds a Primary | Pods on it stop with no grace, the shutdown path of [Operations](operations.md) applies | Apply `deploy/openshift/node` before the nodes hold data |
| A Pool stays `Building` naming PVs and `vgimportdevices` | PVs on the node but no devices file | `vgimportdevices <vg>` per VG, never `--all` |
| The agent logs `use_devicesfile is 1 but the devices file does not exist` at start | The node skipped step 2: its LVM scans DRBD devices | Apply `deploy/openshift/node`, or create the file by hand as the unit does |
| A node got PVs after step 2 outside LVM's own commands (a cloned disk) | They are not in the devices file; the host's LVM ignores them | `lvmdevices --adddev <device>` |
| A single node cluster | Replicated classes never bind: no peer | Use `r-drbd-local` |
| Compact cluster, data on control plane nodes | Works; the agent tolerates every taint | Apply the master KubeletConfig too |
| Kernel log lines `helper command: /sbin/drbdadm ... exit code` | The agent could not set `usermode_helper` (no write access to sysfs) | Harmless; check the agent log for the reason |
