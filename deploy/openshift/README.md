# deploy/openshift

What OpenShift needs on top of `deploy/base`; the walkthrough is in [docs/openshift.md](../../docs/openshift.md).

- `kmm/`: a Kernel Module Management `Module` and the Dockerfile that builds DRBD 9 for each node's kernel with the Driver Toolkit.
  `oc apply -k deploy/openshift/kmm` once the KMM operator runs.
- `node/`: `KubeletConfig` objects that turn graceful node shutdown on, and `MachineConfig` units that give each node an LVM devices file so the host's LVM never scans DRBD devices.
  One rollout, each node of a pool reboots once.
