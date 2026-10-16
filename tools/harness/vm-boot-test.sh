#!/usr/bin/env bash
# A KubeVirt VM boots from a disk provisioned by r-drbd.
# Needs the r-drbd stack deployed, a Pool with tier $TIER, KubeVirt and CDI.
# KEEP=1 leaves everything in place for inspection.
set -euo pipefail
CTX=${CTX:-}
NS=${NS:-r-drbd-vmtest}
TIER=${TIER:-test}
SC=r-drbd-vmtest-local
SRC=${SRC:-docker://quay.io/kubevirt/cirros-container-disk-demo:latest}
K="kubectl${CTX:+ --context $CTX}"
cleanup() {
  [ "${KEEP:-0}" = 1 ] && return
  $K delete ns "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  $K delete storageclass "$SC" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT
$K apply -f - <<Y
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: $SC}
provisioner: r-drbd.io
parameters: {tier: "$TIER", replicas: "1"}
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
Y
$K create ns "$NS" --dry-run=client -o yaml | $K apply -f -
$K apply -n "$NS" -f - <<Y
apiVersion: kubevirt.io/v1
kind: VirtualMachine
metadata: {name: cirros}
spec:
  runStrategy: Always
  dataVolumeTemplates:
    - metadata: {name: cirros-root}
      spec:
        source: {registry: {url: "$SRC"}}
        storage:
          storageClassName: $SC
          accessModes: [ReadWriteOnce]
          resources: {requests: {storage: 1Gi}}
  template:
    spec:
      domain:
        devices:
          disks:
            - {name: root, disk: {bus: virtio}}
        resources: {requests: {memory: 256Mi}}
      volumes:
        - {name: root, dataVolume: {name: cirros-root}}
Y
start=$(date +%s)
echo "waiting for the DataVolume import"
$K wait -n "$NS" dv/cirros-root --for=condition=Ready --timeout=600s
echo "import done after $(( $(date +%s) - start )) s"
$K get volumes.r-drbd.io -o wide
echo "waiting for the VMI to run"
$K wait -n "$NS" vmi/cirros --for=jsonpath='{.status.phase}'=Running --timeout=300s
echo "VM running after $(( $(date +%s) - start )) s"
$K get -n "$NS" vmi cirros -o jsonpath='{.status.phase} on {.status.nodeName}{"\n"}'
$K get pvc -n "$NS"
