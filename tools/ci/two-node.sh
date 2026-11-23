#!/usr/bin/env bash
# r-drbd across two VMs of the CI disk: node1 the control plane, node2 a
# worker, both with a spare disk. The PR's image and deploy/base, one Pool
# built from a blank disk, one on a VG made by hand, then smoke checks of
# what one host cannot show. Each wait is bounded; on failure the cluster
# state, the r-drbd logs and every node's journal land in $WORKDIR/logs.
# Usage: two-node.sh DISK_DIR IMAGE_ARCHIVE
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
DISK=$(realpath "$1")
image=$(realpath "$2")
WORKDIR=$(realpath -m "${WORKDIR:-$(mktemp -d)}")
NODES=2
mkdir -p "$WORKDIR"
# shellcheck source=vm.sh
. "$here/vm.sh"

NS=r-drbd-ci
SYS=r-drbd-system
VMS=$(seq 1 "$NODES")
k() { kubectl --kubeconfig "$WORKDIR/kubeconfig" "$@"; }
kn() { k -n "$NS" "$@"; }

# Host memory and each VM's resident size, every 10 s.
mem_watch() {
    local n pid
    while sleep 10; do
        printf '%s host_used_mib=%s' "$(date +%T)" "$(free -m | awk '/^Mem:/ {print $3}')"
        for n in $VMS; do
            pid=$(sudo cat "$WORKDIR/qemu$n.pid" 2>/dev/null) || continue
            printf ' vm%s_rss_mib=%s' "$n" "$(($(ps -o rss= -p "$pid" || echo 0) / 1024))"
        done
        echo
    done >> "$WORKDIR/logs/memory.log"
}

mem_peaks() {
    awk '{for (i = 2; i <= NF; i++) {split($i, kv, "="); if (kv[2] > max[kv[1]]) max[kv[1]] = kv[2]}}
        END {for (f in max) printf "peak %s %s\n", f, max[f]}' "$WORKDIR/logs/memory.log" | sort
    local n
    for n in $VMS; do echo "node$n: $(vm "$n" free -m | awk '/^Mem:/ {print "guest used " $3 " of " $2 " MiB"}')"; done
}

diagnostics() {
    {
        k get nodes -o wide
        k get pools.r-drbd.io,volumes.r-drbd.io,snapshots.r-drbd.io -o yaml
        k get pv,volumesnapshotcontents
        kn get pvc,volumesnapshots,pods -o wide
        k get events -A --sort-by=.lastTimestamp | tail -80
    } > "$WORKDIR/logs/cluster.log" 2>&1 || true
    k -n "$SYS" logs --all-containers --prefix --timestamps -l 'app in (r-drbd-agent, r-drbd-controller)' \
        --tail=-1 --max-log-requests=20 > "$WORKDIR/logs/r-drbd.log" 2>&1 || true
    local p
    for p in $(kn get pods -o name 2>/dev/null); do
        kn logs "$p" > "$WORKDIR/logs/pod-${p#pod/}.log" 2>&1 || true
    done
    # shellcheck disable=SC2086
    vm_logs $VMS
}

cleanup() {
    local rc=$?
    kill "${MEM_PID:-}" 2>/dev/null || true
    if [ "$rc" != 0 ]; then
        log "FAILED (rc=$rc): logs in $WORKDIR/logs"
        diagnostics
    fi
    exit "$rc"
}

# --- Cluster ---------------------------------------------------------------
vm_tools
vm_net_bridge
trap cleanup EXIT
mem_watch &
MEM_PID=$!
vm_boot 1 5120 8
vm_boot 2 5120 8
# shellcheck disable=SC2086
vm_ssh_up $VMS
# shellcheck disable=SC2086
vm_join $VMS
vm_kubeconfig "$WORKDIR/kubeconfig"
k get crd volumesnapshots.snapshot.storage.k8s.io >/dev/null || die "no VolumeSnapshot CRD on the cluster"

log "loading the PR image on every node"
for n in $VMS; do
    vm_put "$n" "$image" /root/r-drbd.tar
    vm "$n" skopeo copy -q oci-archive:/root/r-drbd.tar containers-storage:localhost/r-drbd:ci
done

log "deploying the PR's deploy/base"
k apply --server-side -k "$here/deploy" >/dev/null
k -n "$SYS" rollout status ds/r-drbd-agent --timeout=300s
k -n "$SYS" rollout status deploy/r-drbd-controller --timeout=300s

log "Pools: node1 from a blank disk, node2 on a VG made by hand"
vm 2 vgcreate -q ci-vg /dev/disk/by-id/virtio-data2
vm 2 lvcreate -q -y --type thin-pool -l 90%FREE -n thin ci-vg
k apply -f - <<EOF
apiVersion: r-drbd.io/v1alpha1
kind: Pool
metadata: {name: node1-devices}
spec:
  node: node1
  tier: nvme
  vg: ci-devices
  thinPool: thin
  devices: [/dev/disk/by-id/virtio-data1]
  reclaimPolicy: Delete
---
apiVersion: r-drbd.io/v1alpha1
kind: Pool
metadata: {name: node2-vg}
spec: {node: node2, tier: nvme, vg: ci-vg, thinPool: thin}
EOF
k wait pools.r-drbd.io --all --for=condition=Available --timeout=300s

k create namespace "$NS" >/dev/null
k label --overwrite namespace "$NS" pod-security.kubernetes.io/enforce=privileged >/dev/null
# Already on every node: the checks pull nothing.
UTIL=$(k -n openshift-ovn-kubernetes get ds ovnkube-node -o jsonpath='{.spec.template.spec.containers[0].image}')

# --- Helpers -----------------------------------------------------------------
pvc() { # pvc NAME CLASS SIZE [SNAPSHOT]
    local src=""
    [ -z "${4:-}" ] || src="dataSource: {apiGroup: snapshot.storage.k8s.io, kind: VolumeSnapshot, name: $4}"
    kn apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $1}
spec:
  storageClassName: $2
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: $3}}
  $src
EOF
}

pod() { # pod NAME NODE CLAIM [SCRIPT]: the claim at /data; waits for Ready
    kn apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $1}
spec:
  nodeSelector: {kubernetes.io/hostname: $2}
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  containers:
  - name: c
    image: $UTIL
    command: [sh, -c, '${4:-sleep infinity}']
    volumeMounts: [{name: d, mountPath: /data}]
  volumes: [{name: d, persistentVolumeClaim: {claimName: $3}}]
EOF
    kn wait "pod/$1" --for=condition=Ready --timeout=300s >/dev/null
}

px() { kn exec "$1" -- sh -c "$2"; }
gone() { kn delete --wait --timeout=300s "$@" >/dev/null; }

snap() { # snap NAME CLAIM: waits for readyToUse
    kn apply -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: $1}
spec: {volumeSnapshotClassName: r-drbd, source: {persistentVolumeClaimName: $2}}
EOF
    kn wait "volumesnapshot/$1" --for=jsonpath='{.status.readyToUse}'=true --timeout=300s >/dev/null
}

volume() { kn get pvc "$1" -o jsonpath='{.spec.volumeName}'; }
phase() { k get volumes.r-drbd.io "$1" -o jsonpath='{.status.phase}'; }
phase_is() { [ "$(phase "$1")" = "$2" ]; }
write_pattern() { px "$1" 'dd if=/dev/urandom of=/data/pattern bs=1M count=64 conv=fsync status=none && sha256sum /data/pattern'; }
read_pattern() { px "$1" 'sha256sum /data/pattern'; }
size_gib() { px "$1" 'df -B1G --output=size /data | tail -1 | tr -d " "'; }
grew() { [ "$(size_gib "$1")" -ge "$2" ]; }

# --- Local volumes -----------------------------------------------------------
local_volume() { # local_volume N: provision, mount, write, snapshot, restore, expand, delete
    local n=$1 sum
    pvc "local$n" r-drbd-local 1Gi
    pod "local$n" "node$n" "local$n"
    sum=$(write_pattern "local$n")
    snap "local$n-snap" "local$n"
    pvc "local$n-restore" r-drbd-local 1Gi "local$n-snap"
    pod "local$n-restore" "node$n" "local$n-restore"
    [ "$(read_pattern "local$n-restore")" = "$sum" ] || die "local$n: the restore differs"
    kn patch pvc "local$n" -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}' >/dev/null
    retry 180 "local$n grew to 2 GiB" grew "local$n" 2
    [ "$(read_pattern "local$n")" = "$sum" ] || die "local$n: data changed by the expand"
    gone pod "local$n" "local$n-restore"
    gone pvc "local$n-restore"
    gone volumesnapshot "local$n-snap"
    gone pvc "local$n"
    log "ok: local volume on node$n"
}
log "local volume on node1 and node2, at once"
local_volume 1 &
l1=$!
local_volume 2 &
l2=$!
wait "$l1" || die "local volume on node1"
wait "$l2" || die "local volume on node2"

# --- Replicated volume -------------------------------------------------------
log "replicas 2: write on node1, read on node2"
pvc rep r-drbd-replicated 1Gi
pod rep-a node1 rep
REP=$(volume rep)
retry 300 "$REP Ready" phase_is "$REP" Ready
sum=$(write_pattern rep-a)
gone pod rep-a
pod rep-b node2 rep
[ "$(read_pattern rep-b)" = "$sum" ] || die "rep: node2 reads other data than node1 wrote"
gone pod rep-b

log "snapshot of replicas 2, restored with no resync"
pod rep-a node1 rep
snap rep-snap rep
handle=$(k get volumesnapshotcontent "$(kn get volumesnapshot rep-snap -o jsonpath='{.status.boundVolumeSnapshotContentName}')" \
    -o jsonpath='{.status.snapshotHandle}')
identical() { [ "$(k get snapshots.r-drbd.io "$handle" -o jsonpath='{.status.copiesIdentical}')" = true ]; }
retry 300 "$handle copiesIdentical" identical
pvc rep-restore r-drbd-replicated 1Gi rep-snap
pod rep-restore node1 rep-restore
RESTORE=$(volume rep-restore)
retry 300 "$RESTORE Ready" phase_is "$RESTORE" Ready
[ "$(read_pattern rep-restore)" = "$sum" ] || die "rep-restore: data differs from the source"
# DRBD logs every replication state change to the kernel log.
for n in 1 2; do
    klog=$(vm "$n" journalctl -k -b --no-pager | grep -F "$RESTORE" || true)
    [ -n "$klog" ] || die "node$n: no kernel log line for $RESTORE"
    if grep -E 'SyncSource|SyncTarget' <<< "$klog"; then die "$RESTORE resynced on node$n"; fi
done
gone pod rep-restore
gone pvc rep-restore
gone volumesnapshot rep-snap

log "expand while attached: XFS grows"
kn patch pvc rep -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}' >/dev/null
retry 300 "rep grew to 2 GiB" grew rep-a 2
retry 300 "$REP Ready after the expand" phase_is "$REP" Ready
[ "$(read_pattern rep-a)" = "$sum" ] || die "rep: data changed by the expand"
gone pod rep-a

# Each file is written and fsynced, then summed; the writer stops at the
# stop file and exits non-zero on the first IO error.
# One line: it lands in a YAML flow scalar.
WRITER='set -e; rm -f /data/stop; i=0; : > /data/sums; while [ ! -e /data/stop ]; do dd if=/dev/urandom of=/data/f$i bs=256k count=1 conv=fsync status=none; sha256sum /data/f$i >> /data/sums; i=$((i+1)); sleep 0.5; done; sync; echo wrote $i'
written() { px rep-io 'wc -l < /data/sums'; }
stop_writer() {
    px rep-io 'touch /data/stop'
    kn wait pod/rep-io --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s >/dev/null ||
        die "the writer failed: $(kn logs rep-io | tail -5)"
}
verify_on() { # verify_on NODE: every file the writer summed reads back there
    pod rep-check "$1" rep
    px rep-check 'cd /data && sha256sum -c --quiet sums' || die "rep: data differs on $1"
    gone pod rep-check
}

agents() { k -n "$SYS" get pod -l app=r-drbd-agent -o jsonpath='{range .items[*]}{.metadata.name} {.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}'; }
agents_replaced() { # every agent pod new and Ready
    local now
    now=$(agents)
    [ "$(grep -c ' True$' <<< "$now")" = "$NODES" ] && [ "$(wc -l <<< "$now")" = "$NODES" ] &&
        ! grep -qxF -f <(cut -d' ' -f1 <<< "$old_agents") <(cut -d' ' -f1 <<< "$now")
}

log "agent restart during IO"
pod rep-io node1 rep "$WRITER"
sleep 10
old_agents=$(agents)
k -n "$SYS" delete pod -l app=r-drbd-agent --wait=false >/dev/null
retry 300 "every agent restarted" agents_replaced
before=$(written)
sleep 10
[ "$(written)" -gt "$before" ] || die "no write went through after the agent restart"
stop_writer
gone pod rep-io
verify_on node2

log "hard reset of node2"
pod rep-io node1 rep "$WRITER"
sleep 5
vm_monitor 2 system_reset
retry 120 "$REP Degraded" phase_is "$REP" Degraded
retry 600 "node2 back" vm 2 true
vm_boot_id 2 > "$WORKDIR/boot2"
retry 600 "node2 Ready" _node_ready 2
retry 600 "$REP Ready after node2 returned" phase_is "$REP" Ready
stop_writer
gone pod rep-io
verify_on node2

# --- Teardown ----------------------------------------------------------------
log "delete everything: no LV, DRBD resource or resource file left"
gone pvc rep
no_volumes() { [ -z "$(k get volumes.r-drbd.io -o name)" ] && [ -z "$(k get pv -o name)" ]; }
retry 300 "every Volume and PV gone" no_volumes
for n in 1 2; do
    vg=ci-vg
    [ "$n" = 1 ] && vg=ci-devices
    lvs=$(vm "$n" lvs --noheadings -o lv_name "$vg" | tr -d ' ' | grep -vx thin || true)
    [ -z "$lvs" ] || die "node$n: LVs left: $lvs"
done
for n in $VMS; do
    agent=$(k -n "$SYS" get pod -l app=r-drbd-agent --field-selector "spec.nodeName=node$n" -o name)
    res=$(k -n "$SYS" exec "$agent" -c agent -- drbdsetup status)
    # With none, drbdsetup prints a comment line.
    res=$(grep -v '^#' <<< "$res" || true)
    [ -z "$res" ] || die "node$n: DRBD resources left: $res"
    files=$(vm "$n" find /var/lib/r-drbd -name '*.res')
    [ -z "$files" ] || die "node$n: resource files left: $files"
done

log "Pools deleted: the built VG goes, the hand-made one stays"
k delete pools.r-drbd.io --all --wait --timeout=300s >/dev/null
devices_vg_gone() { ! vm 1 vgs ci-devices; }
retry 300 "node1 VG ci-devices removed" devices_vg_gone
vm 2 lvs ci-vg/thin >/dev/null || die "node2: the hand-made thin pool is gone"

vm_same_boot 1
vm_same_boot 2
mem_peaks | tee "$WORKDIR/logs/memory-peaks.log"
log "two-node lane passed"
