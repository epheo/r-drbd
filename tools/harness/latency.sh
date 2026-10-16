#!/usr/bin/env bash
# Provisioning latency of one StorageClass, in seconds, as markdown tables.
# Usage: latency.sh CLASS [N]   e.g. latency.sh local 10
# Env: CONTEXT (current context), SNAPCLASS (for an r-drbd.io class: R_DRBD_SNAPCLASS, default r-drbd), TIMEOUT (300).
set -uo pipefail

CLASS=${1:?usage: latency.sh CLASS [N]}
N=${2:-10}
CONTEXT=${CONTEXT:-}
NS=r-drbd-latency
TIMEOUT=${TIMEOUT:-300}
IMAGE=quay.io/centos/centos:stream10-minimal
k() { kubectl ${CONTEXT:+--context "$CONTEXT"} "$@"; }

if [ -z "${SNAPCLASS:-}" ]; then
  case "$(k get sc "$CLASS" -o jsonpath='{.provisioner}')" in
    r-drbd.io) SNAPCLASS=${R_DRBD_SNAPCLASS:-r-drbd} ;;
    *) echo "set SNAPCLASS for $CLASS" >&2; exit 2 ;;
  esac
fi

now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN{printf "%.1f", b-a}'; }

# Polls every 0.5 s until "$@" succeeds; fails after TIMEOUT s.
wait_for() {
  local t0; t0=$(now)
  until "$@"; do
    awk -v a="$t0" -v b="$(now)" -v t="$TIMEOUT" 'BEGIN{exit !(b-a>t)}' && return 1
    sleep 0.5
  done
}
pvc_bound() { [ "$(k -n $NS get pvc "$1" -o jsonpath='{.status.phase}' 2>/dev/null)" = Bound ]; }
# The pod runs about 1 s; a 0.5 s poll can land after it finished.
pod_running() {
  case "$(k -n $NS get pod "$1" -o jsonpath='{.status.phase}' 2>/dev/null)" in
    Running|Succeeded) return 0 ;; *) return 1 ;;
  esac
}
pod_done() { [ "$(k -n $NS get pod "$1" -o jsonpath='{.status.phase}' 2>/dev/null)" = Succeeded ]; }
snap_ready() { [ "$(k -n $NS get volumesnapshot "$1" -o jsonpath='{.status.readyToUse}' 2>/dev/null)" = true ]; }
# PV gone too: the backend volume is deleted only then.
gone() {
  local o
  for o in "$@"; do
    case "$o" in
      pv/*) k get "$o" >/dev/null 2>&1 && return 1 ;;
      *) k -n $NS get "$o" >/dev/null 2>&1 && return 1 ;;
    esac
  done
  return 0
}

pvc_yaml() { # name [snapshot]
  cat <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $1}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: $CLASS
  resources: {requests: {storage: 1Gi}}
EOF
  [ -n "${2:-}" ] && cat <<EOF
  dataSource: {apiGroup: snapshot.storage.k8s.io, kind: VolumeSnapshot, name: $2}
EOF
}
pod_yaml() { # name pvc
  cat <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $1}
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    fsGroup: 1000
    seccompProfile: {type: RuntimeDefault}
  containers:
  - name: c
    image: $IMAGE
    command: [sh, -c, 'echo ok > /data/f && sleep 1']
    securityContext:
      allowPrivilegeEscalation: false
      capabilities: {drop: [ALL]}
    volumeMounts: [{name: d, mountPath: /data}]
  volumes:
  - name: d
    persistentVolumeClaim: {claimName: $2}
EOF
}
snap_yaml() { # name pvc
  cat <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: $1}
spec:
  volumeSnapshotClassName: $SNAPCLASS
  source: {persistentVolumeClaimName: $2}
EOF
}
pv_of() { k -n $NS get pvc "$1" -o jsonpath='{.spec.volumeName}' 2>/dev/null; }

setup_ns() {
  if k get ns $NS >/dev/null 2>&1; then
    k delete ns $NS --wait=true >/dev/null
  fi
  k create ns $NS >/dev/null
  k label ns $NS pod-security.kubernetes.io/enforce=restricted \
    pod-security.kubernetes.io/enforce-version=latest --overwrite >/dev/null
  # OpenShift: restricted-v2 SCC rejects runAsUser 1000 and fsGroup 1000 outside the namespace UID range.
  if k get clusterrole system:openshift:scc:nonroot-v2 >/dev/null 2>&1; then
    k -n $NS create rolebinding scc-nonroot-v2 --clusterrole=system:openshift:scc:nonroot-v2 \
      --serviceaccount=$NS:default >/dev/null
  fi
  # The default SA appears asynchronously; pod creation fails without it.
  wait_for k -n $NS get sa default >/dev/null 2>&1
}

FAILURES=""
fail() { # iteration step
  FAILURES+="### iteration $1: $2 timed out after ${TIMEOUT}s"$'\n```\n'
  FAILURES+="$(k -n $NS get events --sort-by=.lastTimestamp 2>&1 | tail -40)"$'\n```\n'
  echo "iteration $1: $2 FAILED" >&2
}

# Deletes everything of the iteration, waits for PVs to go, so the next one starts clean.
cleanup_iter() {
  local pvs="" p
  for p in $(k -n $NS get pvc -o name 2>/dev/null); do
    pvs+=" pv/$(k -n $NS get "$p" -o jsonpath='{.spec.volumeName}')"
  done
  k -n $NS delete pod,volumesnapshot,pvc --all --wait=false >/dev/null 2>&1
  local objs=() o
  for o in $pvs; do [ "$o" != pv/ ] && objs+=("$o"); done
  wait_for gone "${objs[@]}" || echo "cleanup: PVs still present:${pvs}" >&2
  wait_for no_objects || echo "cleanup: objects still present in $NS" >&2
}

no_objects() { [ -z "$(k -n $NS get pod,volumesnapshot,pvc -o name 2>/dev/null)" ]; }

COLS=(pvc_bound pod_running delete snap_ready restore_running restore_delete snap_delete)
declare -A R

iteration() {
  local i=$1 t pv rpv
  local pvc=pvc-$i pod=pod-$i snap=snap-$i rpvc=rpvc-$i rpod=rpod-$i

  # WaitForFirstConsumer: Bound needs the pod, so both are created back to back.
  t=$(now); pvc_yaml $pvc | k -n $NS create -f - >/dev/null
  local tp; tp=$(now); pod_yaml $pod $pvc | k -n $NS create -f - >/dev/null
  wait_for pvc_bound $pvc || { fail $i pvc_bound; return 1; }
  R[$i,pvc_bound]=$(since $t)
  wait_for pod_running $pod || { fail $i pod_running; return 1; }
  R[$i,pod_running]=$(since $tp)
  wait_for pod_done $pod || { fail $i pod_succeeded; return 1; }
  pv=$(pv_of $pvc)

  t=$(now); snap_yaml $snap $pvc | k -n $NS create -f - >/dev/null
  wait_for snap_ready $snap || { fail $i snap_ready; return 1; }
  R[$i,snap_ready]=$(since $t)

  t=$(now)
  pvc_yaml $rpvc $snap | k -n $NS create -f - >/dev/null
  pod_yaml $rpod $rpvc | k -n $NS create -f - >/dev/null
  wait_for pod_running $rpod || { fail $i restore_running; return 1; }
  R[$i,restore_running]=$(since $t)
  wait_for pod_done $rpod || { fail $i restore_succeeded; return 1; }
  rpv=$(pv_of $rpvc)

  t=$(now); k -n $NS delete pod/$rpod pvc/$rpvc --wait=false >/dev/null
  wait_for gone pod/$rpod pvc/$rpvc pv/$rpv || { fail $i restore_delete; return 1; }
  R[$i,restore_delete]=$(since $t)

  # Snapshot deleted before its source: some backends refuse to drop a volume with snapshots.
  t=$(now); k -n $NS delete volumesnapshot/$snap --wait=false >/dev/null
  wait_for gone volumesnapshot/$snap || { fail $i snap_delete; return 1; }
  R[$i,snap_delete]=$(since $t)

  t=$(now); k -n $NS delete pod/$pod pvc/$pvc --wait=false >/dev/null
  wait_for gone pod/$pod pvc/$pvc pv/$pv || { fail $i delete; return 1; }
  R[$i,delete]=$(since $t)
}

# Nearest-rank percentile over the non-empty values on stdin.
pct() { sort -n | awk -v p="$1" 'NF{a[++n]=$1} END{if(!n){print "-";exit} r=int((p*n+99)/100); if(r<1)r=1; print a[r]}'; }

setup_ns
for i in $(seq 1 "$N"); do
  echo "iteration $i/$N" >&2
  iteration "$i"
  cleanup_iter
done
k delete ns $NS --wait=true >/dev/null

echo "## $CLASS (snapshot class $SNAPCLASS), N=$N, seconds"
echo
printf '| iter |'; printf ' %s |' "${COLS[@]}"; echo
printf '|---|'; printf -- '---|%.0s' "${COLS[@]}"; echo
for i in $(seq 1 "$N"); do
  printf '| %s |' "$i"
  for c in "${COLS[@]}"; do printf ' %s |' "${R[$i,$c]:-FAIL}"; done; echo
done
for p in 50 95; do
  printf '| p%s |' "$p"
  for c in "${COLS[@]}"; do
    printf ' %s |' "$(for i in $(seq 1 "$N"); do echo "${R[$i,$c]:-}"; done | pct $p)"
  done; echo
done
if [ -n "$FAILURES" ]; then
  echo; echo "## failures"; echo; printf '%s' "$FAILURES"
fi
