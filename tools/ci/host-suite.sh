#!/usr/bin/env bash
# The agent host tests on one VM of the CI disk, run as the agent runs:
# in a privileged container of the PR image, host tools through the host
# mount namespace, the static test binary mounted in.
# Usage: host-suite.sh DISK_DIR IMAGE_ARCHIVE TEST_BINARY [TEST_ARGS...]
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
DISK=$(realpath "$1")
image=$(realpath "$2")
bin=$(realpath "$3")
shift 3
WORKDIR=$(realpath -m "${WORKDIR:-$(mktemp -d)}")
mkdir -p "$WORKDIR"
# shellcheck source=vm.sh
. "$here/vm.sh"

VG=ci-test
DIR=/var/tmp/r-drbd-hosttest

vm_tools
vm_net_user
vm_boot 1 4096 16
trap 'rc=$?; [ "$rc" = 0 ] || vm_logs 1; exit "$rc"' EXIT
vm_ssh_up 1
# Stopping MicroShift under a running health check would turn it red.
vm_green 1
vm 1 systemctl stop microshift
vm 1 hostnamectl set-hostname host-suite

vm_put 1 "$image" /root/r-drbd.tar
vm_put 1 "$bin" /root/host-test
vm 1 chmod 755 /root/host-test
vm 1 skopeo copy -q oci-archive:/root/r-drbd.tar containers-storage:localhost/r-drbd:ci
vm 1 vgcreate -q "$VG" /dev/disk/by-id/virtio-data1
vm 1 lvcreate -q -y --type thin-pool -l 90%FREE -n thin "$VG"
vm 1 mkdir -p "$DIR" /var/lib/r-drbd /etc/drbd.d /var/lib/drbd /run/drbd
# The agent writes this include at start; drbdadm finds no resource without it.
vm 1 sh -c 'echo "include \"/var/lib/r-drbd/*.res\";" > /etc/drbd.d/r-drbd.res'

# The agent DaemonSet's mounts, and the scratch dir its mounts land in.
log "host suite"
vm 1 podman run --rm --privileged --pid host --network host --uts host \
    -v /dev:/dev:rslave \
    -v /var/lib/r-drbd:/var/lib/r-drbd \
    -v /etc/drbd.d:/etc/drbd.d \
    -v /var/lib/drbd:/var/lib/drbd \
    -v /run/drbd:/var/run/drbd \
    -v "$DIR:$DIR:rslave" \
    -v /root/host-test:/host-test:ro \
    -e R_DRBD_HOST_TEST=1 -e R_DRBD_HOST_NS=/proc/1/ns -e RUST_BACKTRACE=1 \
    -e R_DRBD_TEST_VG="$VG" -e R_DRBD_TEST_THIN=thin -e R_DRBD_TEST_DIR="$DIR" \
    --entrypoint /host-test localhost/r-drbd:ci \
    --ignored --test-threads=1 "$@" 2>&1 | tee "$WORKDIR/logs/host-suite.log"
vm_same_boot 1
