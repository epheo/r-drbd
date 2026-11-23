#!/usr/bin/env bash
# The VM disk the lanes boot: the pinned MicroShift image, made by
# bootc-image-builder into a compressed qcow2. It holds no ssh key: vm.sh
# gives root one per run.
# Every change but a serial console is in /etc. MicroShift leaves out the
# r-drbd it embeds and greenboot does not wait for it: each lane deploys
# the PR's own. One boot proves greenboot goes green with no reboot
# before the disk is kept.
# Usage: disk.sh OUTDIR. Needs sudo, podman, KVM and about 30 GB free;
# WORK names the scratch dir.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
out=$(realpath -m "$1")
work=${WORK:-$(mktemp -d)}
pins=$here/microshift.Containerfile
bib=$(sed -n 's/^FROM \([^ ]*bootc-image-builder[^ ]*\).*/\1/p' "$pins")
image=$(sed -n 's/^FROM \([^ ]*microshift[^ ]*\).*/\1/p' "$pins")
[ -n "$bib" ] && [ -n "$image" ] || { echo "no pins in $pins" >&2; exit 1; }
pm() { sudo podman "$@"; }

mkdir -p "$out" "$work/output"

# kustomizePaths globs match every manifests.d entry but 000-r-*, in the
# order MicroShift applies them by default. Go globs: a dash in a class is
# escaped, or the whole config fails to parse and greenboot goes red.
cat > "$work/config.toml" <<EOF
# A boot that never reaches ssh shows why in the console log.
[customizations.kernel]
append = "console=ttyS0,115200"

[[customizations.files]]
path = "/etc/microshift/config.d/90-r-drbd-ci.yaml"
data = '''
manifests:
  kustomizePaths:
  - /usr/lib/microshift/manifests
  - /usr/lib/microshift/manifests.d/000-[^r]*
  - /usr/lib/microshift/manifests.d/000[^\-]*
  - /usr/lib/microshift/manifests.d/00[^0]*
  - /usr/lib/microshift/manifests.d/0[^0]*
  - /usr/lib/microshift/manifests.d/[^0]*
  - /etc/microshift/manifests
  - /etc/microshift/manifests.d/*
'''

[[customizations.files]]
path = "/etc/greenboot/check/required.d/50_microshift_r_drbd_check.sh"
mode = "0755"
data = '''
#!/bin/bash
# CI disk: r-drbd is deployed by the lane after boot.
exit 0
'''
EOF

pm pull -q "$bib" >/dev/null
pm pull -q "$image" >/dev/null
echo "manifests.d in $image:"
pm run --rm --entrypoint ls "$image" /usr/lib/microshift/manifests.d
pm run --rm --privileged --security-opt label=type:unconfined_t \
    -v "$work/config.toml:/config.toml:ro" \
    -v "$work/output:/output" \
    -v /var/lib/containers/storage:/var/lib/containers/storage \
    "$bib" --type qcow2 --config /config.toml "$image"
# zstd: the lanes read the disk through it, and zstd decompresses fastest.
pm run --rm -v "$work:/work" -v "$out:/out" --entrypoint qemu-img "$bib" \
    convert -c -O qcow2 -o compression_type=zstd /work/output/qcow2/disk.qcow2 /out/disk.qcow2
sudo chown "$(id -u):$(id -g)" "$out/disk.qcow2"
ls -l "$out"

WORKDIR=$work/proof DISK=$out
. "$here/vm.sh"
vm_tools
vm_net_user
vm_boot 1 6144 1
trap 'rc=$?; [ "$rc" = 0 ] || vm_logs 1; exit "$rc"' EXIT
vm_ssh_up 1
vm_green 1
vm_same_boot 1
vm_kill 1
log "greenboot green on the first boot, no reboot"
