#!/usr/bin/env bash
# The VM disk as an OCI artifact in DISK_REPO, one tag per disk key.
# Usage: disk-oci.sh exists TAG | push DIR TAG | pull TAG DIR
# With GITHUB_TOKEN set, logs in to the registry first.
set -euo pipefail

ORAS_VERSION=1.3.6
ORAS_SHA256=66fb0ad7206f3f6fa77ed5b552049d364ccf4b50b1684554f731f76302b52ca0
: "${DISK_REPO:?}"
bin=${RUNNER_TEMP:-/tmp}/oras-$ORAS_VERSION
oras=$bin/oras

if [ ! -x "$oras" ]; then
    mkdir -p "$bin"
    curl -sSfL -o "$bin/oras.tar.gz" \
        "https://github.com/oras-project/oras/releases/download/v$ORAS_VERSION/oras_${ORAS_VERSION}_linux_amd64.tar.gz"
    echo "$ORAS_SHA256  $bin/oras.tar.gz" | sha256sum -c - >/dev/null
    tar -xzf "$bin/oras.tar.gz" -C "$bin" oras
fi
if [ -n "${GITHUB_TOKEN:-}" ]; then
    "$oras" login "${DISK_REPO%%/*}" -u "${GITHUB_ACTOR:-ci}" --password-stdin <<< "$GITHUB_TOKEN" >/dev/null
fi

case $1 in
    exists) "$oras" manifest fetch "$DISK_REPO:$2" >/dev/null 2>&1 ;;
    # The source annotation links the package to this repository.
    push)
        cd "$2"
        "$oras" push "$DISK_REPO:$3" \
            --artifact-type application/vnd.r-drbd.ci-disk.v1 \
            --annotation "org.opencontainers.image.source=${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:?}" \
            disk.qcow2:application/vnd.r-drbd.ci-disk.qcow2
        ;;
    pull) "$oras" pull "$DISK_REPO:$2" -o "$3" ;;
    *) echo "usage: $0 exists TAG | push DIR TAG | pull TAG DIR" >&2; exit 2 ;;
esac
