#!/usr/bin/env bash
# csi-sanity against the r-drbd CSI controller plugin on envtest.
# The cargo test starts etcd and kube-apiserver (KUBEBUILDER_ASSETS, else the
# setup-envtest default dir), the
# controller on a Unix socket, a fake agent and a fake node for NodeGetInfo,
# then runs csi-sanity twice: Identity on the controller socket, then the
# Controller suites. Extra csi-sanity flags go in CSI_SANITY_ARGS.
# One apiserver only; the full suite caps itself at 4 (tests/support), and
# --test-threads=2 helps further on a loaded machine.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export R_DRBD_CSI_SANITY=1
export CSI_SANITY="${CSI_SANITY:-$(command -v csi-sanity || echo "${GOPATH:-$HOME/go}/bin/csi-sanity")}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$repo/target/controller}"

cd "$repo"
exec cargo test -p controller --test csi_sanity -- --nocapture "$@"
