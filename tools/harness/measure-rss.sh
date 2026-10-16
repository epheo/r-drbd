#!/usr/bin/env bash
# VmRSS of the r-drbd processes on a node, in KiB.
set -euo pipefail
HOST=${1:?usage: measure-rss.sh SSH_HOST}
ssh "$HOST" '
for pid in $(pgrep -x r-drbd || true); do
  [ -r /proc/$pid/status ] || continue
  echo "r-drbd $(tr "\0" " " < /proc/$pid/cmdline | cut -c1-40) VmRSS_kB=$(awk "/VmRSS/{print \$2}" /proc/$pid/status)"
done'
