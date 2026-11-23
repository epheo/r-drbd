# shellcheck shell=bash
# VM helpers for the CI lanes: boot the CI disk under QEMU, network it,
# reach it over ssh, join workers and collect logs. Adapted from
# epheo/microshift at 13c2fbcc8e2c (scripts/multinode-test.sh,
# scripts/lib-qemu.sh, .github/actions/vm-host), Apache-2.0.
#
# Sourced. The caller sets WORKDIR (scratch and logs) and DISK (the
# directory holding disk.qcow2, from disk.sh), then calls vm_tools and
# vm_net_user or vm_net_bridge before vm_boot. The disk holds no key: each
# run makes its own and hands the public half to root by a systemd
# credential, so the published disk opens to no one.

CI_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
QEMU_IMAGE=localhost/r-drbd-ci-qemu
BR=rdrbd-br0
# Clear of 10.42 and 10.43: MicroShift's pods and services live there.
NET=192.168.101
KCFG=/var/lib/microshift/resources/kubeadmin/kubeconfig
VM_NET=

log() { echo "--- $(date +%T) $*"; }
die() { echo "FAIL: $*" >&2; exit 1; }

retry() { # retry SECONDS DESCRIPTION CMD...
    local deadline=$(($(date +%s) + $1)) what=$2
    shift 2
    until "$@" >/dev/null 2>&1; do
        [ "$(date +%s)" -lt "$deadline" ] || die "timed out waiting for: $what"
        sleep 5
    done
    log "ok: $what"
}

vm_tools() {
    mkdir -p "$WORKDIR/logs"
    rm -f "$WORKDIR/id" "$WORKDIR/id.pub"
    ssh-keygen -q -t ed25519 -N '' -C r-drbd-ci -f "$WORKDIR/id"
    sudo podman build -q -t "$QEMU_IMAGE" -f "$CI_DIR/qemu.Containerfile" "$CI_DIR" >/dev/null
}

_tool() { # _tool LOG CMD...: CMD of the QEMU image, detached, output to LOG
    sudo podman run -d --rm --privileged --network host --pid host \
        -v /dev:/dev -v "$WORKDIR:$WORKDIR" -v "$DISK:$DISK:ro" \
        "$QEMU_IMAGE" sh -c 'log=$1; shift; exec "$@" > "$log" 2>&1' sh "$@" >/dev/null
}

# --pid host: the pidfile holds a host PID, which vm_kill can signal.
_wait_pidfile() { # _wait_pidfile PIDFILE LOG
    local _
    for _ in $(seq 60); do
        sudo test -s "$1" && return 0
        sleep 1
    done
    cat "$2" >&2 || true
    die "no $1 after 60 s"
}

# One NAT-less user network per VM; ssh on 127.0.0.1:222N.
vm_net_user() { VM_NET=user; }

# One L2 for every VM, addresses pinned by MAC, NAT to the outside.
vm_net_bridge() {
    VM_NET=bridge
    sudo ip link add "$BR" type bridge 2>/dev/null || true
    sudo ip addr replace "$NET.1/24" dev "$BR"
    sudo ip link set "$BR" up
    sudo sysctl -qw net.ipv4.ip_forward=1
    sudo iptables -t nat -C POSTROUTING -s "$NET.0/24" ! -o "$BR" -j MASQUERADE 2>/dev/null ||
        sudo iptables -t nat -A POSTROUTING -s "$NET.0/24" ! -o "$BR" -j MASQUERADE
    # Docker on the runner drops forwarded traffic by default.
    sudo iptables -C FORWARD -i "$BR" -j ACCEPT 2>/dev/null || sudo iptables -I FORWARD -i "$BR" -j ACCEPT
    sudo iptables -C FORWARD -o "$BR" -j ACCEPT 2>/dev/null || sudo iptables -I FORWARD -o "$BR" -j ACCEPT
    sudo rm -f "$WORKDIR/dnsmasq.pid"
    _tool "$WORKDIR/logs/dnsmasq.log" dnsmasq --keep-in-foreground --interface="$BR" --bind-interfaces \
        --except-interface=lo --dhcp-range="$NET.10,$NET.50,12h" \
        --dhcp-host="$(vm_mac 1),$NET.11" --dhcp-host="$(vm_mac 2),$NET.12" \
        --dhcp-host="$(vm_mac 3),$NET.13" --pid-file="$WORKDIR/dnsmasq.pid"
    _wait_pidfile "$WORKDIR/dnsmasq.pid" "$WORKDIR/logs/dnsmasq.log"
}

vm_mac() { echo "52:54:00:45:00:0$1"; }
vm_ip() { echo "$NET.1$1"; }

vm_boot() { # vm_boot N MEMORY_MIB DATA_DISK_GIB
    local n=$1 mem=$2 data=$3 net
    sudo podman run --rm -v "$WORKDIR:$WORKDIR" -v "$DISK:$DISK:ro" "$QEMU_IMAGE" \
        qemu-img create -q -f qcow2 -F qcow2 -b "$DISK/disk.qcow2" "$WORKDIR/vm$n.qcow2"
    truncate -s "${data}G" "$WORKDIR/data$n.raw"
    if [ "$VM_NET" = bridge ]; then
        sudo ip tuntap add "rdrbd-tap$n" mode tap 2>/dev/null || true
        sudo ip link set "rdrbd-tap$n" master "$BR" up
        net="tap,id=n0,ifname=rdrbd-tap$n,script=no,downscript=no"
    else
        # ipv6=off: an early router advertisement makes MicroShift pick
        # IPv6 and the node never turns Ready.
        net="user,id=n0,restrict=on,ipv6=off,hostfwd=tcp:127.0.0.1:$((2220 + n))-:22"
    fi
    sudo rm -f "$WORKDIR/qemu$n.pid"
    _tool "$WORKDIR/logs/qemu$n.log" qemu-system-x86_64 -machine accel=kvm -cpu host -smp 4 -m "$mem" \
        -drive "file=$WORKDIR/vm$n.qcow2,if=none,id=root,format=qcow2" \
        -device virtio-blk-pci,drive=root,bootindex=0 \
        -drive "file=$WORKDIR/data$n.raw,if=none,id=data,format=raw" \
        -device "virtio-blk-pci,drive=data,serial=data$n" \
        -netdev "$net" -device "virtio-net-pci,netdev=n0,mac=$(vm_mac "$n")" \
        -device virtio-rng-pci -display none \
        -smbios "type=11,value=io.systemd.credential:ssh.authorized_keys.root=$(cat "$WORKDIR/id.pub")" \
        -serial "file:$WORKDIR/logs/console$n.log" \
        -monitor "unix:$WORKDIR/monitor$n.sock,server=on,wait=off" \
        -pidfile "$WORKDIR/qemu$n.pid"
    _wait_pidfile "$WORKDIR/qemu$n.pid" "$WORKDIR/logs/qemu$n.log"
}

vm_monitor() { # vm_monitor N COMMAND: a QEMU monitor command and its answer
    sudo timeout 10 python3 -c '
import socket, sys, time
s = socket.socket(socket.AF_UNIX)
s.connect(sys.argv[1])
s.settimeout(2)
time.sleep(0.5)
s.sendall(sys.argv[2].encode() + b"\n")
time.sleep(1)
try:
    print(s.recv(65536).decode(errors="replace"))
except socket.timeout:
    pass
' "$WORKDIR/monitor$1.sock" "$2"
}

vm_kill() { # vm_kill N
    sudo kill "$(sudo cat "$WORKDIR/qemu$1.pid")" 2>/dev/null || true
}

vm() { # vm N CMD...: CMD as root on VM N; ssh would split the args again
    local n=$1 host=127.0.0.1 port=$((2220 + $1))
    shift
    if [ "$VM_NET" = bridge ]; then host=$(vm_ip "$n") port=22; fi
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=5 -o ServerAliveInterval=10 -o LogLevel=ERROR \
        -i "$WORKDIR/id" -p "$port" "root@$host" "$(printf '%q ' "$@")"
}

vm_put() { # vm_put N FILE DEST
    vm "$1" sh -c "cat > '$3'" < "$2"
}

vm_boot_id() { vm "$1" cat /proc/sys/kernel/random/boot_id; }

vm_ssh_up() { # vm_ssh_up N...: waits for ssh, records each boot id
    local n
    for n in "$@"; do
        retry 600 "vm$n ssh" vm "$n" true
        vm_boot_id "$n" > "$WORKDIR/boot$n"
    done
}

# A reboot greenboot ordered, or any other, changes the boot id.
vm_same_boot() { # vm_same_boot N
    [ "$(vm_boot_id "$1")" = "$(cat "$WORKDIR/boot$1")" ] || die "vm$1 rebooted on its own"
}

# Both greenboot implementations; -b: only this boot counts.
_green() { vm "$1" journalctl -b -u greenboot-healthcheck --no-pager | grep -qiE 'Status is GREEN|health-check passed'; }
_red() { vm "$1" journalctl -b -u greenboot-healthcheck --no-pager | grep -qiE 'Status is RED|health-check failed'; }
_greenboot_done() { _green "$1" || _red "$1"; }
vm_green() { # vm_green N: waits for greenboot, fails on red
    retry 900 "vm$1 greenboot done" _greenboot_done "$1"
    _green "$1" || die "vm$1 greenboot red"
}

koc() { vm 1 oc --kubeconfig "$KCFG" "$@"; }

_settled() { # every pod of the single-node first boot Running
    local n=$1
    vm "$n" sh -c "k='oc --kubeconfig $KCFG'; \$k get pods -A --no-headers | grep -q . &&
        ! \$k get pods -A --no-headers | grep -vE 'Running|Completed' | grep -q ."
}

_node_config() { # VM N becomes nodeN on its bridge address, state wiped
    local n=$1 ip
    ip=$(vm_ip "$n")
    # A running health check fights the wipe.
    vm "$n" sh -c 'systemctl stop greenboot-healthcheck; systemctl reset-failed greenboot-healthcheck; true'
    vm "$n" hostnamectl set-hostname "node$n"
    vm "$n" sh -c "printf 'node:\n  hostnameOverride: node$n\n  nodeIP: $ip\napiServer:\n  subjectAltNames:\n  - $ip\n' > /etc/microshift/config.d/20-multinode.yaml"
    vm "$n" sh -c 'echo 1 | microshift-cleanup-data --all --keep-images'
}

_node_ready() { koc get node "node$1" --no-headers | grep -q ' Ready'; }

vm_join() { # vm_join N...: VM 1 the controller, the others workers
    local n hosts=""
    for n in "$@"; do
        retry 900 "vm$n first boot settled" _settled "$n"
        hosts="$hosts$(vm_ip "$n") node$n\n"
    done
    # The apiserver reaches kubelets by node name.
    for n in "$@"; do vm "$n" sh -c "printf '$hosts' >> /etc/hosts"; done
    _node_config 1
    vm 1 systemctl enable --now microshift
    retry 600 "node1 Ready" _node_ready 1
    for n in "$@"; do
        [ "$n" = 1 ] && continue
        _node_config "$n"
        vm "$n" microshift-profile worker
        vm "$n" systemctl enable microshift
        vm 1 microshift worker-join-kubeconfig --address "$(vm_ip 1)" --duration 30m \
            > "$WORKDIR/join-kubeconfig"
        vm_put "$n" "$WORKDIR/join-kubeconfig" /root/join-kubeconfig
        vm "$n" microshift add-node --worker --kubeconfig /root/join-kubeconfig
        vm "$n" rm -f /root/join-kubeconfig
        retry 900 "node$n Ready" _node_ready "$n"
    done
    rm -f "$WORKDIR/join-kubeconfig"
}

# The admin kubeconfig for node1's bridge address: the localhost one
# trusts another signer.
vm_kubeconfig() { # vm_kubeconfig FILE
    vm 1 cat "/var/lib/microshift/resources/kubeadmin/$(vm_ip 1)/kubeconfig" > "$1"
}

vm_logs() { # vm_logs N...: journal, kernel log and storage state of each VM
    local n
    for n in "$@"; do
        vm "$n" journalctl -b --no-pager > "$WORKDIR/logs/node$n-journal.log" 2>&1 || true
        vm "$n" journalctl -k -b --no-pager > "$WORKDIR/logs/node$n-kernel.log" 2>&1 || true
        vm "$n" sh -c 'lsblk; pvs; vgs; lvs -a; cat /proc/drbd; ls -l /var/lib/r-drbd /etc/drbd.d' \
            > "$WORKDIR/logs/node$n-storage.log" 2>&1 || true
        # The screen shows what a VM that never reached ssh is stuck on.
        { vm_monitor "$n" "info status"; vm_monitor "$n" "info usernet"; } \
            > "$WORKDIR/logs/node$n-qemu.log" 2>&1 || true
        vm_monitor "$n" "screendump $WORKDIR/logs/node$n-screen.ppm" >/dev/null 2>&1 || true
    done
    { sudo ss -ltnp; sudo podman ps -a; } > "$WORKDIR/logs/host.log" 2>&1 || true
    sudo chmod -R a+rX "$WORKDIR/logs" || true
}
