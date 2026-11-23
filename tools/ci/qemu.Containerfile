# QEMU and dnsmasq for the VM lanes, from Fedora rather than the runner's
# Ubuntu mirrors. The VMs run headless: the core package is enough.
FROM quay.io/fedora/fedora:45@sha256:62d5eaec1e8a34f6cb94934f806420754611834c7eef8a4f50b9cdbcd1c90c10
RUN dnf -y install --setopt=install_weak_deps=False qemu-system-x86-core qemu-img dnsmasq \
 && dnf clean all
