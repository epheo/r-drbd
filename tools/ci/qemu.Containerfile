# QEMU and dnsmasq for the VM lanes, from Fedora rather than the runner's
# Ubuntu mirrors. The VMs run headless: the core package is enough.
# Docker Hub, not quay.io: quay.io/fedora deletes a digest once its tag
# moves, which broke the pin within days.
FROM docker.io/library/fedora:45@sha256:a7d72ba2745b100a3351087472e5ee7b2450af7cf331e7673007e3817612839c
RUN dnf -y install --setopt=install_weak_deps=False qemu-system-x86-core qemu-img dnsmasq \
 && dnf clean all
