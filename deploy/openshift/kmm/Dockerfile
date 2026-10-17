# DRBD 9 kernel module for the node's kernel, built in the cluster by
# Kernel Module Management from the Driver Toolkit of the running release.
# KMM sets DTK_AUTO and KERNEL_FULL_VERSION.
ARG DTK_AUTO

# The toolkit ships no patch(1), which DRBD's kernel compat step runs; its
# only needs are libraries every RHEL base has.
FROM registry.access.redhat.com/ubi9/ubi:9.6 AS tools
RUN dnf -y install patch && dnf clean all

FROM ${DTK_AUTO} AS build
ARG KERNEL_FULL_VERSION
ARG DRBD_VERSION=9.3.4
ARG DRBD_SHA256=a2de4b7ffc43fdabad01b0bf7283b910eb6401b0f788e33a65ae94fc0588545c
COPY --from=tools /usr/bin/patch /usr/bin/patch
WORKDIR /src
RUN curl -fsSLO https://pkg.linbit.com/downloads/drbd/9/drbd-${DRBD_VERSION}.tar.gz \
 && echo "${DRBD_SHA256}  drbd-${DRBD_VERSION}.tar.gz" | sha256sum -c - \
 && tar xzf drbd-${DRBD_VERSION}.tar.gz
# A kernel the release has no compat patch for is patched through LINBIT's
# online service (spaas.drbd.io); without egress, add coccinelle here.
RUN make -C drbd-${DRBD_VERSION} module KDIR=/usr/src/kernels/${KERNEL_FULL_VERSION}
# drbd needs in-tree modules on some kernels (libcrc32c on RHEL 9). They
# resolve through `host`: the toolkit's modules for depmod here, the
# node's own in the KMM worker, which mounts them at /host/lib/modules.
RUN install -D -t /opt/lib/modules/${KERNEL_FULL_VERSION} \
      drbd-${DRBD_VERSION}/drbd/build-current/drbd.ko \
      drbd-${DRBD_VERSION}/drbd/build-current/drbd_transport_tcp.ko \
 && strip --strip-debug /opt/lib/modules/${KERNEL_FULL_VERSION}/*.ko \
 && ln -s /lib/modules/${KERNEL_FULL_VERSION} /opt/lib/modules/${KERNEL_FULL_VERSION}/host \
 && depmod -e -F /usr/src/kernels/${KERNEL_FULL_VERSION}/System.map -b /opt ${KERNEL_FULL_VERSION} 2>&1 | tee /tmp/depmod.log \
 && ! grep -q 'unknown symbol' /tmp/depmod.log \
 && grep -q '^drbd_transport_tcp.ko: drbd.ko' /opt/lib/modules/${KERNEL_FULL_VERSION}/modules.dep

FROM registry.access.redhat.com/ubi9/ubi-minimal:9.6
ARG KERNEL_FULL_VERSION
COPY --from=build /opt/lib/modules/${KERNEL_FULL_VERSION} /opt/lib/modules/${KERNEL_FULL_VERSION}
RUN ln -sfn /host/lib/modules/${KERNEL_FULL_VERSION} /opt/lib/modules/${KERNEL_FULL_VERSION}/host
