# Static binaries on scratch. The agent runs drbd-utils from this image, so
# hosts need only the DRBD kernel module; lvm, mkfs and mount still run from
# the host mount namespace.
FROM docker.io/library/rust:1.99.0-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked --target x86_64-unknown-linux-musl -p r-drbd \
 && install -D target/x86_64-unknown-linux-musl/release/r-drbd /out/r-drbd

FROM docker.io/library/alpine:3.22 AS drbd-utils
ARG DRBD_UTILS_VERSION=9.35.0
ARG DRBD_UTILS_SHA256=8815afb25c84fafadf6ef43be68805d261c5f33ec29016121a327821acaf6ade
RUN apk add --no-cache bash build-base flex linux-headers
WORKDIR /src
RUN wget -q https://pkg.linbit.com/downloads/drbd/utils/drbd-utils-${DRBD_UTILS_VERSION}.tar.gz \
 && echo "${DRBD_UTILS_SHA256}  drbd-utils-${DRBD_UTILS_VERSION}.tar.gz" | sha256sum -c - \
 && tar xzf drbd-utils-${DRBD_UTILS_VERSION}.tar.gz
# No udev rules and no systemd units: the agent alone acts on its resources.
# drbd-utils is GPLv2: its source tarball ships beside the binaries.
RUN cd drbd-utils-${DRBD_UTILS_VERSION} \
 && ./configure --prefix=/usr --sysconfdir=/etc --localstatedir=/var \
      --without-manual --without-84support --without-drbdmon --without-udev \
      --without-pacemaker --without-heartbeat --without-rgmanager \
      --without-bashcompletion --without-keyutils --with-initscripttype=none \
      LDFLAGS=-static \
 && make -C user/shared && make -C user/v9 \
 && mkdir -p /out/usr/sbin /out/usr/share/licenses/drbd-utils /out/usr/share/src/drbd-utils /out/tmp \
 && cp user/v9/drbdadm user/v9/drbdsetup user/v9/drbdmeta /out/usr/sbin/ \
 && strip /out/usr/sbin/* \
 && cp COPYING /out/usr/share/licenses/drbd-utils/ \
 && cp ../drbd-utils-${DRBD_UTILS_VERSION}.tar.gz /out/usr/share/src/drbd-utils/ \
 && chmod 1777 /out/tmp
COPY deploy/image/drbd.conf /out/etc/drbd.conf

FROM scratch
COPY --from=drbd-utils /out/ /
COPY --from=build /out/r-drbd /r-drbd
ENV PATH=/usr/sbin
USER 0
ENTRYPOINT ["/r-drbd"]
