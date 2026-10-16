# Static musl binary on scratch: the agent runs the host's tools from the
# host mount namespace, so the image carries nothing else.
FROM docker.io/library/rust:1.99-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked --target x86_64-unknown-linux-musl -p r-drbd \
 && install -D target/x86_64-unknown-linux-musl/release/r-drbd /out/r-drbd

FROM scratch
COPY --from=build /out/r-drbd /r-drbd
USER 0
ENTRYPOINT ["/r-drbd"]
