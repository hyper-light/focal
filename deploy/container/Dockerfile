# syntax=docker/dockerfile:1
# Focal as one static binary in an empty image (doc 08 §5; 24 §24).
#
# Stage 1 builds the musl binary with the same pinned Rust Alpine image the
# release workflow uses (scripts/release/platforms.json). Stage 2 is
# `scratch`: nothing but the binary, an unprivileged user and the data
# directory, so there is no base image to pin or to trust. The node never
# needs a shell, a libc or system certificates: it pins its invitation CA.
#
#   docker build -f deploy/container/Dockerfile -t focal:0.1.0 .
#   docker run --rm -v focal-data:/var/lib/focal -v ./focal.yaml:/etc/focal/focal.yaml:ro \
#     -p 7443:7443/udp focal:0.1.0 start --advertise host.example:7443
#
# The data volume must belong to uid 65532: `docker run --user 0 ... prepare-volume
# --owner 65532:65532` once, or the Kubernetes init step the renderer emits.
FROM rust:1.94.1-alpine@sha256:77237dd363a0b127bb5ef532c2d64c0deb380b738e43a9c4bdac73398d6d0a08 AS build
# The same packages, flags and target the release workflow's musl lane uses.
RUN apk add --no-cache build-base protoc
WORKDIR /src
COPY . .
ENV RUSTFLAGS="-C target-feature=+crt-static" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=cc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=cc
RUN cargo build --release --locked -p focal-node --bin focal \
        --target "$(uname -m)-unknown-linux-musl" \
    && install -D -m 0755 "target/$(uname -m)-unknown-linux-musl/release/focal" /out/focal \
    && install -d -m 0700 -o 65532 -g 65532 /out/var/lib/focal \
    && install -d -m 0755 /out/etc/focal \
    && printf 'focal:x:65532:65532:focal:/var/lib/focal:/sbin/nologin\n' > /out/passwd \
    && printf 'focal:x:65532:\n' > /out/group

FROM scratch
COPY --from=build /out/passwd /etc/passwd
COPY --from=build /out/group /etc/group
COPY --from=build /out/focal /focal
COPY --from=build --chown=65532:65532 /out/var/lib/focal /var/lib/focal
COPY --from=build /out/etc/focal /etc/focal
USER 65532:65532
VOLUME ["/var/lib/focal"]
EXPOSE 7443/udp
STOPSIGNAL SIGTERM
ENTRYPOINT ["/focal", "--config", "/etc/focal/focal.yaml", "--data-dir", "/var/lib/focal"]
CMD ["start"]
