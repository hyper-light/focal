# syntax=docker/dockerfile:1
# focal's arm of the comparison (docs/qualification/competitive-p99.md): the
# node and the load generator, built as the release's musl lane builds them,
# on Alpine so the bench can run `tc netem` and a shell beside them. Not the
# shipped image (deploy/container/Dockerfile is: `scratch`, the binary alone).
#
#   docker build -f tools/compare/compose/focal.Dockerfile -t focal:bench .
FROM rust:1.94.1-alpine@sha256:77237dd363a0b127bb5ef532c2d64c0deb380b738e43a9c4bdac73398d6d0a08 AS build
RUN apk add --no-cache build-base protoc
WORKDIR /src
COPY . .
ENV RUSTFLAGS="-C target-feature=+crt-static" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=cc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=cc
RUN --mount=type=cache,id=focal-bench-target,target=/src/target \
    --mount=type=cache,id=focal-bench-registry,target=/usr/local/cargo/registry \
    cargo build --release --locked -p focal-node --bin focal \
        --target "$(uname -m)-unknown-linux-musl" \
    && cargo build --release --locked -p focal-load \
        --target "$(uname -m)-unknown-linux-musl" \
    && install -D -m 0755 "target/$(uname -m)-unknown-linux-musl/release/focal" /out/focal \
    && install -D -m 0755 "target/$(uname -m)-unknown-linux-musl/release/focal-load" /out/focal-load

FROM alpine:3.22
# The data and client directories private, as the shipped image makes its
# own: a named volume takes the mode of the directory it first mounts over.
RUN apk add --no-cache iproute2 && install -d -m 0700 /data /client
COPY --from=build /out/focal /usr/local/bin/focal
COPY --from=build /out/focal-load /usr/local/bin/focal-load
ENTRYPOINT []
