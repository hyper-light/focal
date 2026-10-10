# syntax=docker/dockerfile:1
# focal's arm of the comparison (docs/qualification/competitive-p99.md): the
# node, focal's load generator and the competitors' generator (focal-compare),
# so every system is driven from the same container under the same limits, built as the release's musl lane builds them,
# on Alpine so the bench can run `tc netem` and a shell beside them. Not the
# shipped image (deploy/container/Dockerfile is: `scratch`, the binary alone).
#
#   docker build -f tools/compare/compose/focal.Dockerfile -t focal:bench .
# Base images from Docker's official images' ECR Public mirror, by the same
# digests as Docker Hub's: its anonymous pull limit, shared by the runners,
# refused pulls mid-comparison.
FROM public.ecr.aws/docker/library/rust:1.98.1-alpine@sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f AS build
RUN apk add --no-cache build-base protoc bash perl linux-headers
WORKDIR /src
COPY . .
ENV RUSTFLAGS="-C target-feature=+crt-static" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=cc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=cc
RUN --mount=type=cache,id=focal-bench-target,target=/src/target \
    --mount=type=cache,id=focal-bench-registry,target=/usr/local/cargo/registry \
    cargo build --release --locked -p focal-node --bin focal \
        --target "$(uname -m)-unknown-linux-musl" \
    && cargo build --release --locked -p focal-load -p focal-compare \
        --target "$(uname -m)-unknown-linux-musl" \
    && install -D -m 0755 "target/$(uname -m)-unknown-linux-musl/release/focal" /out/focal \
    && install -D -m 0755 "target/$(uname -m)-unknown-linux-musl/release/focal-load" /out/focal-load \
    && install -D -m 0755 "target/$(uname -m)-unknown-linux-musl/release/focal-compare" /out/focal-compare

FROM public.ecr.aws/docker/library/alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8
# The data and client directories private, as the shipped image makes its
# own: a named volume takes the mode of the directory it first mounts over.
RUN apk add --no-cache iproute2 && install -d -m 0700 /data /client
COPY --from=build /out/focal /usr/local/bin/focal
COPY --from=build /out/focal-load /usr/local/bin/focal-load
COPY --from=build /out/focal-compare /usr/local/bin/focal-compare
ENTRYPOINT []
