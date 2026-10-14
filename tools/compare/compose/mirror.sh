#!/usr/bin/env bash
# The comparison's pinned images copied, by digest, into the repository's own registry, once.
# The hosted runners share their egress addresses, so anonymous pulls from public registries
# share their limits: Docker Hub's pull limit refused pulls mid-comparison, and public.ecr.aws
# then answered `429 Too Many Requests: Data limit exceeded` as runs went on. A registry the
# workflow's own token reads has no such limit for it. An image already copied is not pulled
# again from its public source; the copy keeps the source's digest, so every reference stays
# pinned to the same bytes.
#
#   mirror.sh REGISTRY       e.g. ghcr.io/hyper-light/focal-bench
#
# Prints `BENCH_REGISTRY=REGISTRY` once every image is there, for a step's output; prints
# nothing and leaves the public sources in use when any copy cannot be made (a registry that
# refuses the token), so a mirror's failure never fails a comparison.
set -uo pipefail
registry="$1"
# SOURCE NAME TAG DIGEST: every image the comparison builds from or runs, as compose.yaml and
# focal.Dockerfile pin them.
images=(
  "public.ecr.aws/docker/library/rust rust 1.98.1-alpine sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f"
  "public.ecr.aws/docker/library/alpine alpine 3.22 sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
  "public.ecr.aws/docker/library/nats nats 2.12-alpine sha256:b270f5e2428354c0335612694d7dd2fb588148e567a5757fdff325ef9c9332e6"
  "public.ecr.aws/docker/library/redis redis 8.2-alpine sha256:b51665e66f00759be7c3152ad5ac3c66fb2f619c13ef62dea7cc1f9914524635"
  "docker.io/apache/kafka kafka 4.1.0 sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f"
)
for line in "${images[@]}"; do
  read -r source name tag digest <<< "$line"
  target="$registry/$name"
  # Each registry call bounded: a hung registry is a missing copy, not a hung job.
  if timeout 120 docker buildx imagetools inspect "$target@$digest" > /dev/null 2>&1; then
    echo "mirror: $name@$digest already there" >&2
    continue
  fi
  if ! timeout 900 docker buildx imagetools create --tag "$target:$tag" "$source@$digest" >&2; then
    echo "mirror: $name could not be copied; the public sources stay in use" >&2
    exit 0
  fi
  if ! timeout 120 docker buildx imagetools inspect "$target@$digest" > /dev/null 2>&1; then
    echo "mirror: $name's copy is not at $digest; the public sources stay in use" >&2
    exit 0
  fi
  echo "mirror: $name@$digest copied" >&2
done
echo "BENCH_REGISTRY=$registry"
