#!/bin/bash
# Remove exactly this harness's containers, volumes and network, by name.
for node in fc-founder fc-host-a fc-host-b; do
  docker rm -f "$node" >/dev/null 2>&1 || true
  docker volume rm "$node" "$node-invite" >/dev/null 2>&1 || true
done
docker volume rm fc-client >/dev/null 2>&1 || true
docker network rm focal-chaos >/dev/null 2>&1 || true
echo down
