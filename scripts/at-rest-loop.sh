#!/usr/bin/env bash
# The at-rest journey (crates/focal-node/tests/cli_at_rest.rs, doc 29 §12.12) run again and again
# on one or two CPUs beside a busy loop on each of them, as the plan's step 8 asks: a node's
# conversions, reads and retries hold while others take its machine's CPU. Runs alternate between
# one CPU and two (odd runs on CPU 0 alone, even runs on CPUs 0 and 1). Linux (taskset). Bounded:
# FOCAL_AT_REST_RUNS runs (20 by default), each held by the test's own waits, which are charged to
# the progress of what they wait on; the busy loops end with the script, however it ends.
set -euo pipefail
runs="${FOCAL_AT_REST_RUNS:-20}"
task_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$task_root"
# The test's executable, built once.
exe="$(bash scripts/cargo.sh test --locked -p focal-node --test cli_at_rest --no-run \
  --message-format=json | python3 -c '
import json, sys
for line in sys.stdin:
    try:
        message = json.loads(line)
    except ValueError:
        continue
    if message.get("reason") == "compiler-artifact" \
            and message["target"]["name"] == "cli_at_rest" and message.get("executable"):
        print(message["executable"])
')"
test -x "$exe"
burners=()
stop_burners() {
  for pid in "${burners[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${burners[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap stop_burners EXIT
for cpu in 0 1; do
  taskset -c "$cpu" sh -c 'while :; do :; done' &
  burners+=("$!")
done
for run in $(seq 1 "$runs"); do
  if (( run % 2 )); then cpus=0; else cpus=0-1; fi
  began=$(date +%s)
  if taskset -c "$cpus" "$exe" --exact every_store_converts_once_the_fence_opens_the_at_rest_level \
      --test-threads=1 --nocapture; then
    echo "at-rest run $run of $runs: passed on CPUs $cpus in $(( $(date +%s) - began )) s"
  else
    echo "at-rest run $run of $runs: FAILED on CPUs $cpus after $(( $(date +%s) - began )) s"
    exit 1
  fi
done
echo "at-rest: all $runs runs passed, on one CPU and on two, beside a busy loop on each"
