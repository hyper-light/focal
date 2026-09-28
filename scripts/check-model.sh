#!/usr/bin/env bash
# Check the models of docs/models with TLC. The checker is fetched once, by
# its digest, into the directory named by TLA_TOOLS (default: target/tla).
#
#   scripts/check-model.sh           the model every change is checked with
#   scripts/check-model.sh five      five voters; minutes to hours
#   scripts/check-model.sh wrong     the rule the core does not follow: the
#                                    check passes when the checker refuses it
set -euo pipefail
task_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
tools="${TLA_TOOLS:-$task_root/target/tla}"
jar="$tools/tla2tools.jar"
digest=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
mkdir -p "$tools"
sum() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}
if [ ! -f "$jar" ] || [ "$(sum "$jar")" != "$digest" ]; then
  curl -sSfL -o "$jar" \
    https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
  if [ "$(sum "$jar")" != "$digest" ]; then
    echo "tla2tools.jar is not the one this script names" >&2
    exit 1
  fi
fi
case "${1:-}" in
  "") config=FastTrack.cfg ;;
  five) config=FastTrackFive.cfg ;;
  wrong) config=FastTrackWrong.cfg ;;
  *) echo "unknown model: $1" >&2; exit 2 ;;
esac
work="$tools/run-${config%.cfg}"
rm -rf "$work"
mkdir -p "$work"
cp "$task_root/docs/models/FastTrack.tla" "$task_root/docs/models/$config" "$work/"
cd "$work"
status=0
java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock \
  -config "$config" FastTrack.tla > out.log 2>&1 || status=$?
grep -E "states generated|Invariant .* is violated|^Error|Finished in" out.log | tail -5
rm -rf "$work/states"
if [ "${1:-}" = wrong ]; then
  # 12: a property was violated.
  if [ "$status" -eq 12 ] && grep -q "Invariant LeaderHolds is violated" out.log; then
    echo "the checker refuses the wrong rule"
    exit 0
  fi
  echo "the checker did not refuse the wrong rule (exit $status)" >&2
  exit 1
fi
exit "$status"
