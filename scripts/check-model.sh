#!/usr/bin/env bash
# Check the models of docs/models with TLC. The checker is fetched once, by
# its digest, into the directory named by TLA_TOOLS (default: target/tla).
#
#   scripts/check-model.sh           three voters, three terms, one index:
#                                    elections and what one that is elected
#                                    takes
#   scripts/check-model.sh round     three voters, two terms, two indexes: the
#                                    fast quorum as the core counts it
#   scripts/check-model.sh reached   the same, and the claim that no index is
#                                    ever committed by what members hold by
#                                    themselves: the checker must refuse it,
#                                    or `round` checks nothing of the fast track
#   scripts/check-model.sh wrong     one that is elected takes the entry its
#                                    voters hold least: refused
#   scripts/check-model.sh anyround  a fast quorum counted without the round
#                                    of its votes: refused
#   scripts/check-model.sh four      `anyround` with the round counted: four
#                                    voters, three terms, and every state
#                                    holds what `anyround` breaks
#
# Nothing the checker takes grows without a bound.
#   States   A configuration states how many distinct states it has
#            (StateBudget) and the checker stops at one more (WithinBudget).
#            A model that passes must have exactly that many: a change that
#            makes it larger or smaller is refused until they are counted
#            and stated again.
#   Memory   TLC_MEMORY_MB of heap, and as much again outside it for the
#            fingerprints: the checker can take no more, whatever the
#            machine has. The default is what the largest configuration
#            here was measured to need: its 2,462,010 states pass in 256 MB
#            as fast as in 1,024, at 404 MB resident.
#   Disk     The states of one run, removed when the run ends, however it
#            ends.
#   Threads  TLC_WORKERS, one by default: beside other work the checker
#            takes one core, and a runner that does nothing else says `auto`.
#            A configuration the checker must refuse always runs with one,
#            so that it stops at the same state every time.
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
# What the checker must find violated, for a configuration it must refuse.
refused=""
case "${1:-}" in
  "") config=FastTrack.cfg ;;
  round) config=FastTrackRound.cfg ;;
  reached) config=FastTrackReached.cfg; refused=NoFastByHeld ;;
  wrong) config=FastTrackWrong.cfg; refused=LeaderHolds ;;
  anyround) config=FastTrackAnyRound.cfg; refused=LeaderHolds ;;
  four) config=FastTrackFour.cfg ;;
  *) echo "unknown model: $1" >&2; exit 2 ;;
esac
memory="${TLC_MEMORY_MB:-256}"
workers="${TLC_WORKERS:-1}"
if [ -n "$refused" ]; then
  workers=1
fi
budget="$(sed -n 's/^ *StateBudget *= *\([0-9][0-9]*\) *$/\1/p' "$task_root/docs/models/$config")"
if [ -z "$budget" ]; then
  echo "$config states no StateBudget" >&2
  exit 1
fi
work="$tools/run-${config%.cfg}"
mkdir -p "$work"
trap 'rm -rf "${work:?}/states"' EXIT
cp "$task_root/docs/models/FastTrack.tla" "$task_root/docs/models/$config" "$work/"
cd "$work"
status=0
java "-Xmx${memory}m" "-XX:MaxDirectMemorySize=${memory}m" -XX:+UseParallelGC \
  -cp "$jar" tlc2.TLC -workers "$workers" -deadlock \
  -config "$config" FastTrack.tla > out.log 2>&1 || status=$?
grep -E "states generated|Invariant .* is violated|^Error|Finished in" out.log | tail -5
found="$(sed -n 's/^[0-9]* states generated, \([0-9]*\) distinct states found.*/\1/p' out.log | tail -1)"
# 12: a property was violated.
if [ "$status" -eq 12 ] && grep -q "Invariant WithinBudget is violated" out.log; then
  echo "$config has more than the $budget states it states: count them, and state them" >&2
  exit 1
fi
if [ -n "$refused" ]; then
  if [ "$status" -eq 12 ] && grep -q "Invariant $refused is violated" out.log; then
    echo "the checker refuses it"
    exit 0
  fi
  echo "the checker did not refuse it (exit $status)" >&2
  exit 1
fi
if [ "$status" -ne 0 ]; then
  exit "$status"
fi
if [ "$found" != "$budget" ]; then
  echo "$config has $found states and states $budget: state what it has" >&2
  exit 1
fi
