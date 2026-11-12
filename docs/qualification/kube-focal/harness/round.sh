#!/bin/bash
# One round of the kube-focal build, with its node and git service alive for exactly the round:
# the architect (merges what passed, posts what is due), the workers together, the architect
# again (receives what they delivered), the evaluator (validates it). Every wait is counted.
# Stops early, printing DONE, when the architect's first turn of the round answers DONE.
# Usage: round.sh
set -u
K=/Users/adalundhe/fc2k
BIN=$K/bin/focal
PY=/opt/homebrew/bin/python3.12
ADDRESS=$(cat "$K/address")

# The role's next turn number: one past the highest it has run.
next() {
  local n=0 f
  for f in "$K/runs/$1-"*.jsonl; do
    [ -e "$f" ] || continue
    f=${f##*-}; f=${f%.jsonl}
    case "$f" in ''|*[!0-9]*) continue;; esac
    [ "$f" -gt "$n" ] && n=$f
  done
  echo $((n + 1))
}
run() { "$K/turn.sh" "$1" "$(next "$1")"; }

# TERM, then at most 60 s for it to end, then KILL.
stop() {
  local pid=$1 i
  [ -n "$pid" ] || return 0
  kill -TERM "$pid" 2>/dev/null || return 0
  for i in $(seq 1 300); do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.2; done
  kill -KILL "$pid" 2>/dev/null
}

# A node an earlier round left behind, only if the recorded PID is still this binary.
OLD=$(cat "$K/node.pid" 2>/dev/null)
if [ -n "$OLD" ] && ps -o command= -p "$OLD" 2>/dev/null | grep -qF "$BIN"; then stop "$OLD"; fi

NODE=""; GIT=""
cleanup() { stop "$GIT"; stop "$NODE"; }
trap cleanup EXIT

: > "$K/node.out"
"$BIN" --data-dir /tmp/fc2k/n start node --advertise "$ADDRESS" > "$K/node.out" 2>> "$K/node.err" &
NODE=$!
echo "$NODE" > "$K/node.pid"
for i in $(seq 1 900); do
  grep -q '"condition"' "$K/node.out" 2>/dev/null && break
  kill -0 "$NODE" 2>/dev/null || { echo "round: the node ended before it was ready"; exit 1; }
  sleep 0.2
done
grep -q '"condition": *"Ready"' "$K/node.out" || { echo "round: the node never reported Ready"; exit 1; }

"$PY" -I "$K/gitsvc.py" &
GIT=$!
for i in $(seq 1 100); do /usr/bin/nc -z 127.0.0.1 47100 2>/dev/null && break; sleep 0.2; done
/usr/bin/nc -z 127.0.0.1 47100 2>/dev/null || { echo "round: the git service did not listen"; exit 1; }

N=$(next architect)
run architect
if [ "$(tr -d '[:space:]' < "$K/runs/architect-$N.last.txt" 2>/dev/null)" = DONE ]; then
  echo "DONE"
  exit 0
fi
run worker-1 &
W1=$!
run worker-2 &
W2=$!
run worker-3 &
W3=$!
wait "$W1" "$W2" "$W3"
run architect
run evaluator
