#!/bin/bash
# The ledger the Kubernetes build runs through: one native node, and one enrolled participant per
# agent, each its own client data directory. Usage: setup.sh FOCAL
set -u
BIN=$1
N=/tmp/fc2k/n
A=/tmp/fc2k/a
mkdir -p /tmp/fc2k "$A"; chmod 700 /tmp/fc2k
if [ ! -f "$N/IDENTITY" ]; then
  mkdir -p "$N"; chmod 700 "$N"
  "$BIN" --data-dir "$N" activate native > /Users/adalundhe/fc2k/activate.json 2>&1 || { echo "activate failed"; cat /Users/adalundhe/fc2k/activate.json; exit 1; }
fi
PORT=$(/opt/homebrew/bin/python3.12 -I -c 'import socket; s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
echo "127.0.0.1:$PORT" > /Users/adalundhe/fc2k/address
nohup "$BIN" --data-dir "$N" start node --advertise "127.0.0.1:$PORT" > /Users/adalundhe/fc2k/node.out 2> /Users/adalundhe/fc2k/node.err &
echo $! > /Users/adalundhe/fc2k/node.pid
for i in $(seq 1 900); do grep -q '"condition"' /Users/adalundhe/fc2k/node.out 2>/dev/null && break; sleep 0.2; done
grep -o '"condition": *"[A-Za-z]*"' /Users/adalundhe/fc2k/node.out | head -1
for agent in architect worker-1 worker-2 worker-3 evaluator; do
  D=$A/$agent
  [ -d "$D/CLIENT.contexts" ] && { echo "$agent: enrolled already"; continue; }
  mkdir -p "$D"; chmod 700 "$D"
  "$BIN" --data-dir "$N" invite client --name "$agent" --output "$D/$agent.invite" > "$D/invite.json" 2>&1 || { echo "$agent: invite failed"; cat "$D/invite.json"; continue; }
  "$BIN" --data-dir "$D" enroll context "$agent" --invite-file "$D/$agent.invite" > "$D/enroll.json" 2>&1 && echo "$agent: enrolled" || { echo "$agent: enroll failed"; tail -3 "$D/enroll.json"; }
done
