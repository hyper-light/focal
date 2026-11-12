#!/bin/bash
# One turn of one agent: Codex headless in the agent's working copy, focal's MCP server as its
# ledger connection (the agent's own enrolled context), its writes kept to its working copy, its
# crates downloaded into a cargo home of its own. Usage: turn.sh ROLE TURN
set -u
ROLE=$1; TURN=$2
K=/Users/adalundhe/fc2k
BIN=$K/bin/focal
case "$ROLE" in architect) CWD=$K/kube; PROMPT_ROLE=$K/role-architect.md;;
  evaluator) CWD=$K/wt/evaluator; PROMPT_ROLE=$K/role-evaluator.md;;
  worker-*) CWD=$K/wt/$ROLE; PROMPT_ROLE=$K/role-worker.md;;
  *) echo "unknown role $ROLE"; exit 2;; esac
ID=$(/opt/homebrew/bin/python3.12 -I -c "import json; print(json.load(open('$K/participants.json'))['$ROLE'])")
PROMPT="$(sed -e "s/ROLE (participant ID)/$ROLE ($ID)/" -e "s/user.name=\"ROLE\"/user.name=\"$ROLE\"/" -e "s/ROLE@kube-focal/$ROLE@kube-focal/" $K/preamble.md)

$(cat $PROMPT_ROLE)"
mkdir -p $K/runs
export CARGO_INCREMENTAL=0 PATH=$K/bin:$PATH CARGO_HOME=$CWD/.cargo-home RUSTUP_TOOLCHAIN=1.98.1 CARGO_TARGET_DIR=$CWD/target
grep -q "^.cargo-home" $CWD/.git/info/exclude 2>/dev/null || printf '.cargo-home/\ntarget/\n' >> $CWD/.git/info/exclude
START=$(date +%s)
codex exec --json --skip-git-repo-check -C "$CWD" --sandbox workspace-write \
  -c sandbox_workspace_write.network_access=true \
  -c approval_policy='"never"' \
  -c "mcp_servers.focal.command=\"$BIN\"" \
  -c "mcp_servers.focal.args=[\"--data-dir\",\"/tmp/fc2k/a/$ROLE\",\"--client-context\",\"$ROLE\",\"serve\",\"mcp\"]" \
  -c mcp_servers.focal.tool_timeout_sec=300 \
  -c 'mcp_servers.focal.tools={"request.acknowledge"={approval_mode="approve"},"request.retry"={approval_mode="approve"},"claim.cancel"={approval_mode="approve"},"monitor.cancel"={approval_mode="approve"}}' \
  -o "$K/runs/$ROLE-$TURN.last.txt" "$PROMPT" \
  > "$K/runs/$ROLE-$TURN.jsonl" 2> "$K/runs/$ROLE-$TURN.err"
CODE=$?
echo "$ROLE turn $TURN: exit $CODE in $(( $(date +%s) - START )) s: $(tail -c 300 $K/runs/$ROLE-$TURN.last.txt 2>/dev/null | tr '\n' ' ')"
