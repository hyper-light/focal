#!/usr/bin/env bash
# MCP Inspector CLI against a real focal adapter.
set -euo pipefail
: "${FOCAL_BIN:?FOCAL_BIN names the focal binary}"
: "${FOCAL_DATA_DIR:?FOCAL_DATA_DIR names a running founder node's data directory}"
command -v npx >/dev/null 2>&1 || exit 3
run() {
  npx --yes @modelcontextprotocol/inspector --cli "$FOCAL_BIN" --data-dir "$FOCAL_DATA_DIR" mcp serve "$@"
}
npx --yes @modelcontextprotocol/inspector --version
tools=$(run --method tools/list)
printf '%s\n' "$tools" | grep -q '"name"' || { echo "no tools listed" >&2; exit 1; }
if printf '%s\n' "$tools" | grep -q '"ledger.standing"'; then
  result=$(run --method tools/call --tool-name ledger.standing)
else
  result=$(run --method tools/call --tool-name ledger.summary)
fi
printf '%s\n' "$result" | grep -q '"condition"' || { echo "no structured application result" >&2; exit 1; }
echo "inspector: ok"
