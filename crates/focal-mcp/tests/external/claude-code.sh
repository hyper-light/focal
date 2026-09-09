#!/usr/bin/env bash
# Claude Code registers the adapter as a local stdio MCP server and lists it.
set -euo pipefail
: "${FOCAL_BIN:?FOCAL_BIN names the focal binary}"
: "${FOCAL_DATA_DIR:?FOCAL_DATA_DIR names a running founder node's data directory}"
command -v claude >/dev/null 2>&1 || exit 3
name="focal-qualification-$$"
cleanup() { claude mcp remove "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
claude --version
claude mcp add "$name" -- "$FOCAL_BIN" --data-dir "$FOCAL_DATA_DIR" mcp serve
claude mcp get "$name" | grep -q "$name"
# A non-interactive turn that must reach the adapter: the reply names a tool.
claude -p "List the names of the MCP tools offered by the server named $name, one per line, nothing else." --allowedTools "mcp__${name}__*" | grep -Eq 'claim\.get|ledger\.(standing|summary)'
echo "claude-code: ok"
