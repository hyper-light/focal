# kube-focal: a Kubernetes in Rust, built by agents through focal

Terminating condition 2's second part, recorded ([09](../../archictecutre/09-implementation-status.md),
"2026-11-11 — Agents build a Kubernetes in Rust through focal, and a thousand agents run the work
flow past 50 claims a second"; [terminating conditions](../terminating-conditions.md) §2).

| File | What it is |
|---|---|
| `BRIEF.md` | The brief the agents were given, the repository's first commit |
| `kube-focal.bundle` | The repository: `main` at 77c9e2a and every worker branch the architect fetched (`git clone kube-focal.bundle kube-focal`) |
| `ledger.json` | The ledger read back through the architect's MCP connection after the build: the claims and evaluations its live views list, its events, and every turn's MCP calls and tokens from the transcripts (`harness/record.py`) |
| `harness/` | How the turns ran: one turn (`turn.sh`), one round (`round.sh`), the node and enrollments (`setup.sh`), the git service and its client (`gitsvc.py`, `kgit`), the prompts (`preamble.md`, `role-*.md`), the read-back (`record.py`, `summarize.py`) |

The harness ran on one Mac from `~/fc2k` (the clones, prompts and transcripts) with the node and the
five client contexts under `/tmp/fc2k`; paths in the scripts are those. Each agent turn was
`codex exec` (codex-cli 0.166.0, later 0.167.0; gpt-6.1-sol at reasoning effort max) in a
workspace-write sandbox, its only way to the ledger `focal serve mcp` as its own enrolled context.

The live views omit what has retired to the archive, so `ledger.json`'s claim and event lists are
the part still live at the end; the counts in the status entry come from the transcripts' committed
operations (197) and the ledger's sequence (200).
