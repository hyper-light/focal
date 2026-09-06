# Durable monitor registration and observation

A monitor records bounded wait predicates under an active claim issued by the
authenticated participant. It is existing ledger metadata, separate from the
four object families. Registration does not post a claim, execute participant
work, or make a tool timeout into a business outcome.

```sh
focal monitor register \
  --owner OWNER_CLAIM_ID \
  --root satisfied:WAITED_CLAIM_ID \
  --timer TIMER_ID --generation 1 --at UNIX_SECONDS
focal monitor get MONITOR_ID --format json
```

The owner and every waited claim must already exist in the selected ledger.
The issuer may choose `satisfied`, `terminal`, or `released` predicates. All
listed predicates must become true, unless the owner's own scope is released.
These predicates differ: a failed or canceled claim is terminal, and need not
be satisfied. A release can follow owner cancellation or a trusted timer input;
the monitor record does not encode a release reason.

There are 1–256 distinct predicates. The timer ID, positive generation, and
deadline are explicit. The node adapter uses Unix seconds for ledger logical
time; the reducer requires the deadline to be in the future at admission.
Registration alone does not promise that a timer executor is installed. The
ordinary participant tools do not expose trusted timer firing or impersonate
the runtime. Observe actual committed facts with `monitor get` or durable watch.

The equivalent MCP calls are `monitor.register` and `monitor.get`:

```json
{
  "operation_id": "RESERVED_M1_OPERATION_REFERENCE",
  "owner": "00000000000000000000000000000001",
  "roots": [
    {"predicate": "satisfied", "claim": "00000000000000000000000000000002"}
  ],
  "deadline": {
    "timer": "00000000000000000000000000000003",
    "generation": 1,
    "at": 4102444800
  }
}
```

Replace example identities and the deadline with the intended values. Optional
`monitor` supplies an exact monitor ID; otherwise preparation generates and
persists it. `focal schema show monitor.register` describes the shared strict
input. CLI `--json`, `--yaml`, and `--file` use that same document; they cannot be
mixed with authored field flags.

Human CLI registration automatically retains an exact managed request. MCP
callers reserve an operation reference before registration and acknowledge it
only after retaining the result. A lost reply is recovered through the original
request reference. Registering again with another reference is new work, not a
monitor retry.

`monitor.get` returns the actual registration and optional released sequence at
a fresh quorum prefix, with ledger, route, and applied index. It does not mutate
monitor state or consume a managed request ordinal. Missing monitors produce
`not_found`; CLI structured output also retains the observed page before exit
code 4. This read cannot request a historical lease or infer why release occurred.

These commands supply durable predicates and observation for participant-owned
continuations. They do not implement nested consultation authorization, policy
for corrective claims, or the independent object lifecycle migration. See the
[interface contract](archictecutre/19-cli-mcp-implementation.md),
[manual CLI guide](manual-cli.md), and [MCP guide](mcp.md).
