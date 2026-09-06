# Single binary, with a client/runtime seam kept internally

Hecate ships as one binary: `hecate` is the terminal client and the runtime (VMM, VFS,
ledger, provider gateways) in a single process — ease of use is the product priority,
and libkrun-as-library (ADR-0001) makes an embedded VMM real. There is no daemon
requirement and no separate mode to keep correct.

Internally, the client and runtime still communicate through a **seam** — and the seam
speaks the *real wire protocol*, not a channel that merely resembles one. The protocol
has three bindings: in-memory (single binary, default), unix socket (local attach), and
remote transport. All three carry the same frames, the same delta streams, the same
cursors. Local runs the full machinery in degenerate form — there is no "embedded-mode
bug" class because there is no embedded mode, only a shorter wire. Multi-terminal
attach and remote runtimes are therefore binding changes, not redesigns.

The counterexample is Sylk, whose message bus was welded inside an agent package and
whose UI held ~25 live pointers into the runtime — every distribution ambition died on
that retrofit. The seam costs almost nothing now; its absence costs a rewrite later.
