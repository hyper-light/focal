# A ground-up dual-stack UDP/TCP protocol for the claims plane

The ledger, delta streaming, summon control, and health traffic speak a custom
protocol built from scratch on a UDP/TCP dual stack, modeled on hyperscale's dual-stack
protocol — not gRPC, not QUIC-off-the-shelf, and not MCP. MCP remains the wire for the
tool/skill plane only.

The claims plane has invariants a request/response tool protocol fits poorly: totally
ordered, resumable, sequence-numbered delta streams; watermark replay; delivery classes
(some traffic sheddable, consult-resolution traffic never); and dedup keyed on delta
identity. Owning the protocol lets those invariants live in the wire format instead of
being emulated above a general-purpose RPC layer.

## Consequences

We own framing, reliability, and versioning end to end — including the parts a standard
stack would have given us for free (flow control, congestion behavior, TLS integration).
The protocol specification lives in `docs/specs/PROTOCOL.md` (accepted
2026-08-18: two planes over UDP — header-encrypted control datagrams +
owned hecate-quic sessions; no TCP in the mesh) with the security pipeline
in `docs/specs/WIRE_SECURITY.md`; `LEDGER.md` §7 is the architecture-level
summary and defers to both. Interop with anything external happens at the
MCP boundary, never by exposing the claims protocol.
