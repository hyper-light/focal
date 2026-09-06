# SPEC: FANOUT — composed SNS-shape topic router

Status: ACCEPTED 2026-08-20 (grilling finalization). Folds the RabbitMQ / Kafka
fan-out study and the user-settled principles: the **opt-in-optimization
principle** (SNS-compatible reliable-push default; architecture-native
optimizations opt-in behind a declared workload property) and the **push/pull
distinction** (SNS is push, SQS is pull). A **composed** primitive — it owns
routing and delegates durability per subscription to QUEUE (pull) and CACHE
pub-sub (push); building it standalone would duplicate them. Sibling of CACHE
and QUEUE on the shared substrate (CACHE §0). References: End-to-End Arguments
(TOCS 1984); RabbitMQ (exchange/binding model, `credit_flow`); Kafka
(consumer-groups = N cursors over one log); AWS SNS (message-attribute *and*
body filtering — our attributes-default is a deliberate divergence).

## 0. Shared substrate

Per CACHE §0.

## 1. Role & framing

A single-owner **topic router**: topics, subscriptions, filter policies, the
1→N replication step, per-group FIFO sequencing, and DLQ wiring. It owns
*routing*, not storage — the topic is a consensus-backed config descriptor
holding no storage. It **delegates durability per subscription**: an ephemeral
subscription → CACHE pub-sub (at-most-once **push**); a durable subscription →
QUEUE (at-least-once **pull**) → its own DLQ.

- **Default** (safe, SNS-compatible): standard **reliable durable push**
  (delivery with retries + DLQ) to any subscriber, including external ones;
  **attribute-based filtering** (content-free).
- **Opt-in** (declared property, §10): **push-notify + pull-recover** (log-cursor
  subscribers); **ephemeral at-most-once**; **body filtering** (authorized
  delivery edge).
- **Strictly-better defaults** (§6): per-subscription credit isolation (publisher
  never blocks); first-consumer-of-the-delta-stream (no second outbox).

## 2. Data model

```rust
struct TopicRouter {
    topic: TopicDescriptor,                 // consensus-backed config; owns NO storage
    subs:  Vec<Subscription>,
    seq:   GroupSequencer,                  // per FIFO group; lease+fence
    dedup: Blake3Window,                    // dedup over a derived window
}
struct Subscription { delivery: DeliveryMode, filter: FilterPolicy, dlq: Option<QueueRef>, order: OrderScope }
enum DeliveryMode { Durable(QueueRef), Ephemeral(ChannelId) }   // pull (default) | push (opt-in)
enum FilterPolicy { Fanout, Direct(Key), Topic(Pattern), Headers(AttrMatch), Body(AuthEdgeExpr) }
```

The `FilterPolicy` variants are RabbitMQ's exchange taxonomy (fanout / direct /
topic / headers); `Body` is the opt-in variant (§5).

## 3. Hot path

```rust
fn route(&mut self, ev: Event) {
    if self.dedup.seen(ev.id) { return; }
    let seqd = self.seq.stamp(ev.order_group());              // per-group FIFO, cross-group parallel
    for s in &self.subs {
        if !s.filter.matches_metadata(&ev.attributes) { continue; }  // attributes only on the fast path
        match &s.delivery {
            DeliveryMode::Durable(q)   => self.queue(q).enqueue(ev.as_ref()),   // at-least-once pull
            DeliveryMode::Ephemeral(c) => self.pubsub.publish(*c, ev.arena_ref()), // at-most-once push
        }
    }
}
```

It is the **first (and only) cursor** on the ledger's delta stream — a routing
table, not a second outbox (LEDGER_CORE "no outbox" holds). Kafka's
consumer-groups (N independent cursors over one durable log) validate the shape.

## 4. Delivery — push-notify + pull-recover

The default is **standard reliable durable push**: durable delivery with retries
and a DLQ, to any subscriber including external ones. This is the SNS-compatible
safe default and the one place broker-held delivery state is genuinely warranted
(External subscribers cannot cursor our log; the End-to-End Argument names
guaranteed-delivery + receipts as endpoint functions, needed exactly here).

**Opt-in for subscribers that declare they cursor our log**: **push-notify +
pull-recover** — a cheap at-most-once notification hint, with the subscriber
pulling the authoritative data by cursor from the log. The End-to-End Argument
makes this theorem-backed: correctness lives at the endpoint (the cursor), and a
reliable delivery primitive would be redundant to correctness — pure cost. A
dropped notify costs latency (recovered on the next notify or a reconcile), never
correctness.

## 5. Filtering

Default is **attribute-based** (content-free): routing decisions read frame
attributes/headers only, never the payload body — RabbitMQ's four exchange types
never route on the body, and the mesh transport carries the body as a
`ContentRef` while the warden makes every verdict on metadata.

**Body filtering is opt-in** (SNS offers it; this is a deliberate divergence
from the default, not from RabbitMQ). It runs at the **authorized delivery
edge**, which must read every topic message body to filter (including
filtered-out ones), so it requires an **IAM content-read grant on the topic** —
an explicit opt-in trust grant. It is **off the fast path** (materialize + parse
the body per message), at a per-message latency cost. The mesh transport and the
warden's verdicts stay content-free either way; the content-free law's core is
untouched.

## 6. Amplification & non-interference

Amplification is **credit-governed, arena fan-out with no copy, bodies as
`ContentRef`, publisher never blocks**. Per-subscription **credit isolation** is
the strictly-better default (RabbitMQ's `credit_flow` shape, but its backpressure
never propagates to block the publisher). A 1→millions fan-out cannot starve or
head-of-line-block control/quorum/claims traffic; the PROTOCOL non-interference
scale-walk gains a **fan-out-degree axis** (1 → millions of subscriptions), which
must show flat p99 and flat memory for those classes.

## 7. Replication & placement

The **topic registry** is CAS-first via CONSENSUS. **Durable subscriptions *are*
queue partitions** (replicated as queues, inheriting the QUEUE env-derived
durability — do not double-classify). **Ephemeral subscriptions** are per-node
pub-sub, unreplicated. The per-group FIFO sequencer is lease+fence. Cross-region
fan-out is async.

## 8. Boot & chokepoint

Topic-registry classifies **CAS-first** and the group sequencer **lease+fence**
in the CONSENSUS §6 roster; topic/subscription/filter descriptors register in
REGISTRY (§2b, `set_ref_if` = CAS-first); the `topic.*` IAM actions
(`publish`/`create_subscription`/`subscribe`/`set_filter_policy`/
`delete_subscription`/`redrive`) compile to PEP. Pub-sub fire-and-forget rides
the existing `DATAGRAM_SUPERSEDE` frame class (WIRE_SECURITY) — no new frame
class, only new flow identities.

**Sealed topics (amended 2026-08-22, COLLECTOR acceptance):** a topic MAY declare
`sealed: true` at registration, carrying its closed member list in the descriptor.
On a sealed topic, `create_subscription`/`subscribe` are **unrepresentable** —
refused at the topic registry itself, not merely IAM-denied — and membership
changes only by re-registering the descriptor (CAS-first, an auditable
publication). The collector's live-consumer topic (`COLLECTOR.md` §9) is sealed:
its four members bind at boot and nothing can join at runtime.

## 9. Laptop / N=1

In-process routing; ephemeral subscriptions are direct arena fan-out; durable
subscriptions are one local queue each. Same `route`.

## 10. Opt-in optimizations (declared-property table)

| Declared property | Optimization unlocked |
|---|---|
| (none — default) | reliable durable push + attribute filtering, SNS-compatible |
| log-cursor subscriber | **push-notify + pull-recover** (no durable per-message delivery) |
| ephemeral tolerance | **at-most-once** ephemeral subscription |
| IAM content-read grant on the topic | **body filtering** at the authorized delivery edge (off fast-path) |

## 11. Failure handling (FAULTS §5)

Ephemeral-loss under partition = **Degraded**, confined to the ephemeral class
(never work-loss). Durable subscriptions inherit the QUEUE dispositions. Every
drop counted.

## 12. Acceptance criteria

1. Reliable durable push (default) delivers to any subscriber including external
   ones, with retries + DLQ.
2. Attribute filtering is content-free — verified structurally (no routing
   verdict reads payload plaintext).
3. Body filtering is opt-in, IAM content-read-gated on the topic, and runs at
   the delivery edge off the fast path.
4. Push-notify + pull-recover is opt-in for log-cursor subscribers; a fully
   dropped-notify subscriber still converges via its cursor.
5. Per-subscription credit isolation — the publisher never blocks.
6. Exactly **one** cursor reads the ledger delta stream (a routing table, not a
   second outbox).
7. The fan-out-degree sweep (1 → millions of subs) shows flat p99 and flat
   memory for control/quorum/claims.
8. The topic registry is CAS-first and boot-classified; the group sequencer is
   lease+fence.
9. Durable subscriptions are already-classified queue partitions (not
   double-classified); ephemeral subscriptions introduce no roster entry.
10. Cross-region fan-out is async; no synchronous WAN on the publish path.

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
