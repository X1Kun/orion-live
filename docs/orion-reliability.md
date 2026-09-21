# Orion Reliability

## 1. Project Purpose

Orion Live is a production-oriented Go backend for the interaction plane of a live-streaming product. Its core purpose is to demonstrate and verify reliable real-time delivery across authenticated WebSocket connections, MySQL, RabbitMQ, and Redis.

The core release focuses on:

- Authenticated live-session lifecycle management
- Bounded WebSocket connection and room ownership
- Reliable cross-instance event delivery
- Transactional publication of `live_session.ended`
- Durable chat with asynchronous MySQL persistence
- Idempotent message consumption and history recovery
- Distributed rate limiting with explicit failure behavior
- Operational metrics, graceful shutdown, deployment, and failure testing

Media ingestion, transcoding, live audio/video transmission, storage, CDN delivery, and payment processing remain outside Orion Live.

The project intentionally favors one complete, measurable interaction path over broad product coverage. Replay products, a full gift system, business dashboards, and speculative cache layers are deferred until the core path is verified.

## 2. Delivery Scope

### Core release

| Capability | Required behavior |
| --- | --- |
| Authentication | Register, log in, validate short-lived JWTs, and protect HTTP and WebSocket endpoints. |
| Live sessions | Create, read, start, and end `LiveSession` records through guarded state transitions. |
| WebSocket rooms | Admit authenticated users only to `LIVE` sessions with bounded per-instance and per-user connections. |
| Messaging | Route versioned interaction events through RabbitMQ with confirms, mandatory routing, reconnection, retry, and DLQ isolation. |
| Session-ended Outbox | Commit `LIVE → ENDED` and `live_session.ended` in one MySQL transaction, then publish asynchronously. |
| Persistent chat | Confirm accepted messages through RabbitMQ, broadcast them across API instances, and persist them idempotently. |
| Chat history | Provide cursor-based history for room entry and bounded reconnect reconciliation. |
| Rate limiting | Enforce distributed Chat admission through Redis and fail closed when the limiter cannot provide a safe answer. |
| Operations | Expose bounded-cardinality metrics, structured logs, health signals, graceful shutdown, and repeatable failure evidence. |
| Deployment exercise | Run at least two API replicas and a Worker in one container orchestration environment with probes, resource bounds, and graceful rollout settings. |

### Optional extensions

After the core release is verified, choose at most one optional extension when additional depth is useful:

| Extension | Learning value |
| --- | --- |
| Live reactions | Reuse the event path for non-retrying client intent and eventually consistent aggregation. |
| Gift-effect credits | Demonstrate row locks, bounded transaction retry, credit consumption, and Transactional Outbox consistency. |

### Deferred product work

- Replay metadata, feeds, callbacks, and replay comments
- A complete gift-provider integration and payment-related workflow
- Live Interaction Insights and a business analytics API
- Redis read caching, request coalescing, database fallback circuits, and local degraded rate limiting
- Multiple optional interaction types solely to increase feature count

## 3. Terminology

| Term | Meaning |
| --- | --- |
| Interaction event | A versioned record distributed through RabbitMQ, such as `live_session.ended` or `chat.message.accepted`. |
| User ID | The authenticated account identity obtained from the validated JWT. Clients cannot override it in message payloads. |
| Message ID | A UUIDv4 created by the client for one logical Chat send. Retries reuse it. |
| Event ID | A stable identity for one event occurrence and all of its publication or delivery retries. |
| Correlation ID | The identity of one request or workflow execution across API, Publisher, and Consumer logs. It is not a deduplication key or Prometheus label. |
| Chat business key | `(live_session_id, user_id, message_id)`, which protects domain uniqueness independently of event delivery identity. |
| Accepted at | The UTC server time captured once after admission and preserved as the source event time. |
| History cursor | The auto-increment MySQL row ID used for stable Chat history pagination. It is not a message identity. |
| Publisher Confirm | RabbitMQ acknowledgement that the broker accepted responsibility for a publication. |
| Mandatory routing | RabbitMQ return behavior for a publication that did not reach any queue. It does not prove every required binding received the event. |
| Transactional Outbox | A database record committed atomically with a business mutation and published later. |
| Inbox | A Consumer-side record keyed by `(consumer_name, event_id)` that prevents duplicate business effects. |
| Send gate | Process-local admission state that rejects new client interaction frames when a session or messaging invariant is unavailable. |
| Poison message | Input that repeatedly fails for a deterministic reason. |
| Dead-letter queue | A queue that isolates messages after their retry budget is exhausted. |
| Fail closed | Refuse a new interaction when the dependency needed to make a safe admission decision is unavailable. |

## 4. Core Architecture

```text
HTTP / WebSocket Clients
           │
           ▼
┌───────────────────────────┐       rate limit       ┌───────┐
│        API Server         │◀──────────────────────▶│ Redis │
│ auth / live / WS / chat   │                       └───────┘
└────────┬─────────┬────────┘
         │         │
         │         └── live-session transaction ──▶ MySQL
         │                                                │
         │ direct confirmed Chat publish                  │ pending Outbox
         │                                                ▼
         │                                         Outbox Publisher
         │                                                │
         └──────────────────┬─────────────────────────────┘
                            ▼
                 ┌────────────────────────┐
                 │   RabbitMQ Interaction │
                 │        Exchange        │
                 └────────┬───────────────┘
                          │
             ┌────────────┴────────────────────┐
             ▼                                 ▼
    per-API realtime queue            Persistence Queue
             │                                 │
             ▼                                 ├─▶ Retry Queue
    Realtime Subscriber                        └─▶ DLQ
             │                                 │
             ▼                                 ▼
    WebSocket Hub / Rooms             Persistence Consumer
                                               │
                                               ▼
                                             MySQL

All processes ──▶ Prometheus metrics + structured logs
```

The baseline deployment uses one RabbitMQ broker node. Durable queues and retained broker storage survive a broker process or container restart, but do not provide broker-host or disk failover.

### Component responsibilities

| Component | Responsibility |
| --- | --- |
| API Server | Authentication, validation, LiveSession mutations, WebSocket lifecycle, admission, and direct Chat publication. |
| WebSocket Hub | Own process-local Rooms, bounded Client queues, slow-client removal, and graceful connection shutdown. |
| Topology Initializer | Declare the exchange, durable processing queue, retry queue, DLQ, and required bindings idempotently. |
| Realtime Subscriber | Consume the API instance's ephemeral queue and deliver events to local Rooms. |
| Outbox Publisher | Lease committed events, publish with confirms, and use a claim token to fence stale publishers. |
| Persistence Consumer | Persist Chat idempotently and acknowledge only after its local transaction commits. |
| MySQL | Authoritative store for users, sessions, Chat history, Outbox, and Inbox records. |
| Redis | Distributed interaction admission. It is not an authoritative store for durable business data. |
| RabbitMQ | Buffer and route events independently to real-time delivery and persistence. |

## 5. Key Design

### 5.1 LiveSession and WebSocket lifecycle

```text
LiveSession(SCHEDULED)
→ LiveSession(LIVE)
→ LiveSession(ENDED)
```

- One host may schedule multiple sessions but may have only one `LIVE` session at a time.
- State transitions use conditional MySQL updates so concurrent Start or End requests produce one successful transition.
- WebSocket upgrades require a valid JWT and a `LIVE` session before the server allocates a connection.
- Each API instance enforces a global connection limit and a per-user limit before Upgrade.
- One process-local Room is keyed by `live_session_id`; it is not another business entity.
- One Client represents one physical WebSocket lifetime and belongs to at most one Room.
- The Hub uses bounded Room and Client queues. Slow Clients are disconnected without blocking a Room.
- Process shutdown stops new admission, closes HTTP and WebSocket workloads concurrently, and waits within one process-level deadline.

Ending a session has a weak distributed cutoff for high-volume interactions. An API rejects new frames after observing `ENDED`, while an already accepted tail event remains valid and is never retracted.

### 5.2 Interaction event contract and RabbitMQ topology

Every event uses a versioned envelope:

```text
event_id
event_type
schema_version
correlation_id
user_id
live_session_id
occurred_at
payload
```

- `event_id` identifies one logical event and is reused by every publication retry and Broker redelivery.
- Consumer Inbox deduplication uses `(consumer_name, event_id)`.
- Chat domain deduplication separately uses `(live_session_id, user_id, message_id)`, protecting against a Producer bug that emits two Event IDs for one logical message.
- The generic envelope does not carry a second idempotency key or a producer-supplied request hash.
- Chat payload conflicts are classified by comparing the stored canonical business fields with the incoming payload after a business-key conflict.
- `correlation_id` connects logs and future trace context. It does not participate in uniqueness or metrics labels.

Core routing keys:

```text
orion.interaction.events
├─ live_session.ended
└─ chat.message.accepted
```

Optional extensions may add `reaction.created`, `gift.sent`, or `gift_effect_comment.created` without changing the core envelope.

The core topology contains:

```text
one RabbitMQ broker node
└─ orion.interaction.events
   ├─ one exclusive, auto-delete realtime queue per API instance
   └─ one shared durable Persistence Queue
      ├─ dedicated delayed Retry Queue
      └─ dedicated DLQ
```

- API instance count, logical queue count, and broker-node count are independent deployment dimensions.
- Realtime delivery is best effort. Each API instance owns a queue for only its locally connected Clients.
- Realtime queues bind `live_session.ended` and `chat.message.accepted`.
- The Persistence Queue binds `chat.message.accepted`; `live_session.ended` remains authoritative in the LiveSession table and Outbox.
- Persistence Consumer replicas compete on the shared durable queue.
- Durable processing messages use persistent delivery mode.
- Publishers wait for Confirm and enable mandatory routing.
- The API send gate opens only after required topology validation succeeds.
- Connections, channels, QoS, publishers, and Consumers are recreated with exponential backoff and jitter.
- Orion uses one recovery owner: the application Client recreates connections, while Publisher and Consumer components recreate their channels. The experimental `amqp091-go` automatic Recovery mechanism remains disabled to avoid overlapping recovery state machines.
- Consumer acknowledgements occur only after processing succeeds or an idempotent duplicate is proven safe.
- Retryable failures enter the delayed retry queue with a bounded attempt count. Deterministic and exhausted failures enter the DLQ.

Mandatory routing proves that at least one queue matched. It does not independently prove that every required queue was bound, so startup and periodic topology validation remain explicit reliability checks.

The self-contained development topology declares retry TTL and dead-letter routing through queue `x-arguments`. Before a stable deployed queue becomes operational data, mutable TTL, DLX, and length settings move to RabbitMQ Policy managed by deployment IaC. Policies are not configured through the runtime AMQP identity.

### 5.3 `live_session.ended` Transactional Outbox

The first transactional event is `live_session.ended`:

```text
BEGIN
  require current status = LIVE
  capture database UTC ended_at once
  update LiveSession to ENDED using that timestamp
  insert live_session.ended Outbox event with the same occurred_at
COMMIT
```

- The state transition and event either commit together or both fail.
- The HTTP response succeeds after the MySQL transaction commits; RabbitMQ availability does not decide whether End is durable.
- Outbox records contain `status`, `available_at`, `claimed_by`, `claim_token`, `lease_until`, `attempt_count`, `last_error`, and `published_at`.
- A Publisher claims a bounded batch and commits the lease before network publication.
- Publisher Confirm marks an event `PUBLISHED` only when both `event_id` and the current `claim_token` match.
- A lease expiry may cause duplicate publication, so downstream Consumers use stable `event_id` values and idempotent processing.
- Publication retries use bounded exponential backoff. Deterministic and exhausted failures enter `FAILED` for alerting and controlled recovery.

The realtime subscriber uses `live_session.ended` to close the local send gate and notify connected Clients. MySQL remains authoritative: a restarted API has no old in-memory Room, and every new WebSocket admission checks the current session state even if that process missed the event.

### 5.4 Ordinary Chat pipeline

```text
WebSocket chat.send with client-generated message_id
→ obtain user_id from the authenticated connection
→ validate frame and apply Redis admission
→ capture accepted_at once in UTC
→ derive stable event_id
→ publish chat.message.accepted
→ verify routing and wait for Publisher Confirm
→ return chat.ack(status = accepted)
→ Realtime Subscriber broadcasts to local Rooms
→ Persistence Consumer writes MySQL idempotently
```

- `(live_session_id, user_id, message_id)` uniquely identifies a Chat message.
- A deliberate retry reuses `message_id`; a new send creates a new UUIDv4.
- The API never accepts `user_id` from the frame payload.
- Publication retries reuse the original `event_id` and `accepted_at`.
- Ordinary Chat publishes directly to RabbitMQ. It does not create an API-side Outbox row.
- Success is returned only after confirmed, routable publication while the required topology is healthy.
- A duplicate business key with the same canonical content has no additional effect.
- A duplicate business key with different canonical content preserves the first committed value, records a conflict metric and audit log, and does not enter an automatic retry loop.
- Realtime delivery is provisional. Persisted history is authoritative after reconciliation.

History queries use:

```sql
WHERE live_session_id = ? AND id > ?
ORDER BY id
LIMIT ?
```

Joining Clients fetch recent context. Reconnecting Clients query after their last cursor several times within a bounded recovery window and merge WebSocket and HTTP copies by `(live_session_id, user_id, message_id)`.

A finite recovery window cannot prove completeness without a persistence watermark. Later history refreshes may discover messages that persisted after the window.

### 5.5 Consumer Inbox and retry behavior

- Durable Consumers store `UNIQUE(consumer_name, event_id)` Inbox records.
- The Inbox insert and business mutation commit in the same MySQL transaction.
- A duplicate Inbox key skips repeated business work and is acknowledged safely.
- Retry and DLQ redrive preserve the original event identity, source time, idempotency key, and business identifiers.
- Automatic retry and supported redrive durations are bounded.
- Inbox retention must outlive every supported automatic retry or redrive window.
- Poison messages cannot block healthy queue traffic indefinitely.

The system provides at-least-once delivery with one logical database effect, not exactly-once delivery.

### 5.6 Redis admission and failure behavior

- Redis Lua scripts atomically enforce distributed token buckets for Chat users and Rooms.
- Redis operations use a short deadline so WebSocket goroutines do not block on a degraded dependency.
- Exceeding a healthy rate limit returns `429`.
- If Redis cannot make a safe admission decision, new Chat sends return `503`.
- Established WebSocket connections remain available for receiving events.
- Redis recovery probes restore Chat admission automatically.
- MySQL and RabbitMQ remain authoritative for durable state and accepted events.

The first release deliberately omits local in-memory degradation, `MAX_API_REPLICAS` budget division, read caching, cache invalidation, request coalescing, and database fallback circuits. Those features require measured load or availability requirements before implementation.

### 5.7 Observability and deployment

Prometheus records operational behavior, including:

- HTTP latency and response counts
- Active and rejected WebSocket connections
- Publish Confirm latency and failures
- Required-binding health
- Outbox pending count, oldest age, retries, and failures
- Consumer retry, DLQ, processing, and persistence latency
- Redis admission failures and recovery
- Chat conflict counts

High-cardinality identifiers such as `user_id`, `message_id`, and `event_id` never appear as metric labels. Structured logs carry correlation and event identifiers for diagnosis.

`/healthz` reports process liveness. `/readyz` reports whether a process should receive new traffic. Readiness closes before graceful shutdown begins.

The deployment exercise must demonstrate:

- Two API replicas and at least one Worker
- Liveness and readiness probes
- Resource requests and limits
- Secrets supplied outside the image
- A termination grace period longer than the process shutdown timeout
- Rolling replacement without accepting new work on draining replicas
- Cross-instance Chat delivery and recovery after one API restart

Kubernetes is the preferred learning target. Full GitOps, a service mesh, cluster provisioning, and multi-region operation are outside the first release.

## 6. Consistency and Reliability

### Consistency model

| Interaction | Guarantee | Design |
| --- | --- | --- |
| LiveSession transition | Atomic conditional state change | MySQL is authoritative and rejects invalid or concurrent transitions. |
| Session-ended publication | Durable eventual publication | The state change and Outbox event commit in one transaction; publication is at least once. |
| Chat acceptance | Conditional durable asynchronous acceptance | Success requires confirmed, routable RabbitMQ publication while required topology checks pass. |
| Chat persistence | Eventual consistency | The first committed business key and canonical content become authoritative. |
| Real-time delivery | Best effort | Per-API realtime queues serve connected Clients; history repairs missed Chat messages. |
| Interaction cutoff | Weak distributed cutoff | APIs close send gates after observing `ENDED`; previously accepted tail events remain valid. |
| Redis admission | Fail closed | New Chat sends are refused when the distributed limiter cannot safely decide. |

### Reliability targets

| Scenario | Expected behavior |
| --- | --- |
| Duplicate durable delivery | Inbox or a business unique key produces one logical MySQL effect. |
| Poison message | Bounded retries end in the Persistence DLQ. |
| RabbitMQ unavailable during Chat | No accepted Chat ACK is returned. |
| RabbitMQ unavailable after session End | The committed Outbox event remains pending and retries later. |
| API crashes after Outbox publication but before the `PUBLISHED` fencing update | The event may publish again; stable identity prevents duplicate effects. |
| Required binding missing | Readiness and the interaction send gate close until topology is restored. |
| Redis unavailable | New Chat sends return `503`; established connections may continue receiving. |
| Persistence Consumer or MySQL unavailable | RabbitMQ buffers accepted Chat within configured queue and retry bounds. |
| Slow WebSocket Client | Its bounded queue fills and only that Client is disconnected. |
| API process restarts | Local Rooms disappear, RabbitMQ reconnects, and Clients reconnect and reconcile history. |
| `SIGTERM` during load | Admission stops and HTTP, WebSocket, Publisher, and Consumer work share one bounded shutdown period. |

## 7. Verification Strategy

| Layer | Required evidence |
| --- | --- |
| Unit | State transitions, event identity, payload-conflict classification, retry classification, Outbox fencing, admission limits, and error mapping. |
| MySQL integration | Migrations, concurrent transitions, Outbox atomicity, claim expiry, fencing updates, Inbox deduplication, and Chat conflicts. |
| RabbitMQ integration | Topology, confirms, mandatory returns, reconnect, retry/DLQ routing, acknowledgements, and duplicate delivery. |
| Race and fuzz | Hub/Room lifecycle, slow Clients, malformed envelopes, frame bounds, and concurrent shutdown. |
| End to end | Two API replicas verify cross-instance Chat, session-end propagation, persistence, reconnect reconciliation, and Redis refusal/recovery. |
| Load | Sustained Chat measures p95/p99 acceptance latency, Confirm latency, queue depth, persistence delay, and slow-client removal. |
| Failure injection | Restart or pause API, Worker, RabbitMQ, Redis, and MySQL; remove a binding; expire an Outbox lease; send `SIGTERM` under load. |
| Deployment | Record rollout behavior, probe transitions, resource use, commands, recovery time, and residual risk. |

Repeatable commands, results, measurements, and known limitations are recorded in `PRODUCTION_READINESS.md`. Local tests or a single successful demonstration do not justify a production-ready claim.

## 8. Implementation Roadmap

1. **Foundation — implemented:** configuration validation, migrations, secrets cleanup, public errors, health endpoints, metrics, Docker Compose, and CI.
2. **Authentication and LiveSession — implemented:** registration, login, JWT middleware, lifecycle APIs, MySQL constraints, and concurrency tests.
3. **WebSocket safety — implemented baseline:** authenticated upgrade, `LIVE` admission, bounded Hub/Room/Client queues, connection limits, heartbeats, origin checks, race tests, and graceful shutdown. Client interaction frames remain disabled.
4. **Messaging foundation — in progress:** the event envelope, maintained AMQP client, durable core topology, Confirmed Publisher, mandatory routing, retry/DLQ declarations, and connection recovery are implemented. The per-API realtime subscriber and runtime topology health are next.
5. **Session-ended Outbox:** Outbox migration, fenced claim and lease, Publisher loop, atomic End transaction, `live_session.ended` publication, and process-local send-gate propagation.
6. **Persistent Chat:** `chat.send`, UUIDv4 message identity, Redis admission, Confirmed Publish, `chat.ack`, cross-instance broadcast, Inbox persistence, history, and bounded reconnect recovery.
7. **Operational deployment:** two API replicas, Worker lifecycle, orchestration manifests, probes, resources, rollout behavior, metrics, load tests, and failure injection.
8. **Optional extension:** implement at most one of Reaction aggregation or Gift-effect credit transactions after the core release evidence is complete.

Each roadmap item includes implementation, focused tests, operational metrics, failure behavior, and documentation. A new business feature does not create a second messaging framework.

## 9. Out of Scope for the First Release

- Media upload, transcoding, live video delivery, recording storage, or CDN integration
- Replay metadata, replay feeds, media callbacks, and replay comments
- Payment processing, refunds, or a financial-grade ledger
- A complete gift-provider product flow
- A business analytics dashboard or Live Interaction Insights API
- Redis read caching and complex local degradation
- Kafka, Kafka Streams, or event sourcing
- Multi-node RabbitMQ clustering and Quorum Queue failover
- Multi-region deployment, strict global ordering, or zero-tail global interaction cutoff
- Full presence tracking or authoritative online-user state
- Full GitOps, service mesh adoption, or a custom Kubernetes Operator
- Claiming production readiness without repeatable deployment and failure evidence
