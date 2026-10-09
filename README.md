# Orion Live

A production-oriented Go backend for the interaction plane of a live-streaming product.

Orion Live focuses on authenticated live sessions, bounded WebSocket interaction, reliable RabbitMQ delivery, Transactional Outbox publication, durable chat, Redis admission control, and operational verification. Media delivery and broad product features remain outside the core release.

## Current status

The repository is being rebuilt from its original video-oriented prototype. The current clean baseline contains:

- User registration and login with short-lived JWT access tokens
- Stable public error responses
- Strict environment configuration validation
- MySQL, Redis, and RabbitMQ connectivity
- Versioned, checksummed MySQL migrations
- Liveness, readiness, and Prometheus endpoints
- Graceful HTTP shutdown
- Authenticated live-session creation and lifecycle management
- Authenticated WebSocket connections to live rooms
- Versioned interaction events, durable RabbitMQ topology, and confirmed mandatory publication
- Per-API realtime RabbitMQ subscription and process-local Hub delivery
- Transactional `live_session.ended` Outbox with leased, fenced publication
- Confirmed WebSocket Chat ingress with atomic Redis admission
- Durable Chat persistence with bounded concurrent processing, Inbox deduplication, retry, and DLQ handling
- Authenticated Chat history with stable cursor pagination
- `room.ready` synchronization and bounded reconnect recovery coverage
- Kustomize-based two-replica API and migration Job manifests
- Disposable Kind infrastructure and cross-Pod resilience E2E automation
- Prometheus/Grafana stack with Orion and RabbitMQ targets, alerts, and dashboard
- Correctness-aware Chat load harness with a reproducible smoke profile
- A minimal Docker Compose development environment
- CI gates for formatting, static analysis, compilation, image construction, Compose/Kustomize validation, secrets, Kind Smoke on pull requests, and full Kind Resilience on main and schedule

Load experiments and operational diagnostics now cover throughput, hot-room fan-out, idle connections, mixed traffic, bursts, and CPU/Publisher-concurrency comparisons. The verified three-node Kind release and resilience workflow is documented in [deploy/k8s/README.md](deploy/k8s/README.md), the monitoring stack in [deploy/k8s/observability/README.md](deploy/k8s/observability/README.md), and target behavior in [docs/orion-reliability.md](docs/orion-reliability.md).

The load harness and its correctness guarantees are documented in [docs/load-testing.md](docs/load-testing.md). The smoke profile validates the harness; measured capacity evidence is intentionally recorded separately.

The repeatable post-optimization profiles and curated results are documented in [docs/performance/chat-baseline.md](docs/performance/chat-baseline.md).

## Test results

Tests run on an i7-9750H laptop (6 cores / 12 threads, approximately 32 GiB RAM), using three Kind nodes, two API Pods, and single-node MySQL, Redis, and RabbitMQ. Connections are assigned evenly through two direct Pod port-forwards. These observations include local storage and tunnel overhead; they establish tested workloads, not production or maximum capacity.

| Workload | Observed result | Interpretation |
| --- | --- | --- |
| Chat throughput: 25 msg/s, 20 connections, 2 minutes | 3,000 accepted and persisted; 60,000 realtime deliveries; ACK p95 327 ms | Representative successful run with 4 Persistence Workers per Pod |
| Chat throughput: 50 msg/s, 20 connections, 2 minutes | 6,000 accepted and persisted; 120,000 realtime deliveries; ACK p95 315 ms | Representative successful run with 8 Persistence Workers per Pod; later runs exposed variability |
| Hot-room fan-out: 1,000 connections, 5 msg/s, 2 minutes | 600 messages persisted; 600,000 realtime deliveries; broadcast p95 51 ms | Counts matched without rejected messages |
| Idle connection hold: 2,000 connections, 2 minutes | All connections held for the test duration | Reaches the configured two-Pod admission ceiling; does not measure active-traffic capacity |
| Mixed hot room: 1,000 connections, 20 senders, 50 msg/s, 2 minutes | 6,000 persisted and 6,000,000 realtime deliveries at 1 CPU per Pod | Correctness completed, but ACK/broadcast p95 of 1.53/1.26 s exceeded the proposed latency targets |

Tests also retained failures: 500m CPU per Pod produced heavy throttling and inbound-overload closure in hot-room traffic; repeated runs observed Redis Admission timeouts and shared-disk latency spikes. Sustained 25 msg/s mixed traffic is not yet consistently within the zero-rejection and latency objectives. A successful count check alone is not a performance SLO pass.

[Curated report snapshots](docs/performance/evidence/representative-runs.json) preserve source run IDs, measured values, and harness revision metadata. [Performance findings](docs/performance/chat-baseline.md) and [latency investigation](docs/performance/latency-investigation.md) explain selection, failures, and remaining uncertainty. Raw local artifacts are ignored by Git.

## Optimizations driven by test evidence

| Evidence | Implemented change | Validation and tradeoff |
| --- | --- | --- |
| Serialized publishing held a mutex while waiting for Confirm | Bounded independent Publisher Channels, one in-flight publication per lane; Outbox remains sequential | Retested the previously failing 5 msg/s workload with complete ACK/broadcast/persistence counts. More lanes consume resources and cannot remove broker or disk delays |
| Sequential Persistence Consumers plateaued near 10 msg/s | Configurable bounded Workers per Consumer with individual Ack/Reject and Session cancellation | Four Workers per Pod reached approximately 26 msg/s in one experiment; eight completed a 50 msg/s run. Higher concurrency increases database work; gains are not linear or guaranteed |
| Slow Chat processing prevented timely Pong reads in a controlled reproduction | Separate WebSocket reader and ordered processor, connected by a bounded inbound queue | Race-tested heartbeat continuity, message order, overflow cancellation, and shutdown. A later load run observed explicit 1013 overload instead of silently growing the queue |
| Client-visible latency could not be explained by aggregate CPU alone | Inbound/outbound wait histograms, Publisher lane timing, Redis pool metrics, per-Pod CPU, and host I/O sampling | Localized rejection events to Admission timeouts and correlated some with disk stalls. Shared-I/O causality and latency stability remain open |
| Failed setup and metric requests produced misleading or missing summaries | Explicit failure stages and History-check state; isolated, retried metric collection | Regression checks cover failed HTTP/JSON collection. Unverified persistence is shown as n/a, not zero writes |

The resulting engineering loop is reproducible: define a workload, check message correctness, correlate latency with dependency/resource metrics, change one constraint, and compare matched runs. Remaining multi-room, concurrent-History, and traffic-under-failure scenarios are follow-up work; existing Kind resilience E2E is a separate functional check.

## Local development

Requirements:

- Go 1.24 or newer
- Docker with Docker Compose

Create local configuration and replace every placeholder credential:

```bash
cp .env.example .env
```

Start the stack:

```bash
make up
```

The migration container runs before the API. Once the API is ready:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

Useful endpoints:

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process liveness |
| `GET` | `/readyz` | MySQL, Redis, and RabbitMQ readiness |
| `GET` | `/metrics` | Prometheus metrics |
| `POST` | `/api/v1/users/register` | Create an account |
| `POST` | `/api/v1/users/login` | Obtain an access token |
| `GET` | `/api/v1/profile` | Validate an access token |
| `POST` | `/api/v1/live-sessions` | Create a scheduled live session |
| `GET` | `/api/v1/live-sessions/:id` | Read a live session |
| `POST` | `/api/v1/live-sessions/:id/start` | Start a scheduled live session |
| `POST` | `/api/v1/live-sessions/:id/end` | End a live session |
| `GET` | `/api/v1/live-sessions/:id/messages?limit=50` | Read recent persisted Chat history |
| `GET` | `/api/v1/live-sessions/:id/ws` | Join a live room through a WebSocket upgrade |

The current WebSocket endpoint requires a Bearer token in the upgrade request and accepts only `LIVE` sessions. After joining the process-local Room it emits `room.ready`, applies atomic Redis admission to validated `chat.send` frames, publishes `chat.message.accepted` with RabbitMQ confirms, and returns `chat.ack` only after successful publication. A durable Consumer persists accepted messages with Inbox and business-key idempotency. Clients combine buffered realtime events with bounded cursor-based History queries to repair join and reconnect gaps.

Run the baseline quality gates without starting dependencies:

```bash
make check
```

Render the API and migration Kubernetes Kustomizations without connecting to a cluster:

```bash
make k8s-render
```

On a Linux host with Kind, Docker, kubectl, and sufficient inotify limits, run the complete deployment and resilience workflow:

```bash
GOPROXY=https://goproxy.cn,direct make kind-e2e
```

Run the infrastructure integration tests against dedicated MySQL and RabbitMQ instances:

```bash
ORION_TEST_MYSQL_DSN='orion:password@tcp(127.0.0.1:3306)/orion_test?charset=utf8mb4&parseTime=true&loc=UTC&multiStatements=true' \
ORION_TEST_RABBITMQ_URL='amqp://orion:password@127.0.0.1:5672/' \
ORION_TEST_REDIS_URL='redis://:password@127.0.0.1:6379/0' \
  make test-integration
```

Stop local services without deleting their volumes:

```bash
make down
```

## Repository layout

```text
cmd/server       API process
cmd/migrate      versioned migration process
internal         application code
migrations       embedded SQL migrations and runner
pkg              infrastructure clients and logging
docs             target system design
```

## Security

The repository contains no working credentials. `.env` files are ignored; `.env.example` contains placeholders only. The example environment is intended for local development, not an internet-facing deployment.
