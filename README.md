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
- Durable Chat persistence with Inbox deduplication, bounded retry, and DLQ handling
- Authenticated Chat history with stable cursor pagination
- A minimal Docker Compose development environment
- CI gates for formatting, static analysis, compilation, image construction, Compose validation, and secret scanning

Bounded reconnect recovery and deployment evidence will be added in focused increments. Reaction aggregation or Gift-effect credits may be added later as one optional extension. Their target behavior is documented in [docs/orion-reliability.md](docs/orion-reliability.md).

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
| `GET` | `/api/v1/live-sessions/:id/messages?after_id=0&limit=50` | Read persisted Chat history |
| `GET` | `/api/v1/live-sessions/:id/ws` | Join a live room through a WebSocket upgrade |

The current WebSocket endpoint requires a Bearer token in the upgrade request and accepts only `LIVE` sessions. It applies atomic Redis admission to validated `chat.send` frames, publishes `chat.message.accepted` with RabbitMQ confirms, and returns `chat.ack` only after successful publication. A durable Consumer persists accepted messages with Inbox and business-key idempotency, and authenticated Clients can recover persisted messages through cursor-based history. Connection limits are enforced per API instance; bounded reconnect recovery remains.

Run the baseline quality gates without starting dependencies:

```bash
make check
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
