# Orion Live

A production-oriented Go backend for the interaction plane of a live-streaming product.

Orion Live focuses on authenticated live sessions, reliable WebSocket interaction, durable chat, live reactions, gift-effect comments, replay metadata, and interaction analytics. Media ingestion, transcoding, storage, and CDN delivery remain outside the project.

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
- A minimal Docker Compose development environment
- CI gates for formatting, static analysis, compilation, image construction, Compose validation, and secret scanning

WebSocket chat, messaging topology, reactions, gifts, analytics, and replay APIs will be added in focused increments. Their target behavior is documented in [docs/orion-reliability.md](docs/orion-reliability.md).

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

Run the baseline quality gates without starting dependencies:

```bash
make check
```

Run the migration and repository integration tests against a dedicated MySQL database:

```bash
ORION_TEST_MYSQL_DSN='orion:password@tcp(127.0.0.1:3306)/orion_test?charset=utf8mb4&parseTime=true&loc=UTC&multiStatements=true' \
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
