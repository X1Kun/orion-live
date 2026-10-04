# Chat load testing

`cmd/chatload` provides a correctness-aware baseline for Orion's complete Chat path. It creates isolated users and a live session, opens authenticated WebSocket connections, sends Chat messages at a fixed aggregate rate, and waits for both acknowledgements and realtime fan-out. It then queries cursor-based History until every accepted message is persisted exactly once.

The default smoke profile is deliberately small:

```text
10 WebSocket connections
1 message per second
10 seconds
10 accepted messages
100 expected realtime deliveries
```

Run it against a local Compose deployment:

```bash
make load-smoke
```

Run it against an existing Orion Kind deployment with automatic port forwarding:

```bash
make kind-load-smoke
```

Override the profile with `LOAD_ARGS`:

```bash
make kind-load-smoke LOAD_ARGS='-connections 50 -message-rate 20 -duration 30s'
```

`-connections-per-user` reduces account setup cost for connection-capacity experiments, but it must not exceed the deployment's `WEBSOCKET_MAX_CONNECTIONS_PER_USER` setting. Keep it at one when measuring per-user Chat admission limits.

Alternatively, invoke the command directly. `ORION_LOAD_BASE_URL` can replace `-base-url`.

```bash
go run ./cmd/chatload -base-url http://127.0.0.1:8080 -connections 10 -message-rate 5 -duration 10s
```

The command writes one JSON report to standard output and exits non-zero if it observes missing or duplicate acknowledgements, missing or duplicate realtime delivery, persistence mismatch, an unexpected protocol frame, or a rejection ratio above `-max-error-rate`. Results describe the load generator and the local environment; they are not production capacity claims.

User registration, login, live-session creation, and WebSocket connection setup happen before `started_at`; setup cost is therefore excluded from the send and latency measurements.

The report also distinguishes messages that returned a rejected ACK but were subsequently observed in realtime or History. That outcome indicates an ambiguous publish result, such as a publisher-confirm timeout after RabbitMQ accepted the message, and fails the correctness check independently of the configured rejection threshold.

The pull-request Kind workflow runs this smoke profile after the existing Kubernetes E2E. Connection-capacity, steady-state throughput, hot-room fan-out, and traffic-under-failure profiles are intentionally left for subsequent evidence commits so that workload definitions and measured results remain reviewable.
