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

Use connection-only mode to hold authenticated WebSockets without generating Chat traffic:

```bash
make kind-load-smoke LOAD_ARGS='-mode connections -connections 300 -connections-per-user 3 -duration 1m'
```

Alternatively, invoke the command directly. `ORION_LOAD_BASE_URL` can replace `-base-url`.

```bash
go run ./cmd/chatload -base-url http://127.0.0.1:8080 -connections 10 -message-rate 5 -duration 10s
```

The command writes one JSON report to standard output and can persist an indented copy with `-output`. Setup failures occur before measurement and therefore return only an error instead of a misleading zero-value report. Once measurement begins, partial reports are retained even when the command exits non-zero.

When `-output` is provided, setup failures also save a report with status, failure stage, and failure reason, while keeping standard output free of unmeasured zero-value results. Reports mark Persistence verification as `not_checked`, `incomplete`, `complete`, or `not_applicable`. Summary displays unverified counts as `n/a`; legacy reports without verification metadata remain explicitly unknown. `complete` means History verification finished; overall correctness can still fail subsequently.

Chat mode exits non-zero if it observes missing or duplicate acknowledgements, missing or duplicate realtime delivery, persistence mismatch, an unexpected protocol frame, or a rejection ratio above `-max-error-rate`. Connections mode fails on any unexpected disconnect during the hold period. Results describe the load generator and the local environment; they are not production capacity claims.

User registration, login, live-session creation, and WebSocket connection setup happen before `started_at`; setup cost is therefore excluded from the send and latency measurements.

The report also distinguishes messages that returned a rejected ACK but were subsequently observed in realtime or History. That outcome indicates an ambiguous publish result, such as a publisher-confirm timeout after RabbitMQ accepted the message, and fails the correctness check independently of the configured rejection threshold.

The pull-request Kind workflow runs the small smoke profile after the existing Kubernetes E2E. The larger post-optimization suite captures steady-state throughput, hot-room fan-out, idle connection capacity, environment evidence, and Prometheus range queries:

```bash
make observability-install
make kind-load-baseline
```

Raw artifacts are written below `artifacts/load/` and intentionally ignored by Git. Curated conclusions belong in [performance/chat-baseline.md](performance/chat-baseline.md).

Long suites can set `ORION_LOAD_CONTINUE_ON_FAILURE=true` to preserve a failed profile, wait for the Persistence Queue to drain, and continue with later scenarios. `ORION_FANOUT_RATE` controls the aggregate Chat rate used by Fan-out profiles.

After deploying the candidate build, `make kind-capacity` runs the complete capacity map: Persistence concurrency 4/8 A/B, throughput, Fan-out connection and rate scaling, idle connection limits, and a 50 msg/s burst. It restores the original Persistence concurrency when finished. Capacity failures are retained as evidence and do not prevent later suites from running.

## Measurement boundary

For anomaly diagnosis, run `make kind-capacity CAPACITY_PHASE=diagnostics`. It repeats concurrency-4 rates 25/50 and concurrency-8 rate 40 three times each, retaining profile exit codes, errors, API logs, and Admission metrics. Run `make kind-capacity CAPACITY_PHASE=mixed` for concurrency-8 profiles with 1000 total connections and 20 sending connections: steady 25 msg/s, then 10 msg/s background with a middle 50 msg/s burst lasting 30 seconds. Both commands restore the original concurrency configuration on exit.

The baseline script resolves the two ready API Pods, opens one Port-forward to each Pod, and assigns WebSocket connections round-robin across the two addresses. It therefore measures an explicit 50/50 two-replica application workload instead of relying on `kubectl port-forward service/...`, which normally selects one backend Pod for the lifetime of the tunnel.

This still does not measure Kubernetes Service, Ingress, cloud load balancer, or production network capacity. The two `kubectl port-forward` processes and the local load-generator host remain part of the test path and may become the limiting component. If client-observed latency rises while both API Pods remain unsaturated and server-side latency stays flat, repeat the experiment through NodePort or Ingress before attributing the limit to Orion.

## Reading the evidence

Start with `summary.md`; it separates client-visible correctness and latency from infrastructure signals including Publish Confirm, persistence lag, queue depth, database waits, failures, CPU, memory, and throttling. Each profile directory then contains:

```text
report.json       exact load parameters and client-observed results
metrics/*.json    raw Prometheus query-range responses
```

The raw Prometheus files are audit evidence and input for later analysis, not the primary human interface. In normal operation Prometheus retains time series centrally, Grafana renders dashboards, and alerts evaluate selected queries continuously. Access the local dashboards with the commands in [deploy/k8s/observability/README.md](../deploy/k8s/observability/README.md).

Use symptoms together rather than interpreting one metric in isolation:

| Symptom | Supporting signal | Likely boundary |
| --- | --- | --- |
| ACK latency and Publish Confirm latency rise together | API CPU remains below its limit | RabbitMQ confirm, disk, or Publisher capacity |
| ACK latency rises with CPU throttling | Container CPU approaches its limit | API CPU limit or CPU-heavy application path |
| Broadcast latency rises while ACK stays stable | Slow-client removals or API CPU rises | Room fan-out or WebSocket write path |
| Persistence lag and Persistence Queue depth rise | Publish and broadcast remain stable | Consumer or MySQL write path |
| Persistence throughput plateaus and processing p95 is high | Consumer loops are sequential while MySQL is not saturated | Consumer concurrency boundary |
| SQL wait rate rises and in-use connections reach the pool maximum | Persistence and HTTP latency rise | MySQL pool or slow query bottleneck |
| Client latency rises while server metrics remain flat | Port-forward/load-generator CPU or network is saturated | Test harness boundary |

Short runs validate plumbing but are not performance evidence. Prometheus scrapes every 15 seconds and histogram rates use a one-minute window, so formal profiles run long enough to contain multiple scrapes and leave a settling interval between profiles.
