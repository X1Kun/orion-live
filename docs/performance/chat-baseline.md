# Chat performance observations

This report summarizes measured local workloads and failure boundaries from Orion's performance work. Representative results are selected to show what completed, not to claim repeatable maximum capacity. Raw logs remain in local `artifacts/load/`; [curated JSON snapshots](evidence/representative-runs.json) are committed so the numerical claims can be inspected.

## Environment and method

- Host: Intel i7-9750H, 6 physical cores / 12 threads, approximately 32 GiB RAM.
- Cluster: one Kind control-plane and two workers on the same host, with two API Pods and single-replica MySQL, Redis, and RabbitMQ infrastructure.
- Routing: a separate port-forward to each API Pod, with WebSocket connections assigned round-robin. This exercises cross-Pod RabbitMQ realtime delivery but bypasses Service/Ingress balancing.
- Evidence: client message identities/counts and latency percentiles; cursor-based History verification; Prometheus time series; API logs; host/Redis diagnostics in later runs.
- Configuration varies between experiments. Persistence Workers, Publisher lanes, CPU limits, and measurement duration must be read with each run. Stored `runtime.git_revision` identifies the load-harness build; many experiments used an uncommitted working tree, so it is not an exact deployed-server artifact identifier.

Client p95 covers the whole measured run. Infrastructure p95 summaries are peaks of rolling histogram estimates, usually over the latter half of the sending interval. They are not the same statistic, and quantiles cannot be added or subtracted to calculate stage latency. Short bursts and interrupted tests have particularly limited steady-state metric coverage.

## Representative results and provenance

| Source run and profile | Configuration / workload | Observation |
| --- | --- | --- |
| `20261005T162518Z/throughput-12mps` | Two sequential Consumers; 20 connections; 12 msg/s for 120 s | 1,440 messages completed in 139.8 s. Queue growth and drain time established a processing boundary around 10 msg/s in that environment |
| `full-20261005T173730Z/throughput-25mps` | 4 Persistence Workers per Pod; 20 connections; 25 msg/s for 120 s | 3,000 accepted/persisted, 60,000 deliveries, ACK p95 326.7 ms; completed in 121.3 s |
| `capacity-20261006T040007Z/persistence-c8/throughput-50mps` | 8 Persistence Workers per Pod; 20 connections; 50 msg/s for 120 s | 6,000 accepted/persisted, 120,000 deliveries, ACK p95 315.4 ms; completed in 121.9 s |
| `capacity-20261006T040007Z/fanout-connections/fanout-1000c-5mps` | 1,000 connections; 5 msg/s for 120 s | 600 persisted, 600,000 deliveries, broadcast p95 51.2 ms |
| `capacity-20261006T040007Z/idle-connections/connections-2000c` | 2,000 connections; no Chat sends; 120 s hold | Connection hold completed. This is the configured admission ceiling, not an active-message benchmark |
| `capacity-20261008T061504Z/mixed-cpu-1/mixed-hotroom` | 1 CPU per Pod; 1,000 connections / 20 senders; 50 msg/s for 120 s | 6,000 persisted, 6,000,000 deliveries, ACK/broadcast p95 1.53/1.26 s. Counts passed; latency targets did not |

Legacy snapshots lack explicit status and History-check metadata. Counts and successful completion were interpreted with the retained run evidence; null legacy fields must not be treated as new-format pass metadata.

## Failures that constrain the conclusions

- `capacity-20261008T061504Z/mixed-cpu-500m/mixed-hotroom`: stopped at about 54.5 s with an explicit 1013 inbound-queue-full closure. History verification did not run; the stored zero persistence count is not proof of zero writes. CPU throttling increased substantially in this configuration.
- `capacity-20261009T055014Z/fixed-p16-c8/mixed-25`: all 2,990 accepted messages persisted and reached 300 clients, but 10 of 3,000 sends were rejected. API logs identified Admission deadlines; correctness failed the zero-rejection requirement.
- Other repeats found Publisher-timeout messages that later appeared in realtime and History. Timeout is an uncertain publish result, and the existing rejected-ACK representation needs further work.
- Several profiles failed at account registration before load began. Such failures constrain test reliability and must not be reported as message-capacity results.
- Some Redis deadline incidents coincided with high host I/O wait and disk write latency. Root-level process sampling established MySQL as a substantial writer, but did not isolate it as the sole cause. Duplicate full-process sampling also added I/O and should be reduced in future investigations.

## Optimizations and validation

The original serialized Publisher was replaced by independent bounded lanes, preserving mandatory-return/Confirm ownership. The sequential Persistence path gained configurable bounded Workers, preserving transactional Inbox/business-key idempotency and individual acknowledgements. WebSocket reads were separated from ordered Chat processing after a controlled reproduction demonstrated heartbeat starvation. Bounded inbound overflow now has an explicit 1013 response.

Validation includes unit/race tests for concurrency and lifecycle, Publisher reconnect/mandatory-return integration coverage, and actual Kind load comparisons. Measured improvements are workload- and environment-dependent; these changes do not establish linear scaling with Worker or Channel count.

Queue, Redis pool, and host metrics were added to distinguish business processing, resource waits, and overload. Report generation now distinguishes setup, send, delivery, persistence, and validation failures, with separate metric-collection completeness. This makes failures reviewable instead of hiding them behind successful final counts or zero-value reports.

## Current release boundary

The project has tested idle-connection and fan-out capability, observed concurrency improvements, and documented explicit overload/recovery mechanisms. It has not established a consistently zero-rejection 25 msg/s mixed-load SLO or sustained 50 msg/s production capacity. Proposed latency objectives remain ACK p95 below one second, broadcast p95 below 250 ms, and persistence lag p95 below two seconds; successful completion alone does not verify them.

Multi-room interference, concurrent History traffic, and failure injection under sustained Chat load remain follow-up work. Existing Kind resilience tests verify functional recovery separately. Reproduction commands and diagnostic interpretation are in [load testing](../load-testing.md), [latency investigation](latency-investigation.md), and [focused I/O diagnostics](focused-io-diagnostics.md).
