# Stability diagnostics

Run on the already deployed candidate with monitoring available:

```bash
make kind-capacity CAPACITY_PHASE=stability
```

The suite holds Persistence concurrency at eight and repeats 25 msg/s three times. It then compares three mixed profiles at 500m and one CPU per API Pod: 1,000 connections with 20 senders at 25 msg/s for 10 minutes, the same connections at 50 msg/s for two minutes, and a 60-second background at 10 msg/s followed by a 30-second burst at 50 msg/s and another 60-second background. These are single-room profiles; multi-room interference, concurrent History traffic, and failure-under-load are separate coverage still to be implemented. Both CPU limit and concurrency are restored on exit. Do not run another deployment or capacity suite concurrently because they modify the same resources and use the same local ports.

Each suite captures UTC host `vmstat` output; `iostat` output is captured when installed. Missing disk sampling is explicitly recorded. Install the distribution's `sysstat` package before running if disk wait evidence is needed. Node-exporter metrics in Kind are supporting containerized-node evidence, not a substitute for host disk sampling.

When available, host `pidstat` records process CPU, memory, and I/O once per second. `processes-before.txt` maps PIDs to process names without command-line arguments. Per-process I/O visibility depends on host permissions; absent or zero process counters do not exclude kernel writeback or inaccessible processes. Correlate PID activity with the same UTC timestamps in `iostat` and the API errors.

Each profile saves `metrics-collection.json` independently of its load report. Failed queries are retried once, with the final raw response and curl error retained. They mark collection incomplete but do not discard the load report or stop remaining queries. Empty series are listed separately from failed queries. Summary is written after every profile, including setup failure; setup failures use the real attempt time for metric queries rather than the zero measurement timestamp. Overall suites exit non-zero for incomplete metric collection as well as test failures.

Profiles retain before/after Redis INFO, SLOWLOG, and LATENCY snapshots, API logs, Pod state/events, and per-Pod CPU, throttling, Admission, lane acquisition, Publish, and frame processing curves. Redis latency monitoring is not enabled dynamically; an empty LATENCY response is not proof of zero Redis delay. Snapshot counters require differences between before/after, while SLOWLOG may contain older entries and must be filtered by timestamp. No credentials are written by these commands.

WebSocket queue-wait histograms measure enqueue to dequeue using in-process monotonic timestamps. Inbound wait excludes Admission/Publish work; outbound wait includes ACK and realtime frames and excludes socket-write time. Discarded frames do not produce wait samples. Histogram quantiles cannot be added or subtracted to reconstruct one message's latency.

The Redis collector exposes client-local total/idle connections, configured pool size, hits, misses, pool-wait timeouts, and stale connections without performing Redis network I/O on scrape. Misses indicate no idle connection was found, not necessarily pool exhaustion. Pool timeout counters do not include every context cancellation or Redis command timeout. Diagnose pool pressure using these signals together with Admission latency and errors; do not increase pool size on misses alone.

Interpret evidence in temporal order:

| Signal | Interpretation and next check |
| --- | --- |
| Admission spikes on both Pods together | Correlate Redis snapshots with host scheduling, swap, and disk waits. The timing alone does not identify the underlying cause. |
| Lane waiting rises before ACK latency | Publisher capacity or delayed confirms are creating upstream waiting. Compare Publish curves and broker evidence. |
| ACK rises beyond frame processing | Inbound queue, network/tunnel, or client observation adds time outside application processing. Quantiles cannot be subtracted to calculate queue time. |
| Mixed burst improves at one CPU with less throttling | CPU quota is contributing. One A/B run remains provisional; repeat representative profiles before final sizing. |
| Persistence throughput falls while CPU stays low | Transaction/storage/network waits remain possible. Low CPU alone does not exclude MySQL as a contributor. |
| Host swap or disk wait coincides with multiple services slowing | Treat host interference as a candidate cause and repeat with controlled host load. |

The suite does not claim to resolve historical incidents. Keep failed runs and quantify repeat-to-repeat differences before selecting final defaults. Two direct Pod tunnels remain part of the test path; a later NodePort/Ingress verification is needed for deployment-level conclusions.
