# Publisher concurrency experiments

```bash
make kind-capacity CAPACITY_PHASE=publisher
```

The suite fixes each API Pod at one CPU and Persistence concurrency eight, then tests Publisher concurrency 8, 16, 32 and 64 per Pod. Each candidate runs three profiles for 90 seconds. Three rounds alternate forward and reverse candidate order; all original configuration values are restored on exit. Failed profiles and incomplete metric collection remain visible. Do not run other deployment or load workflows concurrently.

| Profile | Total connections | Sending connections | Aggregate Chat rate |
| --- | ---: | ---: | ---: |
| Light | 20 | 20 | 25 msg/s |
| Mixed fan-out | 1000 | 20 | 50 msg/s |
| Many senders | 1000 | 128 | 80 msg/s |

Twenty senders evenly split across two Pods allow at most ten simultaneous business operations per Pod, so lane counts above sixteen may be idle in the first two profiles. The third profile permits sixty-four active sending connections per Pod; its increased workload can also expose API CPU or Persistence limits. It is exploratory and is not assumed to pass. Eighty msg/s remains below the configured 100 msg/s room rate, but recovery after queueing can still cause legitimate rate-limit rejections.

Use `ORION_PUBLISHER_REPEATS=1 ORION_PUBLISHER_DURATION=60s` for an initial screen. `ORION_PUBLISHER_CONCURRENCIES='8 16'` narrows a repeat experiment. Keep matched workload and resource parameters when comparing candidates.

Compare all repeats rather than choosing the best run. A useful candidate reduces lane waiting and client ACK latency without increasing rejection, CPU throttling, persistence lag, or disk waits. More Channels increase broker/client state and outstanding durable work; they do not increase disk or CPU capacity. If improvement stops at sixteen, retaining thirty-two or sixty-four needs a separate workload justification.

Per-connection application processing remains sequential, but the system does not provide global Chat order across connections, Pods or AMQP Channels. RabbitMQ realtime arrival order, AcceptedAt order, and MySQL commit/cursor order are separate. Concurrent Persistence already allows commits to finish out of order. Clients use message identity for deduplication and persisted cursors for pagination, not a claim of globally ordered publication. This suite checks identity/count correctness and latency, not a global-order guarantee.
