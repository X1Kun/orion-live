# Chat performance baseline

This report records Orion's post-Publisher-pool performance on the documented three-node Kind environment. It separates correctness smoke evidence from capacity evidence and does not present local laptop results as production sizing guidance.

## Environment

Complete machine, Git, Pod placement, and resource configuration evidence is captured by `scripts/load/kind-baseline.sh` in `artifacts/load/<run>/environment.txt`. Summarize the retained run here after executing the suite:

| Property | Value |
| --- | --- |
| Git revision | Pending measurement |
| Host CPU and memory | Pending measurement |
| Kind topology | 1 control-plane and 2 workers |
| API replicas | 2 |
| Chat Publisher lanes | 8 per API Pod |
| Traffic entry | One Port-forward per API Pod; connections assigned 50/50 |
| Test duration per profile | 60 seconds |

## Method

The suite performs one unmeasured warm-up and then executes three profile groups. Every Chat profile requires accepted acknowledgements, exact realtime fan-out, eventual single persistence, and no ambiguous rejected outcome. Prometheus range-query responses are stored beside each load report.

The application capacity result excludes Service/Ingress load balancing: traffic is routed directly and evenly to two named Pods. It includes local load-generator and `kubectl port-forward` overhead and therefore cannot be presented as production Ingress capacity.

```bash
make kind-create kind-deploy
make observability-install
make kind-load-baseline
```

Use a shorter non-evidentiary run to validate the scripts:

```bash
ORION_LOAD_DURATION=10s \
ORION_LOAD_COOLDOWN=1s \
ORION_THROUGHPUT_RATES='1 5' \
ORION_FANOUT_CONNECTIONS='10 50' \
ORION_CONNECTION_COUNTS='100 300' \
  make kind-load-baseline
```

## Steady-state throughput

Twenty connected users share one LiveSession. Aggregate Chat rate increases without crossing the configured per-user or room admission limits.

| Rate | Accepted | Rejected | ACK p95 | Broadcast p95 | Publish p95 | Persistence lag p95 | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 msg/s | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| 5 msg/s | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| 10 msg/s | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| 25 msg/s | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| 50 msg/s | Pending | Pending | Pending | Pending | Pending | Pending | Pending |

## Hot-room fan-out

The message rate remains 1 msg/s while the number of clients in one process-distributed room increases.

| Connections | Expected deliveries/s | Delivered | Broadcast p95 | Slow-client removals | Result |
| ---: | ---: | ---: | ---: | ---: | --- |
| 10 | 10 | Pending | Pending | Pending | Pending |
| 50 | 50 | Pending | Pending | Pending | Pending |
| 100 | 100 | Pending | Pending | Pending | Pending |
| 300 | 300 | Pending | Pending | Pending | Pending |

## Idle WebSocket capacity

Connections are held for the configured duration without sending Chat messages. Any unexpected disconnect fails the profile.

| Connections | Setup result | Held for | API CPU | API memory | CPU throttling | Result |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 100 | Pending | Pending | Pending | Pending | Pending | Pending |
| 300 | Pending | Pending | Pending | Pending | Pending | Pending |
| 600 | Pending | Pending | Pending | Pending | Pending | Pending |
| 1000 | Pending | Pending | Pending | Pending | Pending | Pending |

## Findings

Pending execution. Record the first latency knee, the first correctness failure if one occurs, whether RabbitMQ or MySQL queues accumulate, and whether CPU throttling explains the result. Optimization belongs in a subsequent PR so this report continues to describe one immutable revision.
