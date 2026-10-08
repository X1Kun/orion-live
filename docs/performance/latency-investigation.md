# Latency investigation

Evidence directories: `full-20261005T173730Z`, `capacity-20261006T040007Z`, `capacity-20261006T070540Z`, `capacity-20261006T075039Z`, and `capacity-20261008T033543Z`. Raw artifacts remain local; these findings summarize retained observations, not production capacity guarantees.

## Confirmed findings

- Eleven rejected sends in concurrency-4 throughput-25 run 2 were Redis Admission deadline failures. Both API Pods logged failures at 07:20:20.674–07:20:20.675 UTC. This establishes a shared dependency-path incident, not its underlying network, Redis, or host cause.
- Two interrupted sends occurred about 63 seconds into measurement and surfaced as `websocket: close sent`. Their reports stopped before History verification, so persisted=0 does not establish zero database writes.
- Before the fix, the WebSocket reader executed Chat acceptance synchronously. A local controlled reproduction used a healthy client that continuously read and replied to Ping, a 150ms Pong timeout, and 400ms application processing. The server timed out its next read and closed the connection. This proves the heartbeat-starvation mechanism, but does not prove it caused the historical incidents.
- Mixed steady traffic completed 3,000 messages and 3,000,000 realtime deliveries. Mixed burst completed 2,700 messages and 2,700,000 deliveries. Burst API throttled-period ratio peaked around 53%, with ACK/broadcast p95 around 2.8/2.6 seconds. CPU quota pressure is a supported hypothesis for this workload.

## Concurrency and backpressure evidence

- The original two sequential Persistence Consumers plateaued around 10 messages/s. Four Workers per Pod reached approximately 26 messages/s in one run. This improvement was not linear and subsequent runs varied; no universal safe capacity is claimed.
- In `capacity-20261006T040007Z`, eight Workers per Pod persisted all 6,000 accepted messages at 50 messages/s in approximately 121.9 seconds. Four Workers at the same rate did not complete History verification within the allowed window. Different run order and host conditions limit causal precision, but the observed concurrency improvement is supported.
- After reader/processor separation, `capacity-20261008T033543Z` recorded a definite `1013: WebSocket inbound queue is full` during concurrency-4 throughput-50. The processor remained sequential per connection and the bounded queue contained at most 16 pending frames; overload now has an explicit observed failure reason.
- That interrupted profile did not perform History verification. Its legacy persisted=0 field is an unmeasured value, not proof of zero writes. Persistence metrics recorded ongoing writes. Several later concurrency-4 repetitions failed during account registration before measurement.
- All three concurrency-8 throughput-40 repetitions completed 4,800 accepted messages. ACK p95 varied from approximately 0.94 to 1.94 seconds; Persistence rolling-p95 peaks varied from approximately 2.36 to 9.63 seconds. The first two runs accumulated roughly 220 queued messages and drained about five seconds after sending ended. Successful completion does not imply that latency objectives were met.

## Tradeoffs and open work

Persistence Workers overlap transaction waits at the cost of more concurrent database work. The per-connection inbound queue preserves message order and heartbeat reads, but adds bounded waiting and closes overloaded connections with 1013. Disconnect discards pending frames and may leave an in-flight publication uncertain; clients must reconcile unconfirmed message IDs through History.

Simultaneous Redis Admission timeouts, registration HTTP timeouts, and run-to-run transaction latency remain unresolved. Low MySQL CPU and zero SQL-pool waits do not exclude storage or shared-host delays. Inbound capacity, Worker concurrency, or timeouts should not be raised solely to make failing profiles pass.

## Remaining uncertainty

CPU and SQL-pool counters do not measure host scheduling pauses, disk commit latency, Redis server latency, or tunnel delays. Historical artifacts cannot identify which caused the simultaneous Admission timeouts. The current evidence does not establish a global application-lock bottleneck.

## Added diagnostic evidence

- Unexpected WebSocket reader/writer errors are visible at warning level.
- Publisher acquisition duration separates pool waiting from total publication time.
- WebSocket frame processing duration measures time spent between socket reads.
- Load send failures include the first observed reader error when available.
- Baseline capture includes both new duration histograms alongside Admission metrics and API logs.

Repeat one failing profile with host `vmstat` and disk `iostat` sampling, then correlate the first error by timestamp. The heartbeat fix now keeps reading control frames while processing Chat through a bounded per-connection queue, preserving order and closing with 1013 on inbound overflow. Regression tests cover slow processing across heartbeat deadlines, order, overflow cancellation, and shutdown. Shared-dependency latency remains a separate investigation.
