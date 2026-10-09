# Focused shared-I/O diagnostics

```bash
make kind-capacity CAPACITY_PHASE=focused
```

This short follow-up fixes each API Pod at one CPU, Publisher concurrency sixteen, and Persistence concurrency eight. It runs five profiles, each for two minutes, with twenty sending connections throughout:

| Order | Connections | Aggregate rate | Purpose |
| ---: | ---: | ---: | --- |
| 1 | 20 | 10 msg/s | Lower-load control before the candidate |
| 2 | 20 | 25 msg/s | First candidate observation |
| 3 | 300 | 25 msg/s | More recipients with the same senders and rate |
| 4 | 20 | 25 msg/s | Repeat candidate after mixed traffic |
| 5 | 20 | 10 msg/s | Lower-load control after the candidate |

The configuration stays fixed within the suite, and original values are restored on exit. Queue drainage and scrape settling apply between profiles. Host vmstat/iostat/pidstat, Redis snapshots, API logs and per-Pod metrics are retained. These profiles are an exploratory comparison; they do not randomize host conditions or prove a root cause on their own.

Compare the two 25 msg/s profiles and the two controls. If low-load controls also deteriorate alongside disk wait, investigate shared host I/O rather than treating lane count as the only cause. Compare the mixed profile against the two twenty-connection candidates for recipient-count effects. For a latency incident, locate the first Admission/Publish change, inspect inbound waiting afterward, then correlate the same UTC interval with disk and process I/O. Process I/O visibility depends on host permissions and kernel writeback attribution.

Formal client criteria remain separate from completion: zero rejected/ambiguous outcomes, complete realtime/history counts, ACK p95 below one second, broadcast p95 below 250ms, and no sustained persistence backlog. The script checks correctness; latency objectives still require analysis.

Use `ORION_FOCUSED_DURATION=60s` for a shorter screen. The one-minute metric windows make this less reliable for steady-state analysis. `ORION_FOCUSED_AUDIENCE=1000` restores the larger recipient workload when the shared-I/O question is understood. Avoid concurrent deployments, other load suites, and unrelated heavy work; record unavoidable background activity rather than deleting failed evidence.
