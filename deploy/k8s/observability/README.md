# Orion observability

This optional layer uses the pinned `kube-prometheus-stack` Helm chart to install Prometheus Operator, Prometheus, Grafana, Alertmanager, and node-exporter. Orion owns only its ServiceMonitors, alert rules, dashboard, and chart values.

The separate `helm-values-kind.yaml` disables kube-state-metrics because its only official image is served from `registry.k8s.io`, which is not reachable from the validated local environment, and pins Grafana to the labeled observability worker. The common values retain standard kube-state-metrics and do not contain Kind node selectors, so external clusters keep Kubernetes object-state metrics.

The application and development infrastructure must already be running:

```bash
make kind-create kind-deploy
make observability-install
make observability-verify
```

After generating traffic, verify the complete Chat metric path:

```bash
make kind-smoke
ORION_VERIFY_PIPELINE_METRICS=true make observability-verify
```

The chart and its Kind Grafana preload are pinned together:

```text
kube-prometheus-stack: 91.8.2
Grafana: 13.2.3-distroless
```

Access Prometheus and Grafana locally:

```bash
kubectl -n monitoring port-forward service/prometheus-operated 9090:9090
kubectl -n monitoring port-forward service/orion-monitoring-grafana 3000:80
```

Read the generated Grafana admin password:

```bash
kubectl -n monitoring get secret orion-monitoring-grafana \
  -o jsonpath='{.data.admin-password}' | base64 -d
echo
```

The dashboard covers HTTP traffic and latency, active WebSockets, RabbitMQ Publish Confirm latency, Chat admission, persistence outcomes and lag, SQL pool use, Outbox state, and RabbitMQ queue depth. The RabbitMQ ServiceMonitor keeps the default aggregated endpoint and adds a filtered `/metrics/detailed` scrape for only Orion queue coarse metrics; it does not enable the expensive all-object endpoint. Prometheus rules cover target readiness, publication failures, unavailable admission, failed or stalled Outbox events, and Persistence DLQ depth.

Queue diagnostics panels show per-Pod inbound/outbound wait p95 and local Redis connection-pool statistics. Outbound wait combines ACK and realtime frames. Redis misses are idle-connection misses, while pool timeout counters cover the client's pool wait timer rather than all Admission deadline errors. Use these with Admission latency and logs before tuning Redis pool size.

Delete the optional stack with:

```bash
make observability-delete
```

This development stack does not configure external alert notification receivers or long-term metric storage. Those are environment-specific production concerns.
