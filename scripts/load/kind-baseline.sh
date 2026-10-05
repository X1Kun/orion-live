#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cluster_name=${ORION_KIND_CLUSTER:-orion-live}
namespace=orion-live
phase=${1:-all}
duration=${ORION_LOAD_DURATION:-60s}
cooldown=${ORION_LOAD_COOLDOWN:-35s}
scrape_lag=${ORION_LOAD_SCRAPE_LAG:-35s}
output_root=${ORION_LOAD_OUTPUT_DIR:-${project_root}/artifacts/load/$(date -u +%Y%m%dT%H%M%SZ)}

case "${phase}" in
  all|throughput|fanout|connections) ;;
  *)
    echo "usage: $0 [all|throughput|fanout|connections]" >&2
    exit 1
    ;;
esac

kubectl config use-context "kind-${cluster_name}" >/dev/null
kubectl -n "${namespace}" rollout status deployment/orion-api --timeout=2m
command -v jq >/dev/null || { echo "jq is required to summarize baseline results" >&2; exit 1; }
if ! kubectl -n monitoring get service prometheus-operated >/dev/null 2>&1; then
  echo "Prometheus is not installed; run make observability-install first" >&2
  exit 1
fi

mkdir -p "${output_root}"
work_dir=$(mktemp -d)
api_a_forward_pid=
api_b_forward_pid=
prometheus_forward_pid=
cleanup() {
  [[ -z "${api_a_forward_pid}" ]] || kill "${api_a_forward_pid}" >/dev/null 2>&1 || true
  [[ -z "${api_b_forward_pid}" ]] || kill "${api_b_forward_pid}" >/dev/null 2>&1 || true
  [[ -z "${prometheus_forward_pid}" ]] || kill "${prometheus_forward_pid}" >/dev/null 2>&1 || true
  rm -rf "${work_dir}"
}
trap cleanup EXIT

mapfile -t api_pods < <(
  kubectl -n "${namespace}" get pods \
    -l app.kubernetes.io/name=orion-live,app.kubernetes.io/component=api \
    --no-headers | awk '$2 == "1/1" && $3 == "Running" {print $1}' | sort
)
if [[ ${#api_pods[@]} -ne 2 ]]; then
  echo "expected exactly two ready API Pods, found ${#api_pods[@]}" >&2
  exit 1
fi

kubectl -n "${namespace}" port-forward "pod/${api_pods[0]}" 18081:8080 >"${work_dir}/api-a.log" 2>&1 &
api_a_forward_pid=$!
kubectl -n "${namespace}" port-forward "pod/${api_pods[1]}" 18082:8080 >"${work_dir}/api-b.log" 2>&1 &
api_b_forward_pid=$!
kubectl -n monitoring port-forward service/prometheus-operated 19090:9090 >"${work_dir}/prometheus.log" 2>&1 &
prometheus_forward_pid=$!

wait_for_url() {
  local url=$1
  local log_file=$2
  for _ in $(seq 1 60); do
    if curl --connect-timeout 1 --max-time 2 --fail --silent "${url}" >/dev/null; then
      return
    fi
    sleep 1
  done
  echo "endpoint did not become ready: ${url}" >&2
  cat "${log_file}" >&2
  exit 1
}

wait_for_url http://127.0.0.1:18081/readyz "${work_dir}/api-a.log"
wait_for_url http://127.0.0.1:18082/readyz "${work_dir}/api-b.log"
wait_for_url http://127.0.0.1:19090/-/ready "${work_dir}/prometheus.log"

wait_for_query() {
  local query=$1
  local response=""
  for _ in $(seq 1 60); do
    response=$(curl --fail --silent --get --data-urlencode "query=${query}" http://127.0.0.1:19090/api/v1/query)
    if ! grep -q '"result":\[\]' <<<"${response}"; then
      return
    fi
    sleep 1
  done
  echo "Prometheus query returned no data: ${query}" >&2
  exit 1
}

wait_for_empty_persistence_queue() {
  local query='rabbitmq_detailed_queue_messages{queue="orion.interaction.persistence"} == 0'
  local response=""
  for _ in $(seq 1 300); do
    response=$(curl --fail --silent --get --data-urlencode "query=${query}" http://127.0.0.1:19090/api/v1/query)
    if ! grep -q '"result":\[\]' <<<"${response}"; then
      return
    fi
    sleep 1
  done
  echo "Persistence Queue did not drain before the next profile" >&2
  exit 1
}

wait_for_query 'count(up{namespace="orion-live",service="orion-api"} == 1) == 2'
wait_for_query 'rabbitmq_detailed_queue_messages{queue="orion.interaction.persistence"}'

binary="${work_dir}/chatload"
go build -o "${binary}" ./cmd/chatload

{
  date -u +"captured_at=%Y-%m-%dT%H:%M:%SZ"
  git rev-parse HEAD
  git status --short
  uname -a
  lscpu
  free -h
  kubectl get nodes -o wide
  kubectl -n "${namespace}" get pods -o wide
  kubectl -n "${namespace}" get deployment/orion-api statefulset/mysql statefulset/redis statefulset/rabbitmq -o yaml
} >"${output_root}/environment.txt"

common_args=(
  -base-url http://127.0.0.1:18081
  -connection-base-urls http://127.0.0.1:18081,http://127.0.0.1:18082
)

metric_names=(
  publish_p95
  publish_failures
  persistence_lag_p95
  persistence_processing_p95
  persistence_throughput
  persistence_queue_depth
  database_wait_rate
  websocket_connections
  api_cpu
  api_memory
  api_cpu_throttling
  mysql_cpu
  mysql_memory
  slow_client_removals
)
metric_queries=(
  'histogram_quantile(0.95, sum by (le) (rate(orion_rabbitmq_publish_duration_seconds_bucket{event_type="chat.message.accepted"}[1m])))'
  'sum(rate(orion_rabbitmq_publish_total{event_type="chat.message.accepted",result!="confirmed"}[1m]))'
  'histogram_quantile(0.95, sum by (le) (rate(orion_chat_persistence_lag_seconds_bucket[1m])))'
  'histogram_quantile(0.95, sum by (le) (rate(orion_persistence_processing_duration_seconds_bucket[1m])))'
  'sum(rate(orion_persistence_events_total{result="persisted"}[1m]))'
  'sum(rabbitmq_detailed_queue_messages{queue="orion.interaction.persistence"})'
  'sum(rate(orion_db_wait_total[1m]))'
  'sum(orion_websocket_connections)'
  'sum(rate(container_cpu_usage_seconds_total{namespace="orion-live",pod=~"orion-api-.*",container="api"}[1m]))'
  'sum(container_memory_working_set_bytes{namespace="orion-live",pod=~"orion-api-.*",container="api"})'
  'sum(rate(container_cpu_cfs_throttled_periods_total{namespace="orion-live",pod=~"orion-api-.*",container="api"}[1m])) / clamp_min(sum(rate(container_cpu_cfs_periods_total{namespace="orion-live",pod=~"orion-api-.*",container="api"}[1m])), 1)'
  'sum(rate(container_cpu_usage_seconds_total{namespace="orion-live",pod="mysql-0",container="mysql"}[1m]))'
  'sum(container_memory_working_set_bytes{namespace="orion-live",pod="mysql-0",container="mysql"})'
  'sum(increase(orion_websocket_slow_client_removals_total[1m]))'
)

capture_metrics() {
  local profile_dir=$1
  local start=$2
  local end=$3
  mkdir -p "${profile_dir}/metrics"
  for index in "${!metric_names[@]}"; do
    curl --fail --silent --get \
      --data-urlencode "query=${metric_queries[index]}" \
      --data-urlencode "start=${start}" \
      --data-urlencode "end=${end}" \
      --data-urlencode "step=5" \
      http://127.0.0.1:19090/api/v1/query_range \
      >"${profile_dir}/metrics/${metric_names[index]}.json"
  done
}

write_summary() {
  "${project_root}/scripts/load/summarize.sh" "${output_root}" >"${output_root}/summary.md"
}

run_profile() {
  local name=$1
  shift
  local profile_dir="${output_root}/${name}"
  mkdir -p "${profile_dir}"
  local start
  local end
  local status=0
  wait_for_empty_persistence_queue
  echo "running ${name}"
  "${binary}" "${common_args[@]}" -output "${profile_dir}/report.json" "$@" >/dev/null || status=$?
  if [[ -s "${profile_dir}/report.json" ]]; then
    start=$(date -u -d "$(jq -r '.started_at' "${profile_dir}/report.json")" +%s)
  else
    start=$(date -u +%s)
  fi
  sleep "${scrape_lag}"
  end=$(date -u +%s)
  capture_metrics "${profile_dir}" "${start}" "${end}"
  if (( status != 0 )); then
    write_summary
    cat "${output_root}/summary.md"
    echo "profile failed: ${name}; inspect ${profile_dir}" >&2
    exit "${status}"
  fi
  sleep "${cooldown}"
}

echo "warming up the Chat pipeline"
wait_for_empty_persistence_queue
"${binary}" "${common_args[@]}" -connections 10 -message-rate 1 -duration 10s >/dev/null
sleep "${scrape_lag}"
wait_for_query 'orion_rabbitmq_publish_duration_seconds_count{event_type="chat.message.accepted"}'
sleep "${cooldown}"

if [[ "${phase}" == all || "${phase}" == throughput ]]; then
  for rate in ${ORION_THROUGHPUT_RATES:-1 5 10 25 50}; do
    run_profile "throughput-${rate}mps" -mode chat -connections 20 -message-rate "${rate}" -duration "${duration}" -drain-timeout 1m -history-timeout 1m
  done
fi

if [[ "${phase}" == all || "${phase}" == fanout ]]; then
  for connections in ${ORION_FANOUT_CONNECTIONS:-10 50 100 300}; do
    run_profile "fanout-${connections}c" -mode chat -connections "${connections}" -message-rate 1 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
  done
fi

if [[ "${phase}" == all || "${phase}" == connections ]]; then
  for connections in ${ORION_CONNECTION_COUNTS:-100 300 600 1000}; do
    run_profile "connections-${connections}c" -mode connections -connections "${connections}" -connections-per-user 3 -duration "${duration}"
  done
fi

write_summary
cat "${output_root}/summary.md"
echo "baseline artifacts: ${output_root}"
