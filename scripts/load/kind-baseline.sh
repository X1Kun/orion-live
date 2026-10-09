#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cluster_name=${ORION_KIND_CLUSTER:-orion-live}
namespace=orion-live
phase=${1:-all}
duration=${ORION_LOAD_DURATION:-60s}
cooldown=${ORION_LOAD_COOLDOWN:-35s}
scrape_lag=${ORION_LOAD_SCRAPE_LAG:-35s}
continue_on_failure=${ORION_LOAD_CONTINUE_ON_FAILURE:-false}
fanout_rate=${ORION_FANOUT_RATE:-1}
fanout_rates=${ORION_FANOUT_RATES:-${fanout_rate}}
output_root=${ORION_LOAD_OUTPUT_DIR:-${project_root}/artifacts/load/$(date -u +%Y%m%dT%H%M%SZ)}
failed_profiles=()
failed_collections=()
source "${project_root}/scripts/load/metrics.sh"

case "${phase}" in
  all|throughput|fanout|connections|mixed|publisher|focused) ;;
  *)
    echo "usage: $0 [all|throughput|fanout|connections|mixed|publisher|focused]" >&2
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
sampler_pids=()
cleanup() {
  for pid in "${sampler_pids[@]}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
  [[ -z "${api_a_forward_pid}" ]] || kill "${api_a_forward_pid}" >/dev/null 2>&1 || true
  [[ -z "${api_b_forward_pid}" ]] || kill "${api_b_forward_pid}" >/dev/null 2>&1 || true
  [[ -z "${prometheus_forward_pid}" ]] || kill "${prometheus_forward_pid}" >/dev/null 2>&1 || true
  rm -rf "${work_dir}"
}
trap cleanup EXIT

if [[ "${ORION_LOAD_DIAGNOSTICS:-false}" == true ]]; then
  mkdir -p "${output_root}/diagnostics"
  date -u +%Y-%m-%dT%H:%M:%SZ >"${output_root}/diagnostics/started-at.txt"
  if command -v vmstat >/dev/null; then
    TZ=UTC vmstat -t 1 >"${output_root}/diagnostics/vmstat.txt" 2>&1 &
    sampler_pids+=("$!")
  fi
  if command -v iostat >/dev/null; then
    TZ=UTC iostat -xz -t 1 >"${output_root}/diagnostics/iostat.txt" 2>&1 &
    sampler_pids+=("$!")
  else
    echo "iostat unavailable; disk latency capture requires sysstat" >"${output_root}/diagnostics/iostat-unavailable.txt"
  fi
  if command -v pidstat >/dev/null; then
    TZ=UTC pidstat -d -r -u -h -p ALL 1 >"${output_root}/diagnostics/pidstat.txt" 2>&1 &
    sampler_pids+=("$!")
  else
    echo "pidstat unavailable; process I/O attribution requires sysstat" >"${output_root}/diagnostics/pidstat-unavailable.txt"
  fi
  ps -eo pid,ppid,comm >"${output_root}/diagnostics/processes-before.txt"
fi

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
    response=$(curl --fail --silent --max-time 5 --get --data-urlencode "query=${query}" http://127.0.0.1:19090/api/v1/query) || response=''
    if jq -e '.status == "success" and (.data.result | length > 0)' <<<"${response}" >/dev/null; then
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
    response=$(curl --fail --silent --max-time 5 --get --data-urlencode "query=${query}" http://127.0.0.1:19090/api/v1/query) || response=''
    if jq -e '.status == "success" and (.data.result | length > 0) and all(.data.result[]; .value[1] == "0")' <<<"${response}" >/dev/null; then
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
  kubectl -n "${namespace}" get configmap/orion-runtime-config -o yaml
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
  admission_unavailable
  admission_p95
  publisher_acquire_p95
  frame_processing_p95
  api_cpu_by_pod
  api_throttling_by_pod
  admission_by_pod
  publisher_acquire_by_pod
  publish_by_pod
  frame_processing_by_pod
  node_io_wait
  scrape_targets
  inbound_wait_p95
  outbound_wait_p95
  redis_pool_connections
  redis_pool_size
  redis_pool_timeouts
  redis_pool_misses
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
  'sum(rate(orion_chat_admission_total{result="unavailable"}[1m]))'
  'histogram_quantile(0.95, sum by (le) (rate(orion_chat_admission_duration_seconds_bucket[1m])))'
  'histogram_quantile(0.95, sum by (le) (rate(orion_rabbitmq_publish_acquire_duration_seconds_bucket[1m])))'
  'histogram_quantile(0.95, sum by (le) (rate(orion_websocket_frame_processing_duration_seconds_bucket[1m])))'
  'sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="orion-live",pod=~"orion-api-.*",container="api"}[1m]))'
  'sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{namespace="orion-live",pod=~"orion-api-.*",container="api"}[1m])) / clamp_min(sum by (pod) (rate(container_cpu_cfs_periods_total{namespace="orion-live",pod=~"orion-api-.*",container="api"}[1m])), 0.001)'
  'histogram_quantile(0.95, sum by (le,pod) (rate(orion_chat_admission_duration_seconds_bucket[1m])))'
  'histogram_quantile(0.95, sum by (le,pod) (rate(orion_rabbitmq_publish_acquire_duration_seconds_bucket[1m])))'
  'histogram_quantile(0.95, sum by (le,pod) (rate(orion_rabbitmq_publish_duration_seconds_bucket{event_type="chat.message.accepted"}[1m])))'
  'histogram_quantile(0.95, sum by (le,pod) (rate(orion_websocket_frame_processing_duration_seconds_bucket[1m])))'
  'avg by (instance) (rate(node_cpu_seconds_total{mode="iowait"}[1m]))'
  'up{namespace="orion-live"}'
  'histogram_quantile(0.95, sum by (le,pod) (rate(orion_websocket_queue_wait_seconds_bucket{direction="inbound"}[1m])))'
  'histogram_quantile(0.95, sum by (le,pod) (rate(orion_websocket_queue_wait_seconds_bucket{direction="outbound"}[1m])))'
  'orion_redis_pool_connections'
  'orion_redis_pool_size'
  'increase(orion_redis_pool_timeouts_total[1m])'
  'rate(orion_redis_pool_misses_total[1m])'
)

write_summary() {
  "${project_root}/scripts/load/summarize.sh" "${output_root}" >"${output_root}/summary.md"
}

capture_redis() {
  local destination=$1
  [[ "${ORION_LOAD_DIAGNOSTICS:-false}" == true ]] || return 0
  {
    date -u +%Y-%m-%dT%H:%M:%SZ
    kubectl -n "${namespace}" exec redis-0 -- sh -c '
      export REDISCLI_AUTH="$REDIS_PASSWORD"
      for section in stats clients commandstats persistence cpu; do
        redis-cli --no-auth-warning INFO "$section"
      done
      redis-cli --no-auth-warning SLOWLOG GET 20
      redis-cli --no-auth-warning LATENCY LATEST
    '
  } >"${destination}" 2>&1 || true
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
  capture_redis "${profile_dir}/redis-before.txt"
  log_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  start=$(date -u +%s)
  echo "running ${name}"
  "${binary}" "${common_args[@]}" -output "${profile_dir}/report.json" "$@" >"${profile_dir}/stdout.json" 2>"${profile_dir}/error.txt" || status=$?
  printf '%s\n' "${status}" >"${profile_dir}/exit-code.txt"
  for pod in "${api_pods[@]}"; do
    kubectl -n "${namespace}" logs "${pod}" --timestamps --since-time="${log_start}" >"${profile_dir}/${pod}.log" 2>&1 || true
  done
  if [[ -s "${profile_dir}/report.json" ]] && jq -e '.started_at != "0001-01-01T00:00:00Z" and (.started_at != null)' "${profile_dir}/report.json" >/dev/null; then
    start=$(date -u -d "$(jq -r '.started_at' "${profile_dir}/report.json")" +%s)
  fi
  sleep "${scrape_lag}"
  capture_redis "${profile_dir}/redis-after.txt"
  end=$(date -u +%s)
  if ! capture_metrics "${profile_dir}" "${start}" "${end}"; then
    failed_collections+=("${name}")
  fi
  kubectl -n "${namespace}" get pods -o wide >"${profile_dir}/pods-after.txt" 2>&1 || true
  kubectl -n "${namespace}" get events --sort-by=.lastTimestamp >"${profile_dir}/events-after.txt" 2>&1 || true
  write_summary
  if (( status != 0 )); then
    write_summary
    cat "${output_root}/summary.md"
    echo "profile failed: ${name}; inspect ${profile_dir}" >&2
    if [[ "${continue_on_failure}" == true ]]; then
      failed_profiles+=("${name}")
      sleep "${cooldown}"
      return
    fi
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
  for repetition in $(seq 1 "${ORION_LOAD_REPEATS:-1}"); do
    for rate in ${ORION_THROUGHPUT_RATES:-1 5 10 25 50}; do
      run_profile "throughput-${rate}mps-run${repetition}" -mode chat -connections 20 -message-rate "${rate}" -duration "${duration}" -drain-timeout 1m -history-timeout 1m
    done
  done
fi

if [[ "${phase}" == mixed ]]; then
  run_profile mixed-steady -connections 1000 -senders 20 -message-rate 25 -duration "${ORION_MIXED_STEADY_DURATION:-2m}" -drain-timeout 1m -history-timeout 1m
  run_profile mixed-hotroom -connections 1000 -senders 20 -message-rate 50 -duration "${ORION_MIXED_HOTROOM_DURATION:-2m}" -drain-timeout 1m -history-timeout 1m
  run_profile mixed-burst -connections 1000 -senders 20 -message-rate 10 -duration 2m -burst-rate 50 -burst-duration 30s -drain-timeout 1m -history-timeout 1m
fi

if [[ "${phase}" == publisher ]]; then
  run_profile publisher-light -connections 20 -senders 20 -message-rate 25 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
  run_profile publisher-mixed -connections 1000 -senders 20 -message-rate 50 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
  run_profile publisher-many-senders -connections 1000 -senders 128 -message-rate 80 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
fi

if [[ "${phase}" == focused ]]; then
  run_profile control-before -connections 20 -senders 20 -message-rate 10 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
  run_profile chat-25-run1 -connections 20 -senders 20 -message-rate 25 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
  run_profile mixed-25 -connections "${ORION_FOCUSED_AUDIENCE:-300}" -senders 20 -message-rate 25 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
  run_profile chat-25-run2 -connections 20 -senders 20 -message-rate 25 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
  run_profile control-after -connections 20 -senders 20 -message-rate 10 -duration "${duration}" -drain-timeout 1m -history-timeout 1m
fi

if [[ "${phase}" == all || "${phase}" == fanout ]]; then
  for rate in ${fanout_rates}; do
    for connections in ${ORION_FANOUT_CONNECTIONS:-10 50 100 300}; do
      run_profile "fanout-${connections}c-${rate}mps" -mode chat -connections "${connections}" -message-rate "${rate}" -duration "${duration}" -drain-timeout 1m -history-timeout 1m
    done
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
if (( ${#failed_profiles[@]} > 0 )); then
  echo "failed profiles: ${failed_profiles[*]}" >&2
  exit 1
fi
if (( ${#failed_collections[@]} > 0 )); then
  echo "profiles with incomplete metrics: ${failed_collections[*]}" >&2
  exit 1
fi
