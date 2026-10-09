#!/usr/bin/env bash
set -euo pipefail

result_dir=${1:?usage: summarize.sh <baseline-result-directory>}
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

metric_max() {
  local file=$1
  local scale=${2:-1}
  local empty_value=${3:-n/a}
  local window_start=${4:-0}
  local window_end=${5:-9999999999}
  if [[ ! -s "${file}" ]] || ! jq -e '.status == "success" and (.data.result | type == "array")' "${file}" >/dev/null 2>&1; then
    echo "${empty_value}"
    return
  fi
  jq -r --argjson scale "${scale}" --arg empty "${empty_value}" \
    --argjson window_start "${window_start}" --argjson window_end "${window_end}" '
    [.data.result[].values[]? | select(.[0] >= $window_start and .[0] <= $window_end) | .[1] | select(. != "NaN") | tonumber] |
    if length == 0 then $empty else ((max * $scale * 100) | round / 100) end
  ' "${file}"
}

reports() {
  find "${result_dir}" -mindepth 2 -maxdepth 2 -name report.json -type f | sort
}

echo "# Load baseline summary"
echo
echo "## Metric collection"
echo
echo "| Profile | Collection | Failed queries | Empty series |"
echo "| --- | --- | --- | --- |"
while IFS= read -r report; do
  profile_dir=$(dirname "${report}")
  if [[ -s "${profile_dir}/metrics-collection.json" ]]; then
    jq -r --arg profile "$(basename "${profile_dir}")" \
      '"| " + $profile + " | " + .status + " | " + (.failed_queries|join(", ")) + " | " + (.empty_series|join(", ")) + " |"' "${profile_dir}/metrics-collection.json"
  else
    printf '| %s | unknown (legacy) | — | — |\n' "$(basename "${profile_dir}")"
  fi
done < <(reports)
echo
echo "## Client-observed results"
echo
echo "| Profile | Status | Failure stage | Mode | Connections | Rate | Accepted | Rejected | Deliveries | Persisted | Persistence check | ACK p95 ms | Broadcast p95 ms |"
echo "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |"

while IFS= read -r report; do
  profile=$(basename "$(dirname "${report}")")
  values=$(jq -r '[.mode, .connections, .message_rate_per_second, .accepted, .rejected, .realtime_deliveries, .persisted_messages, .ack_latency.p95_ms, .broadcast_latency.p95_ms] | @tsv' "${report}")
  IFS=$'\t' read -r mode connections rate accepted rejected deliveries persisted ack_p95 broadcast_p95 <<<"${values}"
  status=$(jq -r '.status // "unknown (legacy)"' "${report}")
  failure_stage=$(jq -r '.failure_stage // "—"' "${report}")
  persistence_check=$(jq -r '.persistence_check // "unknown (legacy)"' "${report}")
  case "${persistence_check}" in
    not_checked|not_applicable) persisted=n/a ;;
    'unknown (legacy)') [[ "${persisted}" != 0 ]] || persisted='unknown' ;;
  esac
  if [[ "${mode}" == connections ]]; then
    ack_p95=n/a
    broadcast_p95=n/a
  else
    ack_p95=$(printf '%.2f' "${ack_p95}")
    broadcast_p95=$(printf '%.2f' "${broadcast_p95}")
  fi
  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "${profile}" "${status}" "${failure_stage}" "${mode}" "${connections}" "${rate}" "${accepted}" "${rejected}" \
    "${deliveries}" "${persisted}" "${persistence_check}" "${ack_p95}" "${broadcast_p95}"
done < <(reports)

echo
echo "## Infrastructure signals"
echo
echo "| Profile | Publish p95 max ms | Processing p95 max ms | Persistence lag p95 max ms | Persisted/s max | Queue max | DB waits/s max | Publish failures/s max | Slow clients | API CPU max | API memory max MiB | API throttling max % | MySQL CPU max | MySQL memory max MiB |"
echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"

while IFS= read -r report; do
  profile_dir=$(dirname "${report}")
  profile=$(basename "${profile_dir}")
  mode=$(jq -r '.mode' "${report}")
  if [[ $(jq -r '.started_at // ""' "${report}") == 0001-* ]]; then
    continue
  fi
  measurement_start=$(date -u -d "$(jq -r '.started_at' "${report}")" +%s)
  measurement_seconds=$(jq -r '.send_duration_seconds | floor' "${report}")
  steady_start=$((measurement_start + measurement_seconds / 2))
  measurement_end=$((measurement_start + measurement_seconds))
  publish_p95=$(metric_max "${profile_dir}/metrics/publish_p95.json" 1000 n/a "${steady_start}" "${measurement_end}")
  processing_p95=$(metric_max "${profile_dir}/metrics/persistence_processing_p95.json" 1000 n/a "${steady_start}" "${measurement_end}")
  persistence_p95=$(metric_max "${profile_dir}/metrics/persistence_lag_p95.json" 1000 n/a "${steady_start}" "${measurement_end}")
  if [[ "${mode}" == connections ]]; then
    publish_p95=n/a
    processing_p95=n/a
    persistence_p95=n/a
  fi
  persistence_throughput=$(metric_max "${profile_dir}/metrics/persistence_throughput.json" 1 n/a "${steady_start}" "${measurement_end}")
  queue_depth=$(metric_max "${profile_dir}/metrics/persistence_queue_depth.json" 1 n/a "${steady_start}" "${measurement_end}")
  database_wait=$(metric_max "${profile_dir}/metrics/database_wait_rate.json" 1 0 "${steady_start}" "${measurement_end}")
  publish_failures=$(metric_max "${profile_dir}/metrics/publish_failures.json" 1 0 "${steady_start}" "${measurement_end}")
  slow_clients=$(metric_max "${profile_dir}/metrics/slow_client_removals.json" 1 0 "${steady_start}" "${measurement_end}")
  cpu=$(metric_max "${profile_dir}/metrics/api_cpu.json" 1 n/a "${steady_start}" "${measurement_end}")
  memory=$(metric_max "${profile_dir}/metrics/api_memory.json" 0.00000095367431640625 n/a "${steady_start}" "${measurement_end}")
  throttling=$(metric_max "${profile_dir}/metrics/api_cpu_throttling.json" 100 n/a "${steady_start}" "${measurement_end}")
  mysql_cpu=$(metric_max "${profile_dir}/metrics/mysql_cpu.json" 1 n/a "${steady_start}" "${measurement_end}")
  mysql_memory=$(metric_max "${profile_dir}/metrics/mysql_memory.json" 0.00000095367431640625 n/a "${steady_start}" "${measurement_end}")
  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "${profile}" "${publish_p95}" "${processing_p95}" "${persistence_p95}" "${persistence_throughput}" \
    "${queue_depth}" "${database_wait}" "${publish_failures}" "${slow_clients}" "${cpu}" "${memory}" \
    "${throttling}" "${mysql_cpu}" "${mysql_memory}"
done < <(reports)

echo
echo "## Pipeline diagnosis (rolling p95 peaks, ms)"
echo
echo "| Profile | Inbound wait | Admission | Publisher lane wait | Frame processing | Outbound wait | Redis pool timeout increase (1m max) |"
echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"
while IFS= read -r report; do
  profile_dir=$(dirname "${report}")
  started=$(jq -r '.started_at' "${report}")
  [[ "${started}" != 0001-* ]] || continue
  measurement_start=$(date -u -d "${started}" +%s)
  measurement_seconds=$(jq -r '.send_duration_seconds | floor' "${report}")
  steady_start=$((measurement_start + measurement_seconds / 2))
  measurement_end=$((measurement_start + measurement_seconds))
  admission=$(metric_max "${profile_dir}/metrics/admission_p95.json" 1000 n/a "${steady_start}" "${measurement_end}")
  acquire=$(metric_max "${profile_dir}/metrics/publisher_acquire_p95.json" 1000 n/a "${steady_start}" "${measurement_end}")
  processing=$(metric_max "${profile_dir}/metrics/frame_processing_p95.json" 1000 n/a "${steady_start}" "${measurement_end}")
  inbound_wait=$(metric_max "${profile_dir}/metrics/inbound_wait_p95.json" 1000 n/a "${steady_start}" "${measurement_end}")
  outbound_wait=$(metric_max "${profile_dir}/metrics/outbound_wait_p95.json" 1000 n/a "${steady_start}" "${measurement_end}")
  redis_timeouts=$(metric_max "${profile_dir}/metrics/redis_pool_timeouts.json" 1 n/a "${steady_start}" "${measurement_end}")
  printf '| %s | %s | %s | %s | %s | %s | %s |\n' "$(basename "${profile_dir}")" "${inbound_wait}" "${admission}" "${acquire}" "${processing}" "${outbound_wait}" "${redis_timeouts}"
done < <(reports)

echo
echo "## Failure details"
echo
while IFS= read -r report; do
  jq -r --arg profile "$(basename "$(dirname "${report}")")" '
    select(.status == "failed") |
    "- " + $profile + " [" + .failure_stage + "]: " +
    (.failure_reason | gsub("[\\r\\n]"; " "))
  ' "${report}"
done < <(reports)
