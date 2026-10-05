#!/usr/bin/env bash
set -euo pipefail

result_dir=${1:?usage: summarize.sh <baseline-result-directory>}
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

metric_max() {
  local file=$1
  local scale=${2:-1}
  local empty_value=${3:-n/a}
  if [[ ! -s "${file}" ]]; then
    echo "${empty_value}"
    return
  fi
  jq -r --argjson scale "${scale}" --arg empty "${empty_value}" '
    [.data.result[].values[]?[1] | select(. != "NaN") | tonumber] |
    if length == 0 then $empty else ((max * $scale * 100) | round / 100) end
  ' "${file}"
}

reports() {
  find "${result_dir}" -mindepth 2 -maxdepth 2 -name report.json -type f | sort
}

echo "# Load baseline summary"
echo
echo "## Client-observed results"
echo
echo "| Profile | Mode | Connections | Rate | Accepted | Rejected | Deliveries | Persisted | ACK p95 ms | Broadcast p95 ms |"
echo "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"

while IFS= read -r report; do
  profile=$(basename "$(dirname "${report}")")
  values=$(jq -r '[.mode, .connections, .message_rate_per_second, .accepted, .rejected, .realtime_deliveries, .persisted_messages, .ack_latency.p95_ms, .broadcast_latency.p95_ms] | @tsv' "${report}")
  IFS=$'\t' read -r mode connections rate accepted rejected deliveries persisted ack_p95 broadcast_p95 <<<"${values}"
  if [[ "${mode}" == connections ]]; then
    ack_p95=n/a
    broadcast_p95=n/a
  else
    ack_p95=$(printf '%.2f' "${ack_p95}")
    broadcast_p95=$(printf '%.2f' "${broadcast_p95}")
  fi
  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "${profile}" "${mode}" "${connections}" "${rate}" "${accepted}" "${rejected}" \
    "${deliveries}" "${persisted}" "${ack_p95}" "${broadcast_p95}"
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
  publish_p95=$(metric_max "${profile_dir}/metrics/publish_p95.json" 1000)
  processing_p95=$(metric_max "${profile_dir}/metrics/persistence_processing_p95.json" 1000)
  persistence_p95=$(metric_max "${profile_dir}/metrics/persistence_lag_p95.json" 1000)
  if [[ "${mode}" == connections ]]; then
    publish_p95=n/a
    processing_p95=n/a
    persistence_p95=n/a
  fi
  persistence_throughput=$(metric_max "${profile_dir}/metrics/persistence_throughput.json")
  queue_depth=$(metric_max "${profile_dir}/metrics/persistence_queue_depth.json")
  database_wait=$(metric_max "${profile_dir}/metrics/database_wait_rate.json" 1 0)
  publish_failures=$(metric_max "${profile_dir}/metrics/publish_failures.json" 1 0)
  slow_clients=$(metric_max "${profile_dir}/metrics/slow_client_removals.json" 1 0)
  cpu=$(metric_max "${profile_dir}/metrics/api_cpu.json")
  memory=$(metric_max "${profile_dir}/metrics/api_memory.json" 0.00000095367431640625)
  throttling=$(metric_max "${profile_dir}/metrics/api_cpu_throttling.json" 100)
  mysql_cpu=$(metric_max "${profile_dir}/metrics/mysql_cpu.json")
  mysql_memory=$(metric_max "${profile_dir}/metrics/mysql_memory.json" 0.00000095367431640625)
  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "${profile}" "${publish_p95}" "${processing_p95}" "${persistence_p95}" "${persistence_throughput}" \
    "${queue_depth}" "${database_wait}" "${publish_failures}" "${slow_clients}" "${cpu}" "${memory}" \
    "${throttling}" "${mysql_cpu}" "${mysql_memory}"
done < <(reports)
