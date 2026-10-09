#!/usr/bin/env bash

# Called by kind-baseline.sh; keeps a failed scrape separate from test results.
capture_metric() {
  local endpoint=$1 query=$2 start=$3 end=$4 destination=$5
  local attempt
  for attempt in 1 2; do
    if curl --fail --silent --show-error --max-time 15 --get \
      --data-urlencode "query=${query}" --data-urlencode "start=${start}" \
      --data-urlencode "end=${end}" --data-urlencode "step=5" \
      "${endpoint}/api/v1/query_range" >"${destination}.response" 2>"${destination}.error"; then
      if jq -e '.status == "success" and (.data.result | type == "array")' "${destination}.response" >/dev/null 2>&1; then
        mv "${destination}.response" "${destination}"
        return 0
      fi
    fi
    if [[ "${attempt}" == 1 ]]; then sleep 1; fi
  done
  return 1
}

capture_metrics() {
  local profile_dir=$1 start=$2 end=$3
  local index destination
  local failures='[]' missing='[]'
  mkdir -p "${profile_dir}/metrics"
  for index in "${!metric_names[@]}"; do
    destination="${profile_dir}/metrics/${metric_names[index]}.json"
    if capture_metric http://127.0.0.1:19090 "${metric_queries[index]}" "${start}" "${end}" "${destination}"; then
      if jq -e '.data.result | length == 0' "${destination}" >/dev/null; then
        missing=$(jq -c --arg name "${metric_names[index]}" '. + [$name]' <<<"${missing}")
      fi
    else
      failures=$(jq -c --arg name "${metric_names[index]}" '. + [$name]' <<<"${failures}")
      echo "metric capture incomplete: ${metric_names[index]}" >&2
    fi
  done
  jq -n --argjson failures "${failures}" --argjson missing "${missing}" \
    '{status: (if ($failures|length) == 0 then "complete" else "incomplete" end), failed_queries:$failures, empty_series:$missing}' \
    >"${profile_dir}/metrics-collection.json"
  [[ "${failures}" == '[]' ]]
}
