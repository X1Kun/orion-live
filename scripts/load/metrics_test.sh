#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "${project_root}/scripts/load/metrics.sh"
test_dir=$(mktemp -d)
trap 'rm -rf "${test_dir}"' EXIT

# Simulate valid, HTTP-failed, empty and invalid responses without network I/O.
curl() {
  local query="" arg
  for arg in "$@"; do
    if [[ "${arg}" == query=* ]]; then query=${arg#query=}; fi
  done
  case "${query}" in
    good) jq -n '{status:"success",data:{result:[{values:[[100,"1"]]}]}}' ;;
    bad) printf 'HTTP 400\n' >&2; return 22 ;;
    empty) jq -n '{status:"success",data:{result:[]}}' ;;
    invalid) printf 'invalid response\n' ;;
  esac
}
sleep() { :; }

metric_names=(good bad empty invalid)
metric_queries=(good bad empty invalid)
if capture_metrics "${test_dir}" 100 200 2>"${test_dir}/stderr.txt"; then
  echo "expected incomplete collection" >&2
  exit 1
fi
jq -e '.status=="incomplete" and .failed_queries==["bad","invalid"] and .empty_series==["empty"]' \
  "${test_dir}/metrics-collection.json" >/dev/null
test -s "${test_dir}/metrics/good.json"
test -s "${test_dir}/metrics/empty.json"
test ! -f "${test_dir}/metrics/invalid.json"
test -s "${test_dir}/metrics/invalid.json.response"
test -s "${test_dir}/metrics/bad.json.error"
echo "metric capture failure isolation passed"
