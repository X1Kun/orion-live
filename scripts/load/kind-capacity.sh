#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cluster_name=${ORION_KIND_CLUSTER:-orion-live}
namespace=orion-live
output_root=${ORION_CAPACITY_OUTPUT_DIR:-${project_root}/artifacts/load/capacity-$(date -u +%Y%m%dT%H%M%SZ)}
failed_suites=()

kubectl config use-context "kind-${cluster_name}" >/dev/null
original_concurrency=$(kubectl -n "${namespace}" get configmap orion-runtime-config -o jsonpath='{.data.PERSISTENCE_CONCURRENCY}')

set_concurrency() {
  local concurrency=$1
  kubectl -n "${namespace}" patch configmap orion-runtime-config --type merge \
    -p "{\"data\":{\"PERSISTENCE_CONCURRENCY\":\"${concurrency}\"}}" >/dev/null
  kubectl -n "${namespace}" rollout restart deployment/orion-api >/dev/null
  kubectl -n "${namespace}" rollout status deployment/orion-api --timeout=5m
}

restore_concurrency() {
  set +e
  set_concurrency "${original_concurrency}"
}
trap restore_concurrency EXIT

run_suite() {
  local name=$1
  local phase=$2
  shift 2
  local suite_dir="${output_root}/${name}"
  echo "running suite ${name}"
  if ! env \
    ORION_LOAD_OUTPUT_DIR="${suite_dir}" \
    ORION_LOAD_CONTINUE_ON_FAILURE=true \
    ORION_LOAD_COOLDOWN=35s \
    ORION_LOAD_SCRAPE_LAG=35s \
    "$@" \
    make -C "${project_root}" kind-load-baseline LOAD_BASELINE_PHASE="${phase}"; then
    if [[ ! -s "${suite_dir}/summary.md" ]]; then
      echo "suite failed before producing evidence: ${name}" >&2
      exit 1
    fi
    failed_suites+=("${name}")
  fi
}

mkdir -p "${output_root}"

if [[ "${1:-}" == diagnostics ]]; then
  set_concurrency 4
  run_suite repeat-c4 throughput ORION_LOAD_DURATION=2m ORION_LOAD_REPEATS=3 ORION_THROUGHPUT_RATES='25 50'
  set_concurrency 8
  run_suite repeat-c8 throughput ORION_LOAD_DURATION=2m ORION_LOAD_REPEATS=3 ORION_THROUGHPUT_RATES=40
  echo "diagnostic artifacts: ${output_root}"
  exit 0
fi

if [[ "${1:-}" == mixed ]]; then
  set_concurrency 8
  run_suite mixed-c8 mixed
  echo "mixed artifacts: ${output_root}"
  exit 0
fi

set_concurrency 4
run_suite persistence-c4 throughput \
  ORION_LOAD_DURATION=2m \
  ORION_THROUGHPUT_RATES='25 40 50'

set_concurrency 8
run_suite persistence-c8 throughput \
  ORION_LOAD_DURATION=2m \
  ORION_THROUGHPUT_RATES='25 40 50'

run_suite fanout-connections fanout \
  ORION_LOAD_DURATION=2m \
  ORION_FANOUT_RATES=5 \
  ORION_FANOUT_CONNECTIONS='300 600 1000'

run_suite fanout-rate fanout \
  ORION_LOAD_DURATION=2m \
  ORION_FANOUT_RATES='5 10 25' \
  ORION_FANOUT_CONNECTIONS=300

run_suite idle-connections connections \
  ORION_LOAD_DURATION=2m \
  ORION_CONNECTION_COUNTS='1200 1600 1900 2000'

run_suite burst-50 throughput \
  ORION_LOAD_DURATION=30s \
  ORION_THROUGHPUT_RATES=50

echo "capacity artifacts: ${output_root}"
if (( ${#failed_suites[@]} > 0 )); then
  echo "suites containing expected capacity failures: ${failed_suites[*]}"
fi
