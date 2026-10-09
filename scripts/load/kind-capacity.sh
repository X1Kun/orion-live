#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cluster_name=${ORION_KIND_CLUSTER:-orion-live}
namespace=orion-live
output_root=${ORION_CAPACITY_OUTPUT_DIR:-${project_root}/artifacts/load/capacity-$(date -u +%Y%m%dT%H%M%SZ)}
failed_suites=()
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
case "${1:-}" in
  ''|diagnostics|mixed|stability|publisher|focused) ;;
  *) echo "usage: $0 [diagnostics|mixed|stability|publisher|focused]" >&2; exit 1 ;;
esac

kubectl config use-context "kind-${cluster_name}" >/dev/null
original_concurrency=$(kubectl -n "${namespace}" get configmap orion-runtime-config -o jsonpath='{.data.PERSISTENCE_CONCURRENCY}')
original_publish_concurrency=$(kubectl -n "${namespace}" get configmap orion-runtime-config -o jsonpath='{.data.RABBITMQ_CHAT_PUBLISH_CONCURRENCY}')
original_resources=$(kubectl -n "${namespace}" get deployment orion-api -o json | jq -c '.spec.template.spec.containers[] | select(.name == "api") | .resources')
cpu_changed=false
publisher_changed=false

set_concurrency() {
  local concurrency=$1
  kubectl -n "${namespace}" patch configmap orion-runtime-config --type merge \
    -p "{\"data\":{\"PERSISTENCE_CONCURRENCY\":\"${concurrency}\"}}" >/dev/null
  kubectl -n "${namespace}" rollout restart deployment/orion-api >/dev/null
  kubectl -n "${namespace}" rollout status deployment/orion-api --timeout=5m
}

restore_concurrency() {
  set +e
  if [[ "${publisher_changed}" == true ]]; then
    if [[ -n "${original_publish_concurrency}" ]]; then
      kubectl -n "${namespace}" patch configmap orion-runtime-config --type merge \
        -p "{\"data\":{\"RABBITMQ_CHAT_PUBLISH_CONCURRENCY\":\"${original_publish_concurrency}\"}}" >/dev/null
    else
      kubectl -n "${namespace}" patch configmap orion-runtime-config --type merge \
        -p '{"data":{"RABBITMQ_CHAT_PUBLISH_CONCURRENCY":null}}' >/dev/null
    fi
  fi
  if [[ "${cpu_changed}" == true ]]; then
    kubectl -n "${namespace}" patch deployment orion-api --type strategic \
      -p "{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"api\",\"resources\":${original_resources}}]}}}}" >/dev/null
  fi
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

if [[ "${1:-}" == focused ]]; then
  cpu_changed=true
  publisher_changed=true
  kubectl -n "${namespace}" patch configmap orion-runtime-config --type merge \
    -p '{"data":{"RABBITMQ_CHAT_PUBLISH_CONCURRENCY":"16","PERSISTENCE_CONCURRENCY":"8"}}' >/dev/null
  kubectl -n "${namespace}" set resources deployment/orion-api -c api --limits=cpu=1 >/dev/null
  kubectl -n "${namespace}" rollout restart deployment/orion-api >/dev/null
  kubectl -n "${namespace}" rollout status deployment/orion-api --timeout=5m
  run_suite fixed-p16-c8 focused ORION_LOAD_DIAGNOSTICS=true \
    ORION_LOAD_DURATION="${ORION_FOCUSED_DURATION:-2m}" \
    ORION_FOCUSED_AUDIENCE="${ORION_FOCUSED_AUDIENCE:-300}"
  echo "focused artifacts: ${output_root}"
  if (( ${#failed_suites[@]} > 0 )); then
    echo "failed suites: ${failed_suites[*]}" >&2
    exit 1
  fi
  exit 0
fi

if [[ "${1:-}" == publisher ]]; then
  read -r -a candidates <<<"${ORION_PUBLISHER_CONCURRENCIES:-8 16 32 64}"
  repetitions=${ORION_PUBLISHER_REPEATS:-3}
  if [[ ! "${repetitions}" =~ ^[1-9][0-9]*$ || ${#candidates[@]} -eq 0 ]]; then
    echo "publisher repeats must be a positive integer and candidates must be nonempty" >&2
    exit 1
  fi
  for candidate in "${candidates[@]}"; do
    if [[ ! "${candidate}" =~ ^[1-9][0-9]*$ ]] || (( candidate > 64 )); then
      echo "Publisher concurrency must be between 1 and 64" >&2
      exit 1
    fi
  done
  cpu_changed=true
  kubectl -n "${namespace}" set resources deployment/orion-api -c api --limits=cpu=1 >/dev/null
  set_concurrency 8
  for ((round=1; round<=repetitions; round++)); do
    for ((offset=0; offset<${#candidates[@]}; offset++)); do
      index=${offset}
      if (( round % 2 == 0 )); then index=$((${#candidates[@]} - 1 - offset)); fi
      candidate=${candidates[index]}
      publisher_changed=true
      kubectl -n "${namespace}" patch configmap orion-runtime-config --type merge \
        -p "{\"data\":{\"RABBITMQ_CHAT_PUBLISH_CONCURRENCY\":\"${candidate}\"}}" >/dev/null
      kubectl -n "${namespace}" rollout restart deployment/orion-api >/dev/null
      kubectl -n "${namespace}" rollout status deployment/orion-api --timeout=5m
      run_suite "publisher-${candidate}-run${round}" publisher \
        ORION_LOAD_DIAGNOSTICS=true ORION_LOAD_DURATION="${ORION_PUBLISHER_DURATION:-90s}"
    done
  done
  echo "publisher artifacts: ${output_root}"
  if (( ${#failed_suites[@]} > 0 )); then
    echo "failed suites: ${failed_suites[*]}" >&2
    exit 1
  fi
  exit 0
fi

if [[ "${1:-}" == stability ]]; then
  set_concurrency 8
  run_suite repeated-25 throughput ORION_LOAD_DIAGNOSTICS=true ORION_LOAD_DURATION=2m ORION_LOAD_REPEATS=3 ORION_THROUGHPUT_RATES=25
  for cpu in 500m 1; do
    cpu_changed=true
    kubectl -n "${namespace}" set resources deployment/orion-api -c api --limits="cpu=${cpu}" >/dev/null
    kubectl -n "${namespace}" rollout status deployment/orion-api --timeout=5m
    run_suite "mixed-cpu-${cpu}" mixed ORION_LOAD_DIAGNOSTICS=true \
      ORION_MIXED_STEADY_DURATION="${ORION_MIXED_STEADY_DURATION:-10m}" \
      ORION_MIXED_HOTROOM_DURATION="${ORION_MIXED_HOTROOM_DURATION:-2m}"
  done
  echo "stability artifacts: ${output_root}"
  if (( ${#failed_suites[@]} > 0 )); then
    echo "failed suites: ${failed_suites[*]}" >&2
    exit 1
  fi
  exit 0
fi

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
