#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cluster_name=${ORION_KIND_CLUSTER:-orion-live}
namespace=orion-live
phase=${1:-all}

case "${phase}" in
  smoke)
    test_pattern='^TestKubernetesSmoke$'
    ;;
  resilience)
    test_pattern='^TestKubernetesResilience$'
    ;;
  all)
    test_pattern='^TestKubernetes(Smoke|Resilience)$'
    ;;
  *)
    echo "usage: $0 [smoke|resilience|all]" >&2
    exit 1
    ;;
esac

kubectl config use-context "kind-${cluster_name}" >/dev/null
mapfile -t api_pods < <(
  kubectl -n "${namespace}" get pods \
    -l app.kubernetes.io/name=orion-live,app.kubernetes.io/component=api \
    --no-headers | awk '$2 == "1/1" && $3 == "Running" {print $1}' | sort
)

if [[ ${#api_pods[@]} -ne 2 ]]; then
  echo "expected exactly two running API Pods, found ${#api_pods[@]}" >&2
  exit 1
fi

forward_dir=$(mktemp -d)
cleanup() {
  jobs -pr | xargs -r kill
  rm -rf "${forward_dir}"
}
trap cleanup EXIT

(
  while true; do
    kubectl -n "${namespace}" port-forward "pod/${api_pods[0]}" 18081:8080 || true
    sleep 1
  done
) >"${forward_dir}/api-a.log" 2>&1 &
(
  while true; do
    kubectl -n "${namespace}" port-forward "pod/${api_pods[1]}" 18082:8080 || true
    sleep 1
  done
) >"${forward_dir}/api-b.log" 2>&1 &
(
  while true; do
    kubectl -n "${namespace}" port-forward service/mysql 13306:3306 || true
    sleep 1
  done
) >"${forward_dir}/mysql.log" 2>&1 &

for url in http://127.0.0.1:18081/readyz http://127.0.0.1:18082/readyz; do
  for _ in $(seq 1 60); do
    if curl --fail --silent "${url}" >/dev/null; then
      break
    fi
    sleep 1
  done
  curl --fail --silent "${url}" >/dev/null
done

db_password=$(kubectl -n "${namespace}" get secret orion-runtime-secrets -o jsonpath='{.data.DB_PASSWORD}' | base64 -d)
export ORION_K8S_API_A_URL=http://127.0.0.1:18081
export ORION_K8S_API_B_URL=http://127.0.0.1:18082
export ORION_K8S_API_A_POD=${api_pods[0]}
export ORION_K8S_NAMESPACE=${namespace}
export ORION_K8S_MYSQL_DSN="orion:${db_password}@tcp(127.0.0.1:13306)/orion?charset=utf8mb4&parseTime=true&loc=UTC"

go test -tags=kubernetes -run "${test_pattern}" -count=1 -v ./tests/kubernetes
