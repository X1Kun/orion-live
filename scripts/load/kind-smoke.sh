#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cluster_name=${ORION_KIND_CLUSTER:-orion-live}
namespace=orion-live
local_port=${ORION_LOAD_PORT:-18080}

kubectl config use-context "kind-${cluster_name}" >/dev/null
kubectl -n "${namespace}" rollout status deployment/orion-api --timeout=2m

forward_dir=$(mktemp -d)
forward_pid=
cleanup() {
  if [[ -n "${forward_pid}" ]]; then
    kill "${forward_pid}" >/dev/null 2>&1 || true
  fi
  rm -rf "${forward_dir}"
}
trap cleanup EXIT

kubectl -n "${namespace}" port-forward service/orion-api "${local_port}:80" \
  >"${forward_dir}/api.log" 2>&1 &
forward_pid=$!

base_url="http://127.0.0.1:${local_port}"
ready=false
for _ in $(seq 1 60); do
  if curl --connect-timeout 1 --max-time 2 --fail --silent "${base_url}/readyz" >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "Orion API port-forward did not become ready:" >&2
  cat "${forward_dir}/api.log" >&2
  exit 1
fi

cd "${project_root}"
go run ./cmd/chatload -base-url "${base_url}" "$@"
