#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cluster_name=${ORION_KIND_CLUSTER:-orion-live}
namespace=orion-live
image=ghcr.io/x1kun/orion-live:latest
go_proxy=${GOPROXY:-https://proxy.golang.org,direct}

kubectl config use-context "kind-${cluster_name}" >/dev/null

storage_node=$(kubectl get nodes -l orion.live/storage=true -o jsonpath='{.items[0].metadata.name}')
if [[ -z "${storage_node}" ]]; then
  echo "no node has orion.live/storage=true" >&2
  exit 1
fi
mapfile -t api_nodes < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
if [[ ${#api_nodes[@]} -eq 0 ]]; then
  api_nodes=("${storage_node}")
fi
api_node_list=$(IFS=,; echo "${api_nodes[*]}")

docker build --build-arg "GOPROXY=${go_proxy}" --tag "${image}" "${project_root}"
kind load docker-image --name "${cluster_name}" --nodes "${api_node_list}" "${image}"

dependency_images=(
  mysql:8.4.11
  redis:7.4.11-alpine
  rabbitmq:4.3.5-management-alpine
)
for dependency_image in "${dependency_images[@]}"; do
  if ! docker image inspect "${dependency_image}" >/dev/null 2>&1; then
    docker pull "${dependency_image}"
  fi
  kind load docker-image --name "${cluster_name}" --nodes "${storage_node}" "${dependency_image}"
done

kubectl apply -f "${project_root}/deploy/k8s/base/namespace.yaml"
kubectl apply -f "${project_root}/deploy/k8s/base/configmap.yaml"

random_hex() {
  openssl rand -hex "$1"
}

if ! kubectl -n "${namespace}" get secret orion-runtime-secrets >/dev/null 2>&1; then
  kubectl -n "${namespace}" create secret generic orion-runtime-secrets \
    --from-literal=DB_USER=orion \
    --from-literal="DB_PASSWORD=$(random_hex 24)" \
    --from-literal="REDIS_PASSWORD=$(random_hex 24)" \
    --from-literal=RABBITMQ_USER=orion \
    --from-literal="RABBITMQ_PASSWORD=$(random_hex 24)" \
    --from-literal="JWT_SECRET_KEY=$(random_hex 32)"
fi

if ! kubectl -n "${namespace}" get secret orion-infrastructure-secrets >/dev/null 2>&1; then
  kubectl -n "${namespace}" create secret generic orion-infrastructure-secrets \
    --from-literal="MYSQL_ROOT_PASSWORD=$(random_hex 24)" \
    --from-literal="RABBITMQ_ERLANG_COOKIE=$(random_hex 32)"
fi

kubectl apply -k "${project_root}/deploy/k8s/dev"
kubectl -n "${namespace}" rollout status statefulset/mysql --timeout=10m
kubectl -n "${namespace}" rollout status statefulset/redis --timeout=5m
kubectl -n "${namespace}" rollout status statefulset/rabbitmq --timeout=5m

kubectl -n "${namespace}" delete job orion-migrate --ignore-not-found
kubectl apply -k "${project_root}/deploy/k8s/migration"
kubectl -n "${namespace}" wait --for=condition=complete job/orion-migrate --timeout=5m
kubectl -n "${namespace}" logs job/orion-migrate

deployment_existed=false
if kubectl -n "${namespace}" get deployment orion-api >/dev/null 2>&1; then
  deployment_existed=true
fi
kubectl apply -k "${project_root}/deploy/k8s/overlays/kind"
if [[ "${deployment_existed}" == true ]]; then
  kubectl -n "${namespace}" rollout restart deployment/orion-api
fi
kubectl -n "${namespace}" rollout status deployment/orion-api --timeout=5m

kubectl -n "${namespace}" get pods,service,pvc,pdb
