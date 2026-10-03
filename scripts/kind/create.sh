#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cluster_name=${ORION_KIND_CLUSTER:-orion-live}
cluster_config=${ORION_KIND_CONFIG:-${project_root}/deploy/k8s/kind/cluster.yaml}

if ! kind get clusters | grep -qx "${cluster_name}"; then
  env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
    kind create cluster --name "${cluster_name}" --config "${cluster_config}"
fi

kubectl config use-context "kind-${cluster_name}" >/dev/null
if ! kubectl -n kube-system rollout status daemonset/kube-proxy --timeout=2m; then
  echo "kube-proxy did not become ready. On Linux, check fs.inotify.max_user_instances and fs.inotify.max_user_watches." >&2
  exit 1
fi
if ! kubectl -n kube-system rollout status deployment/coredns --timeout=2m; then
  echo "CoreDNS did not become ready; verify Kind Service networking before deploying Orion." >&2
  exit 1
fi
if ! kubectl -n local-path-storage rollout status deployment/local-path-provisioner --timeout=2m; then
  echo "Kind local-path provisioner did not become ready; dynamic PVC provisioning is unavailable." >&2
  exit 1
fi
storage_node=${ORION_KIND_STORAGE_NODE:-}
if [[ -z "${storage_node}" ]]; then
  if kubectl get node "${cluster_name}-worker" >/dev/null 2>&1; then
    storage_node=${cluster_name}-worker
  else
    storage_node=${cluster_name}-control-plane
  fi
fi
if ! kubectl get node "${storage_node}" >/dev/null 2>&1; then
  echo "storage node ${storage_node} does not exist" >&2
  exit 1
fi
kubectl label nodes -l orion.live/storage=true orion.live/storage- >/dev/null 2>&1 || true
kubectl label node "${storage_node}" orion.live/storage=true --overwrite

observability_node=${ORION_KIND_OBSERVABILITY_NODE:-}
if [[ -z "${observability_node}" ]]; then
  if kubectl get node "${cluster_name}-worker2" >/dev/null 2>&1; then
    observability_node=${cluster_name}-worker2
  else
    observability_node=${storage_node}
  fi
fi
if ! kubectl get node "${observability_node}" >/dev/null 2>&1; then
  echo "observability node ${observability_node} does not exist" >&2
  exit 1
fi
kubectl label nodes -l orion.live/observability=true orion.live/observability- >/dev/null 2>&1 || true
kubectl label node "${observability_node}" orion.live/observability=true --overwrite
kubectl apply -f "${project_root}/deploy/k8s/dev/storage-class.yaml"

echo "Available StorageClasses:"
kubectl get storageclass
kubectl get storageclass orion-local >/dev/null
