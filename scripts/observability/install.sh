#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
chart_version=91.8.2
release_name=orion-monitoring
namespace=monitoring
grafana_image=grafana/grafana:13.2.3-distroless
values=(--values "${project_root}/deploy/k8s/observability/helm-values.yaml")

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
helm repo update prometheus-community

cluster_name=${ORION_KIND_CLUSTER:-orion-live}
if kind get clusters 2>/dev/null | grep -qx "${cluster_name}"; then
  values+=(--values "${project_root}/deploy/k8s/observability/helm-values-kind.yaml")
  observability_node=$(kubectl get nodes -l orion.live/observability=true -o jsonpath='{.items[0].metadata.name}')
  if [[ -z "${observability_node}" ]]; then
    echo "Kind observability node label is missing" >&2
    exit 1
  fi
  if ! docker image inspect "${grafana_image}" >/dev/null 2>&1; then
    docker pull "${grafana_image}"
  fi
  kind load docker-image --name "${cluster_name}" --nodes "${observability_node}" "${grafana_image}"
fi

helm upgrade --install "${release_name}" \
  prometheus-community/kube-prometheus-stack \
  --version "${chart_version}" \
  --namespace "${namespace}" \
  --create-namespace \
  "${values[@]}" \
  --wait \
  --timeout 15m

kubectl apply -k "${project_root}/deploy/k8s/observability"
kubectl -n "${namespace}" wait --for=condition=Available deployment -l app.kubernetes.io/name=grafana --timeout=5m

echo "Run 'make observability-verify' to verify Orion and RabbitMQ targets."
