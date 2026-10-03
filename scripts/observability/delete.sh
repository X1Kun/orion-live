#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

kubectl delete -k "${project_root}/deploy/k8s/observability" --ignore-not-found
helm uninstall orion-monitoring --namespace monitoring --ignore-not-found
kubectl delete namespace monitoring --ignore-not-found
