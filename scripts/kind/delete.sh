#!/usr/bin/env bash
set -euo pipefail

cluster_name=${ORION_KIND_CLUSTER:-orion-live}
kind delete cluster --name "${cluster_name}"
