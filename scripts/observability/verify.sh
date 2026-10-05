#!/usr/bin/env bash
set -euo pipefail

namespace=monitoring
forward_log=$(mktemp)
grafana_forward_log=$(mktemp)
cleanup() {
  jobs -pr | xargs -r kill
  rm -f "${forward_log}" "${grafana_forward_log}"
}
trap cleanup EXIT

kubectl -n "${namespace}" port-forward service/prometheus-operated 19090:9090 >"${forward_log}" 2>&1 &
kubectl -n "${namespace}" port-forward service/orion-monitoring-grafana 13000:80 >"${grafana_forward_log}" 2>&1 &

for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:19090/-/ready >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:19090/-/ready >/dev/null

verify_query() {
  local query=$1
  local response=""
  for _ in $(seq 1 60); do
    response=$(curl --fail --silent --get --data-urlencode "query=${query}" http://127.0.0.1:19090/api/v1/query)
    if grep -q '"result":\[' <<<"${response}" && ! grep -q '"result":\[\]' <<<"${response}"; then
      return
    fi
    sleep 1
  done
  echo "Prometheus query returned no series: ${query}" >&2
  echo "${response}" >&2
  exit 1
}

verify_query 'up{namespace="orion-live",service="orion-api"} == 1'
verify_query 'orion_db_connections'
verify_query 'orion_outbox_collection_success == 1'
verify_query 'rabbitmq_detailed_queue_messages{queue="orion.interaction.persistence"}'

if [[ "${ORION_VERIFY_PIPELINE_METRICS:-false}" == "true" ]]; then
  verify_query 'orion_http_requests_total'
  verify_query 'orion_rabbitmq_publish_total'
  verify_query 'orion_chat_admission_duration_seconds_count'
  verify_query 'orion_persistence_processing_duration_seconds_count'
  verify_query 'orion_chat_persistence_lag_seconds_count'
fi

grafana_password=$(kubectl -n "${namespace}" get secret orion-monitoring-grafana -o jsonpath='{.data.admin-password}' | base64 -d)
for _ in $(seq 1 60); do
  if curl --fail --silent --user "admin:${grafana_password}" http://127.0.0.1:13000/api/health >/dev/null; then
    break
  fi
  sleep 1
done
dashboard=$(curl --fail --silent --user "admin:${grafana_password}" http://127.0.0.1:13000/api/dashboards/uid/orion-live)
if ! grep -q '"title":"Orion Live"' <<<"${dashboard}"; then
  echo "Grafana did not load the Orion Live dashboard" >&2
  exit 1
fi

rules=$(curl --fail --silent 'http://127.0.0.1:19090/api/v1/rules?type=alert')
if ! grep -q 'OrionRabbitMQPublishFailures' <<<"${rules}" || grep -q '"health":"err"' <<<"${rules}"; then
  echo "Prometheus did not load healthy Orion alert rules" >&2
  exit 1
fi

kubectl -n "${namespace}" get pods
kubectl -n orion-live get servicemonitor,prometheusrule
echo "Orion API and RabbitMQ targets are producing metrics; Prometheus rules and the Grafana dashboard are loaded."
