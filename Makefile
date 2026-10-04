.PHONY: build check test-integration compose-config k8s-render observability-render load-smoke kind-load-smoke kind-create kind-deploy kind-smoke kind-resilience kind-test kind-e2e kind-delete observability-install observability-verify observability-delete migrate up down logs

build:
	go build ./cmd/server ./cmd/migrate

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test ./...
	go build ./...

test-integration:
	test -n "$$ORION_TEST_MYSQL_DSN"
	test -n "$$ORION_TEST_RABBITMQ_URL"
	test -n "$$ORION_TEST_REDIS_URL"
	go test -tags=integration -count=1 ./tests/integration

compose-config:
	docker compose config --quiet

k8s-render:
	kubectl kustomize deploy/k8s/base >/dev/null
	kubectl kustomize deploy/k8s/migration >/dev/null
	kubectl kustomize deploy/k8s/dev >/dev/null
	kubectl kustomize deploy/k8s/overlays/kind >/dev/null
	kubectl kustomize deploy/k8s/observability >/dev/null

observability-render:
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
	helm repo update prometheus-community
	helm template orion-monitoring prometheus-community/kube-prometheus-stack --version 91.8.2 --namespace monitoring -f deploy/k8s/observability/helm-values.yaml -f deploy/k8s/observability/helm-values-kind.yaml >/dev/null

load-smoke:
	go run ./cmd/chatload $(LOAD_ARGS)

kind-load-smoke:
	./scripts/load/kind-smoke.sh $(LOAD_ARGS)

kind-create:
	./scripts/kind/create.sh

kind-deploy:
	./scripts/kind/deploy.sh

kind-smoke:
	./scripts/kind/test.sh smoke

kind-resilience:
	./scripts/kind/test.sh resilience

kind-test:
	./scripts/kind/test.sh all

kind-e2e: kind-create kind-deploy kind-test

kind-delete:
	./scripts/kind/delete.sh

observability-install:
	./scripts/observability/install.sh

observability-verify:
	./scripts/observability/verify.sh

observability-delete:
	./scripts/observability/delete.sh

migrate:
	docker compose run --rm migrate

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api
