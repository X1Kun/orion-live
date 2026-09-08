.PHONY: build check test-integration compose-config migrate up down logs

build:
	go build ./cmd/server ./cmd/migrate

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test ./...
	go build ./...

test-integration:
	test -n "$$ORION_TEST_MYSQL_DSN"
	go test -tags=integration -count=1 ./tests/integration

compose-config:
	docker compose config --quiet

migrate:
	docker compose run --rm migrate

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api
