FROM golang:1.24-alpine AS build

ARG GOPROXY=https://proxy.golang.org,direct
WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY=${GOPROXY} go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orion-api ./cmd/server \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orion-migrate ./cmd/migrate

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 orion \
    && adduser -S -D -H -u 10001 -G orion orion
COPY --from=build /out/orion-api /usr/local/bin/orion-api
COPY --from=build /out/orion-migrate /usr/local/bin/orion-migrate

USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orion-api"]
