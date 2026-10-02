# Kubernetes deployment

The production-oriented base deploys the Orion API while MySQL, Redis, and RabbitMQ remain external dependencies. The `dev` Kustomization adds single-node StatefulSets for repeatable Kind testing; these dependency manifests are not production topology.

## Resources

The application base contains:

- a two-replica `Deployment` with rolling updates;
- a `ClusterIP` Service;
- startup, liveness, and readiness probes;
- CPU and memory requests and limits;
- a PodDisruptionBudget with one available replica;
- a non-root, read-only container security context;
- Prometheus scrape annotations.

Each API process currently owns its Realtime Subscriber, one competing Persistence Consumer, and one fenced Outbox Relay. The two replicas therefore exercise the existing multi-instance coordination model without introducing a second worker binary or Deployment.

The development dependency layer contains:

- an explicit dynamic `orion-local` StorageClass backed by Kind's local-path provisioner;
- single-replica MySQL, Redis, and RabbitMQ StatefulSets;
- one PVC and one Headless Service per dependency;
- explicit official-image environment mappings, probes, and resource bounds;
- a separate infrastructure Secret for the MySQL root password and RabbitMQ Erlang cookie.

The Migration Job has its own `migration/kustomization.yaml`. A deployment must wait for that Job before applying the API Deployment.

## Configure

Update the dependency hostnames and database name in `base/configmap.yaml`. Pin the same immutable image tag or digest in both `base/kustomization.yaml` and `migration/kustomization.yaml`; `latest` is only a repository placeholder outside the disposable Kind workflow.

The production-oriented base keeps `CHAT_ADMISSION_TIMEOUT=100ms`. The Kind overlay raises it to 500ms while remaining fail-closed and bounded, allowing for scheduler pauses when all workloads share one local node.

Create a local Secret manifest from the example:

```bash
cp deploy/k8s/base/secret.example.yaml deploy/k8s/base/secret.yaml
```

Replace every placeholder before applying it. `secret.yaml` is ignored by Git. A real environment should provision this Secret through its secret manager rather than commit plaintext credentials.

## Automated Kind evidence

Run the complete development sequence with:

```bash
make kind-e2e
```

It creates a disposable three-node Kind cluster without inheriting host-local HTTP proxy variables, verifies kube-proxy, CoreDNS, the local-path provisioner, and StorageClasses, builds the exact `ghcr.io/x1kun/orion-live:latest` image tag, loads Orion only into API workers, loads dependency images only into the labeled storage worker, creates both Secrets, dynamically provisions the dependency PVCs, runs the Migration Job, spreads two API Pods across nodes, fixes port-forwards to distinct Pods, and runs the Kubernetes E2E test.

Override the cluster name or Go proxy when needed:

```bash
ORION_KIND_CLUSTER=orion-live GOPROXY=https://goproxy.cn,direct make kind-e2e
```

On a resource-constrained host, opt into the single-node fallback:

```bash
ORION_KIND_CONFIG=deploy/k8s/kind/cluster-single-node.yaml make kind-e2e
```

Only one node receives the local storage label. The default is the first worker in multi-node mode and the control-plane in single-node fallback mode. `WaitForFirstConsumer` then provisions all dependency volumes with affinity to that node. Override the choice when required:

```bash
ORION_KIND_STORAGE_NODE=orion-live-worker make kind-e2e
```

Delete the disposable cluster and all dynamically provisioned local development volumes with:

```bash
make kind-delete
```

When the cluster is already deployed, use the short feedback loops independently:

```bash
make kind-smoke       # migration, two Ready APIs, cross-node Chat, History and DB/Inbox uniqueness
make kind-resilience  # Pod replacement, dependency outages, final persistence and PDB Eviction
```

`make kind-test` runs both suites against the existing cluster, while `make kind-e2e` creates, deploys, and runs both from scratch.

CI keeps the same split: pull requests run the independent Kind Smoke job, while pushes to `main`, the weekly schedule, and manual workflow dispatch run full Resilience. This keeps PR feedback bounded without weakening the release evidence.

Kind requires sufficient host inotify capacity. If kube-proxy exits with `too many open files`, increase the host limits before retrying, for example:

```bash
sudo sysctl -w fs.inotify.max_user_instances=1024
sudo sysctl -w fs.inotify.max_user_watches=524288
```

The automation waits for kube-proxy and CoreDNS before creating Orion resources, so a broken Service network fails early instead of surfacing later as application DNS errors.

## Deploy

Create the namespace, runtime configuration, and Secret first:

```bash
kubectl apply -f deploy/k8s/base/namespace.yaml
kubectl apply -f deploy/k8s/base/configmap.yaml
kubectl apply -f deploy/k8s/base/secret.yaml
```

Run migrations as a release step and wait for completion:

```bash
kubectl -n orion-live delete job orion-migrate --ignore-not-found
kubectl apply -k deploy/k8s/migration
kubectl -n orion-live wait --for=condition=complete job/orion-migrate --timeout=5m
kubectl -n orion-live logs job/orion-migrate
```

Only after the Job succeeds, apply and observe the API rollout:

```bash
kubectl apply -k deploy/k8s/base
kubectl -n orion-live rollout status deployment/orion-api --timeout=5m
kubectl -n orion-live get pods,service,pdb
```

For local access:

```bash
kubectl -n orion-live port-forward service/orion-api 8080:80
```

## Shutdown contract

The application marks readiness false and concurrently shuts down HTTP, WebSocket, Outbox, Realtime, and Persistence work when it receives `SIGTERM`. `terminationGracePeriodSeconds` is 30 seconds while `PROCESS_SHUTDOWN_TIMEOUT` is 10 seconds, leaving time for Kubernetes endpoint propagation and process exit.

The disposable three-node Kind workflow verifies cross-node API behavior, Pod replacement, cursor recovery, dependency failure semantics, final persistence recovery, and PDB enforcement through the Eviction API. It does not turn the single-replica dependency StatefulSets into production infrastructure, prove dependency node failover, or establish measured production resource limits; observability and load evidence remain separate phases.
