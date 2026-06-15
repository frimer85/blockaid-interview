# Purchase Events Platform — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans

**Goal:** Build, deploy, and validate the event-driven purchase system from [`docs/DESIGN.md`](../../DESIGN.md) on Kubernetes — gateway → Kafka → processor → Postgres — meeting the resilience (1 AZ + 1 node), cross-AZ-cost, autoscaling, and observability requirements.

**Architecture:** A stateless **Customer Gateway** (HTTP) produces `purchase.created` events to a **Kafka** topic keyed by `user_id` and serves reads from **Postgres** read replicas; a **Processor** consumer group idempotently upserts events into Postgres. All components run on **EKS** across 3 AZs, with cross-AZ traffic minimized via topology-aware routing and Kafka rack-aware fetch. Local development and CI use a 3-node **kind** cluster whose nodes are labeled as fake AZs.

**Tech Stack:** Go 1.22 (gateway + processor), [franz-go](https://github.com/twmb/franz-go) (Kafka client, rack-aware fetch), [pgx v5](https://github.com/jackc/pgx) (Postgres), Strimzi (Kafka operator), CloudNativePG (Postgres operator), KEDA (lag autoscaling), Helm (packaging), kube-prometheus-stack + Loki + OpenTelemetry (observability), GitHub Actions (CI/CD), k6 (load), kind (local/CI cluster).

> **Language note:** Go is chosen so this plan contains real, compilable code. The services are thin; porting to Python/FastAPI + `confluent-kafka` + `asyncpg` is a mechanical swap and changes only Phases 3–4. Everything else (operators, Helm, K8s, CI/CD) is language-agnostic.

---

## Conventions (apply to every task)

- **TDD:** write the test first, watch it fail, implement, watch it pass.
- **Granularity:** each checkbox is ~2–5 minutes. Each **task** ends with a `git commit`.
- **No placeholders:** every step has the real path, command, and expected output.
- **Branch:** work on `feat/implementation`; never commit to `main` directly.
- **Commit message footer:** `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Namespaces:** `app` (services), `kafka` (Strimzi), `cnpg-system` + `db` (Postgres), `keda`, `observability`.

## Milestone Overview

| Phase | Milestone deliverable | Exit criteria (how we know it's done) | Requirements |
|---|---|---|---|
| 0 | Repo scaffold + 3-AZ kind cluster | `make up` yields a 3-node cluster with zone labels; `go build ./...` passes | — |
| 1 | Postgres (CloudNativePG) running | `db` cluster `Ready`; `purchases` table exists; `-ro`/`-rw` services resolve | F3, F5, N1 |
| 2 | Kafka (Strimzi) running | Kafka CR `Ready`; `purchases` topic 12p/RF3/ISR2; rack-aware | F2, N1 |
| 3 | Gateway service | Unit tests green; image builds; POST enqueues, GET reads (local) | F1, F4 |
| 4 | Processor service | Unit tests green; image builds; event → row upsert (local) | F3, F5 |
| 5 | Full system on K8s (Helm) | `helm install` + e2e smoke (POST→GET eventually) passes on kind | N1, N4, N5, N6 |
| 6 | Cross-AZ + resilience hardening | Failure drill (drain AZ + node) keeps the API available | N2, N3 |
| 7 | Observability stack | Dashboards show RED + consumer lag + e2e lag; test alert fires | N7 |
| 8 | CI/CD (GitHub Actions) | PR runs CI green (incl. kind smoke); CD deploys on merge | Bonus |
| 9 | Load + resilience validation | k6 thresholds pass; drill results recorded in `docs/` | N3, N6 |

---

## Phase 0 — Repository scaffold & local 3-AZ cluster

**Deliverable:** a reproducible local cluster and a compiling Go workspace.

### Task 0.1 — Go workspace & directory layout

**Files:** Create `go.mod`, `services/gateway/main.go`, `services/processor/main.go`, `.gitignore` (exists).

- [ ] `git checkout -b feat/implementation`
- [ ] `go mod init github.com/frimer85/blockaid-interview` (expected: creates `go.mod`)
- [ ] Create `services/gateway/main.go` and `services/processor/main.go` each with a minimal `package main; func main() {}`.
- [ ] `go build ./...` → expected: no output, exit 0.
- [ ] **Commit:** `chore: go workspace + service skeletons`

### Task 0.2 — Makefile & kind 3-AZ config

**Files:** Create `Makefile`, `deploy/kind/kind-3az.yaml`.

- [ ] Create `deploy/kind/kind-3az.yaml` — 1 control-plane + 3 workers:
  ```yaml
  kind: Cluster
  apiVersion: kind.x-k8s.io/v1alpha4
  nodes:
    - role: control-plane
    - role: worker
      labels: { topology.kubernetes.io/zone: az-a }
    - role: worker
      labels: { topology.kubernetes.io/zone: az-b }
    - role: worker
      labels: { topology.kubernetes.io/zone: az-c }
  ```
- [ ] Create `Makefile` with targets: `up` (create cluster), `down`, `build` (`go build ./...`), `test` (`go test ./...`), `images` (docker build both services), `kind-load` (`kind load docker-image`), `deploy` (helm install), `smoke` (e2e script), `load` (k6).
- [ ] **Commit:** `chore: Makefile + kind 3-AZ cluster config`

### Task 0.3 — Bring up and verify the cluster

- [ ] `make up` → `kind create cluster --config deploy/kind/kind-3az.yaml`
- [ ] `kubectl get nodes -L topology.kubernetes.io/zone` → expected: 3 workers showing `az-a/az-b/az-c`.
- [ ] **Commit:** `docs: record local cluster bring-up` (add a one-paragraph note to `README` Quickstart).

---

## Phase 1 — Database layer (CloudNativePG)

**Deliverable:** a 3-instance Postgres cluster spread across the fake AZs, with the `purchases` schema applied and read/write Services available.

### Task 1.1 — Install the CloudNativePG operator

- [ ] `helm repo add cnpg https://cloudnative-pg.github.io/charts && helm repo update`
- [ ] `helm install cnpg cnpg/cloudnative-pg -n cnpg-system --create-namespace`
- [ ] `kubectl -n cnpg-system rollout status deploy/cnpg-cloudnative-pg` → expected: `successfully rolled out`.
- [ ] **Commit:** `deploy: install CloudNativePG operator`

### Task 1.2 — Postgres cluster manifest

**Files:** Create `deploy/db/cluster.yaml`.

- [ ] Create the `Cluster` CR: `instances: 3`, `primaryUpdateStrategy: unsupervised`, synchronous replication quorum, per-AZ spread:
  ```yaml
  apiVersion: postgresql.cnpg.io/v1
  kind: Cluster
  metadata: { name: purchases-db, namespace: db }
  spec:
    instances: 3
    postgresql:
      synchronous: { method: any, number: 1 }   # quorum ANY 1 (DESIGN §8.3)
    storage: { size: 5Gi }
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector: { matchLabels: { cnpg.io/cluster: purchases-db } }
  ```
- [ ] `kubectl create ns db && kubectl apply -f deploy/db/cluster.yaml`
- [ ] `kubectl -n db wait --for=condition=Ready cluster/purchases-db --timeout=300s` → expected: `condition met`.
- [ ] `kubectl -n db get svc` → expected: `purchases-db-rw` and `purchases-db-ro` present (write vs read Services).
- [ ] **Commit:** `deploy: Postgres cluster (3 instances, sync ANY 1, AZ spread)`

### Task 1.3 — Schema migration

**Files:** Create `db/migrations/0001_purchases.up.sql` (the table from DESIGN §4.3), `deploy/db/migrate-job.yaml`.

- [ ] Create `db/migrations/0001_purchases.up.sql` with the `purchases` table + `idx_purchases_user_time` index.
- [ ] Create a `Job` that runs `migrate` against `purchases-db-rw` (image `migrate/migrate`, mounts the SQL via ConfigMap, reads the connection secret `purchases-db-app`).
- [ ] `kubectl apply -f deploy/db/migrate-job.yaml && kubectl -n db wait --for=condition=complete job/db-migrate --timeout=120s`
- [ ] Verify: `kubectl -n db exec -it purchases-db-1 -- psql -U postgres -d app -c '\d purchases'` → expected: table with `event_id` PK.
- [ ] **Commit:** `db: purchases schema + migration job`

---

## Phase 2 — Event stream (Strimzi Kafka)

**Deliverable:** a rack-aware KRaft Kafka cluster with the `purchases` topic.

### Task 2.1 — Install the Strimzi operator

- [ ] `helm repo add strimzi https://strimzi.io/charts/ && helm repo update`
- [ ] `helm install strimzi strimzi/strimzi-kafka-operator -n kafka --create-namespace`
- [ ] `kubectl -n kafka rollout status deploy/strimzi-cluster-operator` → expected: rolled out.
- [ ] **Commit:** `deploy: install Strimzi operator`

### Task 2.2 — Kafka cluster + topic (KRaft, rack-aware)

**Files:** Create `deploy/kafka/kafka.yaml`, `deploy/kafka/topic.yaml`.

> On a 3-node kind cluster, use **3 brokers (1/AZ)** for local; the EKS overlay uses **6 brokers (2/AZ)** per DESIGN §8.2. Parameterize via Helm later (Phase 5); here use 3 for local.

- [ ] Create `KafkaNodePool` + `Kafka` CR (KRaft): brokers `replicas: 3`, controllers `replicas: 3`, `rack.topologyKey: topology.kubernetes.io/zone`, listeners with TLS, `config: { offsets.topic.replication.factor: 3, default.replication.factor: 3, min.insync.replicas: 2, replica.selector.class: org.apache.kafka.common.replica.RackAwareReplicaSelector }`.
- [ ] Create `KafkaTopic` `purchases`: `partitions: 12`, `replicas: 3`, `config: { min.insync.replicas: "2", retention.ms: "604800000" }`.
- [ ] `kubectl apply -f deploy/kafka/ && kubectl -n kafka wait --for=condition=Ready kafka/purchases --timeout=600s`
- [ ] `kubectl -n kafka get kafkatopic purchases` → expected: `Ready=True`.
- [ ] **Commit:** `deploy: Strimzi Kafka (KRaft, rack-aware) + purchases topic`

---

## Phase 3 — Customer Gateway service (Go, TDD)

**Deliverable:** a tested gateway binary + container that produces events and serves reads.

### Task 3.1 — Config, server skeleton, health probes

**Files:** Create `services/gateway/config.go`, `server.go`, `server_test.go`.

- [ ] **Test first:** `server_test.go` asserts `GET /healthz` → 200 and `GET /readyz` → 503 until deps marked ready. `go test ./services/gateway/...` → expected: FAIL (no handler yet).
- [ ] Implement `server.go` with chi router, `/healthz`, `/readyz` (readiness gated on a `ready atomic.Bool`), `/metrics` (promhttp). `config.go` reads env: `HTTP_ADDR`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, `DB_RO_DSN`.
- [ ] `go test ./services/gateway/...` → expected: PASS.
- [ ] **Commit:** `feat(gateway): server skeleton + health/readiness/metrics`

### Task 3.2 — POST /v1/purchases → produce to Kafka

**Files:** Create `services/gateway/produce.go`, `produce_test.go`. Modify `server.go`.

- [ ] **Test first:** with a mock producer, `POST /v1/purchases` with a valid body returns `202` + a non-empty `event_id`; invalid body (missing `user_id`/negative `price`) returns `400`. Run → FAIL.
- [ ] Implement the handler: validate, derive `event_id` from `Idempotency-Key` (or generate UUIDv4), build the `purchase.created` JSON (DESIGN §4.2), produce with franz-go using **key = `user_id`**. Configure the client with `kgo.RequiredAcks(kgo.AllISRAcks())`, `kgo.ProducerLinger(0)`, idempotent producer (default on in franz-go).
- [ ] `go test ./services/gateway/...` → expected: PASS.
- [ ] **Commit:** `feat(gateway): POST /v1/purchases produces keyed events`

### Task 3.3 — GET /v1/users/{user_id}/purchases → read replica

**Files:** Create `services/gateway/read.go`, `read_test.go`. Modify `server.go`.

- [ ] **Test first:** using `pgxmock` (or a testcontainers Postgres), `GET /v1/users/u-1/purchases` returns rows newest-first and honors `?limit=&cursor=`. Run → FAIL.
- [ ] Implement with `pgxpool` against `DB_RO_DSN` (the `-ro` Service): `SELECT ... FROM purchases WHERE user_id=$1 AND (purchased_at,event_id) < cursor ORDER BY purchased_at DESC, event_id DESC LIMIT $2`. Encode an opaque keyset cursor.
- [ ] `go test ./services/gateway/...` → expected: PASS.
- [ ] **Commit:** `feat(gateway): GET user purchases from read replica (keyset paging)`

### Task 3.4 — Metrics, tracing, wire readiness

**Files:** Modify `server.go`, `produce.go`, `read.go`; create `services/gateway/otel.go`.

- [ ] Add RED metrics (request counter, error counter, latency histogram) and produce success/failure counters.
- [ ] Init OpenTelemetry tracer; **inject W3C trace context into Kafka record headers** on produce (DESIGN §10).
- [ ] `/readyz` flips ready once the Kafka client and `pgxpool` both ping successfully.
- [ ] `go test ./services/gateway/...` → expected: PASS.
- [ ] **Commit:** `feat(gateway): RED metrics + OTel trace propagation`

### Task 3.5 — Container image

**Files:** Create `services/gateway/Dockerfile`.

- [ ] Multi-stage: `golang:1.22` build → `gcr.io/distroless/static:nonroot` runtime; `USER nonroot`; expose 8080.
- [ ] `docker build -t gateway:dev services/gateway` → expected: success.
- [ ] **Commit:** `build(gateway): distroless non-root image`

---

## Phase 4 — Event Processing Service (Go, TDD)

**Deliverable:** a tested consumer that idempotently persists events.

### Task 4.1 — Consumer skeleton (rack-aware group) + health

**Files:** Create `services/processor/config.go`, `consumer.go`, `health.go`, `consumer_test.go`.

- [ ] **Test first:** `consumer_test.go` asserts the message handler rejects malformed JSON (returns a validation error) and accepts a valid event. Run → FAIL.
- [ ] Implement franz-go consumer group: `kgo.ConsumerGroup("processor")`, `kgo.ConsumeTopics("purchases")`, `kgo.Rack(os.Getenv("AZ"))` (KIP-392 fetch-from-closest, DESIGN §7), `kgo.DisableAutoCommit()`. Add `/healthz` + `/readyz` + `/metrics` HTTP server.
- [ ] `go test ./services/processor/...` → expected: PASS.
- [ ] **Commit:** `feat(processor): rack-aware consumer skeleton + health`

### Task 4.2 — Validate + idempotent upsert + commit-after-write

**Files:** Create `services/processor/store.go`, `store_test.go`. Modify `consumer.go`.

- [ ] **Test first (testcontainers Postgres):** inserting the same `event_id` twice yields exactly one row; `store_test.go` → FAIL.
- [ ] Implement `INSERT INTO purchases (...) VALUES (...) ON CONFLICT (event_id) DO NOTHING` via `pgxpool` on `DB_RW_DSN`.
- [ ] In `consumer.go`: extract trace context from headers, validate, upsert, then **commit the offset only after** a successful DB commit (`cl.CommitRecords(ctx, rec)`).
- [ ] `go test ./services/processor/...` → expected: PASS.
- [ ] **Commit:** `feat(processor): idempotent upsert + offset-after-commit`

### Task 4.3 — Metrics/tracing + container

**Files:** Modify `consumer.go`; create `services/processor/Dockerfile`.

- [ ] Add metrics: processed/sec, processing latency, upsert-conflict counter, dead-letter counter; continue the trace span across the async boundary.
- [ ] Multi-stage distroless non-root Dockerfile; `docker build -t processor:dev services/processor` → success.
- [ ] **Commit:** `feat(processor): metrics/tracing + distroless image`

---

## Phase 5 — Deploy the application on Kubernetes (Helm)

**Deliverable:** the full system running on kind with a passing end-to-end smoke test.

### Task 5.1 — Helm umbrella chart

**Files:** Create `deploy/helm/purchases/` (`Chart.yaml`, `values.yaml`, `templates/gateway-deploy.yaml`, `gateway-svc.yaml`, `processor-deploy.yaml`, `secrets/`).

- [ ] Scaffold chart; gateway `Deployment` + `Service` (ClusterIP) + the NLB annotation block (used on EKS only); processor `Deployment`. Wire env from the CNPG `purchases-db-app` secret (`DB_RW_DSN`, `DB_RO_DSN`) and `KAFKA_BROKERS=purchases-kafka-bootstrap.kafka:9092`.
- [ ] `helm lint deploy/helm/purchases` → expected: 0 failures.
- [ ] **Commit:** `deploy: Helm umbrella chart (gateway + processor)`

### Task 5.2 — Probes, resources, spread, PDBs

**Files:** Modify the deployment templates; add `templates/pdb.yaml`.

- [ ] Add `startupProbe`/`livenessProbe` (`/healthz`)/`readinessProbe` (`/readyz`) (DESIGN §6.2).
- [ ] Add `resources` requests/limits per DESIGN §6.3 table (gateway 100m/256Mi, processor 250m/512Mi; mem request==limit).
- [ ] Add `topologySpreadConstraints` (zone + hostname, `maxSkew: 1`) and `PodDisruptionBudget` (`minAvailable`) for both services.
- [ ] `helm template ... | kubeconform -strict -` → expected: valid.
- [ ] **Commit:** `deploy: probes, resources, topology spread, PDBs`

### Task 5.3 — Autoscaling (HPA gateway, KEDA processor)

**Files:** Create `templates/gateway-hpa.yaml`, `templates/processor-scaledobject.yaml`. Add KEDA install to `Makefile`.

- [ ] `helm install keda kedacore/keda -n keda --create-namespace`.
- [ ] Gateway `HorizontalPodAutoscaler`: CPU target 70%, `minReplicas: 3` (the N3-safe floor), `maxReplicas: 12`.
- [ ] Processor KEDA `ScaledObject` using the `kafka` scaler on `purchases` consumer-group lag (`lagThreshold: "100"`), `minReplicaCount: 3`, `maxReplicaCount: 12`.
- [ ] `helm template` → kubeconform clean.
- [ ] **Commit:** `deploy: HPA (gateway) + KEDA lag ScaledObject (processor)`

### Task 5.4 — Deploy + end-to-end smoke

**Files:** Create `test/e2e/smoke.sh`.

- [ ] `make images && make kind-load` (load `gateway:dev`, `processor:dev` into kind).
- [ ] `helm install purchases deploy/helm/purchases -n app --create-namespace`; `kubectl -n app rollout status deploy/gateway deploy/processor`.
- [ ] `smoke.sh`: port-forward gateway → `POST /v1/purchases` (capture `event_id`) → poll `GET /v1/users/<id>/purchases` until the event appears (timeout 30s) → assert present.
- [ ] `make smoke` → expected: `SMOKE OK`.
- [ ] **Commit:** `test: end-to-end smoke (POST → eventual GET) passing on kind`

---

## Phase 6 — Cross-AZ cost & resilience hardening

**Deliverable:** topology-aware routing in place and a passing AZ+node failure drill.

### Task 6.1 — Topology-aware routing + rack-aware reads

**Files:** Modify `gateway-svc.yaml`, db read Service usage, processor env.

- [ ] Set `spec.trafficDistribution: PreferClose` on the gateway Service and ensure the gateway reads via `purchases-db-ro`; set processor `AZ` env from the node's zone via the downward API / `topology.kubernetes.io/zone` (init lookup) so `kgo.Rack` matches.
- [ ] Verify EndpointSlice hints: `kubectl -n app get endpointslice -o yaml | grep -A2 hints` → expected: zone hints present.
- [ ] **Commit:** `deploy: topology-aware routing + rack-aware consumer reads`

### Task 6.2 — Failure drill (N3)

**Files:** Create `test/resilience/az-node-drill.sh`.

- [ ] Script: start a low-rate `POST` loop; `kubectl cordon` + `drain` all nodes labeled `az-a` (simulate AZ loss); then `drain` one node in `az-b` (simulate +1 node); assert the `POST` loop keeps returning `202` and Kafka/PG stay writable; uncordon at end.
- [ ] `bash test/resilience/az-node-drill.sh` → expected: `DRILL OK: API available through AZ+node loss`.
- [ ] **Commit:** `test: AZ+node resilience drill (N3) passing`

---

## Phase 7 — Observability

**Deliverable:** dashboards + alerts + runbooks, with the stack itself on K8s.

### Task 7.1 — Install the stack

- [ ] `helm install kps prometheus-community/kube-prometheus-stack -n observability --create-namespace`.
- [ ] `helm install loki grafana/loki-stack -n observability`.
- [ ] Deploy an OpenTelemetry Collector (`otel/opentelemetry-collector` Helm chart) in `observability`.
- [ ] `kubectl -n observability get pods` → expected: Prometheus, Grafana, Loki, OTel collector `Running`.
- [ ] **Commit:** `deploy: kube-prometheus-stack + Loki + OTel collector`

### Task 7.2 — ServiceMonitors, dashboards, alerts, runbooks

**Files:** Create `deploy/helm/purchases/templates/servicemonitor.yaml`, `prometheusrule.yaml`, `deploy/observability/dashboards/purchases.json`, `docs/runbooks/*.md`.

- [ ] `ServiceMonitor` scraping gateway/processor `/metrics`; verify targets are `up` in Prometheus.
- [ ] `PrometheusRule` alerts from DESIGN §10 (consumer lag high, e2e lag p99 > SLO, under-replicated partitions, gateway 5xx, PG replication lag).
- [ ] Grafana dashboard: gateway RED, processor throughput, **consumer lag per partition**, **end-to-end lag histogram**.
- [ ] One runbook per alert under `docs/runbooks/` (diagnosis → mitigation, matching DESIGN §10 table).
- [ ] Trigger a synthetic lag (scale processor to 0, push events) → expected: the lag alert moves to `firing` in Prometheus, then clears when scaled back.
- [ ] **Commit:** `observability: ServiceMonitors, alerts, dashboards, runbooks`

---

## Phase 8 — CI/CD (GitHub Actions bonus)

**Deliverable:** PR validation (incl. kind smoke) and a deploy workflow.

### Task 8.1 — CI workflow

**Files:** Create `.github/workflows/ci.yaml`.

- [ ] Jobs: `lint` (`golangci-lint`, `hadolint`), `test` (`go test ./... -race`), `build` (both images), `deps-scan` (`google/osv-scanner` on `go.mod`/`go.sum`, fail on known vulns), `manifests` (`helm lint` + `kubeconform`), `smoke` (`helm/kind-action` → install operators (or mock) → `make smoke`).
- [ ] Push the branch; open a PR → expected: all jobs green.
- [ ] **Commit:** `ci: PR validation (lint, test, build, OSV scan, manifests, kind smoke)`

### Task 8.2 — CD workflow

**Files:** Create `.github/workflows/cd.yaml`.

- [ ] Trigger on push to `main` and `v*` tags; `permissions: id-token: write` for **OIDC into AWS** (no static keys); steps: configure AWS creds via OIDC → build & push images to **ECR by digest** → `helm upgrade --install` to EKS → post-deploy `smoke.sh` → on failure `helm rollback`.
- [ ] Add a `workflow_dispatch` manual trigger; document required `AWS_ROLE_ARN`, cluster name, and how a reviewer runs it in the README.
- [ ] Validate YAML with `actionlint` locally → expected: no errors.
- [ ] **Commit:** `cd: OIDC build/push to ECR + helm deploy to EKS + rollback`

---

## Phase 9 — Load & resilience validation

**Deliverable:** documented evidence the system meets N3 and N6.

### Task 9.1 — k6 burst load test

**Files:** Create `test/load.js`.

- [ ] k6 scenario: ramp to a burst of N RPS of `POST /v1/purchases`; `thresholds`: `http_req_failed < 1%`, `http_req_duration p(95) < 250ms`. Observe KEDA scaling the processor as lag forms.
- [ ] `make load` → expected: thresholds pass; record processor replica count vs lag.
- [ ] **Commit:** `test: k6 burst load test + thresholds`

### Task 9.2 — Record results

**Files:** Create `docs/validation/RESULTS.md`.

- [ ] Capture: smoke output, drill output (Phase 6.2), k6 summary, autoscaling graph screenshot, and a short note mapping each to N3/N6.
- [ ] **Commit:** `docs: validation results (load + resilience)`
- [ ] Open the PR for review; link `docs/DESIGN.md` and this plan.

---

## Self-Review Checklist

**1. Spec coverage — every requirement maps to a task:**

| Req | Covered by |
|---|---|
| F1 accept purchases | 3.2 |
| F2 publish to stream | 2.2, 3.2 |
| F3 consume/validate/persist | 4.1, 4.2 |
| F4 return user purchases | 3.3 |
| F5 persist user/username/price/timestamp | 1.3, 4.2 |
| N1 runs on Kubernetes | 1.x, 2.x, 5.x |
| N2 minimize cross-AZ cost | 2.2 (rack-aware), 6.1 (topology routing) |
| N3 survive 1 AZ + 1 node | 1.2, 2.2, 5.2 (spread/PDB), 6.2 (drill) |
| N4 health signals | 3.1, 4.1, 5.2 |
| N5 resource allocations | 5.2 |
| N6 autoscaling | 5.3, 9.1 |
| N7 observability | 3.4, 4.3, 7.x |
| Bonus CI/CD | 8.1, 8.2 |

**2. Placeholder scan:** no "TBD"/"handle edge cases"; every task names files, commands, expected output.

**3. Type/name consistency:** topic `purchases`; key `user_id`; env names `KAFKA_BROKERS`, `DB_RO_DSN`, `DB_RW_DSN`, `AZ`; PK `event_id` — identical across gateway (3.x), processor (4.x), and manifests (5.x).

---

## Execution Handoff

Two ways to execute this plan:

- **Subagent-driven (recommended):** dispatch each task to a fresh implementation subagent (`superpowers:subagent-driven-development`), which runs the TDD loop and commits. Keeps context small and each task independently verifiable. Run phases in order; Phases 1 and 2 can run in parallel after Phase 0; Phase 7 can overlap Phases 5–6.
- **Inline with checkpoints (`superpowers:executing-plans`):** work the checkboxes top-to-bottom in this session, pausing at each **milestone deliverable** (end of a phase) for your review before continuing.

Suggested first checkpoint: **end of Phase 5** (full system running on kind with the e2e smoke passing) — that's the first point where the whole design is demonstrably working end-to-end.
