# ADR-0003 — Cross-AZ data-transfer cost minimization

- **Status:** Accepted
- **Date:** 2026-06-15
- **Deciders:** _(candidate)_
- **Context tags:** cost, networking, topology-aware routing, Kafka rack awareness

## Context

Requirement N2: "Traffic between frontend and backend components must minimize cross
availability zone data transfer costs." On AWS, in-region **cross-AZ EC2↔EC2** traffic
costs **$0.01/GB each direction**; same-AZ is free. With a 3-AZ, multi-replica deployment,
naive routing sends a large share of traffic across AZs. We want every high-volume hop to
stay same-AZ *except where correctness forbids it*.

## Decision

Apply locality at three layers, and explicitly accept the two cross-AZ hops that are
required for correctness.

### 1. Synchronous service-to-service hops → Topology Aware Routing

- Set `Service.spec.trafficDistribution: PreferClose` (K8s ≥1.31; older clusters use the
  `service.kubernetes.io/topology-mode: Auto` annotation). EndpointSlice hints then steer
  callers to **same-AZ endpoints** when healthy ones exist.
- Applies to: client→gateway, gateway→Postgres **read replica**, processor→Kafka broker,
  processor→Postgres.

### 2. Read path locality → one Postgres read replica per AZ

- The gateway's read queries (F4) resolve to the **local-AZ replica** via topology-aware
  routing → reads, the highest-volume query traffic, stay same-AZ and off the primary.

### 3. Kafka consumer reads → rack-aware fetch-from-closest-replica (KIP-392)

- Set `broker.rack` = AZ and enable `RackAwareReplicaSelector`. Consumers fetch from a
  **same-AZ follower replica** instead of always hitting the (possibly remote) leader →
  eliminates cross-AZ **consume** traffic, typically the dominant variable cost.

### Accepted (irreducible) cross-AZ hops

- **Kafka replication (RF=3, one replica per AZ):** mandatory to survive an AZ loss (N3);
  crosses AZs by definition. Accepted as a durability cost.
- **Producer→leader writes & processor→primary writes:** must reach the single leader/
  primary; ~2/3 of pods will be cross-AZ for writes. Bounded and far smaller than read
  fan-out.

## Note on managed alternatives

A managed regional queue (SQS) would make the messaging hop cross-AZ-free outright (see
[ADR-0001](0001-messaging-kafka-vs-sqs.md)) — we chose Kafka on functional grounds and
therefore *engineer* the locality above instead of getting it for free.

## Consequences

- (+) High-volume reads/consumes kept same-AZ; cross-AZ spend reduced to the durability-
  required minimum.
- (−) Topology-aware routing can skew load if an AZ's endpoints are unbalanced — it falls
  back to cross-AZ for correctness, so it's safe but worth watching in dashboards.
- Monitor cross-AZ bytes (VPC flow logs / CUR) to confirm the locality measures hold under
  real traffic.
