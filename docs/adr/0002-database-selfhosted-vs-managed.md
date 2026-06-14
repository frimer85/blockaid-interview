# ADR-0002 — Database: self-hosted Postgres vs managed RDS

- **Status:** Accepted (with a documented production caveat)
- **Date:** 2026-06-15
- **Deciders:** _(candidate)_
- **Context tags:** persistence, HA, cross-AZ, reviewer-runnability

## Context

The system persists purchases (F3/F5) and serves per-user reads (F4). It must survive a
concurrent AZ + node failure (N3), run on Kubernetes (N1), keep reads cheap across AZs
(N2), and a reviewer must be able to run the whole thing from the instructions. The data
is small, append-only, and read by `user_id` — a relational store fits cleanly.

Options weighed: **self-hosted PostgreSQL on EKS via the CloudNativePG operator** vs
**Amazon RDS for PostgreSQL (Multi-AZ)**.

## Comparison

| Dimension | CloudNativePG (in-cluster) | RDS Multi-AZ (managed) |
|---|---|---|
| "Runs on Kubernetes" intent (N1) | ✅ native | ⚠️ external dependency |
| Reviewer runs it locally | ✅ kind/minikube, no cloud acct | ❌ requires AWS |
| HA / automated failover | operator-managed (Patroni-style) | AWS-managed |
| Backups / PITR | operator + object store (Barman) | fully managed |
| Per-AZ read-replica placement (N2) | ✅ explicit pod placement | ⚠️ less placement control |
| Survives AZ+node (N3) | yes, with 4-instance layout we control | yes, AWS-handled |
| Operational burden | **higher** (we run it) | **lower** |
| Demonstrates stateful-on-K8s skill | ✅ strongly | ✗ |

## Decision

**Design and document around CloudNativePG, while writing the application to be agnostic
to the database endpoint so RDS is a drop-in for production.**

- For *this exercise*: CloudNativePG keeps the entire system runnable on a laptop (a stated
  submission requirement), demonstrates operating a stateful workload on K8s (the point of
  the assignment), and gives direct control over **one read replica per AZ** for the
  cross-AZ-cheap read path.
- For a *real production launch*: I would default to **RDS Multi-AZ** to offload HA,
  patching, and backups — the operational savings outweigh the loss of placement control,
  and the app needs no code change (just a different connection string + secret).

Topology: **1 primary + 3 replicas across 3 AZs**, synchronous commit quorum `ANY 1`
(durable across an AZ loss without full 3-replica latency). Sizing justified in
[DESIGN §9.3](../DESIGN.md#93-postgresql).

## Consequences

- (+) Self-contained, reviewer-runnable, controllable replica placement, portable to RDS.
- (−) We own Postgres HA/backup operations in the in-cluster design (the operator automates
  most of it, but it's still ours to monitor).
- The read path depends on replicas being healthy; replication lag is monitored and alerted
  ([DESIGN §11](../DESIGN.md#11-observability-n7)).
