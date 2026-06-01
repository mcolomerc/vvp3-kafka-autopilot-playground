---
name: project-context
description: VVP3 Autopilot Showcase — Kafka producer/consumer generating orders for Flink autoscaler demo
metadata:
  type: project
---

Project name: kafka-flink-autoscaler-vvp3
Working directory: /Users/mcolomerc/kafka-flink-autoscaler-vvp3/
Git repo: No (as of 2026-05-29)

Purpose: End-to-end demo showing VVP3 Autopilot (Flink autoscaler) scaling a streaming pipeline in response to load changes triggered via REST API.

**Why:** Showcase for Ververica customers/prospects demonstrating autoscaler sensitivity and responsiveness.
**How to apply:** All design decisions should favor demo clarity and operator control over raw production hardening.

Kafka topics:
- orders (producer writes Order JSON)
- order-metrics (Flink writes, consumer reads OrderMetrics JSON)

Kafka bootstrap pattern (K8s): kafka-{0,1,2}.kafka-headless.kafka.svc.cluster.local:9092

Serialization: JSON (StringSerializer/StringDeserializer) — no Schema Registry

Delivery semantics:
- Producer: at-least-once (acks=1), plain kafka-clients (not spring-kafka)
- Consumer: at-most-once (auto-commit, logging-only sink)

Stage completion status as of 2026-05-29:
- Stage 4 (Kafka Producer Spring Boot): COMPLETE
- Stage 5 (Kafka Consumer plain Java): COMPLETE
