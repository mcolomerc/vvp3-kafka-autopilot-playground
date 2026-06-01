---
name: project-vvp3-showcase
description: VVP3 Autopilot Showcase project — Flink job structure, versions, and conventions
metadata:
  type: project
---

This is the VVP3 Autopilot Showcase project at /Users/mcolomerc/kafka-flink-autoscaler-vvp3.

**Why:** Demonstrates VVP3 Autopilot autoscaling under peak load (5 000 msg/s).

**How to apply:** When working on any module in this repo, respect these decisions below.

## Key versions
- Flink: 1.20.0 (flink-streaming-java, flink-clients provided; not bundled)
- Kafka connector: flink-connector-kafka 3.3.0-1.20 (bundled in fat JAR)
- Kafka clients: 3.7.0 (OffsetResetStrategy still valid, AutoOffsetResetStrategy also exists)
- Java: 11 (source/target)
- Jackson: 2.17.1 (via jackson-bom)
- JUnit: 5.10.2

## Module layout
- flink-job — Flink 1.20 Java streaming job (maven-shade fat JAR, mainClass = OrdersJob)
- kafka-producer — load generator
- kafka-consumer — metrics consumer

## Flink job architecture (Stage 3, implemented 2026-05-29)
Pipeline: KafkaSource<Order>(topic=orders) → WatermarkStrategy(BoundedOutOfOrderness 5 s) →
filter(orderTimestamp > 0) → keyBy(category + "#" + region) →
TumblingEventTimeWindows(30 s) → aggregate(OrderMetricsAggregateFunction,
OrderMetricsProcessWindowFunction) → KafkaSink<OrderMetrics>(topic=order-metrics, AT_LEAST_ONCE)

State backend: HashMapStateBackend
Checkpointing: 60 s interval, EXACTLY_ONCE, 5 min timeout, retain-on-cancellation
Restart strategy: fixed delay, 3 attempts, 10 s delay
Prometheus metrics reporter configured programmatically on port 9249

## Operator UIDs (required for VVP3 savepoint resume)
- kafka-orders-source
- filter-valid-timestamp
- orders-window-aggregation
- kafka-order-metrics-sink

## Environment variables
- KAFKA_BOOTSTRAP_SERVERS (default: kafka:9092)
- FLINK_PARALLELISM (default: 1)

## Key design decisions
- OrderDeserializationSchema uses KafkaRecordDeserializationSchema<Order> (Flink 1.20 API)
- ObjectMapper is transient + lazily initialised in both serialisation schemas (standard pattern)
- OrderAccumulator uses Set<String> for exact uniqueCustomers count within a window
- AggregateFunction.merge() is implemented for future session window support
- filter lambda has .uid() assigned — needed because Flink 1.20 allows uid on filter operators
- DeliveryGuarantee.AT_LEAST_ONCE used for simplicity (no transactional ID needed)
