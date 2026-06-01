# VVP3 Autopilot · Kafka–Flink Showcase

A realistic, end-to-end streaming pipeline designed to demonstrate and validate the **VVP3 Autopilot** auto-scaling feature on [Ververica Platform 3](https://www.ververica.com).

The showcase generates configurable Order events, processes them through a stateful Flink job, and produces business metrics — all while exposing the signals that VVP3 Autopilot needs to scale the job up and down in response to real load changes.

---

## What it demonstrates

| Capability | How it is shown |
|---|---|
| **Autopilot scale-up** | REST-triggered traffic peak saturates Flink task CPU (>80% `busyTimeMsPerSecond`) → Autopilot increases parallelism and adds TaskManagers |
| **Autopilot scale-down** | After the peak Autopilot observes sustained low utilisation (<20%) and stepwise reduces parallelism |
| **Adaptive resource tuning** | `resourceTuning.mode: autopilot` enabled on the deployment so Autopilot also adjusts per-TM CPU/memory |
| **End-to-end observability** | Grafana dashboard with 6 rows covering Producer, Kafka broker, Flink processing, Autopilot scaling events, resource utilisation, and pod startup latency |

---

## Stack

```
Kafka Producer  ──► Kafka 3.7 (KRaft)  ──► Flink 1.20 on VVP3  ──► Kafka Consumer
     │                   (12 partitions)        (parallelism=4)         (logger)
     │                                               │
     └─── REST API (peak simulation) ───────────────┘
                                         │
                              Prometheus + Grafana
```

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 3.7, KRaft combined mode, 3-node StatefulSet |
| Stream processing | Apache Flink 1.20, deployed on VVP3 (`vera-4.5-jdk11-flink-1.20`) |
| Autoscaler | VVP3 Autopilot (`mode: active`, Adaptive resource tuning) |
| Producer | Spring Boot 3.2, Guava `RateLimiter`, REST API on port 8888 |
| Consumer | Lightweight Java consumer that logs aggregated windows |
| Monitoring | kube-prometheus-stack (Prometheus + Grafana), Kafka JMX Exporter |
| Infrastructure | Kubernetes (EKS), Helm |

---

## Components

### Kafka Producer

A Spring Boot application that sends `Order` events to the `orders` topic at a configurable rate. It exposes a REST API to control throughput at runtime without restarting the pod.

- **Base rate**: 500 msg/s default (configurable via `BASE_THROUGHPUT_MSG_SEC`)
- **Peak simulation**: `POST /api/peak/start` ramps to `base × peakMultiplier` over `rampUpSeconds`, sustains for `peakDurationSeconds`, then ramps back
- **Metrics**: Exposes `kafka.producer.*` client metrics and custom application gauges (`producer.throughput.*`, `producer.peak.active`) via `/actuator/prometheus`

### Flink Orders Job

A stateful Flink DataStream job that consumes from `orders` and publishes windowed metrics to `order-metrics`.

**Pipeline:**
```
KafkaSource<Order>
  → WatermarkStrategy (bounded out-of-orderness 5s)
  → filter(orderTimestamp > 0)
  → map(cpu-load simulation — Math.sin/cos, makes job compute-bound for Autopilot)
  → keyBy(category + "#" + region)          // 30 keys (6 categories × 5 regions)
  → TumblingEventTimeWindows(30s)
  → aggregate(OrderMetricsAggregateFunction, OrderMetricsProcessWindowFunction)
  → KafkaSink<OrderMetrics>(AT_LEAST_ONCE)
```

Key design decisions:
- **No `env.setParallelism()` in code** — VVP3 controls parallelism via the deployment spec, allowing Autopilot to adjust it dynamically
- **Stable operator UIDs** on every operator for savepoint-compatible rescaling
- **CPU-load map step** ensures the job becomes compute-bound at moderate throughput, giving Autopilot's `busyTimeMsPerSecond` signal a clear trigger point

### Kafka Consumer

A minimal Java consumer that reads from `order-metrics` and logs each window result as a structured line. Serves as an end-to-end confirmation that the full pipeline is working.

### Monitoring

The monitoring stack consists of:

- **Prometheus** scraping four custom targets: Flink TaskManagers (port 9999, VVP3-injected `promappmgr` reporter), Flink JobManagers (port 9999, checkpoint metrics), the Kafka producer actuator (port 8888), and the Kafka JMX Exporter (broker-side topic and lag metrics)
- **Grafana dashboard** with six rows: Producer, Kafka Inbound Traffic, Flink Processing, Autopilot Scaling, Resource Utilisation, Autopilot Insights
- **Kafka JMX Exporter** translating broker JMX beans to Prometheus format (topic ingest rate, consumer lag, under-replicated partitions)

All panels use `pod=~".*taskmanager.*"` filters so the Active TaskManagers count and resource panels show only TM pods, not the JobManager.

### Scenario Tooling

Two shell scripts in `scripts/` support structured demo runs:

- **`annotate-scenario.sh`** — posts start/end annotations to Grafana, marking the scenario window on every panel timeline
- **`report-scenario.sh`** — queries Prometheus after a run and prints a structured report (producer throughput, peak consumer lag, Flink processing rate, backpressure, checkpoint duration, min/max TM count). Supports `--csv` for multi-run comparison files

---

## VVP3 Autopilot behaviour

VVP3 Autopilot monitors `busyTimeMsPerSecond` and `isBackPressured` on Flink tasks:

- **Scale-up** triggers when tasks are >80% busy for several consecutive metric windows (~2–5 min). Autopilot stops the current job, increments parallelism, and starts a new job — visible as a new job ID and a discrete step on the Active TaskManagers panel.
- **Scale-down** triggers after sustained low utilisation (<20%) for a longer observation window (~15–30 min). The same stop-restart mechanism applies, stepping down parallelism incrementally.

Each rescaling event creates a **new Flink job** because Flink parallelism is fixed at submission time. VVP3 handles the lifecycle transparently, optionally resuming from a savepoint.

> **VVP3 3.1 known issue**: The default Flink configuration injected by VVP3 3.1 uses the deprecated `metrics.reporter.<name>.class` syntax. Flink 1.20 silently ignores this, so nothing listens on port 9999 out of the box. The deployment spec in `k8s/vvp/orders-job-deployment.yaml` includes the `factory.class` workaround (Portal ticket 45275608473). Remove those two lines once VVP3 ships the fix.

---

## Data model

**Order** (producer → `orders` topic):

| Field | Type | Description |
|---|---|---|
| `orderId` | UUID string | Unique order identifier |
| `customerId` | string | Customer (10,000 pool) |
| `category` | enum | ELECTRONICS · BOOKS · CLOTHING · HOME · SPORTS · FOOD |
| `region` | enum | NORTH · SOUTH · EAST · WEST · CENTRAL |
| `quantity` | int | 1–10 |
| `unitPrice` | double | Category-dependent range |
| `totalAmount` | double | quantity × unitPrice |
| `orderTimestamp` | long (epoch ms) | Event time used for watermarks |

**OrderMetrics** (Flink → `order-metrics` topic):

| Field | Description |
|---|---|
| `windowStart` / `windowEnd` | 30-second tumbling window boundaries |
| `category` + `region` | Window key (30 combinations) |
| `orderCount` | Total orders in window |
| `totalRevenue` / `avgOrderValue` | Aggregated financials |
| `maxOrderValue` / `minOrderValue` | Per-window extremes |
| `uniqueCustomers` | Distinct customer count |

---

## Deployment

Full step-by-step instructions — including building images, deploying Kafka, configuring the monitoring stack, deploying on VVP3, and running peak scenarios — are in **[DEPLOYMENT.md](./DEPLOYMENT.md)**.

Quick reference:
1. [Build Java artifacts](./DEPLOYMENT.md#1-build-java-artifacts)
2. [Build and push Docker images](./DEPLOYMENT.md#2-build-and-push-docker-images)
3. [Deploy Kafka](./DEPLOYMENT.md#3-deploy-kafka)
4. [Deploy monitoring](./DEPLOYMENT.md#4-deploy-monitoring-prometheus--grafana)
5. [Deploy producer](./DEPLOYMENT.md#5-deploy-kafka-producer)
6. [Deploy consumer](./DEPLOYMENT.md#6-deploy-kafka-consumer)
7. [Deploy Flink job on VVP3](./DEPLOYMENT.md#7-deploy-flink-job-on-vvp3)
8. [Verify end-to-end pipeline](./DEPLOYMENT.md#8-verify-end-to-end-pipeline)
9. [Trigger Autopilot scale-up](./DEPLOYMENT.md#9-trigger-autopilot-scale-up)
10. [Scenario reporting](./DEPLOYMENT.md#10-scenario-reporting)

---

## Requirements

- Kubernetes cluster (EKS tested) with at least 3 nodes
- VVP3 installed with a `vvp-deploy` deployment target
- ECR (or any container registry reachable from the cluster)
- `kubectl`, `helm`, `mvn` (Java 17+), `docker` with BuildKit
- `vvctl` CLI configured for your VVP3 instance
- `jq` (for the reporting script)
