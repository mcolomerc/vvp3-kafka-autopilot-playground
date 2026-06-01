---
name: project-kafka-deployment
description: Kafka 3.7 KRaft cluster deployed in k8s/kafka/ for the VVP3 Autopilot Showcase project
metadata:
  type: project
---

Stage 2 of the VVP3 Autopilot Showcase project. A 3-node Apache Kafka 3.7 KRaft cluster (combined broker+controller mode) is deployed in the `kafka` namespace under `k8s/kafka/`.

**Why:** The cluster provides the event streaming backbone for a Kafka+Flink autoscaler demonstration using Ververica Platform 3.

**How to apply:** Follow `k8s/kafka/README.md`. Key bootstrap addresses:
- Same namespace: `kafka:9092`
- Cross-namespace: `kafka.kafka.svc.cluster.local:9092`
- Full broker list: `kafka-0.kafka-headless.kafka.svc.cluster.local:9092,...`

Key configuration choices:
- KRaft combined mode (no ZooKeeper). Cluster-id: `MkU3OEVBNTcwNTJENDM2Qk` — must not change.
- Init container uses `sed` (not `envsubst`) to replace `__NODE_ID__` and `__POD_NAME__` placeholders; `envsubst`/`gettext-base` is not installed in `apache/kafka:3.7.0`.
- `podManagementPolicy: Parallel` — all 3 brokers start concurrently, quorum forms when majority are up.
- `auto.create.topics.enable=false` — topics created by Job `kafka-create-topics` (05-topics-job.yaml).
- Topics: `orders` (12 partitions, RF=3) and `order-metrics` (6 partitions, RF=3).
- PVC per broker: 10Gi, default StorageClass, named `data-kafka-<ordinal>`.
- JVM: `-Xms1g -Xmx2g`, G1GC, 20 ms pause target.
- JMX on port 9999 (no auth, in-cluster scraping only).

**How to apply:** See [[project-vvp3-autopilot-showcase]] for broader context.
