# Kafka KRaft Deployment

Apache Kafka 3.7 in KRaft (ZooKeeper-free) mode — 3-node cluster, combined broker+controller.

## Apply order

```bash
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-configmap.yaml
kubectl apply -f 02-statefulset.yaml
kubectl apply -f 03-services.yaml

# Wait for all 3 pods to be Running and Ready
kubectl rollout status statefulset/kafka -n kafka

kubectl apply -f 05-topics-job.yaml
kubectl wait --for=condition=complete job/kafka-create-topics -n kafka --timeout=120s
```

## Bootstrap addresses

| Use case | Address |
|---|---|
| Same namespace (`kafka`) | `kafka:9092` |
| Other namespaces (e.g. `flink`) | `kafka.kafka.svc.cluster.local:9092` |
| Full broker list (in-cluster) | `kafka-0.kafka-headless.kafka.svc.cluster.local:9092,kafka-1.kafka-headless.kafka.svc.cluster.local:9092,kafka-2.kafka-headless.kafka.svc.cluster.local:9092` |

## Verify cluster health

```bash
# Check pod status
kubectl get pods -n kafka -l app=kafka

# Describe the StatefulSet
kubectl describe statefulset kafka -n kafka

# List topics (from within the cluster or via kubectl exec)
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092

# Check topic details
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server kafka:9092

# Check broker metadata
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# Check controller quorum status (KRaft)
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka:9092 describe --status
```

## Topics created

| Topic | Partitions | Replication Factor |
|---|---|---|
| `orders` | 12 | 3 |
| `order-metrics` | 6 | 3 |
