# Deployment Guide

End-to-end steps to deploy the VVP3 Autopilot showcase on EKS.

**Stack**: Kafka 3.7 (KRaft, `kafka` ns) → Flink 1.20 job on VVP3 → `order-metrics` topic → Consumer logger  
**Registry**: `<AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/vvp3-showcase`  
**Region**: `eu-central-1`

---

## Prerequisites

- `kubectl` configured for the target EKS cluster
- `helm` v3+
- `docker` with BuildKit (Docker Desktop or `buildx`)
- AWS CLI authenticated (`aws sts get-caller-identity` should succeed)
- VVP3 accessible and credentials available
- Maven 3.9+ and Java 17+ on the build machine

---

## 1. Build Java Artifacts

Build all modules from the project root. The fat JARs land in each module's `target/` directory.

```bash
mvn clean package -DskipTests
```

Outputs:
- `flink-job/target/flink-job-1.0.0-SNAPSHOT.jar` — shaded fat JAR for VVP3
- `kafka-producer/target/kafka-producer-1.0.0-SNAPSHOT.jar`
- `kafka-consumer/target/kafka-consumer-1.0.0-SNAPSHOT.jar`

---

## 2. Build and Push Docker Images

### Authenticate with ECR

```bash
aws ecr get-login-password --region eu-central-1 \
  | docker login --username AWS --password-stdin \
    <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com
```

### Kafka Producer

```bash
docker build --platform linux/amd64 \
  -t <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/vvp3-showcase/kafka-producer:1.0.1 \
  kafka-producer/

docker push <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/vvp3-showcase/kafka-producer:1.0.1
```

### Kafka Consumer

```bash
docker build --platform linux/amd64 \
  -t <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/vvp3-showcase/kafka-consumer:1.0.0 \
  kafka-consumer/

docker push <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/vvp3-showcase/kafka-consumer:1.0.0
```

> `--platform linux/amd64` is required when building on an Apple Silicon (M-chip) Mac targeting amd64 EKS nodes.

---

## 3. Deploy Kafka

Apply all Kafka manifests in order. They all target the `kafka` namespace, which is created by the first file.

```bash
kubectl apply -f k8s/kafka/00-namespace.yaml
kubectl apply -f k8s/kafka/01-configmap.yaml
kubectl apply -f k8s/kafka/02-statefulset.yaml
kubectl apply -f k8s/kafka/03-services.yaml
```

Wait for all 3 brokers to become ready (typically 2–3 minutes on a cold EKS cluster):

```bash
kubectl rollout status statefulset/kafka -n kafka --timeout=5m
```

### Create Topics

```bash
kubectl apply -f k8s/kafka/05-topics-job.yaml
```

Verify the Job completed and topics exist:

```bash
kubectl wait --for=condition=complete job/kafka-topics-init -n kafka --timeout=2m

kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected output:
```
order-metrics
orders
```

---

## 4. Deploy Monitoring (Prometheus + Grafana)

Add the Helm repo and install the stack:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace \
  -f k8s/monitoring/kube-prometheus-stack-values.yaml
```

Apply the Grafana dashboard ConfigMap:

```bash
kubectl apply -f k8s/monitoring/grafana-dashboard-configmap.yaml -n monitoring
```

Access Grafana and Prometheus via port-forward:

```bash
kubectl port-forward -n monitoring svc/monitoring-grafana 3000:80 &
kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-prometheus 9090:9090 &
```

| Service | URL | Credentials |
|---|---|---|
| Grafana | `http://localhost:3000` | `admin` / `autopilot-showcase` |
| Prometheus | `http://localhost:9090` | — |

---

## 5. Deploy Kafka Producer

```bash
kubectl apply -f k8s/producer/ -n kafka
```

This creates the ConfigMap, Deployment (scaled to 1 replica), and ClusterIP Service in the `kafka` namespace.

Verify the pod is ready:

```bash
kubectl rollout status deployment/kafka-producer -n kafka --timeout=2m
kubectl logs -n kafka -l app=kafka-producer --tail=5
```

Expected log line every 10 seconds:
```
Produced 5000 messages at 500 msg/s (target: 500 msg/s, errors: 0)
```

To stop the producer:
```bash
kubectl scale deployment/kafka-producer -n kafka --replicas=0
```

To restart it:
```bash
kubectl scale deployment/kafka-producer -n kafka --replicas=1
```

### Producer Throughput Configuration

#### Default values

| Parameter | Default | Description |
|---|---|---|
| `BASE_THROUGHPUT_MSG_SEC` | `500` | Steady-state messages per second |
| `PEAK_MULTIPLIER` | `10` | Peak rate = base × multiplier (e.g. 500 × 10 = 5 000 msg/s) |
| `PEAK_DURATION_SEC` | `300` | How long to sustain the peak (seconds) |
| `RAMP_UP_SEC` | `30` | Linear ramp-up and ramp-down duration (seconds) |

#### Option A — Change defaults before deploying (ConfigMap)

Edit `k8s/producer/configmap.yaml` before running `kubectl apply`:

```yaml
data:
  BASE_THROUGHPUT_MSG_SEC: "200"   # slower steady state
  PEAK_MULTIPLIER: "5"             # 5× instead of 10×
  PEAK_DURATION_SEC: "120"         # 2-minute peak
  RAMP_UP_SEC: "15"                # faster ramp
```

Then apply (or re-apply if already deployed):

```bash
kubectl apply -f k8s/producer/configmap.yaml -n kafka
kubectl rollout restart deployment/kafka-producer -n kafka
```

#### Option B — Change values at runtime (REST API)

Port-forward the producer service first:

```bash
kubectl port-forward -n kafka svc/kafka-producer 8888:8888
```

Send a `POST /api/config` with any subset of fields — omitted fields keep their current value:

```bash
# Change only the base throughput (applies immediately, no restart)
curl -X POST http://localhost:8888/api/config \
  -H "Content-Type: application/json" \
  -d '{"baseThroughput": 200}'

# Reconfigure the full peak shape
curl -X POST http://localhost:8888/api/config \
  -H "Content-Type: application/json" \
  -d '{
    "baseThroughput": 300,
    "peakMultiplier": 5,
    "peakDurationSeconds": 120,
    "rampUpSeconds": 15
  }'

# Verify current config and counters
curl http://localhost:8888/api/status
```

Example response:
```json
{
  "currentThroughput": 300,
  "baseThroughput": 300,
  "peakActive": false,
  "peakMultiplier": 5,
  "peakDurationSeconds": 120,
  "rampUpSeconds": 15,
  "messagesProduced": 84200,
  "errorCount": 0
}
```

> Runtime changes via the API survive for the life of the pod but are not persisted. To make them permanent, update the ConfigMap and restart the deployment (Option A).

---

## 6. Deploy Kafka Consumer

Before deploying, update the image in `k8s/consumer/deployment.yaml` to the tag you pushed in step 2:

```yaml
image: <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/vvp3-showcase/kafka-consumer:1.0.0
imagePullPolicy: IfNotPresent
```

Then apply:

```bash
kubectl apply -f k8s/consumer/ -n kafka
```

The consumer starts from `earliest` offset and logs each `order-metrics` window it receives. It will only show output once the Flink job (step 7) is running.

```bash
kubectl logs -n kafka -l app=kafka-consumer -f
```

---

## 7. Deploy Flink Job on VVP3

The full deployment spec is stored in `k8s/vvp/orders-job-deployment.yaml`. It includes the VVP3 3.1 metrics bug workaround (see note below) and all runtime configuration.

### 7a. Upload the JAR Artifact

```bash
vvctl create artifact flink-job/target/flink-job-1.0.0-SNAPSHOT.jar
```

### 7b. Create the Deployment

```bash
vvctl create deployment -f k8s/vvp/orders-job-deployment.yaml
```

### 7c. Start the Deployment

```bash
vvctl start deployment orders-job
```

> **VVP3 3.1 metrics bug** (Portal ticket 45275608473): VVP3 3.1 injects Prometheus/JMX reporters using the deprecated `metrics.reporter.<name>.class` syntax, which Flink 1.20 silently ignores. The deployment spec includes the workaround — two `factory.class` overrides in `flinkConfiguration`. Remove them once VVP3 ships the fix.

Monitor until the job reaches `RUNNING` state:

```bash
vvctl deployments get orders-job --watch
```

Verify in logs that the Kafka source is consuming from the `orders` topic:
```
Cluster ID: MkU3OEVBNTcwNTJENDM2Qk
```

---

## 8. Verify End-to-End Pipeline

With all components running, confirm the full pipeline is active:

```bash
# Producer sending messages
kubectl logs -n kafka -l app=kafka-producer --tail=3

# Consumer receiving Flink output
kubectl logs -n kafka -l app=kafka-consumer --tail=10
```

Consumer output should show lines like:
```
[window=1748530800000-1748530830000] category=ELECTRONICS region=NORTH orders=47 revenue=4823.50 avg=102.63 customers=44
```

Check consumer lag to confirm Flink is keeping up:

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group flink-orders-consumer
```

---

## 9. Trigger Autopilot Scale-Up

Port-forward the producer service once — all commands below reuse this tunnel:

```bash
kubectl port-forward -n kafka svc/kafka-producer 8888:8888 &
```

### Example: 25 000 → 100 000 msg/s peak scenario

This scenario starts at a steady 25 000 msg/s base, then ramps to 100 000 msg/s (4× multiplier) over 60 seconds, sustains the peak for 5 minutes, and ramps back down. It is designed to push Flink well into backpressure and give Autopilot enough time to observe the lag, trigger a scale-out, and then scale back in after the peak ends.

**Step 1 — set the base throughput and peak shape**

```bash
curl -X POST http://localhost:8888/api/config \
  -H "Content-Type: application/json" \
  -d '{
    "baseThroughput":     25000,
    "peakMultiplier":     4,
    "peakDurationSeconds": 300,
    "rampUpSeconds":      60
  }'
```

**Step 2 — confirm the configuration was applied**

```bash
curl http://localhost:8888/api/status
```

Expected response (peak not yet active):

```json
{
  "currentThroughput": 25000,
  "baseThroughput": 25000,
  "peakActive": false,
  "peakMultiplier": 4,
  "peakDurationSeconds": 300,
  "rampUpSeconds": 60,
  "messagesProduced": 0,
  "errorCount": 0
}
```

**Step 3 — start the peak**

```bash
curl -X POST http://localhost:8888/api/peak/start
```

The producer now ramps from 25 000 to 100 000 msg/s over 60 seconds (~1 250 msg/s per tick), holds the peak for 5 minutes, then ramps back down automatically.

**Step 4 — watch Autopilot react**

Open the Grafana dashboard at `http://localhost:3000` and observe:

| What to watch | Expected behaviour |
|---|---|
| **Producer → Send Rate** | Climbs from 25 000 to 100 000 msg/s during ramp-up |
| **Producer → Peak Active** | Turns red (`PEAK`) at start, green (`INACTIVE`) after ramp-down |
| **Kafka Inbound Traffic → Orders Inbound Rate** | Mirrors the send rate with a short broker-side lag |
| **Kafka Inbound Traffic → Flink Consumer Lag** | Grows as Flink falls behind; triggers Autopilot scale-out |
| **Autopilot Scaling → Active TaskManagers** | Discrete step-up as VVP3 adds TaskManagers |
| **Flink Processing → Backpressure Ratio** | Spikes above 25 % during the overload window |
| **Autopilot Scaling → Active TaskManagers** | Steps back down after the peak ends and lag drains |

**Step 5 — stop the peak early (optional)**

```bash
curl -X POST http://localhost:8888/api/peak/stop
```

The producer immediately begins ramping back to 25 000 msg/s over the configured `rampUpSeconds`.

**Step 6 — reset to default base throughput after the demo**

```bash
curl -X POST http://localhost:8888/api/config \
  -H "Content-Type: application/json" \
  -d '{
    "baseThroughput":     500,
    "peakMultiplier":     10,
    "peakDurationSeconds": 300,
    "rampUpSeconds":      30
  }'
```

---

## 10. Scenario Reporting

The Grafana dashboard is useful for real-time observation, but two additional tools let you capture the numbers from each run for comparison or documentation.

### Prerequisites

```bash
# Prometheus port-forward (keep open in a separate terminal)
kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-prometheus 9090:9090 &

# Grafana port-forward must be running (see step 4)
```

### Mark scenario boundaries with Grafana annotations

Run these immediately before and after a peak to stamp vertical marker lines on every dashboard panel. This makes it easy to zoom into the exact scenario window later.

```bash
# At scenario start
./scripts/annotate-scenario.sh start "25k→100k peak"

# At scenario end (or after ramp-down completes)
./scripts/annotate-scenario.sh end "25k→100k peak"
```

Annotations appear as labelled vertical lines in Grafana. Filter them via the dashboard's **Annotations** menu (tag: `scenario`).

Custom Grafana URL or credentials:

```bash
GRAFANA_URL=http://localhost:3000 \
GRAFANA_PASSWORD=mypassword \
  ./scripts/annotate-scenario.sh start "baseline run"
```

---

### Generate a text report after a run

After a scenario completes (peak has ramped back down), run the reporting script against the look-back window that covers the run:

```bash
./scripts/report-scenario.sh --duration 15 --label "25k→100k peak, 4x multiplier"
```

Example output:

```
╔══════════════════════════════════════════════════╗
  VVP3 Autopilot — Scenario Report
  Scenario : 25k→100k peak, 4x multiplier
  Window   : last 15 minutes
  Generated: 2026-05-30 10:32:00 UTC
╚══════════════════════════════════════════════════╝

PRODUCER
──────────────────────────────────────────────────
  Base throughput (msg/s):             25000
  Peak target rate (msg/s):            100000
  Peak send rate – client (msg/s):     99842
  Avg send rate – client (msg/s):      61204
  Total messages produced:             55082400
  Total send errors:                   0
  Peak error rate (errors/s):          0.00
  Avg request latency (ms):            3.21
  Peak request latency (ms):           18.47

KAFKA BROKER
──────────────────────────────────────────────────
  Peak inbound rate (msg/s):           99961
  Peak consumer lag (messages):        284720
  Current consumer lag (messages):     0

FLINK PROCESSING
──────────────────────────────────────────────────
  Peak records processed (rec/s):      98430
  Avg records processed (rec/s):       59812
  Peak backpressure (%):               87.3
  Avg backpressure (%):                31.6
  Peak checkpoint duration (ms):       4820
  Avg checkpoint duration (ms):        1240

AUTOPILOT SCALING
──────────────────────────────────────────────────
  TaskManagers at start (min):         1
  TaskManagers at peak (max):          6
  TaskManagers now:                    2
```

### Save results to CSV for comparison across runs

Use `--csv` to append a single data line to a results file. The script always emits the header row first, so redirect the first run with `>` and subsequent runs with `>>` (skipping the header):

```bash
# First run — create the file with header
./scripts/report-scenario.sh --duration 15 --label "baseline 500 msg/s" --csv > results.csv

# Subsequent runs — append data only (strip the header line)
./scripts/report-scenario.sh --duration 15 --label "25k→100k peak" --csv \
  | tail -n +2 >> results.csv

./scripts/report-scenario.sh --duration 15 --label "50k→200k peak" --csv \
  | tail -n +2 >> results.csv
```

The CSV contains 24 columns covering all key metrics: timestamps, throughput numbers, consumer lag, Flink processing rate, backpressure, checkpoint duration, and TaskManager scale counts. Open it in a spreadsheet to compare runs side by side.

---

## Teardown

```bash
# Stop producer and consumer
kubectl scale deployment/kafka-producer kafka-consumer -n kafka --replicas=0

# Stop Flink job on VVP3
vvctl deployments stop orders-job

# Remove monitoring
helm uninstall monitoring -n monitoring

# Remove Kafka (WARNING: deletes all data including PVCs)
kubectl delete -f k8s/kafka/
kubectl delete pvc -n kafka --all
```
