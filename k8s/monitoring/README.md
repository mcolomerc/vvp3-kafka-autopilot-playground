# Deploy monitoring stack

## 1. Add Helm repo
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

## 2. Deploy kube-prometheus-stack
```bash
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace \
  -f k8s/monitoring/kube-prometheus-stack-values.yaml
```

## 3. Deploy Kafka JMX exporter
```bash
kubectl apply -f k8s/monitoring/kafka-jmx-exporter.yaml
```

## 4. Deploy Grafana dashboard
```bash
kubectl apply -f k8s/monitoring/grafana-dashboard-configmap.yaml
```

## 5. Access Grafana
```bash
# Option A — NodePort (exposed on port 32000 of any cluster node):
open http://<node-ip>:32000

# Option B — port-forward:
kubectl port-forward -n monitoring svc/monitoring-grafana 3000:80
open http://localhost:3000
```

Default credentials: `admin` / `autopilot-showcase`

Open the **VVP3 Autopilot Showcase** dashboard (folder: *VVP3 Autopilot*).

## 6. Configure Flink metrics in VVP3 deployment

Add the following keys to `flinkConfiguration` in your VVP3 `Deployment` spec so TaskManagers expose Prometheus metrics on port 9249:

```yaml
flinkConfiguration:
  metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
  metrics.reporter.prom.port: "9249"
```

The Prometheus scrape config in `kube-prometheus-stack-values.yaml` already targets pods labelled `component=taskmanager` in the `vvp` namespace on that port.

## Architecture overview

```
Kafka (namespace: kafka)
  └─ JMX port 9999
       └─ kafka-jmx-exporter Deployment (port 5556)
            └─ ServiceMonitor kafka-jmx (namespace: monitoring)
                 └─ Prometheus scrapes every 5 s

Flink TaskManagers (namespace: vvp)
  └─ PrometheusReporter port 9249 (per pod)
       └─ kubernetes_sd_configs pod discovery (label: component=taskmanager)
            └─ Prometheus scrapes every 5 s

Prometheus → Grafana (NodePort 32000)
  └─ Dashboard: VVP3 Autopilot Showcase (auto-refresh 5 s)
```

## Dashboard panels

| Row | Panel | What it shows |
|-----|-------|---------------|
| Kafka Inbound Traffic | Orders Inbound Rate | msg/s into `orders` topic with thresholds at 1 k and 4 k |
| Kafka Inbound Traffic | Flink Consumer Lag | Unprocessed messages — key Autopilot trigger signal |
| Flink Processing | Records Processed / sec | Flink throughput to compare with inbound rate |
| Flink Processing | Backpressure Ratio | % time tasks are back-pressured (gauge) |
| Flink Processing | Checkpoint Duration | Last checkpoint latency in ms |
| Autopilot Scaling | Active TaskManagers | Pod count with step interpolation — shows discrete scale jumps |
| Autopilot Scaling | TaskManager State Size | State bytes per pod — stacked |
| Resource Utilisation | CPU Usage by Pod | Millicores per TM pod |
| Resource Utilisation | Memory Usage by Pod | Working-set MiB per TM pod |
| Resource Utilisation | Pod Count Over Time | Running vs Pending pods during scaling |
| Autopilot Insights | Pod Startup Latency | Seconds from pod creation to readiness |
| Autopilot Insights | End-to-End Watermark Lag | Event-time lag behind wall-clock (negative = behind) |
| Autopilot Insights | Kafka Bytes In Rate | Raw byte ingest rate for `orders` |
