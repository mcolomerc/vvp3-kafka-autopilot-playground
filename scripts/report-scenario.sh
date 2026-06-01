#!/usr/bin/env bash
# report-scenario.sh — query Prometheus and print a structured report for a completed
# Autopilot scenario run.
#
# Usage:
#   ./scripts/report-scenario.sh [OPTIONS]
#
# Options:
#   --duration <minutes>   Look-back window in minutes (default: 30)
#   --prom-url <url>       Prometheus base URL (default: http://localhost:9090)
#   --label <text>         Optional scenario label printed in the report header
#   --csv                  Also emit a single CSV line suitable for appending to a log file
#
# Examples:
#   # Report the last 30 minutes (default)
#   ./scripts/report-scenario.sh
#
#   # Report a specific scenario window with a label
#   ./scripts/report-scenario.sh --duration 15 --label "25k→100k peak, 4x multiplier"
#
#   # Append CSV line to a results file
#   ./scripts/report-scenario.sh --duration 15 --label "baseline" --csv >> results.csv
#
# Prerequisites:
#   - curl
#   - jq  (brew install jq / apt-get install jq)
#   - Prometheus reachable at --prom-url (port-forward if needed — see DEPLOYMENT.md)
#
# Port-forward Prometheus before running:
#   kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-prometheus 9090:9090 &

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DURATION_MIN=30
PROM_URL="http://localhost:9090"
LABEL=""
CSV_MODE=false

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration) DURATION_MIN="$2"; shift 2 ;;
    --prom-url)  PROM_URL="$2";     shift 2 ;;
    --label)     LABEL="$2";        shift 2 ;;
    --csv)       CSV_MODE=true;     shift   ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

RANGE="${DURATION_MIN}m"
STEP="15s"

# ---------------------------------------------------------------------------
# Helper: instant query — returns the scalar value of a PromQL expression or "N/A"
# ---------------------------------------------------------------------------
prom_query() {
  local expr="$1"
  local raw
  raw=$(curl -sG "${PROM_URL}/api/v1/query" \
    --data-urlencode "query=${expr}" \
    2>/dev/null)
  echo "$raw" | jq -r '
    if .data.result | length > 0
    then .data.result[0].value[1]
    else "N/A"
    end'
}

# Helper: format a float to N decimal places (no external tools needed)
fmt() {
  local val="$1"
  local decimals="${2:-0}"
  if [[ "$val" == "N/A" ]]; then echo "N/A"; return; fi
  printf "%.${decimals}f" "$val"
}

# ---------------------------------------------------------------------------
# Check Prometheus is reachable
# ---------------------------------------------------------------------------
if ! curl -sf "${PROM_URL}/-/ready" > /dev/null 2>&1; then
  echo "ERROR: Prometheus not reachable at ${PROM_URL}"
  echo "  Run: kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-prometheus 9090:9090 &"
  exit 1
fi

# ---------------------------------------------------------------------------
# Collect metrics
# ---------------------------------------------------------------------------

# --- Producer ---------------------------------------------------------------
send_rate_peak=$(prom_query "max_over_time(kafka_producer_record_send_rate{application=\"kafka-producer\"}[${RANGE}:${STEP}])")
send_rate_avg=$(prom_query  "avg_over_time(kafka_producer_record_send_rate{application=\"kafka-producer\"}[${RANGE}:${STEP}])")
base_throughput=$(prom_query "max_over_time(producer_throughput_base{application=\"kafka-producer\"}[${RANGE}:${STEP}])")
peak_current=$(prom_query   "max_over_time(producer_throughput_current{application=\"kafka-producer\"}[${RANGE}:${STEP}])")
messages_produced=$(prom_query "producer_messages_produced{application=\"kafka-producer\"}")
error_count=$(prom_query       "producer_messages_errors{application=\"kafka-producer\"}")
producer_error_rate=$(prom_query "max_over_time(kafka_producer_record_error_rate{application=\"kafka-producer\"}[${RANGE}:${STEP}])")
request_latency_avg=$(prom_query "avg_over_time(kafka_producer_request_latency_avg{application=\"kafka-producer\"}[${RANGE}:${STEP}])")
request_latency_peak=$(prom_query "max_over_time(kafka_producer_request_latency_avg{application=\"kafka-producer\"}[${RANGE}:${STEP}])")

# --- Kafka broker -----------------------------------------------------------
broker_inbound_peak=$(prom_query "max_over_time(sum(rate(kafka_server_brokertopicmetrics_messagesin_oneminuterate{topic=\"orders\"}[1m]))[${RANGE}:${STEP}])")
consumer_lag_peak=$(prom_query   "max_over_time(sum(kafka_server_fetcherlagmetrics_consumerlag{topic=\"orders\"})[${RANGE}:${STEP}])")
consumer_lag_current=$(prom_query "sum(kafka_server_fetcherlagmetrics_consumerlag{topic=\"orders\"})")

# --- Flink ------------------------------------------------------------------
flink_records_avg=$(prom_query "avg_over_time(sum(rate(flink_taskmanager_job_task_numRecordsInPerSecond[30s]))[${RANGE}:${STEP}])")
flink_records_peak=$(prom_query "max_over_time(sum(rate(flink_taskmanager_job_task_numRecordsInPerSecond[30s]))[${RANGE}:${STEP}])")
backpressure_max=$(prom_query   "max_over_time(avg(flink_taskmanager_job_task_isBackPressured)[${RANGE}:${STEP}]) * 100")
backpressure_avg=$(prom_query   "avg_over_time(avg(flink_taskmanager_job_task_isBackPressured)[${RANGE}:${STEP}]) * 100")
checkpoint_max=$(prom_query     "max_over_time(flink_jobmanager_job_lastCheckpointDuration[${RANGE}:${STEP}])")
checkpoint_avg=$(prom_query     "avg_over_time(flink_jobmanager_job_lastCheckpointDuration[${RANGE}:${STEP}])")

# --- Autopilot --------------------------------------------------------------
tm_count_max=$(prom_query "max_over_time((count(kube_pod_status_ready{namespace=\"vvp\",condition=\"true\"} == 1) or vector(0))[${RANGE}:${STEP}])")
tm_count_min=$(prom_query "min_over_time((count(kube_pod_status_ready{namespace=\"vvp\",condition=\"true\"} == 1) or vector(0))[${RANGE}:${STEP}])")
tm_count_now=$(prom_query "count(kube_pod_status_ready{namespace=\"vvp\",condition=\"true\"} == 1) or vector(0)")

# ---------------------------------------------------------------------------
# Print report
# ---------------------------------------------------------------------------
NOW=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
SEP="──────────────────────────────────────────────────"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "  VVP3 Autopilot — Scenario Report"
[[ -n "$LABEL" ]] && echo "  Scenario : $LABEL"
echo "  Window   : last ${DURATION_MIN} minutes"
echo "  Generated: ${NOW}"
echo "╚══════════════════════════════════════════════════╝"
echo ""

echo "PRODUCER"
echo "$SEP"
printf "  %-36s %s\n"  "Base throughput (msg/s):"          "$(fmt "$base_throughput")"
printf "  %-36s %s\n"  "Peak target rate (msg/s):"         "$(fmt "$peak_current")"
printf "  %-36s %s\n"  "Peak send rate – client (msg/s):"  "$(fmt "$send_rate_peak")"
printf "  %-36s %s\n"  "Avg send rate – client (msg/s):"   "$(fmt "$send_rate_avg")"
printf "  %-36s %s\n"  "Total messages produced:"          "$(fmt "$messages_produced")"
printf "  %-36s %s\n"  "Total send errors:"                "$(fmt "$error_count")"
printf "  %-36s %s\n"  "Peak error rate (errors/s):"       "$(fmt "$producer_error_rate" 2)"
printf "  %-36s %s\n"  "Avg request latency (ms):"         "$(fmt "$request_latency_avg" 2)"
printf "  %-36s %s\n"  "Peak request latency (ms):"        "$(fmt "$request_latency_peak" 2)"
echo ""

echo "KAFKA BROKER"
echo "$SEP"
printf "  %-36s %s\n"  "Peak inbound rate (msg/s):"        "$(fmt "$broker_inbound_peak")"
printf "  %-36s %s\n"  "Peak consumer lag (messages):"     "$(fmt "$consumer_lag_peak")"
printf "  %-36s %s\n"  "Current consumer lag (messages):"  "$(fmt "$consumer_lag_current")"
echo ""

echo "FLINK PROCESSING"
echo "$SEP"
printf "  %-36s %s\n"  "Peak records processed (rec/s):"   "$(fmt "$flink_records_peak")"
printf "  %-36s %s\n"  "Avg records processed (rec/s):"    "$(fmt "$flink_records_avg")"
printf "  %-36s %s\n"  "Peak backpressure (%):"            "$(fmt "$backpressure_max" 1)"
printf "  %-36s %s\n"  "Avg backpressure (%):"             "$(fmt "$backpressure_avg" 1)"
printf "  %-36s %s\n"  "Peak checkpoint duration (ms):"    "$(fmt "$checkpoint_max")"
printf "  %-36s %s\n"  "Avg checkpoint duration (ms):"     "$(fmt "$checkpoint_avg")"
echo ""

echo "AUTOPILOT SCALING"
echo "$SEP"
printf "  %-36s %s\n"  "TaskManagers at start (min):"      "$(fmt "$tm_count_min")"
printf "  %-36s %s\n"  "TaskManagers at peak (max):"       "$(fmt "$tm_count_max")"
printf "  %-36s %s\n"  "TaskManagers now:"                 "$(fmt "$tm_count_now")"
echo ""

# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------
if [[ "$CSV_MODE" == true ]]; then
  # Print a header if the file doesn't exist / is empty (caller redirects >> file)
  HEADER="timestamp,label,duration_min,base_throughput,peak_target,send_rate_peak,send_rate_avg,messages_produced,errors,error_rate_peak,latency_avg_ms,latency_peak_ms,broker_inbound_peak,consumer_lag_peak,consumer_lag_now,flink_records_peak,flink_records_avg,backpressure_max_pct,backpressure_avg_pct,checkpoint_max_ms,checkpoint_avg_ms,tm_min,tm_max,tm_now"
  echo "$HEADER"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$NOW" \
    "$LABEL" \
    "$DURATION_MIN" \
    "$(fmt "$base_throughput")" \
    "$(fmt "$peak_current")" \
    "$(fmt "$send_rate_peak")" \
    "$(fmt "$send_rate_avg")" \
    "$(fmt "$messages_produced")" \
    "$(fmt "$error_count")" \
    "$(fmt "$producer_error_rate" 4)" \
    "$(fmt "$request_latency_avg" 2)" \
    "$(fmt "$request_latency_peak" 2)" \
    "$(fmt "$broker_inbound_peak")" \
    "$(fmt "$consumer_lag_peak")" \
    "$(fmt "$consumer_lag_current")" \
    "$(fmt "$flink_records_peak")" \
    "$(fmt "$flink_records_avg")" \
    "$(fmt "$backpressure_max" 1)" \
    "$(fmt "$backpressure_avg" 1)" \
    "$(fmt "$checkpoint_max")" \
    "$(fmt "$checkpoint_avg")" \
    "$(fmt "$tm_count_min")" \
    "$(fmt "$tm_count_max")" \
    "$(fmt "$tm_count_now")"
fi
