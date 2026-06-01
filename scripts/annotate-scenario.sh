#!/usr/bin/env bash
# annotate-scenario.sh — post a Grafana annotation to mark a scenario boundary.
#
# Usage:
#   ./scripts/annotate-scenario.sh start "25k→100k peak"
#   ./scripts/annotate-scenario.sh end   "25k→100k peak"
#
# Prerequisites:
#   - Grafana reachable at GRAFANA_URL (port-forward or NodePort)
#   - GRAFANA_USER / GRAFANA_PASSWORD set (default: admin / autopilot-showcase)
#
# The annotation appears as a vertical line on every panel in the dashboard,
# making it easy to align the scenario window when reviewing the timeline.

set -euo pipefail

ACTION="${1:-}"
LABEL="${2:-scenario}"

GRAFANA_URL="${GRAFANA_URL:-http://localhost:32000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-autopilot-showcase}"

if [[ -z "$ACTION" ]]; then
  echo "Usage: $0 <start|end> [label]"
  exit 1
fi

case "$ACTION" in
  start) TEXT="▶ START: ${LABEL}"; TAGS='["scenario","start"]' ;;
  end)   TEXT="⏹ END: ${LABEL}";   TAGS='["scenario","end"]'   ;;
  *)     echo "Action must be 'start' or 'end'"; exit 1 ;;
esac

PAYLOAD=$(printf '{"text":"%s","tags":%s}' "$TEXT" "$TAGS")

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${GRAFANA_URL}/api/annotations" \
  -H "Content-Type: application/json" \
  -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  -d "$PAYLOAD")

if [[ "$HTTP_STATUS" == "200" ]]; then
  echo "Annotation posted: $TEXT"
else
  echo "Warning: Grafana returned HTTP $HTTP_STATUS — annotation may not have been saved."
  echo "  Check that GRAFANA_URL is reachable and credentials are correct."
fi
