#!/bin/sh
# Waits for Kibana, then imports data views, visualisations and the dashboard.
# Safe to re-run: existing objects are overwritten.
set -e
KIBANA="${KIBANA_URL:-http://localhost:5601}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"

printf "waiting for Kibana at %s " "$KIBANA"
i=0
until curl -s "$KIBANA/api/status" | grep -q '"level":"available"'; do
  i=$((i + 1)); [ "$i" -gt 90 ] && { echo " timed out"; exit 1; }
  printf "."; sleep 2
done
echo " up"

curl -s -X POST "$KIBANA/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  --form file=@"$DIR/kibana/spark-observability.ndjson" \
  | grep -o '"successCount":[0-9]*'

echo "Dashboard: $KIBANA/app/dashboards#/view/spark-observability"
