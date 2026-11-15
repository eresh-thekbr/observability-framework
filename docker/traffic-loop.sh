#!/bin/bash
# Continuously launches Spark pipelines so Kibana shows a live, realistic timeline.
# Each run = one Spark application = one trace.
#   ~70% normal, ~15% skewed (straggler task), ~15% failing (corrupt record -> ERROR spans + logs)
# Env: RUNS (0 = forever), PAUSE_MIN / PAUSE_MAX seconds between runs.
RUNS="${RUNS:-0}"
PAUSE_MIN="${PAUSE_MIN:-3}"
PAUSE_MAX="${PAUSE_MAX:-15}"
PIPELINES=(fraud-detection clickstream-funnel risk-aggregation)

i=0
while [ "$RUNS" -eq 0 ] || [ "$i" -lt "$RUNS" ]; do
  i=$((i + 1))
  pipeline=${PIPELINES[$((RANDOM % ${#PIPELINES[@]}))]}
  roll=$((RANDOM % 100))
  if   [ "$roll" -lt 70 ]; then scenario=normal
  elif [ "$roll" -lt 85 ]; then scenario=skew
  else                          scenario=failure
  fi
  echo "[traffic] run #$i pipeline=$pipeline scenario=$scenario"
  start=$(date +%s)
  if OTEL_SERVICE_NAME="$pipeline" OTEL_RESOURCE_ATTRIBUTES="deployment.environment=demo,pipeline.scenario=$scenario" \
       /app/run-job.sh com.example.apps.PipelineSimulator "$pipeline" "$scenario"; then
    status=SUCCEEDED
  else
    status=FAILED
  fi
  echo "[traffic] run #$i $pipeline/$scenario $status in $(( $(date +%s) - start ))s"
  sleep $((PAUSE_MIN + RANDOM % (PAUSE_MAX - PAUSE_MIN + 1)))
done
