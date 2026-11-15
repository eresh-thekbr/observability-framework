#!/usr/bin/env python3
"""Generates kibana/spark-observability.ndjson (data views, Lens panels, saved searches, dashboard).

Run after editing this file:  python3 kibana/build_dashboard.py
Imported by scripts/setup-kibana.sh.
"""
import json
import os

DV_TRACES, DV_LOGS, DV_METRICS = "spark-traces", "spark-logs", "spark-metrics"
APP = "spark_level : application"

# Runtime fields keep the dashboard readable without changing what the collector writes.
TRACE_RUNTIME = {
    "spark_level": {"type": "keyword", "script": {"source": """
def n = doc['name'].size() > 0 ? doc['name'].value : '';
if (n.startsWith('Spark Application')) { emit('application'); }
else if (n.startsWith('Spark Job')) { emit('job'); }
else if (n.startsWith('Spark Stage')) { emit('stage'); }
else if (n.startsWith('Spark Task')) { emit('task'); }
else if (n.startsWith('Spark Executor') || n.startsWith('Spark Environment')) { emit('lifecycle'); }
else if (n.startsWith('Spark ')) { emit('sql'); }
else { emit('other'); }"""}},
    "run_status": {"type": "keyword", "script": {"source": """
if (doc['status.code'].size() > 0 && doc['status.code'].value == 'Error') { emit('FAILED'); } else { emit('OK'); }"""}},
    "duration_sec": {"type": "double", "script": {"source": "emit(doc['duration'].value / 1.0e9);"}},
}

objects = []


def data_view(id_, title, name, runtime=None):
    attrs = {"title": title, "name": name, "timeFieldName": "@timestamp"}
    if runtime:
        attrs["runtimeFieldMap"] = json.dumps(runtime)
    objects.append({"type": "index-pattern", "id": id_, "attributes": attrs, "references": []})


def col_count(label):
    return {"label": label, "customLabel": True, "dataType": "number", "operationType": "count",
            "sourceField": "___records___", "isBucketed": False, "scale": "ratio"}


def col_metric(op, field, label):
    return {"label": label, "customLabel": True, "dataType": "number", "operationType": op, "sourceField": field,
            "isBucketed": False, "scale": "ratio", "params": {"emptyAsNull": True}}


def col_time():
    return {"label": "Time", "customLabel": True, "dataType": "date", "operationType": "date_histogram", "sourceField": "@timestamp",
            "isBucketed": True, "scale": "interval", "params": {"interval": "auto", "includeEmptyRows": True}}


def col_terms(field, label, order_col, size=8):
    return {"label": label, "customLabel": True, "dataType": "string", "operationType": "terms", "sourceField": field,
            "isBucketed": True, "scale": "ordinal",
            "params": {"size": size, "orderBy": {"type": "column", "columnId": order_col},
                       "orderDirection": "desc", "otherBucket": False, "missingBucket": False}}


def lens(id_, title, dv, vis_type, columns, order, visualization, query=""):
    objects.append({
        "type": "lens", "id": id_, "coreMigrationVersion": "8.8.0", "typeMigrationVersion": "8.9.0",
        "attributes": {
            "title": title, "visualizationType": vis_type,
            "state": {
                "datasourceStates": {"formBased": {"layers": {"l1": {
                    "columns": columns, "columnOrder": order, "incompleteColumns": {}}}}},
                "visualization": visualization,
                "query": {"query": query, "language": "kuery"},
                "filters": [],
            },
        },
        "references": [{"type": "index-pattern", "id": dv, "name": "indexpattern-datasource-layer-l1"}],
    })


def metric(id_, title, dv, column, query):
    lens(id_, title, dv, "lnsMetric", {"m": column}, ["m"],
         {"layerId": "l1", "layerType": "data", "metricAccessor": "m"}, query)


def xy(id_, title, dv, series, columns, order, x, ys, split=None, query=""):
    layer = {"layerId": "l1", "layerType": "data", "seriesType": series, "xAccessor": x, "accessors": ys}
    if split:
        layer["splitAccessor"] = split
    lens(id_, title, dv, "lnsXY", columns, order, {
        "legend": {"isVisible": True, "position": "right"}, "valueLabels": "hide",
        "preferredSeriesType": series, "layers": [layer]}, query)


def saved_search(id_, title, dv, query, columns):
    objects.append({
        "type": "search", "id": id_,
        "attributes": {
            "title": title, "columns": columns, "sort": [["@timestamp", "desc"]],
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                "query": {"query": query, "language": "kuery"}, "filter": [],
                "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"})},
        },
        "references": [{"type": "index-pattern", "id": dv,
                        "name": "kibanaSavedObjectMeta.searchSourceJSON.index"}],
    })


data_view(DV_TRACES, "traces-*", "Spark traces", TRACE_RUNTIME)
data_view(DV_LOGS, "logs-*", "Spark logs")
data_view(DV_METRICS, "metrics-*", "Spark metrics")

metric("m-runs", "Pipeline runs", DV_TRACES, col_count("Pipeline runs"), APP)
metric("m-failed", "Failed runs", DV_TRACES, col_count("Failed runs"), APP + " and run_status : FAILED")
metric("m-avg", "Avg run duration (s)", DV_TRACES, col_metric("average", "duration_sec", "Avg run duration (s)"), APP)
metric("m-errlogs", "ERROR logs", DV_LOGS, col_count("ERROR logs"), "severity_text : ERROR")

xy("v-runs-time", "Pipeline runs over time (OK vs FAILED)", DV_TRACES, "bar_stacked",
   {"t": col_time(), "s": col_terms("run_status", "Status", "c", 2), "c": col_count("Runs")},
   ["s", "t", "c"], "t", ["c"], "s", APP)
xy("v-duration", "Avg run duration by pipeline and scenario (s)", DV_TRACES, "bar",
   {"p": col_terms("resource.attributes.service.name", "Pipeline", "d"),
    "sc": col_terms("resource.attributes.pipeline.scenario", "Scenario", "d", 3),
    "d": col_metric("average", "duration_sec", "Avg duration (s)")},
   ["p", "sc", "d"], "p", ["d"], "sc", APP)
xy("v-logs-time", "Logs over time by severity", DV_LOGS, "bar_stacked",
   {"t": col_time(), "s": col_terms("severity_text", "Severity", "c", 5), "c": col_count("Log records")},
   ["s", "t", "c"], "t", ["c"], "s")
xy("v-sql-ops", "Spark SQL executions by operation", DV_TRACES, "bar_horizontal",
   {"l": col_terms("attributes.spark.sql.label", "Operation", "c"), "c": col_count("Executions"),
    "d": col_metric("average", "duration_sec", "Avg duration (s)")},
   ["l", "c", "d"], "l", ["c"], None, "spark_level : sql")
xy("v-tasks", "Running Spark tasks", DV_METRICS, "line",
   {"t": col_time(), "s": col_terms("resource.attributes.service.name", "Pipeline", "m", 5),
    "m": col_metric("max", "metrics.spark.tasks.running", "Running tasks")},
   ["s", "t", "m"], "t", ["m"], "s", "metrics.spark.tasks.running : *")
xy("v-heap", "JVM heap used (bytes)", DV_METRICS, "line",
   {"t": col_time(), "s": col_terms("resource.attributes.service.name", "Pipeline", "m", 5),
    "m": col_metric("max", "metrics.jvm.memory.used", "Heap used")},
   ["s", "t", "m"], "t", ["m"], "s", "attributes.jvm.memory.type : heap")
xy("v-slow-stages", "Slowest stages (max duration, s)", DV_TRACES, "bar_horizontal",
   {"n": col_terms("attributes.spark.stage.name", "Stage", "d", 8),
    "d": col_metric("max", "duration_sec", "Max duration (s)")},
   ["n", "d"], "n", ["d"], None, "spark_level : stage")

xy("v-shuffle", "Shuffle bytes written per pipeline", DV_METRICS, "bar_stacked",
   {"t": col_time(), "s": col_terms("resource.attributes.service.name", "Pipeline", "m", 5),
    "m": col_metric("sum", "metrics.spark.shuffle.write.bytes", "Shuffle bytes written")},
   ["s", "t", "m"], "t", ["m"], "s", "metrics.spark.shuffle.write.bytes : *")
xy("v-flagged", "Flagged transactions (business counter via OtelSpark.count)", DV_METRICS, "bar_stacked",
   {"t": col_time(), "m": col_metric("sum", "metrics.fraud.flagged.transactions", "Flagged transactions")},
   ["t", "m"], "t", ["m"], None, "metrics.fraud.flagged.transactions : *")
saved_search("s-events", "Business events and steps (OtelSpark API)", DV_LOGS, "event_name : * and not event_name : exception",
             ["resource.attributes.service.name", "event_name", "attributes.count", "attributes.path", "trace_id"])

saved_search("s-errors", "ERROR logs with trace context", DV_LOGS, "severity_text : ERROR",
             ["resource.attributes.service.name", "body.text", "trace_id", "span_id"])
saved_search("s-failed-spans", "Failed spans (task -> stage -> job -> app)", DV_TRACES, "run_status : FAILED",
             ["name", "resource.attributes.service.name", "status.message", "duration_sec", "trace_id"])

# 48-column grid
layout = [
    ("m-runs", "lens", 0, 0, 12, 7), ("m-failed", "lens", 12, 0, 12, 7),
    ("m-avg", "lens", 24, 0, 12, 7), ("m-errlogs", "lens", 36, 0, 12, 7),
    ("v-runs-time", "lens", 0, 7, 24, 13), ("v-duration", "lens", 24, 7, 24, 13),
    ("v-logs-time", "lens", 0, 20, 24, 13), ("v-sql-ops", "lens", 24, 20, 24, 13),
    ("v-tasks", "lens", 0, 33, 24, 12), ("v-heap", "lens", 24, 33, 24, 12),
    ("v-slow-stages", "lens", 0, 45, 24, 14), ("s-failed-spans", "search", 24, 45, 24, 14),
    ("v-shuffle", "lens", 0, 59, 24, 12), ("v-flagged", "lens", 24, 59, 24, 12),
    ("s-events", "search", 0, 71, 48, 13),
    ("s-errors", "search", 0, 84, 48, 15),
]
panels, refs = [], []
for i, (oid, typ, x, y, w, h) in enumerate(layout, 1):
    panels.append({"type": typ, "panelIndex": str(i), "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i)},
                   "embeddableConfig": {}, "panelRefName": f"panel_{i}"})
    refs.append({"name": f"panel_{i}", "type": typ, "id": oid})

objects.append({
    "type": "dashboard", "id": "spark-observability",
    "attributes": {
        "title": "Spark Observability (OpenTelemetry)",
        "description": "Spark pipeline runs, failures, logs and JVM metrics from the OTel listener + Java agent",
        "panelsJSON": json.dumps(panels),
        "optionsJSON": json.dumps({"useMargins": True, "syncColors": True, "hidePanelTitles": False}),
        "timeRestore": True, "timeFrom": "now-30m", "timeTo": "now",
        "refreshInterval": {"pause": False, "value": 10000},
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"},
                                                                   "filter": []})},
    },
    "references": refs,
})

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark-observability.ndjson")
with open(out, "w") as f:
    for o in objects:
        f.write(json.dumps(o) + "\n")
print(f"wrote {out} ({len(objects)} saved objects)")
