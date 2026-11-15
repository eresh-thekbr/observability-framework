#!/usr/bin/env python3
"""Render docs/outcome.svg from the live data in Elasticsearch.

Shows what the framework produces: run KPIs, a run timeline (OK vs FAILED), the span waterfall
of the most recent failed run, log severities, and the ERROR log that carries the same trace_id.

Usage:  python3 scripts/render_outcome_svg.py [--es http://localhost:9200] [--out docs/outcome.svg]
Requires only the Python standard library.
"""
import argparse
import json
import urllib.request
from datetime import datetime, timezone
from html import escape

BLUE, RED, GREEN, AMBER, GREY, INK, MUTED = "#2563eb", "#dc2626", "#16a34a", "#d97706", "#9aa3b2", "#1b2230", "#5b6576"
SCENARIO_COLOR = {"normal": BLUE, "skew": AMBER, "failure": RED}
MONO = "Menlo, Consolas, monospace"


def es(base, path, body):
    req = urllib.request.Request(base + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def is_error(src):
    return (src.get("status") or {}).get("code") == "Error"


def ts_ms(src):
    return float(src["@timestamp"]) if not isinstance(src["@timestamp"], str) or src["@timestamp"][0].isdigit() \
        else datetime.fromisoformat(src["@timestamp"].replace("Z", "+00:00")).timestamp() * 1000


def fetch(base):
    apps = es(base, "/traces-*/_search", {
        "size": 500, "sort": [{"@timestamp": "asc"}],
        "_source": ["@timestamp", "name", "duration", "status", "trace_id", "resource.attributes.service.name",
                    "resource.attributes.pipeline.scenario"],
        "query": {"prefix": {"name": "Spark Application"}}})["hits"]["hits"]
    apps = [h["_source"] for h in apps]

    def count(index, query=None):
        return es(base, f"/{index}/_count", {"query": query or {"match_all": {}}})["count"]

    totals = {
        "spans": count("traces-*"),
        "logs": count("logs-*"),
        "logs_traced": count("logs-*", {"exists": {"field": "trace_id"}}),
        "metrics": count("metrics-*"),
    }
    sev = es(base, "/logs-*/_search", {"size": 0, "aggs": {"s": {"terms": {"field": "severity_text", "size": 6}}}})
    severities = {b["key"]: b["doc_count"] for b in sev["aggregations"]["s"]["buckets"]}

    failed = [a for a in apps if is_error(a)]
    waterfall, err_log = [], None
    chosen = None
    if failed:
        # richest of the last few failures, so the waterfall shows OK steps next to the failing one
        recent = failed[-6:]
        chosen = max(recent, key=lambda a: (count("traces-*", {"term": {"trace_id": a["trace_id"]}}), ts_ms(a)))
        tid = chosen["trace_id"]
        spans = es(base, "/traces-*/_search", {
            "size": 2000, "sort": [{"@timestamp": "asc"}],
            "_source": ["@timestamp", "name", "duration", "status", "span_id", "parent_span_id"],
            "query": {"term": {"trace_id": tid}}})["hits"]["hits"]
        waterfall = pick_rows([h["_source"] for h in spans])
        logs = es(base, "/logs-*/_search", {
            "size": 1, "sort": [{"_score": "desc"}, {"@timestamp": "asc"}],
            "_source": ["@timestamp", "body.text", "trace_id", "span_id", "resource.attributes.service.name"],
            "query": {"bool": {"filter": [{"term": {"trace_id": tid}}, {"term": {"severity_text": "ERROR"}}],
                               "should": [{"term": {"attributes.logger.name": "PipelineSimulator"}}]}}}
        )["hits"]["hits"]
        err_log = logs[0]["_source"] if logs else None
    return apps, totals, severities, waterfall, err_log, chosen


def pick_rows(spans, cap=17):
    """Depth-first span tree; keep every non-task span on the failure chain, failed tasks, a couple of OK tasks."""
    by_parent, by_id = {}, {}
    for s in spans:
        by_id[s["span_id"]] = s
        by_parent.setdefault(s.get("parent_span_id") or "", []).append(s)
    root = next((s for s in spans if s["name"].startswith("Spark Application")), None)
    if not root:
        return []
    rows = []

    def subtree_has_error(s):
        return is_error(s) or any(subtree_has_error(c) for c in by_parent.get(s["span_id"], []))

    def walk(s, depth):
        rows.append((depth, s))
        kids = sorted(by_parent.get(s["span_id"], []), key=ts_ms)
        kids = [k for k in kids if not k["name"].startswith(("Spark Executor", "Spark Environment"))]
        ok_tasks = 0
        for k in kids:
            if k["name"].startswith("Spark Task"):
                if is_error(k):
                    walk(k, depth + 1)
                elif ok_tasks < 2:
                    ok_tasks += 1
                    walk(k, depth + 1)
            elif subtree_has_error(k) or not any(subtree_has_error(x) for x in kids):
                walk(k, depth + 1)
            elif k["name"].startswith(("Spark Job", "Spark READ", "Spark JOIN", "Spark AGG", "Spark WRITE",
                                       "Spark SHUFFLE", "Spark WINDOW", "Spark SORT", "Spark TRANSFORM")):
                rows.append((depth + 1, k))  # successful sibling: show it, but don't expand
    walk(root, 0)
    if len(rows) > cap:  # keep the root and the failing chain first
        keep = [r for r in rows if is_error(r[1]) or r[0] == 0]
        rest = [r for r in rows if r not in keep]
        rows = sorted(keep + rest[:max(0, cap - len(keep))], key=lambda r: rows.index(r))
    return rows


def render(apps, totals, severities, waterfall, err_log, failed_app, out):
    W, H = 1400, 980
    o = []
    add = o.append
    add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" color="{INK}" '
        f'font-family="Helvetica, Arial, sans-serif" role="img" aria-label="Outcome of the OtelSpark framework on live '
        f'simulator traffic: {len(apps)} Spark pipeline runs, their status over time, the span waterfall of a failed '
        f'run from application down to the failed task, and the ERROR log linked to it by trace_id.">')
    add(f'<rect x="0" y="0" width="{W}" height="{H}" fill="#f6f7f9"/>')
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    add(f'<text x="24" y="40" font-size="18" font-weight="bold">What the framework produces — live simulator traffic in Elasticsearch</text>')
    add(f'<text x="24" y="62" font-size="12" fill="{MUTED}">Rendered from Elasticsearch by scripts/render_outcome_svg.py · {now} · '
        f'same data as the Kibana dashboard "Spark Observability (OpenTelemetry)"</text>')

    # KPI tiles
    n_fail = sum(1 for a in apps if is_error(a))
    avg = sum(a["duration"] for a in apps) / len(apps) / 1e9 if apps else 0
    traced = 100.0 * totals["logs_traced"] / totals["logs"] if totals["logs"] else 0
    tiles = [("Pipeline runs", f"{len(apps)}", INK), ("Failed runs", f"{n_fail}", RED),
             ("Failure rate", f"{100.0 * n_fail / max(1, len(apps)):.0f}%", RED),
             ("Avg run duration", f"{avg:.1f}s", INK), ("Spans", f"{totals['spans']:,}", BLUE),
             ("Log records", f"{totals['logs']:,}", GREEN), ("Logs with trace_id", f"{traced:.1f}%", GREEN),
             ("Metric points", f"{totals['metrics']:,}", AMBER)]
    tw = (W - 48 - 7 * 12) / 8
    for i, (label, val, color) in enumerate(tiles):
        x = 24 + i * (tw + 12)
        add(f'<rect x="{x:.0f}" y="80" width="{tw:.0f}" height="70" rx="8" fill="#ffffff" stroke="#d5dae2"/>')
        add(f'<text x="{x + 12:.0f}" y="102" font-size="11" fill="{MUTED}">{label}</text>')
        add(f'<text x="{x + 12:.0f}" y="136" font-size="24" font-weight="bold" fill="{color}">{val}</text>')

    # Run timeline: one bar per run, height = duration, colour = scenario, red outline + x if failed
    px, py, pw, ph = 24, 170, 860, 250
    add(f'<rect x="{px}" y="{py}" width="{pw}" height="{ph}" rx="8" fill="#ffffff" stroke="#d5dae2"/>')
    add(f'<text x="{px + 14}" y="{py + 24}" font-size="13" font-weight="bold">Pipeline runs over time</text>')
    add(f'<text x="{px + 14}" y="{py + 41}" font-size="11" fill="{MUTED}">one bar per Spark application (= one trace); height = run duration</text>')
    lx = px + pw - 300
    for j, (k, c) in enumerate(SCENARIO_COLOR.items()):
        add(f'<rect x="{lx + j * 80}" y="{py + 16}" width="10" height="10" fill="{c}"/>'
            f'<text x="{lx + j * 80 + 14}" y="{py + 25}" font-size="11">{k}</text>')
    add(f'<text x="{lx + 240}" y="{py + 25}" font-size="11" fill="{RED}">▲ failed</text>')
    if apps:
        cx0, cx1, cy0, cy1 = px + 50, px + pw - 20, py + 60, py + ph - 34
        t0, t1 = ts_ms(apps[0]), ts_ms(apps[-1]) + 1
        dmax = max(a["duration"] for a in apps) / 1e9
        dmax = max(1.0, (int(dmax / 2) + 1) * 2)
        for k in range(0, int(dmax) + 1, max(1, int(dmax // 4))):
            y = cy1 - (cy1 - cy0) * k / dmax
            add(f'<line x1="{cx0}" y1="{y:.1f}" x2="{cx1}" y2="{y:.1f}" stroke="#eef0f4"/>'
                f'<text x="{cx0 - 8}" y="{y + 4:.1f}" font-size="10" fill="{MUTED}" text-anchor="end">{k}s</text>')
        bw = max(3.0, min(14.0, (cx1 - cx0) / max(1, len(apps)) * 0.7))
        for a in apps:
            x = cx0 + (cx1 - cx0 - bw) * (ts_ms(a) - t0) / (t1 - t0)
            h = (cy1 - cy0) * (a["duration"] / 1e9) / dmax
            sc = a.get("resource", {}).get("attributes", {}).get("pipeline.scenario", "normal")
            add(f'<rect x="{x:.1f}" y="{cy1 - h:.1f}" width="{bw:.1f}" height="{h:.1f}" fill="{SCENARIO_COLOR.get(sc, GREY)}" rx="1.5"/>')
            if is_error(a):
                add(f'<text x="{x + bw / 2:.1f}" y="{cy1 - h - 4:.1f}" font-size="10" fill="{RED}" text-anchor="middle">▲</text>')
        fmt = lambda t: datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%H:%M")
        add(f'<line x1="{cx0}" y1="{cy1}" x2="{cx1}" y2="{cy1}" stroke="{GREY}"/>')
        add(f'<text x="{cx0}" y="{cy1 + 16}" font-size="10" fill="{MUTED}">{fmt(t0)} UTC</text>')
        add(f'<text x="{cx1}" y="{cy1 + 16}" font-size="10" fill="{MUTED}" text-anchor="end">{fmt(t1)} UTC</text>')

    # Logs by severity
    sx, sy, sw, sh = 904, 170, 472, 250
    add(f'<rect x="{sx}" y="{sy}" width="{sw}" height="{sh}" rx="8" fill="#ffffff" stroke="#d5dae2"/>')
    add(f'<text x="{sx + 14}" y="{sy + 24}" font-size="13" font-weight="bold">Log records by severity</text>')
    add(f'<text x="{sx + 14}" y="{sy + 41}" font-size="11" fill="{MUTED}">log4j2 → OpenTelemetryAppender → OTLP → Elasticsearch</text>')
    sev_color = {"INFO": GREEN, "WARN": AMBER, "ERROR": RED, "DEBUG": GREY}
    smax = max(severities.values()) if severities else 1
    for i, k in enumerate(["INFO", "WARN", "ERROR"]):
        v = severities.get(k, 0)
        y = sy + 70 + i * 52
        bwid = (sw - 170) * v / smax
        add(f'<text x="{sx + 14}" y="{y + 18}" font-size="12" font-family="{MONO}">{k}</text>')
        add(f'<rect x="{sx + 70}" y="{y + 4}" width="{max(2, bwid):.1f}" height="20" rx="3" fill="{sev_color[k]}"/>')
        add(f'<text x="{sx + 78 + max(2, bwid):.1f}" y="{y + 19}" font-size="12" font-weight="bold">{v:,}</text>')
    add(f'<text x="{sx + 14}" y="{sy + sh - 16}" font-size="11" fill="{MUTED}">{traced:.1f}% of log records carry the run\'s trace_id</text>')

    # Waterfall of the most recent failed run
    wx, wy, ww = 24, 440, W - 48
    row_h = 24
    wh = 70 + max(1, len(waterfall)) * row_h + 20
    add(f'<rect x="{wx}" y="{wy}" width="{ww}" height="{wh}" rx="8" fill="#ffffff" stroke="#d5dae2"/>')
    if failed_app and waterfall:
        svc = failed_app.get("resource", {}).get("attributes", {}).get("service.name", "")
        add(f'<text x="{wx + 14}" y="{wy + 24}" font-size="13" font-weight="bold">Span waterfall of a failed run</text>')
        add(f'<text x="{wx + 14}" y="{wy + 42}" font-size="11" fill="{MUTED}">service <tspan font-family="{MONO}" fill="{INK}">{escape(svc)}</tspan> · trace_id '
            f'<tspan font-family="{MONO}" fill="{INK}">{failed_app["trace_id"]}</tspan> · the failed task marks its stage, job, SQL execution and application as ERROR</text>')
        t0 = ts_ms(waterfall[0][1])
        t1 = max(ts_ms(s) + s["duration"] / 1e6 for _, s in waterfall)
        name_w, bx0, bx1 = 430, wx + 14 + 430, wx + ww - 90
        for i, (depth, s) in enumerate(waterfall):
            y = wy + 62 + i * row_h
            if i % 2 == 0:
                add(f'<rect x="{wx + 6}" y="{y - 2}" width="{ww - 12}" height="{row_h}" fill="#f8f9fb"/>')
            err = is_error(s)
            label = s["name"] if len(s["name"]) <= 52 - depth * 2 else s["name"][:50 - depth * 2] + "…"
            add(f'<text x="{wx + 14 + depth * 16}" y="{y + 15}" font-size="11.5" font-family="{MONO}" '
                f'fill="{RED if err else INK}">{escape(label)}</text>')
            start = (ts_ms(s) - t0) / (t1 - t0)
            dur = (s["duration"] / 1e6) / (t1 - t0)
            x = bx0 + (bx1 - bx0) * start
            wbar = max(2.0, (bx1 - bx0) * dur)
            add(f'<rect x="{x:.1f}" y="{y + 3}" width="{wbar:.1f}" height="14" rx="3" fill="{RED if err else BLUE}" '
                f'opacity="{1 if err else 0.75}"/>')
            add(f'<text x="{bx1 + 10}" y="{y + 15}" font-size="11" fill="{MUTED}">{s["duration"] / 1e9:.2f}s</text>')
        _ = name_w
    else:
        add(f'<text x="{wx + 14}" y="{wy + 24}" font-size="13" font-weight="bold">Span waterfall</text>'
            f'<text x="{wx + 14}" y="{wy + 46}" font-size="12" fill="{MUTED}">No failed run yet — let `make load` run a few minutes.</text>')

    # Correlated ERROR log
    ly = wy + wh + 20
    add(f'<rect x="24" y="{ly}" width="{W - 48}" height="84" rx="8" fill="#fef2f2" stroke="{RED}"/>')
    add(f'<text x="38" y="{ly + 24}" font-size="13" font-weight="bold" fill="{RED}">The ERROR log for that run, found by the same trace_id</text>')
    if err_log:
        body = err_log.get("body", {}).get("text", "")
        body = body if len(body) <= 150 else body[:148] + "…"
        add(f'<text x="38" y="{ly + 46}" font-size="11.5" font-family="{MONO}">{escape(body)}</text>')
        add(f'<text x="38" y="{ly + 68}" font-size="11" fill="{MUTED}">trace_id <tspan font-family="{MONO}" fill="{INK}">{err_log.get("trace_id", "")}</tspan>'
            f'   span_id <tspan font-family="{MONO}" fill="{INK}">{err_log.get("span_id", "")}</tspan>'
            f'   → in Kibana: Discover, query <tspan font-family="{MONO}" fill="{INK}">trace_id : "{err_log.get("trace_id", "")}"</tspan></text>')
    H2 = ly + 84 + 24
    add('</svg>')
    svg = "\n".join(o).replace(f'height="{H}" viewBox="0 0 {W} {H}"', f'height="{H2}" viewBox="0 0 {W} {H2}"', 1)
    svg = svg.replace(f'<rect x="0" y="0" width="{W}" height="{H}"', f'<rect x="0" y="0" width="{W}" height="{H2}"', 1)
    with open(out, "w") as f:
        f.write(svg)
    print(f"wrote {out}: {len(apps)} runs, {n_fail} failed, {len(waterfall)} waterfall rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--es", default="http://localhost:9200")
    ap.add_argument("--out", default="docs/outcome.svg")
    a = ap.parse_args()
    render(*fetch(a.es), a.out)
