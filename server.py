"""
server.py
=========
Minimal local API server connecting index.html to the two standalone
pipeline scripts — it never talks to run_full_pipeline.py directly.

  POST /api/discover  {url, country, category, types?}
      -> single_site.discover_endpoints() (crawl + correct + type filter only —
         no scoring, no per-type limits, no recovery). types is an optional
         list of endpoint-type keys to restrict to; omit/empty/["all"] returns
         every discovered type.

  POST /api/select-best  {rows: [...]}  (same row shape as /api/measure)
      -> single_site.ai_select_best() — the opt-in "let AI decide" action.
         Runs the existing best-per-type scoring/limit/recovery logic
         (run_full_pipeline.stage3_filter_and_recover) on an already-
         discovered set and returns the picks for the user to review before
         confirming — it never runs automatically.

  POST /api/measure   {rows: [{country, source_domain, endpoint_type, host, url}, ...]}
      -> own_endpoints.test_own_endpoints_df() (Globalping ping+http only).
         Used both for "Run reachability tests" after discovery and for
         "Run my endpoints" after a CSV upload — same call either way, since
         measurement doesn't care where the rows came from.

Run:
    pip install flask
    python server.py

Then open:
    http:// 10.43.27.133:5000          (this machine)
    http://<your-LAN-IP>:5000      (other devices on the same network)
"""

from flask import Flask, request, jsonify, send_from_directory
from pathlib import Path
import math
import os
import pandas as pd

from single_site import discover_endpoints, ai_select_best
from own_endpoints import test_own_endpoints_df

SCRIPT_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(SCRIPT_DIR), static_url_path="")

# stage4_measure's output columns -> the field names index.html's JS expects
RESULT_COLUMN_MAP = {
    "Timestamp_UTC": "ts",
    "Endpoint_Type": "type",
    "Host": "host",
    "URL": "url",
    "HTTP_StatusCode": "status",
    "Success": "success",
    "DNS_ms": "dns",
    "TCP_ms": "tcp",
    "TLS_ms": "tls",
    "TTFB_ms": "ttfb",
    "Download_ms": "download",
    "HTTP_Total_ms": "total",
    "Packets_Sent": "sent",
    "Packets_Received": "recv",
    "Packets_Dropped": "dropped",
    "PacketLoss_pct": "loss",
    "Ping_Min_ms": "pMin",
    "Ping_Avg_ms": "pAvg",
    "Ping_Max_ms": "pMax",
    "Jitter_ms": "jitter",
    "Effective_Latency_ms": "eff",
    "Latency_Type": "latType",
}


def _json_safe(value):
    # json.dumps emits bare NaN/Infinity by default, which isn't valid JSON —
    # browsers' JSON.parse rejects it outright, so every NaN must become null.
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _records_json_safe(df):
    return [{k: _json_safe(v) for k, v in row.items()} for row in df.to_dict(orient="records")]


def rows_for_frontend(results_df):
    if results_df.empty:
        return []
    df = results_df.rename(columns=RESULT_COLUMN_MAP)
    keep = [c for c in RESULT_COLUMN_MAP.values() if c in df.columns]
    return _records_json_safe(df[keep])


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


DISCOVER_COLUMNS = ["type", "host", "url", "source_domain", "country",
                     "benchmark_category", "recommended_brand_app"]


@app.route("/api/discover", methods=["POST"])
def api_discover():
    data = request.get_json(force=True) or {}
    url = str(data.get("url", "")).strip()
    country = str(data.get("country", "")).strip()
    category = str(data.get("category", "")).strip()
    types = data.get("types") or None

    if not url or not country:
        return jsonify({"error": "url and country are required"}), 400

    final_df = discover_endpoints(url, country, category, types=types)

    endpoints = final_df.rename(columns={"corrected_endpoint_type": "type"})
    endpoints = endpoints[[c for c in DISCOVER_COLUMNS if c in endpoints.columns]]

    return jsonify({"endpoints": _records_json_safe(endpoints)})


@app.route("/api/select-best", methods=["POST"])
def api_select_best():
    data = request.get_json(force=True) or {}
    rows = data.get("rows", [])
    if not rows:
        return jsonify({"error": "rows must be a non-empty list"}), 400

    df = pd.DataFrame(rows).rename(columns={"type": "corrected_endpoint_type"})
    for col in ["country", "benchmark_category", "recommended_brand_app", "source_domain", "host", "url"]:
        if col not in df.columns:
            df[col] = ""

    best_df = ai_select_best(df)

    endpoints = best_df.rename(columns={"corrected_endpoint_type": "type"})
    endpoints = endpoints[[c for c in DISCOVER_COLUMNS if c in endpoints.columns]]

    return jsonify({"endpoints": _records_json_safe(endpoints)})


@app.route("/api/measure", methods=["POST"])
def api_measure():
    data = request.get_json(force=True) or {}
    rows = data.get("rows", [])
    if not rows:
        return jsonify({"error": "rows must be a non-empty list"}), 400

    df = pd.DataFrame(rows)
    for col in ["country", "source_domain", "endpoint_type", "host", "url"]:
        if col not in df.columns:
            df[col] = ""

    results_df, failures_df = test_own_endpoints_df(df)

    return jsonify({
        "results": rows_for_frontend(results_df),
        "failures": _records_json_safe(failures_df) if not failures_df.empty else [],
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # Render (and similar hosts) assign this dynamically
    app.run(host="0.0.0.0", port=port, debug=False)
