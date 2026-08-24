"""
single_site.py
===============
Standalone entry point for the "Single site" flow — form → CLI or Python
call. Wraps run_full_pipeline.py without changing it: reuses its stages
(crawl -> correct -> filter+recover -> measure) for exactly one site.

CLI:
    python3 single_site.py --url firstcry.com --country India --category Ecommerce
    python3 single_site.py --url firstcry.com --country India --category Ecommerce --output-dir ./out

    Writes result_<domain>_<timestamp>.csv and failures_<domain>_<timestamp>.csv,
    same as run_full_pipeline.py's own --url mode.

Python call (e.g. from a form handler / server.py):
    from single_site import test_single_site, discover_endpoints
    results_df, failures_df = test_single_site("firstcry.com", "India", "Ecommerce")

    # or just the discovery step (crawl -> correct -> filter), no measurement —
    # this is what server.py's /api/discover uses, so the "review endpoints
    # before running" step in index.html stays a single-site concern:
    endpoints_df = discover_endpoints("firstcry.com", "India", "Ecommerce")
"""

import argparse
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from run_full_pipeline import (
    SCRIPT_DIR,
    CAPTURE_COLUMNS,
    build_single_site_input,
    run_pipeline,
    stage1_crawl_all,
    stage2_correct,
    stage3_filter_and_recover,
    filter_by_types,
)
import pandas as pd


def discover_endpoints(url, country, category="", brand="", types=None):
    """Run the discovery stages (crawl -> correct -> type filter) for one site
    and return every matching discovered endpoint — no scoring, no per-type
    limits, no recovery. types=None (or ["all"]) returns every type found.
    No measurement.
    """
    input_rows = build_single_site_input(url, country, category, brand)
    captured_rows = stage1_crawl_all(input_rows)
    captured_df = pd.DataFrame(captured_rows, columns=CAPTURE_COLUMNS).drop_duplicates(
        subset=["source_domain", "url"]
    )
    corrected_df = stage2_correct(captured_df)
    return filter_by_types(corrected_df, types)


def ai_select_best(rows_df):
    """The opt-in "let AI decide" action — runs the existing best-per-type
    scoring/limit/recovery logic on an already-discovered set of rows.
    rows_df must have a corrected_endpoint_type column.
    """
    return stage3_filter_and_recover(rows_df)


def test_single_site(url, country, category="", brand=""):
    """Run the full single-site pipeline in memory and return (results_df, failures_df).
    Nothing is written to disk — callers decide what/where to save.
    """
    input_rows = build_single_site_input(url, country, category, brand)
    _captured_df, _corrected_df, _final_df, results_df, failures_df = run_pipeline(input_rows)
    return results_df, failures_df


def _parse_cli_args():
    p = argparse.ArgumentParser(description="Run the reachability pipeline for one site (form-driven mode).")
    p.add_argument("--url", required=True, help="Site URL to test")
    p.add_argument("--country", required=True, help="Country name, e.g. 'India' or 'United Kingdom'")
    p.add_argument("--category", default="", help="Benchmark category, e.g. 'Ecommerce'")
    p.add_argument("--brand", default="", help="Optional brand/app label")
    p.add_argument("--output-dir", default=None, help="Directory for output CSVs (default: next to this script)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_cli_args()
    t0 = time.time()
    out_dir = Path(args.output_dir) if args.output_dir else SCRIPT_DIR

    input_rows = build_single_site_input(args.url, args.country, args.category, args.brand)
    print(f"Single-site mode: {input_rows[0]['domain']} ({input_rows[0]['country']})")

    domain_slug = re.sub(r"[^a-zA-Z0-9]+", "_", urlparse(input_rows[0]["domain"]).netloc).strip("_")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_csv = out_dir / f"result_{domain_slug}_{stamp}.csv"
    failures_csv = out_dir / f"failures_{domain_slug}_{stamp}.csv"

    captured_df, corrected_df, final_df, results_df, failures_df = run_pipeline(input_rows)

    results_csv.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(results_csv, index=False, encoding="utf-8")
    failures_df.to_csv(failures_csv, index=False, encoding="utf-8")

    elapsed = time.time() - t0
    print("=" * 60)
    print(f"Elapsed              : {elapsed/60:.1f} min")
    print(f"Endpoints captured   : {len(captured_df)}")
    print(f"Endpoints selected   : {len(final_df)}")
    print(f"Measurements OK      : {len(results_df)}")
    print(f"Measurements failed  : {len(failures_df)}")
    print(f"RESULT_CSV={results_csv}")
    print(f"FAILURES_CSV={failures_csv}")
