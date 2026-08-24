"""
own_endpoints.py
================
Standalone entry point for the "Your own endpoints" flow — a CSV of URLs
you already picked. Wraps run_full_pipeline.py without changing it: skips
crawl/correct/filter entirely and goes straight to Stage 4 (Globalping
measurement).

Required CSV columns: country, source_domain, url
(optional: endpoint_type, host, benchmark_category, recommended_brand_app)

CLI:
    python3 own_endpoints.py --endpoints-csv my_endpoints.csv
    python3 own_endpoints.py --endpoints-csv my_endpoints.csv --output-dir ./out

    Writes result_<name>_<timestamp>.csv and failures_<name>_<timestamp>.csv,
    same as run_full_pipeline.py's own --endpoints-csv mode.

Python call (e.g. from a form handler / server.py):
    from own_endpoints import test_own_endpoints
    results_df, failures_df = test_own_endpoints("my_endpoints.csv")

    # or, if you already have a DataFrame (e.g. parsed from an upload):
    from own_endpoints import test_own_endpoints_df
    results_df, failures_df = test_own_endpoints_df(endpoints_df)
"""

import argparse
import re
import time
from datetime import datetime
from pathlib import Path

from run_full_pipeline import SCRIPT_DIR, load_endpoints_csv, run_measurement_only


def test_own_endpoints(csv_path):
    """Load an uploaded endpoints CSV and run Stage 4 only.
    Returns (results_df, failures_df). Nothing is written to disk.
    """
    endpoints_df = load_endpoints_csv(Path(csv_path))
    return run_measurement_only(endpoints_df)


def test_own_endpoints_df(endpoints_df):
    """Same as test_own_endpoints, but for a DataFrame you already have
    in memory (e.g. parsed from a browser upload) instead of a file path.
    """
    return run_measurement_only(endpoints_df)


def _parse_cli_args():
    p = argparse.ArgumentParser(
        description="Run the reachability pipeline agasinst a CSV of already-chosen endpoints."
    )
    p.add_argument("--endpoints-csv", required=True,
                   help="CSV of already-chosen endpoints (country, source_domain, url, "
                        "and optionally endpoint_type, host, ...) — skips discovery, "
                        "goes straight to measurement")
    p.add_argument("--output-dir", default=None, help="Directory for output CSVs (default: next to this script)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_cli_args()
    t0 = time.time()
    out_dir = Path(args.output_dir) if args.output_dir else SCRIPT_DIR

    endpoints_df = load_endpoints_csv(Path(args.endpoints_csv))
    print(f"Endpoints-upload mode: {len(endpoints_df)} rows from {args.endpoints_csv}")
    results_df, failures_df = run_measurement_only(endpoints_df)

    slug = re.sub(r"[^a-zA-Z0-9]+", "_", Path(args.endpoints_csv).stem).strip("_") or "endpoints"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_csv = out_dir / f"result_{slug}_{stamp}.csv"
    failures_csv = out_dir / f"failures_{slug}_{stamp}.csv"

    results_csv.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(results_csv, index=False, encoding="utf-8")
    failures_df.to_csv(failures_csv, index=False, encoding="utf-8")

    elapsed = time.time() - t0
    print("=" * 60)
    print(f"Elapsed              : {elapsed/60:.1f} min")
    print(f"Endpoints supplied   : {len(endpoints_df)}")
    print(f"Measurements OK      : {len(results_df)}")
    print(f"Measurements failed  : {len(failures_df)}")
    print(f"RESULT_CSV={results_csv}")
    print(f"FAILURES_CSV={failures_csv}")
