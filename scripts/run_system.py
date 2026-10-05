"""Run ONE system's pipeline locally, with LakeLogic telemetry.

    python scripts/run_system.py marketplace/rideflow
    python scripts/run_system.py marketplace/rideflow --windows 4 --no-generate

Telemetry goes where the environment says — never put the key in this file or the repo:

    export LAKELOGIC_OBSERVATORY_ENDPOINT=https://app.lakelogic.io/api/v1/operations/run-logs/ingest
    export LAKELOGIC_API_KEY=llc_sk_...        # the workspace's ingest key
    (PowerShell: $env:LAKELOGIC_API_KEY = "llc_sk_...")

Unset -> the pipeline runs without telemetry (it is optional, like on Databricks).

Steps: 1) generate fresh test data into the system's landing zone (systems with a built-in
simulator; skip with --no-generate), 2) run the system's contracts with the chosen engine.

Known limit (LakeLogic Core): locally, silver/gold contracts that read bronze as Unity
Catalog tables (`table:` sources) cannot resolve them with Polars/DuckDB yet, so a local
run completes bronze and reports silver/gold as failed. On Databricks every layer runs.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Systems with a native test-data simulator (others: --no-generate, bring your own landing).
GENERATORS = {"marketplace/rideflow": "rideflow_marketplace"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("system", help="domain/system, e.g. marketplace/rideflow")
    ap.add_argument("--env", default="local_polars", help="environment in _system.yaml (default local_polars)")
    ap.add_argument("--engine", default="polars", choices=["polars", "duckdb", "pandas"])
    ap.add_argument("--windows", type=int, default=2, help="hours of test data to generate (default 2)")
    ap.add_argument("--no-generate", action="store_true", help="use what is already in the landing zone")
    args = ap.parse_args()

    from lakelogic.core.registry import DomainRegistry
    from lakelogic.pipeline import LakehousePipeline

    system_yaml = ROOT / "contracts" / args.system / "_system.yaml"
    if not system_yaml.exists():
        print(f"No such system: {system_yaml}", file=sys.stderr)
        return 2

    on = bool(os.environ.get("LAKELOGIC_OBSERVATORY_ENDPOINT") and os.environ.get("LAKELOGIC_API_KEY"))
    print(f"Telemetry: {'ON -> ' + os.environ['LAKELOGIC_OBSERVATORY_ENDPOINT'] if on else 'off (set LAKELOGIC_OBSERVATORY_ENDPOINT + LAKELOGIC_API_KEY)'}")

    registry = DomainRegistry.from_yaml(str(system_yaml), environment=args.env)

    if not args.no_generate:
        factory = GENERATORS.get(args.system)
        if not factory:
            print(f"No test-data generator for {args.system}; running on the existing landing zone.")
        else:
            from lakelogic.core.streaming import StreamingSimulator

            sim = getattr(StreamingSimulator, factory)(
                landing_root=registry.storage.landing_root, window_minutes=60,
                start_time=datetime.now(timezone.utc) - timedelta(hours=args.windows),
                seed=42, initial_riders=50, initial_drivers=25,
            )
            rows = sum(w.total_rows for w in sim.run(num_windows=args.windows, micro_batches=1,
                                                       up_to=datetime.now(timezone.utc), resume=True))
            print(f"Generated {rows:,} rows into {registry.storage.landing_root}")

    summary = LakehousePipeline(registry, engine=args.engine).run(environment=args.env, created_by="run_system.py")
    data = summary.to_dict() if hasattr(summary, "to_dict") else summary
    results = (data.get("results") if isinstance(data, dict) else None) or []
    ok = sum(1 for r in results if str(r.get("status", "")).lower() in {"success", "completed", "ok"})
    for r in results:
        print(f"  {str(r.get('status', '?')):<10} {r.get('contract')}")
    print(f"Done: {ok} of {len(results)} contracts succeeded.")
    return 0 if results and ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
