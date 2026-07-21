# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : Continuous Pipeline Driver — Tight-Loop Medallion Processing
# Purpose   : Runs the LakeLogic Medallion pipeline in a continuous loop,
#             processing any new data that lands in the landing zone.
#             Emits telemetry to LakeLogic Cloud after each iteration.
#
# Widgets:
#   registry_path     : UC volume or workspace path to _system.yaml
#   environment       : dev | staging | prod
#   loop_interval_sec : Seconds between iterations (default 300 = 5 min)
#   max_duration_hr   : Maximum runtime in hours before auto-termination (default 24)
#   target_layers     : Layers to process per iteration (default "bronze,silver,gold")
#   engine            : polars | spark
#
# Use Case:
#   Deploy as a Databricks continuous job to keep the lakehouse fresh.
#   The stream/batch producers land new files every 5 minutes;
#   this job picks them up and processes through Bronze → Silver → Gold.
# ═══════════════════════════════════════════════════════════════════════════════


# MAGIC %pip install lakelogic pyyaml polars deltalake

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("registry_path",
    "/Workspace/Shared/data_platform/domains/marketplace/rideflow/_system.yaml",
    "Config - Registry",
)
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Config - Environment")
dbutils.widgets.dropdown("engine", "polars", ["polars", "spark", "pandas"], "Config - Engine")
dbutils.widgets.dropdown("storage_mode", "uc", ["uc", "direct"], "Config - Storage")

dbutils.widgets.text("loop_interval_sec", "300", "Loop - Interval (sec)")
dbutils.widgets.text("max_duration_hr", "24", "Loop - Max Duration (hr)")
dbutils.widgets.text("target_layers", "bronze,silver,gold", "Scope - Layers")
dbutils.widgets.dropdown("parallel", "true", ["true", "false"], "Exec - Parallel")
dbutils.widgets.text("max_workers", "4", "Exec - Workers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Config

# COMMAND ----------

import time
import sys
from datetime import datetime, timezone, timedelta

try:
    spark = spark
except NameError:
    spark = None

try:
    dbutils = dbutils
except NameError:
    class DBUtilsMock:
        class WidgetsMock:
            def get(self, *a, **k): return ""
        widgets = WidgetsMock()
        class JobsMock:
            class TaskValMock:
                def set(self, key, value): pass
            taskValues = TaskValMock()
        jobs = JobsMock()
    dbutils = DBUtilsMock()

# Resolve widgets
REGISTRY_PATH = dbutils.widgets.get("registry_path").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip() or "dev"
ENGINE = dbutils.widgets.get("engine").strip() or "polars"
STORAGE_MODE = dbutils.widgets.get("storage_mode").strip() or "uc"
TARGET_LAYERS = dbutils.widgets.get("target_layers").strip() or "bronze,silver,gold"
LOOP_INTERVAL = int(dbutils.widgets.get("loop_interval_sec").strip() or "300")
MAX_DURATION_HR = float(dbutils.widgets.get("max_duration_hr").strip() or "24")
PARALLEL = dbutils.widgets.get("parallel").lower() == "true"
MAX_WORKERS = int(dbutils.widgets.get("max_workers").strip() or "4")

print(f"Registry:       {REGISTRY_PATH}")
print(f"Environment:    {ENVIRONMENT}")
print(f"Engine:         {ENGINE}")
print(f"Target layers:  {TARGET_LAYERS}")
print(f"Loop interval:  {LOOP_INTERVAL}s")
print(f"Max duration:   {MAX_DURATION_HR}h")
print(f"Parallel:       {PARALLEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Continuous Processing Loop

# COMMAND ----------

from lakelogic.core.registry import DomainRegistry
from lakelogic.pipeline import LakehousePipeline

start_time = datetime.now(timezone.utc)
deadline = start_time + timedelta(hours=MAX_DURATION_HR)
iteration = 0

print(f"\n{'='*70}")
print(f"CONTINUOUS PIPELINE — STARTING")
print(f"{'='*70}")
print(f"  Start:    {start_time.isoformat()}")
print(f"  Deadline: {deadline.isoformat()}")
print(f"{'='*70}\n")

# Load registry once (contracts don't change during the run)
registry = DomainRegistry.from_yaml(
    REGISTRY_PATH,
    environment=ENVIRONMENT,
    storage_mode=STORAGE_MODE,
)
pipeline = LakehousePipeline(registry, engine=ENGINE, spark=spark)

while datetime.now(timezone.utc) < deadline:
    iteration += 1
    iter_start = datetime.now(timezone.utc)

    print(f"\n── Iteration {iteration} ─ {iter_start.strftime('%H:%M:%S')} ──────────────")

    try:
        summary = pipeline.run(
            target_layers=TARGET_LAYERS,
            dry_run=False,
            environment=ENVIRONMENT,
            parallel=PARALLEL,
            max_workers=MAX_WORKERS,
        )

        # Print compact summary
        total_rows = sum(r.get("rows", 0) for r in summary.results if isinstance(r.get("rows"), int))
        failed = sum(1 for r in summary.results if r.get("status") == "failed")
        skipped = sum(1 for r in summary.results if r.get("status") == "skipped")
        succeeded = sum(1 for r in summary.results if r.get("status") == "success")
        duration = (datetime.now(timezone.utc) - iter_start).total_seconds()

        print(f"  ✅ {succeeded} succeeded | ⏭️ {skipped} skipped | ❌ {failed} failed")
        print(f"  📊 {total_rows:,} rows processed in {duration:.1f}s")

        # Expose iteration count for monitoring
        dbutils.jobs.taskValues.set(key="iterations_completed", value=iteration)
        dbutils.jobs.taskValues.set(key="last_run_id", value=summary.run_id)

    except Exception as e:
        print(f"  ❌ Iteration {iteration} failed: {e}")
        # Don't break — continue the loop. Transient failures are expected.

    # ── Sleep until next iteration ───────────────────────────────────────────
    remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
    sleep_time = min(LOOP_INTERVAL, remaining)

    if sleep_time > 0:
        print(f"  💤 Sleeping {sleep_time:.0f}s until next iteration...")
        time.sleep(sleep_time)
    else:
        print(f"  ⏰ Max duration reached — exiting loop")
        break

# ── Final Summary ────────────────────────────────────────────────────────────
elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
print(f"\n{'='*70}")
print(f"CONTINUOUS PIPELINE — COMPLETE")
print(f"{'='*70}")
print(f"  Iterations: {iteration}")
print(f"  Total time: {elapsed/3600:.1f} hours")
print(f"{'='*70}")
