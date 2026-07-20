# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : Pipeline Driver — Registry-driven Medallion Pipeline
# Purpose   : Generic, parameterisable entry-point called by the Databricks
#             workflow job. Chains bronze → silver → gold in contract registry
#             order. Supports targeted layer runs, contract resets, and dry-runs.
#
# Widgets (all have safe defaults — Run All works without manual changes):
#   registry_path  : UC volume or workspace path to _registry.yaml
#   environment    : dev | staging | prod
#   target_layers  : comma-separated layers, e.g. "bronze,silver,gold"
#   reset_layers   : comma-separated layers to DROP before processing
#   reload_layers  : comma-separated layers to TRUNCATE before processing
#   dry_run        : true → validate contracts but DO NOT write anything
#   entity_filter  : comma-separated entity names to restrict processing
#   engine         : polars | spark | pandas
#   parallel       : true → run independent contracts concurrently
#   max_workers    : max parallel threads (default 4)
#
#
# Secrets & Configuration:
#   1. Direct Secret References (Recommended):
#      Use `{{secrets/scope/key}}` in your YAML (e.g. _domain.yaml).
#      Example: `endpoint: "{{secrets/rideflow/LAKELOGIC_OBSERVATORY_ENDPOINT}}"`
#      The framework natively fetches from Databricks Secrets, or dynamically 
#      falls back to direct Azure Key Vault API calls when running locally.
#
#   2. Environment Variables (Legacy/DAB mapping):
#      Use `${VAR_NAME}` in your YAML. Databricks Asset Bundles inject these
#      via `spark_env_vars`. For interactive notebook runs, the script natively
#      pulls the scope into `os.environ` below.
#
# See also:
#   test_data_driver.py      — Generate test landing-zone data
#   right_to_delete_driver.py — GDPR/HIPAA erasure workflows
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %pip install lakelogic==1.40.0 pyyaml polars deltalake

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widgets

# COMMAND ----------
dbutils.widgets.removeAll()

# ── Core ──────────────────────────────────────────────────────────────────────
dbutils.widgets.text("registry_path",
    "/Volumes/rideflow_dev_demo/nondelta/_contracts/marketplace/rideflow/_system.yaml",
    "Config - Registry",
)
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Config - Environment")
dbutils.widgets.dropdown("engine", "spark", ["polars", "spark", "duckdb"], "Config - Engine")
dbutils.widgets.dropdown("storage_mode", "uc", ["uc", "direct"], "Config - Storage")

# ── Layers & Scope ────────────────────────────────────────────────────────────
dbutils.widgets.text("target_layers", "bronze, silver, gold", "Scope - Layers. e.g bronze, silver, gold")
dbutils.widgets.text("entity_filter", "", "Scope - Entities")
dbutils.widgets.text("reset_layers", "bronze, silver, gold", "Scope - Drop")
dbutils.widgets.text("reload_layers", "", "Scope - Truncate")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Exec - Dry Run")
dbutils.widgets.dropdown("ddl_only", "false", ["false", "true"], "Exec - DDL Only")

# ── Reprocessing ──────────────────────────────────────────────────────────────
dbutils.widgets.text("reprocess_from", "", "Reproc - From")
dbutils.widgets.text("reprocess_to", "", "Reproc - To")
dbutils.widgets.text("reprocess_column", "", "Reproc - Col")
dbutils.widgets.text("reprocess_values", "", "Reproc - IDs")

# ── Lineage ───────────────────────────────────────────────────────────────
dbutils.widgets.text("created_by", "", "Lineage - Created By")

# ── Execution Options ─────────────────────────────────────────────────────────
dbutils.widgets.dropdown("show_dag", "true", ["true", "false"], "Exec - Show DAG")
dbutils.widgets.dropdown("parallel", "false", ["false", "true"], "Exec - Parallel")
dbutils.widgets.text("max_workers", "4", "Exec - Workers")

# ── Resilience ────────────────────────────────────────────────────────────────
dbutils.widgets.text("resil_retry_attempts", "1", "Resilience - Retries")
dbutils.widgets.text("resil_retry_wait_sec", "30", "Resilience - Retry Wait (s)")
dbutils.widgets.text("resil_timeout_min", "0", "Resilience - Timeout (m)")
dbutils.widgets.text("resil_max_failures", "0", "Resilience - Max Failures")
dbutils.widgets.text("resil_resume_run", "", "Resilience - Resume Run ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Config

# COMMAND ----------
# COMMAND ----------

import sys
import uuid
import json
from pathlib import Path

# Provide resolving fallbacks if running locally in test vs Databricks 
try:
    spark = spark
except NameError:
    spark = None

# ── Resolve widget values ──────────────────────────────────────────────────────
REGISTRY_PATH = dbutils.widgets.get("registry_path").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip() or "dev"

TARGET_LAYERS = dbutils.widgets.get("target_layers").strip() or "bronze,silver,gold"
RESET_LAYERS = dbutils.widgets.get("reset_layers").strip()
RELOAD_LAYERS = dbutils.widgets.get("reload_layers").strip()
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"
ENTITY_FILTER = dbutils.widgets.get("entity_filter").strip()
ENGINE = dbutils.widgets.get("engine").strip() or "spark"
STORAGE_MODE = dbutils.widgets.get("storage_mode").strip() or "uc"

REPROCESS_FROM = dbutils.widgets.get("reprocess_from").strip() or None
REPROCESS_TO = dbutils.widgets.get("reprocess_to").strip() or None
REPROCESS_COLUMN = dbutils.widgets.get("reprocess_column").strip() or None
_reprocess_vals_raw = dbutils.widgets.get("reprocess_values").strip()
REPROCESS_VALUES = [v.strip() for v in _reprocess_vals_raw.split(",") if v.strip()] if _reprocess_vals_raw else None
DDL_ONLY = dbutils.widgets.get("ddl_only").lower() == "true"

CREATED_BY = dbutils.widgets.get("created_by").strip() or None

SHOW_DAG = dbutils.widgets.get("show_dag").lower() == "true"
PARALLEL = dbutils.widgets.get("parallel").lower() == "true"
MAX_WORKERS = int(dbutils.widgets.get("max_workers").strip() or "4")

RETRY_ATTEMPTS = int(dbutils.widgets.get("resil_retry_attempts").strip() or "1")
RETRY_BASE_WAIT_SECONDS = int(dbutils.widgets.get("resil_retry_wait_sec").strip() or "30")
ENTITY_TIMEOUT_MINUTES = int(dbutils.widgets.get("resil_timeout_min").strip() or "0")
MAX_CONSECUTIVE_FAILURES = int(dbutils.widgets.get("resil_max_failures").strip() or "0")
RESUME_FROM_RUN = dbutils.widgets.get("resil_resume_run").strip() or None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Inject Domain Secrets (Interactive Fallback)
# MAGIC When running interactively outside of a Databricks Job, `spark_env_vars` are not automatically injected.
# MAGIC This cell manually pulls the secret scope and maps them to environment variables (e.g., `rideflow-dev-catalog` → `RIDEFLOW_DEV_CATALOG`).

# COMMAND ----------

import os

# ── Manual Override ───────────────────────────────────────────────────────────
# If you do not have access to the Key Vault or Databricks Secrets, you can 
# uncomment the lines below to pass your variables directly:
#
# os.environ["RIDEFLOW_DEV_CATALOG"] = "your_catalog_name"
# os.environ["RIDEFLOW_DEV_STORAGE_ACCOUNT"] = "your_storage_account"
#
# # Optional (LakeLogic OSS is free, telemetry is not mandatory):
# # os.environ["LAKELOGIC_OBSERVATORY_ENDPOINT"] = "https://your-endpoint.com"
# # os.environ["LAKELOGIC_API_KEY"] = "your_api_key"
#
# # Azure Storage Credentials (Direct mode - no Databricks Unity Catalog credentials detected):
# # Set either SPN or Storage Key
# # os.environ["AZURE_CLIENT_ID"] = "your_spn_client_id"
# # os.environ["AZURE_CLIENT_SECRET"] = "your_spn_client_secret"
# # os.environ["AZURE_TENANT_ID"] = "your_tenant_id"
# # OR
# # os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "your_storage_key"

# ── Catalog — derived from the registry Volume path ───────────────────────────
# Self-contained on UC Volumes (no ADLS/SPN/storage keys). The catalog is read
# straight from the registry path (/Volumes/<catalog>/nondelta/_contracts/...) so
# table names always match the catalog this pipeline is deployed against — one
# source of truth. Set it by pointing registry_path at /Volumes/<your-catalog>/…
_derived_catalog = "rideflow_dev_demo"
if REGISTRY_PATH.startswith("/Volumes/"):
    _parts = REGISTRY_PATH.split("/")
    if len(_parts) > 2 and _parts[2]:
        _derived_catalog = _parts[2]
os.environ.setdefault(f"RIDEFLOW_{ENVIRONMENT.upper()}_CATALOG", _derived_catalog)

# ── Optional: LakeLogic Cloud telemetry (live trust score + Zeus diagnosis) ───
# LakeLogic OSS is free; telemetry is NOT required for the pipeline to run.
# To enable it, store your keys in a Databricks secret scope — NEVER hardcode
# them — and uncomment (using a scope named "rideflow"):
#   os.environ["LAKELOGIC_OBSERVATORY_ENDPOINT"] = dbutils.secrets.get("rideflow", "lakelogic-observatory-endpoint")
#   os.environ["LAKELOGIC_API_KEY"]              = dbutils.secrets.get("rideflow", "lakelogic-api-key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣  Initialize Pipeline & Visualize DAG
# MAGIC
# MAGIC With LakeLogic 0.3.0, orchestration logic (DAG resolution, 
# MAGIC reset workflows) has moved into the `lakelogic.pipeline` 
# MAGIC package so that logic can be unit tested and run locally.
# MAGIC
# MAGIC This notebook is a thin, declarative runner.

# COMMAND ----------

import os
from lakelogic.core.registry import DomainRegistry
from lakelogic.pipeline import LakehousePipeline

try:
    # ── 1. Load the Registry ──────────────────────────────────────────────────
    print(f"Loading Registry: {REGISTRY_PATH} (env: {ENVIRONMENT}, storage: {STORAGE_MODE})")
    registry = DomainRegistry.from_yaml(REGISTRY_PATH, environment=ENVIRONMENT, storage_mode=STORAGE_MODE)
    
    # ── 2. Initialize the Pipeline ────────────────────────────────────────────
    pipeline = LakehousePipeline(registry, engine=ENGINE, spark=spark)
    
    # ── 3. Visualize DAG ─────────────────────────────────────────────────────
    if SHOW_DAG:
        try:
            displayHTML(pipeline.visualize_dag())
        except NameError:
            pass  # displayHTML not available outside Databricks

except Exception as e:
    print(f"\n❌ Pipeline initialization failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣  Execute Pipeline

# COMMAND ----------

try:
    import time
    _start_time = time.time()

    # ── 4. Execute ────────────────────────────────────────────────────────────
    summary = pipeline.run(
        target_layers=TARGET_LAYERS,
        reset_layers=RESET_LAYERS,
        reload_layers=RELOAD_LAYERS,
        dry_run=DRY_RUN,
        entity_filter=ENTITY_FILTER,
        environment=ENVIRONMENT,
        
        reprocess_from=REPROCESS_FROM,
        reprocess_to=REPROCESS_TO,
        reprocess_column=REPROCESS_COLUMN,
        reprocess_values=REPROCESS_VALUES,
        
        parallel=PARALLEL,
        max_workers=MAX_WORKERS,
        
        retry_attempts=RETRY_ATTEMPTS,
        retry_base_wait_seconds=RETRY_BASE_WAIT_SECONDS,
        entity_timeout_minutes=ENTITY_TIMEOUT_MINUTES if ENTITY_TIMEOUT_MINUTES > 0 else None,
        max_consecutive_failures=MAX_CONSECUTIVE_FAILURES,
        resume_from_run=RESUME_FROM_RUN,
        
        ddl_only=DDL_ONLY,
        
        created_by=CREATED_BY,
    )
    
    _duration = time.time() - _start_time
    _duration_mins = _duration / 60.0
    
    # ── 4. Summary ────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("PIPELINE RUN SUMMARY")
    print("=" * 70)
    print(f"  Pipeline run    : {summary.run_id}")
    print(f"  Environment     : {summary.environment}")
    print(f"  Dry run         : {summary.dry_run}")
    print(f"  Engine          : {ENGINE}")
    print(f"  Duration        : {_duration_mins:.2f} minutes")
    print()
    print(f"  {'Contract':<45} {'Layer':<8} {'Status':<14} Rows")
    print(f"  {'-'*45} {'-'*8} {'-'*14} ----")
    
    for r in summary.results:
        contract = r.get("contract", "?")
        layer = r.get("layer", "?")
        status = r.get("status", "?")
        rows = r.get("rows", "-")
        error = r.get("error", "")
        
        print(f"  {contract:<45} {layer:<8} {status:<14} {rows}")
        if error:
            print(f"    ↳ {error}")
    print("=" * 70)

    # Expose summary values to downstream Databricks tasks (e.g. SLO check)
    dbutils.jobs.taskValues.set(key="pipeline_run_id", value=summary.run_id)
    dbutils.jobs.taskValues.set(key="contracts_run", value=len(summary.results))
    dbutils.jobs.taskValues.set(key="environment", value=ENVIRONMENT)
    
    # Surface the ACTUAL per-contract error(s) in the raised exception. The
    # Databricks Jobs API does not return notebook stdout, so a generic "a
    # contract failed" leaves operators blind — put the real error on the
    # exception so it reaches the run's error field. (lakelogic ≥ the version with
    # PipelineRunSummary.failure_details() can use that directly; inlined here so
    # it works against any released engine.)
    _failed = [r for r in summary.results if r.get("status") == "failed"]
    if _failed:
        _details = " | ".join(
            f"{r.get('contract') or r.get('table_name')} [{r.get('layer')}]: "
            f"{(str(r.get('error') or 'see driver logs').strip().splitlines() or ['see driver logs'])[0][:300]}"
            for r in _failed
        )
        raise RuntimeError(f"{len(_failed)} contract(s) failed — {_details}")

except Exception as e:
    print(f"\n❌ Pipeline execution failed: {e}")
    raise
