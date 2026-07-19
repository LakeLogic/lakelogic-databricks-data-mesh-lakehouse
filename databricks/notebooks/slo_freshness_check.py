# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : SLO & Data Quality Assessor
# Purpose   : Evaluates dataset freshness and pipeline scheduling SLAs against
#             rules defined in the Domain `_registry.yaml`.
#             Broadcasys telemetry to LakeLogic Cloud if configured in the registry.
#
# Widgets:
#   registry_path : Required. Path to the evaluated _registry.yaml
#   environment   : dev | staging | prod 
# ═══════════════════════════════════════════════════════════════════════════════


# MAGIC %pip install lakelogic==1.40.0 pyyaml polars deltalake

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Config

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("registry_path", "", "Config - Registry YAML")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Config - Environment")
dbutils.widgets.text("pipeline_run_id", "", "Exec - Upstream Run ID")

REGISTRY_PATH = dbutils.widgets.get("registry_path").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip()
PIPELINE_RUN_ID = dbutils.widgets.get("pipeline_run_id").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Evaluate SLOs

# COMMAND ----------

from lakelogic.core.registry import DomainRegistry
from lakelogic.core.slo import SLOValidator
from lakelogic.core.observer import RemoteObserver
import json

try:
    print(f"Loading Registry: {REGISTRY_PATH} (env: {ENVIRONMENT})")
    registry = DomainRegistry.from_yaml(REGISTRY_PATH, environment=ENVIRONMENT)
    validator = SLOValidator(registry, spark=spark)
    
    print("\nRunning validations...")
    report = validator.run_checks()
    
    # Render Report
    print("\n" + "=" * 70)
    print(" LAKEHOUSE SLO REPORT")
    print("=" * 70)
    print(f"  Domain        : {report.domain}")
    print(f"  System        : {report.system}")
    print(f"  Pass Context  : {'✅ PASSED' if report.passed else '❌ FAILED'}")
    print()
    
    print(f"  {'Type':<10} {'Entity':<35} {'Target (min)':<14} {'Result'}")
    print(f"  {'-'*10} {'-'*35} {'-'*14} {'-'*20}")
    
    for r in report.results:
        target_str = str(r.slo_max_minutes) if getattr(r, "slo_max_minutes", None) else "-"
        result_str = f"{r.status}  ({getattr(r, 'delay_minutes', '?')} min delay)" if r.delay_minutes is not None else r.status
        print(f"  {r.layer:<10} {r.entity:<35} {target_str:<14} {result_str}")
        
    print("=" * 70)
    
    # Cloud sync for SaaS
    if registry.cloud.enabled and registry.cloud.report_url:
        print("\n☁️ Syncing telemetry to LakeLogic Cloud...")
        observer = RemoteObserver()
        report_dict = report.model_dump()
        report_dict["pipeline_run_id"] = PIPELINE_RUN_ID
        report_dict["environment"] = ENVIRONMENT
        try:
            observer.report({"type": "slo", "report": report_dict})
            print("  ✅ Synced successfully.")
        except Exception as e:
            print(f"  ⚠ Failed to sync: {e}")
            
    # Set exit states for Databricks workflows
    dbutils.jobs.taskValues.set(key="slo_passed", value=report.passed)
    if not report.passed:
        raise Exception(f"SLO checks failed: {len(report.failures)} violations detected.")
        
except Exception as e:
    print(f"\n❌ SLO evaluation failed: {e}")
    raise
