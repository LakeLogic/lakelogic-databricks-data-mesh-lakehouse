# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : Right-to-Delete Driver — GDPR / HIPAA Erasure Workflows
# Purpose   : Executes privacy erasure (nullify, hash, or redact) on
#             materialized tables for specified data subjects. Generates
#             audit-ready erasure reports.
#
# Called by:
#   Databricks workflow or run interactively for compliance operations.
# ═══════════════════════════════════════════════════════════════════════════════


# MAGIC %pip install lakelogic pyyaml polars deltalake

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
    "Registry",
)
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Env")
dbutils.widgets.text("entity_filter", "", "Entities")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry Run")
dbutils.widgets.dropdown("engine", "spark", ["polars", "spark", "pandas"], "Engine")

# ── GDPR Erasure ──────────────────────────────────────────────────────────────
dbutils.widgets.text("gdpr_column", "", "GDPR Column")
dbutils.widgets.text("gdpr_ids", "", "GDPR IDs")
dbutils.widgets.dropdown("gdpr_strategy", "nullify", ["nullify", "hash", "redact"], "GDPR Mode")
dbutils.widgets.text("gdpr_salt", "", "GDPR Salt")
dbutils.widgets.text("gdpr_partition_col", "", "GDPR Part.")
dbutils.widgets.text("gdpr_partition_val", "", "GDPR PVal")

# ── HIPAA Erasure ─────────────────────────────────────────────────────────────
dbutils.widgets.text("hipaa_column", "", "HIPAA Col")
dbutils.widgets.text("hipaa_ids", "", "HIPAA IDs")
dbutils.widgets.dropdown("hipaa_strategy", "nullify", ["nullify", "hash", "redact"], "HIPAA Mode")
dbutils.widgets.text("hipaa_salt", "", "HIPAA Salt")
dbutils.widgets.text("hipaa_partition_col", "", "HIPAA Part.")
dbutils.widgets.text("hipaa_partition_val", "", "HIPAA PVal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Config

# COMMAND ----------

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
    dbutils = DBUtilsMock()

REGISTRY_PATH = dbutils.widgets.get("registry_path").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip() or "dev"
ENTITY_FILTER = dbutils.widgets.get("entity_filter").strip()
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"
ENGINE = dbutils.widgets.get("engine").strip() or "spark"

# GDPR
GDPR_COLUMN = dbutils.widgets.get("gdpr_column").strip()
_gdpr_ids_raw = dbutils.widgets.get("gdpr_ids").strip()
GDPR_IDS = [v.strip() for v in _gdpr_ids_raw.split(",") if v.strip()] if _gdpr_ids_raw else []
GDPR_STRATEGY = dbutils.widgets.get("gdpr_strategy").strip() or "nullify"
GDPR_SALT = dbutils.widgets.get("gdpr_salt").strip()
GDPR_PARTITION_COL = dbutils.widgets.get("gdpr_partition_col").strip()
GDPR_PARTITION_VAL = dbutils.widgets.get("gdpr_partition_val").strip()

# HIPAA
HIPAA_COLUMN = dbutils.widgets.get("hipaa_column").strip()
_hipaa_ids_raw = dbutils.widgets.get("hipaa_ids").strip()
HIPAA_IDS = [v.strip() for v in _hipaa_ids_raw.split(",") if v.strip()] if _hipaa_ids_raw else []
HIPAA_STRATEGY = dbutils.widgets.get("hipaa_strategy").strip() or "nullify"
HIPAA_SALT = dbutils.widgets.get("hipaa_salt").strip()
HIPAA_PARTITION_COL = dbutils.widgets.get("hipaa_partition_col").strip()
HIPAA_PARTITION_VAL = dbutils.widgets.get("hipaa_partition_val").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗑️ Execute Right-to-Delete
# MAGIC
# MAGIC Runs privacy erasure passes on all materialized tables matching the
# MAGIC specified data subject column and IDs.

# COMMAND ----------

from lakelogic.core.registry import DomainRegistry
from lakelogic.pipeline import LakehousePipeline

try:
    if not GDPR_COLUMN and not HIPAA_COLUMN:
        print("⚠️ No GDPR column or HIPAA column specified — nothing to erase.")
        print("   Set 'GDPR Column' + 'GDPR IDs' or 'HIPAA Col' + 'HIPAA IDs' to proceed.")
    else:
        print(f"Loading Registry: {REGISTRY_PATH} (env: {ENVIRONMENT})")
        registry = DomainRegistry.from_yaml(REGISTRY_PATH, environment=ENVIRONMENT)

        pipeline = LakehousePipeline(registry, engine=ENGINE, spark=spark)

        if GDPR_COLUMN and GDPR_IDS:
            print(f"\n🔒 GDPR Erasure")
            print(f"   Column   : {GDPR_COLUMN}")
            print(f"   IDs      : {GDPR_IDS}")
            print(f"   Strategy : {GDPR_STRATEGY}")
            print(f"   Dry run  : {DRY_RUN}")

            all_active = registry.get_active_contracts()
            if ENTITY_FILTER:
                entities = {e.strip().lower() for e in ENTITY_FILTER.split(",") if e.strip()}
                all_active = [c for c in all_active if c.entity.lower() in entities]

            gdpr_partition = (
                {"column": GDPR_PARTITION_COL, "value": GDPR_PARTITION_VAL}
                if GDPR_PARTITION_COL else None
            )

            pipeline._execute_gdpr_pass(
                all_active,
                GDPR_COLUMN,
                GDPR_IDS,
                GDPR_STRATEGY,
                GDPR_SALT,
                DRY_RUN,
                partition_filter=gdpr_partition,
            )
            print("   ✅ GDPR erasure complete.")

        if HIPAA_COLUMN and HIPAA_IDS:
            print(f"\n🏥 HIPAA Erasure")
            print(f"   Column   : {HIPAA_COLUMN}")
            print(f"   IDs      : {HIPAA_IDS}")
            print(f"   Strategy : {HIPAA_STRATEGY}")
            print(f"   Dry run  : {DRY_RUN}")

            all_active = registry.get_active_contracts()
            if ENTITY_FILTER:
                entities = {e.strip().lower() for e in ENTITY_FILTER.split(",") if e.strip()}
                all_active = [c for c in all_active if c.entity.lower() in entities]

            hipaa_partition = (
                {"column": HIPAA_PARTITION_COL, "value": HIPAA_PARTITION_VAL}
                if HIPAA_PARTITION_COL else None
            )

            pipeline._execute_hipaa_pass(
                all_active,
                HIPAA_COLUMN,
                HIPAA_IDS,
                HIPAA_STRATEGY,
                HIPAA_SALT,
                DRY_RUN,
                partition_filter=hipaa_partition,
            )
            print("   ✅ HIPAA erasure complete.")

        print("\n" + "=" * 70)
        print("RIGHT-TO-DELETE COMPLETE")
        print("=" * 70)

except Exception as e:
    print(f"\n❌ Right-to-delete failed: {e}")
    raise
