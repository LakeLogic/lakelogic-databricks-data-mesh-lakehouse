# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : provision_all — create EVERYTHING, directly in Databricks (no CLI)
#
# Run this straight from a Databricks notebook (Run All). No Databricks CLI, no
# Asset Bundle deploy, no Terraform state. It provisions the full demo from the
# contracts:
#
#     Unity Catalog                    (default: rideflow_dev_demo)
#       ├── nondelta  (schema)         UC Volumes: _contracts, _logs, landing_<domain>
#       │     └── landing_<domain>/<system>/   (landing folders)
#       ├── quarantine (schema)
#       └── <domain>  (schema)         empty bronze/silver/gold Delta TABLES
#
# How to use:
#   1. In Databricks: Workspace ▸ Repos ▸ Add Repo → clone this GitHub repo
#      (this notebook auto-finds domains_rideflow next to itself). Or set the
#      `contracts_root` widget to the folder that contains domains_rideflow.
#   2. Attach to serverless (or any UC-enabled cluster) and Run All.
#
# Idempotent — every statement is IF NOT EXISTS / ddl_only; safe to re-run.
# To also GENERATE + PROCESS data afterwards, run a domain orchestrator job, or
# run test_data_driver.py then pipeline_driver.py.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %pip install lakelogic pyyaml polars deltalake
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog", "rideflow_dev_demo", "Unity Catalog name")
dbutils.widgets.text("contracts_root", "", "Contracts dir (blank = auto-detect)")
dbutils.widgets.dropdown("create_tables", "true", ["true", "false"], "Create empty tables?")
dbutils.widgets.dropdown("engine", "spark", ["spark", "polars"], "Engine (spark for UC)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📍 Resolve inputs

# COMMAND ----------

import os
import glob
import shutil

CATALOG        = dbutils.widgets.get("catalog").strip() or "rideflow_dev_demo"
CREATE_TABLES  = dbutils.widgets.get("create_tables") == "true"
ENGINE         = dbutils.widgets.get("engine").strip() or "spark"
contracts_root = dbutils.widgets.get("contracts_root").strip()

# The contract registry resolves {catalog} from this env var in UC mode.
os.environ["RIDEFLOW_DEV_CATALOG"] = CATALOG

# Auto-detect domains_rideflow by walking up from this notebook's own location
# (works whether the repo is a Databricks Git folder or bundle-synced).
if not contracts_root:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb_path = ctx.notebookPath().get()
        parts = nb_path.split("/")
        for i in range(len(parts), 0, -1):
            candidate = "/Workspace" + "/".join(parts[:i]) + "/domains_rideflow"
            if os.path.isdir(candidate):
                contracts_root = candidate
                break
    except Exception:
        pass

if not contracts_root or not os.path.isdir(contracts_root):
    raise FileNotFoundError(
        "Could not locate 'domains_rideflow'. Add this repo as a Databricks Git "
        "folder (Repos ▸ Add Repo) and run the notebook from inside it, or set the "
        "'contracts_root' widget to the folder that contains domains_rideflow."
    )

domains = sorted(
    d for d in os.listdir(contracts_root)
    if os.path.isdir(os.path.join(contracts_root, d))
)
# every system registry is a <domain>/<system>/_system.yaml
system_registries = sorted(glob.glob(os.path.join(contracts_root, "*", "*", "_system.yaml")))

print(f"🗂  Catalog        : {CATALOG}")
print(f"📖 Contracts root : {contracts_root}")
print(f"🌐 Domains        : {', '.join(domains)}")
print(f"🧩 System registries: {len(system_registries)}")
print(f"🏗  Create tables  : {CREATE_TABLES} (engine={ENGINE})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Catalog, schemas & UC Volumes
# MAGIC > Creating a catalog needs the metastore `CREATE CATALOG` privilege. Without
# MAGIC > it, ask an admin for an empty catalog and set the `catalog` widget to it —
# MAGIC > this notebook then just adds the schemas, volumes and tables inside it.

# COMMAND ----------

def sql(stmt: str):
    print(f"  → {stmt}")
    spark.sql(stmt)

def _catalog_exists(name: str) -> bool:
    try:
        return spark.sql(f"SHOW CATALOGS LIKE '{name}'").count() > 0
    except Exception:
        return False

# Check existence FIRST — on Default-Storage workspaces `CREATE CATALOG IF NOT
# EXISTS` errors with INVALID_STATE for an EXISTING catalog (it resolves managed
# storage before the existence short-circuit). Only create when genuinely absent.
if _catalog_exists(CATALOG):
    print(f"  ✓ Catalog `{CATALOG}` already exists — skipping creation")
else:
    try:
        sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    except Exception as e:
        print(f"  ⚠ CREATE CATALOG `{CATALOG}` failed: {e}")

if not _catalog_exists(CATALOG):
    raise PermissionError(
        f"Catalog `{CATALOG}` does not exist and could not be created.\n"
        f"On a Default Storage workspace, create it once in the UI "
        f"(Catalog ▸ Create catalog ▸ '{CATALOG}' ▸ Default Storage), or set the "
        f"'catalog' widget to an existing catalog (`SHOW CATALOGS`, e.g. 'workspace') "
        f"and re-run. A metastore admin can also run: "
        f"CREATE CATALOG {CATALOG} MANAGED LOCATION '<storage-path>';"
    )

sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`nondelta`")
sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`nondelta`.`_contracts`")
sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`nondelta`.`_logs`")
sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`quarantine`")

for domain in domains:
    sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{domain}`")
    sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`nondelta`.`landing_{domain}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Landing folders + stage contracts into the `_contracts` Volume

# COMMAND ----------

# landing folders: /Volumes/<catalog>/nondelta/landing_<domain>/<system>
for reg in system_registries:
    system = os.path.basename(os.path.dirname(reg))
    domain = os.path.basename(os.path.dirname(os.path.dirname(reg)))
    folder = f"/Volumes/{CATALOG}/nondelta/landing_{domain}/{system}"
    os.makedirs(folder, exist_ok=True)
    print(f"  📁 {folder}")

# stage the whole contract registry into the _contracts Volume (so the DAB jobs,
# which read the registry from the Volume, also work later)
contracts_vol = f"/Volumes/{CATALOG}/nondelta/_contracts"
os.makedirs(contracts_vol, exist_ok=True)
staged = 0
for domain in domains:
    src = os.path.join(contracts_root, domain)
    dst = os.path.join(contracts_vol, domain)
    shutil.copytree(src, dst, dirs_exist_ok=True)
    staged += sum(len(fs) for _, _, fs in os.walk(dst))
print(f"\n  📦 staged {staged} contract files into {contracts_vol}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Create the tables (empty — DDL only)
# MAGIC Uses the LakeLogic engine in `ddl_only` mode: it creates the bronze/silver/gold
# MAGIC Delta tables from each contract but does **not** read or write any data.

# COMMAND ----------

if not CREATE_TABLES:
    print("Skipping table creation (create_tables = false).")
else:
    from lakelogic.core.registry import DomainRegistry
    from lakelogic.pipeline import LakehousePipeline

    made, failed = 0, 0
    for reg in system_registries:
        system = os.path.basename(os.path.dirname(reg))
        domain = os.path.basename(os.path.dirname(os.path.dirname(reg)))
        try:
            registry = DomainRegistry.from_yaml(reg, environment="dev", storage_mode="uc")
            pipeline = LakehousePipeline(registry, engine=ENGINE, spark=spark)
            summary = pipeline.run(
                target_layers=["bronze", "silver", "gold"],
                ddl_only=True,
                environment="dev",
            )
            n = len(getattr(summary, "results", []) or [])
            made += n
            print(f"  ✓ {domain}/{system:<18} created {n} tables (DDL)")
        except Exception as e:
            failed += 1
            print(f"  ✗ {domain}/{system:<18} {e}")

    print(f"\n  🏗  DDL complete — {made} tables across {len(system_registries)} systems"
          + (f", {failed} systems errored" if failed else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Manifest

# COMMAND ----------

print("═" * 70)
print(f"  Provisioned Unity Catalog: {CATALOG}")
print("═" * 70)
print(f"  Schemas : nondelta, quarantine, {', '.join(domains)}")
print(f"  Volumes : nondelta/_contracts, nondelta/_logs, "
      + ", ".join(f"nondelta/landing_{d}" for d in domains))
print(f"  Tables  : {'created (empty)' if CREATE_TABLES else 'skipped'}")
print()
print("  Next — generate + process data:")
print("    • run a domain orchestrator job, OR")
print("    • run test_data_driver.py then pipeline_driver.py for a system")
print()
print(f"  Teardown: DROP CATALOG `{CATALOG}` CASCADE;")
print("═" * 70)
