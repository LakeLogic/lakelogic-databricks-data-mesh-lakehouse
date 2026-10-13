# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : provision_all — create EVERYTHING, directly in Databricks (no CLI)
#
# Run this straight from a Databricks notebook (Run All). No Databricks CLI, no
# Asset Bundle deploy, no Terraform state. It provisions the full demo from the
# contracts:
#
#     Platform catalog                 (default: governed_rideflow_lakehouse_demo)
#       └── nondelta  (schema)         UC Volumes: _contracts, _logs
#     One catalog per domain           (rideflow_dev_<domain>, named by the system registry)
#       ├── files     (schema)         UC Volume `landing`, with a folder per system
#       ├── bronze / silver / gold     empty Delta TABLES, one schema per layer
#       ├── quarantine (schema)
#       └── lakelogic (schema)         the engine's own tables
#
# How to use:
#   1. In Databricks: Workspace ▸ Repos ▸ Add Repo → clone this GitHub repo
#      (this notebook auto-finds contracts next to itself). Or set the
#      `contracts_root` widget to the folder that contains contracts.
#   2. Attach to serverless (or any UC-enabled cluster) and Run All.
#
# Idempotent — every statement is IF NOT EXISTS / ddl_only; safe to re-run.
# To also GENERATE + PROCESS data afterwards, run a domain orchestrator job, or
# run nb_test_data_driver.py then nb_pipeline_driver.py.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# ── LakeLogic source ──────────────────────────────────────────────────
# Blank widget = install the published package from PyPI (what the demo does).
# Set it to a wheel in the catalog's _wheels Volume to run an UNRELEASED build:
#   platform/scripts/upload_lakelogic_wheel.sh   ->  prints the path to paste here
# That is the only way to exercise a LakeLogic change on Databricks BEFORE it
# is published, rather than discovering a bad release from the demo breaking.
dbutils.widgets.text("lakelogic_wheel", "", "LakeLogic wheel (blank = PyPI)")
dbutils.widgets.text("lakelogic_version", "", "LakeLogic version (blank = latest)")
_wheel = dbutils.widgets.get("lakelogic_wheel").strip()
# --force-reinstall matters: Databricks pre-installs the packages named in
# this cell into the notebook environment at session startup, so pip sees the
# PUBLISHED lakelogic already present. An unreleased wheel carries the SAME
# version number, so pip reports "already satisfied" and silently keeps the
# released code - the run then tests the wrong build while looking correct.
_version = dbutils.widgets.get("lakelogic_version").strip()
# The PyPI branch needs the same protection the wheel branch already had.
# Databricks PRE-INSTALLS the packages named in this cell at session start,
# so a bare `lakelogic` is "already satisfied" by whatever version the
# environment was built with — pip installs nothing and the run silently
# executes the OLD code. That is exactly how a run on "1.51.0" reproduced a
# bug fixed in 1.51.0: it was really running the pre-installed 1.50.0, and
# the null `lakelogic_version` in its telemetry was the only tell.
# An explicit `==` is unsatisfied by an older pre-install, so pip must act;
# `--upgrade` covers the unpinned case.
if _wheel:
    lakelogic_pkg = f"{_wheel} --force-reinstall"
elif _version:
    lakelogic_pkg = f"lakelogic=={_version}"
else:
    lakelogic_pkg = "lakelogic --upgrade"
print(f"Installing LakeLogic from: {lakelogic_pkg}")

# COMMAND ----------

# MAGIC # Only lakelogic is named: pyyaml/polars/deltalake are its own dependencies.
# MAGIC # pyarrow<25: DBR ships 21.0.0, lakelogic needs >=23.0.1, databricks-connect caps <25.
# MAGIC %pip install $lakelogic_pkg "pyarrow<25"
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog", "governed_rideflow_lakehouse_demo", "Unity Catalog name")
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

CATALOG        = dbutils.widgets.get("catalog").strip() or "governed_rideflow_lakehouse_demo"
CREATE_TABLES  = dbutils.widgets.get("create_tables") == "true"
ENGINE         = dbutils.widgets.get("engine").strip() or "spark"
contracts_root = dbutils.widgets.get("contracts_root").strip()

# The contract registry resolves {catalog} from this env var in UC mode.
os.environ["RIDEFLOW_DEV_CATALOG"] = CATALOG

# Auto-detect contracts by walking up from this notebook's own location
# (works whether the repo is a Databricks Git folder or bundle-synced).
if not contracts_root:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb_path = ctx.notebookPath().get()
        parts = nb_path.split("/")
        for i in range(len(parts), 0, -1):
            candidate = "/Workspace" + "/".join(parts[:i]) + "/contracts"
            if os.path.isdir(candidate):
                contracts_root = candidate
                break
    except Exception:
        pass

if not contracts_root or not os.path.isdir(contracts_root):
    raise FileNotFoundError(
        "Could not locate 'contracts'. Add this repo as a Databricks Git "
        "folder (Repos ▸ Add Repo) and run the notebook from inside it, or set the "
        "'contracts_root' widget to the folder that contains contracts."
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
# MAGIC > Creating a catalog needs the metastore <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">CREATE CATALOG</span> privilege. Without
# MAGIC > it, ask an admin for an empty catalog and set the <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">catalog</span> widget to it —
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


def own_catalog(domain: str):
    """The domain's own catalog name, or None when it is a schema of the shared catalog.
    A system registry that declares a schema per layer (`storage.bronze_root` / `silver_root`
    / `gold_root`) names it `<domain_catalog_prefix>_<domain>`, the prefix coming from the
    registry's `dev` environment. The registry decides; this only reads it. Same rule as
    nb_00_setup."""
    import yaml

    for reg in sorted(glob.glob(os.path.join(contracts_root, domain, "*", "_system.yaml"))):
        with open(reg, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        storage = doc.get("storage") or {}
        prefix = ((doc.get("environments") or {}).get("dev") or {}).get("domain_catalog_prefix")
        if prefix and any(storage.get(f"{layer}_root") for layer in ("bronze", "silver", "gold")):
            return f"{prefix}_{domain}"
    return None


def managed_location() -> str:
    """Where a new domain catalog keeps its managed tables: the storage root the platform
    catalog already uses (blank when the metastore supplies one)."""
    rows = spark.sql(f"DESCRIBE CATALOG EXTENDED `{CATALOG}`").collect()
    info = {r[0]: r[1] for r in rows}
    return (info.get("Storage Root") or "").rstrip("/")


def landing_volume(domain: str) -> str:
    own = OWN.get(domain)
    return f"/Volumes/{own}/files/landing" if own else f"/Volumes/{CATALOG}/nondelta/landing_{domain}"


OWN = {domain: own_catalog(domain) for domain in domains}

for domain in domains:
    own = OWN[domain]
    if own:
        # A CATALOG PER DOMAIN: a schema per layer, the landing volume under `files`, rejected
        # rows under `quarantine`, and the engine's own tables under `lakelogic`.
        root = managed_location()
        if not _catalog_exists(own):
            sql(f"CREATE CATALOG IF NOT EXISTS `{own}`" + (f" MANAGED LOCATION '{root}'" if root else ""))
        for schema in ("files", "bronze", "silver", "gold", "quarantine", "lakelogic"):
            sql(f"CREATE SCHEMA IF NOT EXISTS `{own}`.`{schema}`")
        sql(f"CREATE VOLUME IF NOT EXISTS `{own}`.`files`.`landing`")
    else:
        sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{domain}`")
        sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`nondelta`.`landing_{domain}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Landing folders + stage contracts into the <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_contracts</span> Volume

# COMMAND ----------

# landing folders: /Volumes/<domain catalog>/files/landing/<system>
for reg in system_registries:
    system = os.path.basename(os.path.dirname(reg))
    domain = os.path.basename(os.path.dirname(os.path.dirname(reg)))
    folder = f"{landing_volume(domain)}/{system}"
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
# MAGIC Uses the LakeLogic engine in <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">ddl_only</span> mode: it creates the bronze/silver/gold
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
                target_layers="bronze,silver,gold",
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

    # Fail the run when any system errored - a silent SUCCESS here once let a
    # rebuild report green while creating zero tables.
    if failed:
        raise RuntimeError(
            f"{failed} of {len(system_registries)} systems failed DDL - see the lines above."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Manifest

# COMMAND ----------

print("═" * 70)
print(f"  Provisioned Unity Catalog: {CATALOG} (platform volumes)")
print("═" * 70)
print(f"  Catalogs: {', '.join(OWN[d] or f'{CATALOG}.{d}' for d in domains)}")
print("  Schemas : files, bronze, silver, gold, quarantine, lakelogic in each domain catalog")
print(f"  Volumes : {CATALOG}.nondelta._contracts, {CATALOG}.nondelta._logs, "
      "files.landing in each domain catalog")
print(f"  Tables  : {'created (empty)' if CREATE_TABLES else 'skipped'}")
print()
print("  Next — generate + process data:")
print("    • run a domain orchestrator job, OR")
print("    • run nb_test_data_driver.py then nb_pipeline_driver.py for a system")
print()
print("  Teardown: DROP CATALOG <name> CASCADE; for each catalog listed above, then")
print(f"            DROP CATALOG `{CATALOG}` CASCADE;")
print("═" * 70)
