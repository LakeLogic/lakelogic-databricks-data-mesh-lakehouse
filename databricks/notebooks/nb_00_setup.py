# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : 00_setup — Provision the RideFlow demo on Unity Catalog
# Purpose   : One-shot, idempotent bootstrap so a tester can stand up the whole
#             demo on Databricks with NO Azure ADLS and NO external storage.
#
# Creates, from the contract registry (domains_rideflow/):
#   • the Unity Catalog            (default: rideflow_dev_demo)
#   • one schema per domain        (marketing, marketplace, operations, ...)
#   • a `quarantine` schema        (where contract-failing rows are held)
#   • a `nondelta` schema with UC Volumes:
#         _contracts               (the staged contract registry the driver reads)
#         _logs                    (pipeline run logs)
#         landing_<domain>         (the landing zone — a UC Volume, not ADLS)
#   • stages every contract YAML into the _contracts Volume
#
# After this runs, deploy + run a domain orchestrator to generate and process
# data. Everything lives inside the one catalog — tear down with a single
# `DROP CATALOG <catalog> CASCADE`.
#
# Idempotent: every statement is IF NOT EXISTS; safe to re-run.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widgets

# COMMAND ----------

# NOTE: `dbutils.widgets.removeAll()` must NOT be called here. In a job run it destroys
# the values the task passed in `base_parameters`, and the re-declarations below then hand
# back their DEFAULTS instead. That is invisible for `catalog` (the default happens to equal
# what the job passes) but silently disabled `reset`, so the purge never ran and the job
# failed on the orphaned volume it was meant to rebuild.
dbutils.widgets.text("catalog", "rideflow_dev_demo", "Config - Unity Catalog name")
# Where the synced contract registry lives in the workspace. Leave blank to
# auto-detect relative to this notebook. The bootstrap job passes it explicitly.
dbutils.widgets.text("contracts_src", "", "Config - Contracts source dir (workspace)")
# Purge before provisioning. A Unity Catalog catalog lives in the REGIONAL METASTORE,
# not the workspace — so deleting a workspace leaves its catalog behind, still listed,
# with managed storage that this workspace cannot reach. Volumes then "exist" as
# metadata while `/Volumes/<cat>/...` is unusable, which surfaces as a baffling
# `OSError: [Errno 22] Invalid argument`. Reset drops that orphan and rebuilds clean.
dbutils.widgets.dropdown("reset", "false", ["false", "true"],
                         "Config - DROP the catalog first (destructive)")
# Where a NEW catalog's managed data lives. Needed when the metastore has no storage
# root of its own ("Default Storage is enabled in your account") — then plain
# `CREATE CATALOG` is rejected and the location must be given explicitly. Leave blank
# on metastores that do have a root. The principal also needs CREATE MANAGED STORAGE
# on the external location that covers this path.
dbutils.widgets.text("storage_root", "", "Config - MANAGED LOCATION for a new catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📍 Resolve inputs

# COMMAND ----------

import os
import shutil

CATALOG = dbutils.widgets.get("catalog").strip() or "rideflow_dev_demo"
RESET = dbutils.widgets.get("reset").strip().lower() == "true"
STORAGE_ROOT = dbutils.widgets.get("storage_root").strip()
contracts_src = dbutils.widgets.get("contracts_src").strip()

# Auto-detect the synced contract registry by walking UP from this notebook's own
# location and checking each ancestor for a `domains_rideflow` child. Robust to how
# deep the bundle nests the notebook (e.g. .../files/domains_rideflow while the
# notebook is at .../files/databricks/notebooks/_ops/00_setup).
if not contracts_src:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb_path = ctx.notebookPath().get()
        parts = nb_path.split("/")
        for i in range(len(parts), 0, -1):
            candidate = "/Workspace" + "/".join(parts[:i]) + "/domains_rideflow"
            if os.path.isdir(candidate):
                contracts_src = candidate
                break
    except Exception:
        pass

if not contracts_src or not os.path.isdir(contracts_src):
    raise FileNotFoundError(
        f"Contracts source not found: {contracts_src}\n"
        "Deploy the bundle first (`databricks bundle deploy`) so domains_rideflow "
        "is synced into the workspace, or set the 'contracts_src' widget."
    )

# Domains = top-level directories of the contract registry.
domains = sorted(
    d for d in os.listdir(contracts_src)
    if os.path.isdir(os.path.join(contracts_src, d))
)

print(f"🗂  Catalog        : {CATALOG}")
print(f"📖 Contracts src  : {contracts_src}")
print(f"🌐 Domains        : {', '.join(domains)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Create catalog, schemas & volumes
# MAGIC All statements are `IF NOT EXISTS` — safe to re-run.
# MAGIC
# MAGIC > **Permissions:** creating a catalog needs the metastore `CREATE CATALOG`
# MAGIC > privilege. If you don't have it, ask an admin to create an empty catalog
# MAGIC > and set the `catalog` widget to it — this notebook then just adds the
# MAGIC > schemas and volumes inside it.

# COMMAND ----------

def sql(stmt: str):
    print(f"  → {stmt}")
    spark.sql(stmt)

# ── Reset: drop an orphaned catalog before rebuilding ─────────────────────────
# Only ever targets the catalog named in the widget, and refuses the shared ones a
# typo could otherwise destroy. CASCADE takes the schemas, tables and volumes with it,
# which is the point: a half-orphaned catalog cannot be repaired in place.
_PROTECTED = {"main", "system", "samples", "hive_metastore", "workspace", "default"}


def purge_catalog(name: str) -> None:
    if name.lower() in _PROTECTED:
        raise ValueError(
            f"Refusing to drop `{name}` — it is a shared/system catalog. "
            f"Set the 'catalog' widget to the demo catalog you actually want rebuilt."
        )
    print(f"  ⚠ RESET requested — dropping catalog `{name}` and everything in it")
    # Volumes first: a managed volume whose storage is unreachable can block the
    # catalog drop, and the error it raises names the volume rather than the cause.
    try:
        for row in spark.sql(f"SHOW SCHEMAS IN `{name}`").collect():
            schema = row[0]
            if schema == "information_schema":
                continue
            try:
                for v in spark.sql(f"SHOW VOLUMES IN `{name}`.`{schema}`").collect():
                    vol = v[1] if len(v) > 1 else v[0]
                    try:
                        sql(f"DROP VOLUME IF EXISTS `{name}`.`{schema}`.`{vol}`")
                    except Exception as e:            # noqa: BLE001
                        print(f"    · volume {schema}.{vol} not dropped: {e}")
            except Exception:                          # noqa: BLE001
                pass                                   # schema carries no volumes
    except Exception as e:                             # noqa: BLE001
        print(f"    · could not enumerate schemas ({e}) — going straight to DROP CATALOG")

    sql(f"DROP CATALOG IF EXISTS `{name}` CASCADE")
    print(f"  ✓ Catalog `{name}` dropped")


if RESET:
    purge_catalog(CATALOG)


# ── Catalog ───────────────────────────────────────────────────────────────────
def _catalog_exists(name: str) -> bool:
    try:
        return spark.sql(f"SHOW CATALOGS LIKE '{name}'").count() > 0
    except Exception:
        return False

# Check existence FIRST. On Default-Storage workspaces, `CREATE CATALOG IF NOT
# EXISTS` is NOT a clean no-op for an existing catalog — it resolves the managed
# storage root before the existence check and errors with INVALID_STATE. So only
# attempt creation when the catalog is genuinely absent; a re-run stays quiet.
if _catalog_exists(CATALOG):
    print(f"  ✓ Catalog `{CATALOG}` already exists — skipping creation")
else:
    try:
        # A metastore with no storage root rejects a bare CREATE CATALOG; it needs the
        # location spelled out. Pass `storage_root` and the same statement works on both
        # kinds of metastore, which is what makes this bootstrap portable.
        if STORAGE_ROOT:
            sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}` "
                f"MANAGED LOCATION '{STORAGE_ROOT.rstrip('/')}/{CATALOG}'")
        else:
            sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    except Exception as e:
        print(f"  ⚠ CREATE CATALOG `{CATALOG}` failed: {e}")

if not _catalog_exists(CATALOG):
    raise PermissionError(
        f"Catalog `{CATALOG}` does not exist and could not be created.\n"
        f"You most likely lack the metastore CREATE CATALOG privilege (common on "
        f"trial/shared workspaces), or the metastore has no default storage for new "
        f"managed catalogs.\n"
        f"Fix — pick one:\n"
        f"  1. Default Storage workspace? Create the catalog once in the UI — "
        f"Catalog ▸ Create catalog ▸ name it '{CATALOG}' ▸ Default Storage ▸ Create — "
        f"then re-run this notebook.\n"
        f"  2. OR set the 'catalog' widget to an EXISTING catalog you can write to "
        f"(run `SHOW CATALOGS` — e.g. 'workspace') and re-run; this notebook then just "
        f"adds the schemas and volumes inside it.\n"
        f"  3. OR a metastore admin runs:  "
        f"CREATE CATALOG {CATALOG} MANAGED LOCATION '<storage-path>';"
    )

# ── Operational schema for UC Volumes (landing, contracts, logs) ──────────────
sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`nondelta`")
sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`nondelta`.`_contracts`")
sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`nondelta`.`_logs`")

# ── Quarantine schema (contract-failing rows land here) ───────────────────────
sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`quarantine`")

# ── Per-domain schema (delta tables) + landing Volume ─────────────────────────
# We also PRE-CREATE the run-log + SLO Delta tables per domain. The LakeLogic
# engine writes a run-log row per contract; under parallel processing several
# contracts otherwise race to CREATE the shared table on the first run against a
# fresh catalog (first wins, the rest warn TABLE_OR_VIEW_ALREADY_EXISTS and their
# row is dropped). Creating the tables up front means every contract simply
# appends/merges — no race. Schema mirrors lakelogic.core.run_log (the engine
# ALTERs to add any columns a newer version introduces, so this is forward-safe).
_RUN_LOG_COLS = """(
  pipeline_run_id STRING, run_id STRING, timestamp STRING, start_time STRING,
  end_time STRING, run_duration_seconds DOUBLE, engine STRING, contract STRING,
  contract_version STRING, stage STRING, dataset STRING, domain STRING,
  system STRING, environment STRING, data_layer STRING, status STRING,
  error_message STRING, source_path STRING, counts_source BIGINT,
  counts_total BIGINT, counts_good BIGINT, counts_quarantined BIGINT,
  quarantine_ratio DOUBLE, estimated_cost DOUBLE, cost_currency STRING,
  cost_confidence STRING, max_source_mtime DOUBLE, max_watermark_value STRING,
  dlt_state_json STRING, slo_json STRING, report_json STRING
)"""

for domain in domains:
    sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{domain}`")
    sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`nondelta`.`landing_{domain}`")
    # Managed Delta run-log table (matches metadata.run_log_table = {domain_catalog}._pipeline_run_log).
    # _slo_checks is intentionally NOT pre-created: the demo produces no SLO checks,
    # so it never races; the engine will create it with its own schema if needed.
    sql(f"CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{domain}`.`_pipeline_run_log` {_RUN_LOG_COLS} USING DELTA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Stage the contract registry into the `_contracts` Volume
# MAGIC The pipeline driver reads contracts from
# MAGIC `/Volumes/<catalog>/nondelta/_contracts/<domain>/<system>/_system.yaml`,
# MAGIC so we copy the synced registry into that Volume.

# COMMAND ----------

contracts_vol = f"/Volumes/{CATALOG}/nondelta/_contracts"


def assert_volume_writable(path: str) -> None:
    """Prove the Volume is reachable, using the API serverless actually supports.

    This job runs on SERVERLESS compute, where local POSIX writes into `/Volumes` are
    restricted: `os.makedirs` appears to succeed and then `open(..., "w")` fails with
    `[Errno 22] Invalid argument` — which reads exactly like unreachable storage and is
    not. `dbutils.fs` is the supported path, so the probe uses it."""
    probe = f"{path}/lakelogic_write_probe.tmp"
    try:
        dbutils.fs.put(probe, "ok", True)
        dbutils.fs.rm(probe)
    except Exception as e:                                  # noqa: BLE001
        raise OSError(
            f"Volume `{path}` is not writable ({e}). "
            f"If `{CATALOG}` was created by a workspace that no longer exists, its managed "
            f"storage cannot be reached from this one — UC catalogs outlive their workspace. "
            f"Re-run with reset=true to drop and rebuild it, or point the 'catalog' widget "
            f"at a catalog this workspace owns."
        ) from e


assert_volume_writable(contracts_vol)


def copy_into_volume(src: str, dst: str) -> int:
    """Copy a tree into a UC Volume via `dbutils.fs`.

    Neither `shutil.copytree` (it replays POSIX metadata Volumes reject) nor plain
    `open()` (restricted on serverless) works here. `dbutils.fs.cp` with the `file:`
    scheme reads the synced workspace files and writes through the Volumes API."""
    dbutils.fs.cp(f"file:{src}", dst, recurse=True)
    return sum(len(files) for _, _, files in os.walk(src))


copied = 0
for domain in domains:
    src = os.path.join(contracts_src, domain)
    dst = os.path.join(contracts_vol, domain)
    n = copy_into_volume(src, dst)
    copied += n
    print(f"  ✓ {domain:<14} → {dst}  ({n} files)")

print(f"\n📦 Staged {copied} contract files into {contracts_vol}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Manifest

# COMMAND ----------

print("═" * 70)
print(f"  RideFlow demo provisioned on Unity Catalog: {CATALOG}")
print("═" * 70)
print(f"  Schemas   : nondelta, quarantine, {', '.join(domains)}")
print(f"  Volumes   : nondelta/_contracts, nondelta/_logs,")
for domain in domains:
    print(f"              nondelta/landing_{domain}")
print()
print("  Next:")
print("    1. Deploy the workflows:  databricks bundle deploy -t dev")
print("    2. Run a domain orchestrator (e.g. 'marketplace / rideflow — Full")
print("       Pipeline Orchestrator') with enable_test_data / bronze / silver /")
print("       gold = true to generate and process data end to end.")
print()
print(f"  Teardown  : DROP CATALOG `{CATALOG}` CASCADE;")
print("═" * 70)
