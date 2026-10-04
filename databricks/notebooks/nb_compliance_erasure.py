# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : nb_compliance_erasure — GDPR / HIPAA Erasure Workflows
# Purpose   : Executes privacy erasure (nullify, hash, or redact) on
#             materialized tables for specified data subjects. Generates
#             audit-ready erasure reports.
#
# Two modes:
#   * Queue (default) — GDPR IDs and HIPAA IDs both blank: erase every open
#     request in <catalog>.<domain>._lakelogic_erasure_requests for this
#     system, then mark each completed / failed. A dry run marks them
#     dry_run and leaves them open for the next real run.
#   * Explicit — GDPR/HIPAA column + IDs set: erase exactly those IDs.
#
# Called by:
#   Databricks workflow or run interactively for compliance operations.
# ═══════════════════════════════════════════════════════════════════════════════


# ── LakeLogic source ──────────────────────────────────────────────────
# Blank widget = install the published package from PyPI (what the demo does).
# Set it to a wheel in the catalog's _wheels Volume to run an UNRELEASED build:
#   scripts/upload_lakelogic_wheel.sh   ->  prints the path to paste here
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

# Fail loudly if the engine is not the one asked for. A pinned `pip install lakelogic==X`
# that cannot be satisfied (e.g. Databricks' PyPI index has not picked up a fresh release)
# prints an ERROR and the notebook CARRIES ON with the pre-installed version - a run that
# looks fine while executing old code (seen 2026-10-04: 1.69.0 requested, 1.68.0 ran).
import lakelogic as _ll
_want = (dbutils.widgets.get("lakelogic_version") or "").strip()
if _want and _ll.__version__ != _want:
    raise RuntimeError(
        f"LakeLogic {_want} was requested but {_ll.__version__} is installed - the pip install "
        "failed (see the install cell). Re-run once the release is on Databricks' PyPI index."
    )
print(f"LakeLogic {_ll.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# ── Core ──────────────────────────────────────────────────────────────────────
dbutils.widgets.text("registry_path",
    "/Volumes/governed_rideflow_lakehouse_demo/nondelta/_contracts/marketplace/rideflow/_system.yaml",
    "Registry",
)
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Env")
dbutils.widgets.text("entity_filter", "", "Entities")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run")
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

# ── LakeLogic telemetry: where run logs go (optional) ─────────────────────────
# Two job/notebook parameters, the same in every pipeline notebook:
#   telemetry_scope      Databricks secret scope holding `lakelogic-observatory-endpoint`
#                        and `lakelogic-api-key` (default "rideflow"). Point a quick test at
#                        another workspace by passing another scope, e.g. "rideflow-stage".
#   observatory_endpoint Optional URL override (not secret). The API KEY is never a
#                        parameter: parameters show in the run UI, so it comes from the scope.
# The _domain.yaml observatory block reads ${LAKELOGIC_OBSERVATORY_ENDPOINT} and
# ${LAKELOGIC_API_KEY}; this sets them. No scope/keys -> the pipeline runs without telemetry.
import os as _os

def _param(name, default, label):
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        dbutils.widgets.text(name, default, label)
        return dbutils.widgets.get(name).strip()

_TELEMETRY_SCOPE = _param("telemetry_scope", "rideflow", "Telemetry - secret scope")
_ENDPOINT_OVERRIDE = _param("observatory_endpoint", "", "Telemetry - endpoint override (optional)")
for _env, _key in (("LAKELOGIC_OBSERVATORY_ENDPOINT", "lakelogic-observatory-endpoint"),
                   ("LAKELOGIC_API_KEY", "lakelogic-api-key")):
    if _os.environ.get(_env):
        continue  # a job's spark_env_vars wins
    try:
        _os.environ[_env] = dbutils.secrets.get(_TELEMETRY_SCOPE, _key)
    except Exception:
        pass
if _ENDPOINT_OVERRIDE:
    _os.environ["LAKELOGIC_OBSERVATORY_ENDPOINT"] = _ENDPOINT_OVERRIDE
print("LakeLogic telemetry:", "ON -> " + _os.environ["LAKELOGIC_OBSERVATORY_ENDPOINT"]
      if _os.environ.get("LAKELOGIC_OBSERVATORY_ENDPOINT") and _os.environ.get("LAKELOGIC_API_KEY")
      else f"off (no keys in secret scope '{_TELEMETRY_SCOPE}')")
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

# Explicit IDs win; with none given, the request table is the source.
QUEUE_MODE = not GDPR_IDS and not HIPAA_IDS

try:
    if not QUEUE_MODE and not GDPR_COLUMN and not HIPAA_COLUMN:
        print("⚠️ IDs given but no GDPR column or HIPAA column — nothing to erase.")
        print("   Set 'GDPR Column' + 'GDPR IDs' or 'HIPAA Col' + 'HIPAA IDs' to proceed.")
    else:
        print(f"Loading Registry: {REGISTRY_PATH} (env: {ENVIRONMENT})")
        # Catalog derived from the registry Volume path — the same derivation the
        # pipeline driver uses, so `{catalog}` resolves to the tables it wrote.
        import os
        _derived_catalog = "governed_rideflow_lakehouse_demo"
        if REGISTRY_PATH.startswith("/Volumes/"):
            _parts = REGISTRY_PATH.split("/")
            if len(_parts) > 2 and _parts[2]:
                _derived_catalog = _parts[2]
        os.environ.setdefault(f"RIDEFLOW_{ENVIRONMENT.upper()}_CATALOG", _derived_catalog)
        print(f"Catalog: {_derived_catalog}")
        registry = DomainRegistry.from_yaml(REGISTRY_PATH, environment=ENVIRONMENT, storage_mode="uc")

        pipeline = LakehousePipeline(registry, engine=ENGINE, spark=spark)

        if QUEUE_MODE:
            # Requests are rows in _lakelogic_erasure_requests (see README → Privacy).
            # Subject ids are never printed: the request id is the reference.
            print(f"\n📥 Erasure requests (dry run: {DRY_RUN})")
            outcomes = pipeline.process_erasure_requests(
                dry_run=DRY_RUN,
                entity_filter=ENTITY_FILTER,
                gdpr_strategy=GDPR_STRATEGY,
                gdpr_salt=GDPR_SALT,
                hipaa_strategy=HIPAA_STRATEGY,
                hipaa_salt=HIPAA_SALT,
            )
            if not outcomes:
                print("   No open requests for this system.")
            for request_id, status in outcomes.items():
                print(f"   {request_id}: {status}")
            if any(s == "failed" for s in outcomes.values()):
                raise RuntimeError("One or more erasure requests failed — see the statuses above.")

        if not QUEUE_MODE and GDPR_COLUMN and GDPR_IDS:
            print(f"\n🔒 GDPR Erasure")
            print(f"   Column   : {GDPR_COLUMN}")
            print(f"   IDs      : {len(GDPR_IDS)} subject(s)")
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

        if not QUEUE_MODE and HIPAA_COLUMN and HIPAA_IDS:
            print(f"\n🏥 HIPAA Erasure")
            print(f"   Column   : {HIPAA_COLUMN}")
            print(f"   IDs      : {len(HIPAA_IDS)} subject(s)")
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
