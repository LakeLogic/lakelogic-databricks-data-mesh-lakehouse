# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : nb_compliance_retention_check — Retention Check
# Purpose   : Read-only. Compares each table's oldest record to the retention
#             window its domain declares (`retention:` in _domain.yaml), records
#             every observation in the engine's `_lakelogic_retention_evidence`
#             table beside the domain's run log, and reports the results to
#             LakeLogic as retention checks. NEVER deletes.
#
# Modelled on the LakeLogic Build Centre notebook of the same name, adapted to
# this bundle's resolution: the registry is the staged `_system.yaml` in the
# `_contracts` Volume, and the catalog is derived from that Volume path — the
# same derivation nb_pipeline_driver and nb_slo_checks use.
#
# Widgets:
#   registry_path  : Required. /Volumes/<catalog>/nondelta/_contracts/<domain>/<system>/_system.yaml
#   environment    : dev | staging | prod
#   fail_on_breach : true fails the job when a table holds data past its window;
#                    false records and reports only.
# ═══════════════════════════════════════════════════════════════════════════════

dbutils.widgets.text("lakelogic_wheel", "", "LakeLogic wheel (blank = PyPI)")
dbutils.widgets.text("lakelogic_version", "", "LakeLogic version (blank = latest)")
_wheel = dbutils.widgets.get("lakelogic_wheel").strip()
_version = dbutils.widgets.get("lakelogic_version").strip()
# --force-reinstall / == / --upgrade: Databricks pre-installs the packages named in
# the %pip cell at session start, so a bare `lakelogic` is "already satisfied" by an
# older build and the run silently tests the wrong code (see nb_pipeline_driver).
if _wheel:
    lakelogic_pkg = f"{_wheel} --force-reinstall"
elif _version:
    lakelogic_pkg = f"lakelogic=={_version}"
else:
    lakelogic_pkg = "lakelogic --upgrade"
print(f"Installing LakeLogic from: {lakelogic_pkg}")

# COMMAND ----------

# MAGIC # pyarrow<25: DBR ships 21.0.0, lakelogic needs >=23.0.1, databricks-connect caps <25.
# MAGIC %pip install $lakelogic_pkg "pyarrow<25"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Config

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("registry_path", "", "Config - Registry YAML")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Config - Environment")
dbutils.widgets.dropdown("fail_on_breach", "true", ["true", "false"], "Exec - Fail on breach")

REGISTRY_PATH = dbutils.widgets.get("registry_path").strip()
ENVIRONMENT = dbutils.widgets.get("environment").strip() or "dev"
FAIL_ON_BREACH = dbutils.widgets.get("fail_on_breach").strip().lower() != "false"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Load the registry

# COMMAND ----------

import datetime
import os
import uuid

from lakelogic.core.registry import DomainRegistry

print(f"Loading Registry: {REGISTRY_PATH} (env: {ENVIRONMENT})")
# ── Catalog — derived from the registry Volume path ──────────────────────────
# THE SAME DERIVATION THE PIPELINE DRIVER USES. Without it `{catalog}` resolves to
# an empty string and every table name is built as `.marketplace.<table>`.
_derived_catalog = "governed_rideflow_lakehouse_demo"
if REGISTRY_PATH.startswith("/Volumes/"):
    _parts = REGISTRY_PATH.split("/")
    if len(_parts) > 2 and _parts[2]:
        _derived_catalog = _parts[2]
os.environ.setdefault(f"RIDEFLOW_{ENVIRONMENT.upper()}_CATALOG", _derived_catalog)
print(f"Catalog: {_derived_catalog}")

# storage_mode="uc" matches the pipeline driver, so the roots resolve to the tables it wrote.
registry = DomainRegistry.from_yaml(REGISTRY_PATH, environment=ENVIRONMENT, storage_mode="uc")
RUN_ID = str(uuid.uuid4())
DOMAIN = getattr(registry, "domain", None) or REGISTRY_PATH.split("/")[-3]
SYSTEM = getattr(registry, "system", None) or REGISTRY_PATH.split("/")[-2]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧾 Evidence table
# MAGIC
# MAGIC The engine owns the evidence table: its name (`_lakelogic_retention_evidence`),
# MAGIC its schema, and its place — beside the run log this system declares in
# MAGIC `_system.yaml` (`metadata.run_log_table`).

# COMMAND ----------

from lakelogic.core.evidence_tables import resolve_evidence_target, write_evidence_rows

_run_log_table = str((getattr(registry, "metadata", None) or {}).get("run_log_table") or "")
if not _run_log_table or "{" in _run_log_table:
    _run_log_table = f"{registry.storage.domain_catalog}._lakelogic_run_log"
EVIDENCE_METADATA = {"run_log_table": _run_log_table, "run_log_backend": "spark"}
_target = resolve_evidence_target("retention_evidence", EVIDENCE_METADATA, engine_name="spark")
EVIDENCE_TABLE = _target[1] if _target else None
print(f"Retention evidence: {EVIDENCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🕰️ Check retention windows
# MAGIC
# MAGIC Read-only: reports what is held past its window; nothing is deleted.

# COMMAND ----------

from lakelogic.core.slo import SLOValidator

if not registry.retention:
    print(f"{DOMAIN}/{SYSTEM}: no retention declared (not_configured). Nothing to check.")
    results = []
else:
    results = SLOValidator(registry, spark=spark).check_retention()  # noqa: F821

checked_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
rows, breached = [], []
for r in results:
    status = str(getattr(r, "status", "") or "").lower()
    is_breach = status in ("failed", "fail", "breached", "violation") or getattr(r, "passed", None) is False
    if is_breach:
        breached.append(r)
    rows.append({
        "layer": getattr(r, "layer", None),
        "entity": getattr(r, "entity", None),
        "state": "breached" if is_breach else "observed",
    })
    print(f"  {rows[-1]['state']:<9} {getattr(r, 'layer', '?'):<8} {getattr(r, 'entity', '?')}")


def _window(layer):
    ret = registry.retention or {}
    val = ret.get(layer) if isinstance(ret, dict) else getattr(ret, layer, None)
    return str(val) if val else None


try:  # one normaliser for new tokens (ERROR, NO_DATA, BREACHED, OK) and old icon strings
    from lakelogic.core.slo import normalize_status as _token
except ImportError:  # older lakelogic
    def _token(s):
        u = str(s or "").upper()
        return "ERROR" if "ERROR" in u else "NO_DATA" if "NO DATA" in u else None


def _outcome(r, row):
    token = _token(getattr(r, "status", None))
    if token == "ERROR":
        return "error"
    if token == "NO_DATA":
        return "no_data"
    return "breached" if row["state"] == "breached" else "passed"


def _detail(r):
    msg = getattr(r, "message", None)
    if msg:
        return str(msg)
    return str(getattr(r, "status", "") or "").encode("ascii", "ignore").decode().strip()


_now = datetime.datetime.now(datetime.timezone.utc)
# rows_expired is None ("not measured"), never 0: this check is read-only.
_engine_rows = [{
    "run_id": RUN_ID, "timestamp": _now,
    "table_name": f"{DOMAIN}.{row['layer']}.{row['entity']}",
    "policy": _window(row["layer"]),
    "cutoff": getattr(r, "cutoff", None) if isinstance(getattr(r, "cutoff", None), datetime.datetime) else None,
    "rows_expired": None,
    "status": _outcome(r, row),
    "detail": _detail(r),
} for row, r in zip(rows, results)]
written_to = write_evidence_rows("retention_evidence", _engine_rows, EVIDENCE_METADATA, engine_name="spark")
if _engine_rows and not written_to:
    raise Exception(f"Retention evidence was NOT recorded in {EVIDENCE_TABLE} - see the warning above.")
print(f"Recorded {len(_engine_rows) if written_to else 0} observation(s) in {written_to or EVIDENCE_TABLE}.")

# Send results to LakeLogic as retention checks, if the installed lakelogic supports it.
if results:
    import inspect as _inspect

    from lakelogic.core.run_log import emit_slo_report

    if "record_type" in _inspect.signature(emit_slo_report).parameters:
        sent = emit_slo_report(registry, results, environment=ENVIRONMENT,
                               record_type="retention_check", engine="retention")
        print(f"Reported {sent} retention row(s) to LakeLogic.")
    else:
        print("This lakelogic cannot report retention checks separately - upgrade to send them to LakeLogic.")

# Every table is checked and its evidence written BEFORE this.
if breached:
    if FAIL_ON_BREACH:
        raise Exception(f"{len(breached)} table(s) hold data beyond their declared retention period.")
    print(f"{len(breached)} table(s) breach retention; recorded, not failing the job (fail_on_breach=false).")
