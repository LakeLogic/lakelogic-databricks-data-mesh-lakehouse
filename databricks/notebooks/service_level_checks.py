# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : service_level_checks — Service Level Checks
# Purpose   : Evaluates the `slo:` block a domain declares — per-layer freshness
#             and pipeline scheduling — against the tables actually present, and
#             reports the verdict to LakeLogic Cloud so Data Products can show a
#             service level instead of "Not evaluated".
#
# Named for what it CHECKS, not for one of the checks. It was
# `slo_freshness_check`, which described freshness alone while the validator also
# evaluates row counts and schedule — and "SLO" is the acronym, not the thing.
#
# Widgets:
#   registry_path : Required. Path to the evaluated _registry.yaml
#   environment   : dev | staging | prod 
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

import os

from lakelogic.core.registry import DomainRegistry
from lakelogic.core.slo import SLOValidator
from lakelogic.core.run_log import emit_slo_report
import json

try:
    print(f"Loading Registry: {REGISTRY_PATH} (env: {ENVIRONMENT})")
    # ── Catalog — derived from the registry Volume path ──────────────────────
    # THE SAME DERIVATION THE PIPELINE DRIVER USES. Without it `{catalog}` resolves
    # to an empty string and every run-log query is built as
    #     FROM .marketplace._pipeline_run_log
    # which fails with [PARSE_SYNTAX_ERROR] Syntax error at or near '.'. The catalog
    # is read from /Volumes/<catalog>/... so the tables this checks always match the
    # catalog the job is deployed against — one source of truth, same as the pipeline.
    _derived_catalog = "rideflow_dev_demo"
    if REGISTRY_PATH.startswith("/Volumes/"):
        _parts = REGISTRY_PATH.split("/")
        if len(_parts) > 2 and _parts[2]:
            _derived_catalog = _parts[2]
    os.environ.setdefault(f"RIDEFLOW_{ENVIRONMENT.upper()}_CATALOG", _derived_catalog)
    print(f"Catalog: {_derived_catalog}")

    # `storage_mode="uc"` matches the pipeline driver: the roots must resolve to the
    # same Unity Catalog tables the pipeline writes, or the checks read nothing.
    registry = DomainRegistry.from_yaml(REGISTRY_PATH, environment=ENVIRONMENT, storage_mode="uc")
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
    
    # Cloud sync — THE SAME PATH THE PIPELINE DRIVER USES.
    #
    # This used to go through `RemoteObserver`, a different mechanism that is off
    # unless LAKELOGIC_REMOTE_OBSERVER=true, addressed by LINEAGELOGIC_REPORT_URL
    # (an env var nothing sets), and posting a `{"type": "slo"}` body the platform
    # has no handler for. It also checked `registry.cloud.report_url` and then built
    # the observer WITHOUT passing it, so a configured endpoint was ignored. Nothing
    # ever arrived: 0 of 2,082 recorded runs carried an SLO result.
    #
    # `emit_slo_report` uses the observatory config, headers, endpoint and spooling
    # that carry every pipeline run log, and shapes the body so results land in
    # `run_metadata.slo` — where the platform already reads them.
    print("\nSyncing service levels to LakeLogic Cloud...")
    # Pass the REPORT, not `report.results`. The report carries the `check_run_id`
    # that `run_checks()` minted, and `emit_slo_report` stamps it on every row so the
    # platform can group the posts back into one invocation — otherwise it receives N
    # independent rows and cannot say "15 of 71 objectives breached" or send one
    # notification per check run instead of one per failing entity. The emitter
    # duck-types this, so passing the list still works; it just arrives with no id.
    sent = emit_slo_report(
        registry,
        report,
        environment=ENVIRONMENT,
        pipeline_run_id=PIPELINE_RUN_ID or None,
    )
    print(f"  Sent {sent} entity rows.")
            
    # Set exit states for Databricks workflows
    dbutils.jobs.taskValues.set(key="slo_passed", value=report.passed)
    if not report.passed:
        raise Exception(f"SLO checks failed: {len(report.failures)} violations detected.")
        
except Exception as e:
    print(f"\n❌ SLO evaluation failed: {e}")
    raise
