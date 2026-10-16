# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # compliance_erasure - right to erasure (Databricks)
# MAGIC
# MAGIC Plans by default. Changes data only with <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">dry_run = "false"</span> and <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">confirm = "ERASE"</span>. LakeLogic's engine writer records one row per table to <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_lakelogic_erasure_evidence</span>, beside the domain's <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_lakelogic_run_log</span>. Rows carry counts only - never a subject ID.
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC 1. Installs LakeLogic.
# MAGIC 2. Reads the parameters and, optionally, configures telemetry.
# MAGIC 3. Loads the system registry.
# MAGIC 4. Resolves the engine's erasure evidence table.
# MAGIC 5. Plans the erasure per table, applies it only when confirmed, and records one evidence row per table.
# MAGIC
# MAGIC **Parameters**
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">catalog</span> | Unity Catalog this run reads and writes |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">contracts_root</span> | workspace folder holding <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">contracts/</span> |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">domain</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">system</span> | which <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_system.yaml</span> registry to load |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">environment</span> | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">dev</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">staging</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">prod</span> |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">storage_mode</span> | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">catalog</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">path</span> |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">subject_column</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">subject_ids</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">subject_ids_file</span> | who to erase |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">strategy</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">hash_salt</span> | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">nullify</span> or hash; salt for hashing |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">entity_filter</span> | restrict to these entities |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">dry_run</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">confirm</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">case_ref</span> | plan by default; apply needs <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">dry_run=false</span>, <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">confirm=ERASE</span> and a case reference |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">telemetry_endpoint</span>, <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">telemetry_key</span> (or <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">telemetry_key_scope</span> + <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">telemetry_key_secret</span>) | optional run reporting to LakeLogic |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">lakelogic_version</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">lakelogic_wheel</span> | install source (blank = latest from PyPI; a wheel wins) |
# MAGIC
# MAGIC Generated by the LakeLogic Medallion Builder — regenerate, do not hand-edit.
# MAGIC generated by: LakeLogic Medallion Builder
# MAGIC revision: chv1:e7b0c49f07bce8fda7e01b76564ac41c80ddcbb956e46bec30c967061acb4e1a
# MAGIC governance seal: sha256:5dec3512859b80333097a50f1f3bd620ff4ec644a86a89f7ebd2a821ba9cc108
# MAGIC source: estate snapshot snap-d91da9d87d0e48ba
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install LakeLogic
# MAGIC
# MAGIC Databricks pre-installs the packages named in a <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">%pip</span> cell at session start, so a
# MAGIC bare <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">lakelogic</span> can report "already satisfied" and silently keep an older build --
# MAGIC the run then tests the wrong code while looking correct. <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">--upgrade</span> (or an explicit
# MAGIC <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">==</span>) forces pip to act. <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">restartPython()</span> is required after any <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">%pip</span> install
# MAGIC before the new version is importable.

# COMMAND ----------

# DBTITLE 1,Resolve install source
# Declare the parameters (the job passes values for these).
dbutils.widgets.text("lakelogic_version", "", "LakeLogic version (blank = latest)")
dbutils.widgets.text("lakelogic_wheel", "", "LakeLogic wheel (blank = PyPI)")

# Read them.
_version = dbutils.widgets.get("lakelogic_version").strip()
_wheel = dbutils.widgets.get("lakelogic_wheel").strip()
# [synthetic] brings Faker for realistic values; [pdf] reads PDFs for document contracts.
# A wheel wins over a version. `--force-reinstall` because Databricks pre-installs the
# packages named in this cell at session start: without it a same-version wheel is
# "already satisfied" and the run silently executes the PUBLISHED build instead.
if _wheel:
    # `[synthetic,pdf]` on the wheel path too: a bare wheel installs no extras, and the
    # synthetic seed needs reportlab to write documents. pip accepts extras on a local
    # file exactly as it does on a package name.
    lakelogic_pkg = f"{_wheel}[synthetic,pdf] --force-reinstall"
elif _version:
    lakelogic_pkg = f"lakelogic[synthetic,pdf]=={_version}"
else:
    lakelogic_pkg = "lakelogic[synthetic,pdf] --upgrade"
print(f"Installing LakeLogic from: {lakelogic_pkg}")

# COMMAND ----------

# DBTITLE 1,Install LakeLogic
# MAGIC # pyarrow<25: DBR ships an older pyarrow than lakelogic needs, and
# MAGIC # databricks-connect caps it below 25. pyyaml/polars/deltalake arrive as
# MAGIC # lakelogic's own dependencies and are not named here.
# MAGIC %pip install $lakelogic_pkg "pyarrow<25"

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parameters
# MAGIC
# MAGIC Widgets arrive as strings; a job run overrides them per run. One widget per line.

# COMMAND ----------

# DBTITLE 1,Parameters
# Parameters - the pipeline overrides these per (domain, system). Widgets return STRINGS.
# Declare the parameters (the job passes values for these).
dbutils.widgets.text("catalog", "", "catalog")  # the Unity Catalog this run reads and writes
dbutils.widgets.text("contracts_root", "", "contracts_root")
dbutils.widgets.text("domain", "lakehouse", "domain")
dbutils.widgets.text("system", "lakehouse", "system")
dbutils.widgets.text("environment", "dev", "environment")                 # a key of _system.yaml environments
dbutils.widgets.text("storage_mode", "catalog", "storage_mode")
dbutils.widgets.text("subject_column", "", "subject_column")              # the column that identifies a data subject, e.g. rider_id
dbutils.widgets.text("subject_ids", "", "subject_ids")                    # comma-separated IDs - or use subject_ids_file
dbutils.widgets.text("subject_ids_file", "", "subject_ids_file")          # a Volume file with one ID per line, e.g. /Volumes/<catalog>/nondelta/lakelogic_logs/requests/r1.txt
dbutils.widgets.text("strategy", "nullify", "strategy")                   # nullify | hash | redact; used when the contract declares none
dbutils.widgets.text("hash_salt", "", "hash_salt")                        # used by the hash strategy
dbutils.widgets.text("entity_filter", "", "entity_filter")                # comma-separated entities; blank = every contract
dbutils.widgets.text("dry_run", "true", "dry_run")                        # changes data only when dry_run is "false"...
dbutils.widgets.text("confirm", "", "confirm")                            # ...AND confirm is exactly "ERASE"
dbutils.widgets.text("case_ref", "", "case_ref")                          # the request this answers, e.g. a ticket id - required to change data
dbutils.widgets.text("telemetry_endpoint", "", "telemetry_endpoint")      # LakeLogic run-log ingest URL. Empty = do not report.
dbutils.widgets.text("telemetry_key", "", "telemetry_key")                # LakeLogic API key. Supplied per run, never stored here.
dbutils.widgets.text("telemetry_key_scope", "", "telemetry_key_scope")    # OR: the Databricks secret SCOPE holding the key
dbutils.widgets.text("telemetry_key_secret", "", "telemetry_key_secret")  # OR: the secret NAME in that scope. Not a secret itself.

# Read them.
catalog = dbutils.widgets.get("catalog")
contracts_root = dbutils.widgets.get("contracts_root")
domain = dbutils.widgets.get("domain")
system = dbutils.widgets.get("system")
environment = dbutils.widgets.get("environment")
storage_mode = dbutils.widgets.get("storage_mode")
subject_column = dbutils.widgets.get("subject_column")
subject_ids = dbutils.widgets.get("subject_ids")
subject_ids_file = dbutils.widgets.get("subject_ids_file")
strategy = dbutils.widgets.get("strategy")
hash_salt = dbutils.widgets.get("hash_salt")
entity_filter = dbutils.widgets.get("entity_filter")
dry_run = dbutils.widgets.get("dry_run")
confirm = dbutils.widgets.get("confirm")
case_ref = dbutils.widgets.get("case_ref")
telemetry_endpoint = dbutils.widgets.get("telemetry_endpoint")
telemetry_key = dbutils.widgets.get("telemetry_key")
telemetry_key_scope = dbutils.widgets.get("telemetry_key_scope")
telemetry_key_secret = dbutils.widgets.get("telemetry_key_secret")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Telemetry
# MAGIC
# MAGIC Reports this run to LakeLogic only when an endpoint and a key are supplied. The key comes from the run parameter or a Databricks secret scope; it is never printed.

# COMMAND ----------

# DBTITLE 1,Configure telemetry
import os
# Report this run to LakeLogic if an endpoint and key are supplied.
# Key: `telemetry_key` (Run dialog, for test runs), or `telemetry_key_scope` + `telemetry_key_secret`
# (the job run-as identity needs READ on the scope; use this for scheduled runs).
_telemetry_key = str(telemetry_key).strip()
_telemetry_source = "run parameter"
if not _telemetry_key and str(telemetry_key_scope).strip() and str(telemetry_key_secret).strip():
    try:
        _telemetry_key = dbutils.secrets.get(
            str(telemetry_key_scope).strip(), str(telemetry_key_secret).strip()
        ).strip()
        _telemetry_source = "secret scope"
    except Exception as _kv_exc:
        # Print the exception type only, so no secret is echoed.
        print(f"Telemetry: could not read the key from the secret scope ({type(_kv_exc).__name__})")
if str(telemetry_endpoint).strip() and _telemetry_key:
    os.environ["LAKELOGIC_CLOUD_ENDPOINT"] = str(telemetry_endpoint).strip()
    os.environ["LAKELOGIC_CLOUD_API_KEY"] = _telemetry_key
    print(f"Telemetry: reporting to {telemetry_endpoint} (key from {_telemetry_source})")
else:
    print("Telemetry: not configured for this run — run logs stay in the lakehouse.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load the registry
# MAGIC
# MAGIC Resolves <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">{contracts_root}/{domain}/{system}/_system.yaml</span> and the helpers that map each contract to its physical table.

# COMMAND ----------

# DBTITLE 1,Load registry
import os, json, hashlib, datetime, traceback, uuid

os.environ[f"DATABRICKS_{environment.upper()}_CATALOG"] = catalog
REGISTRY_PATH = f"{contracts_root}/{domain}/{system}/_system.yaml"
print(f"Registry: {REGISTRY_PATH}  (env: {environment})")

from lakelogic.core.registry import DomainRegistry

registry = DomainRegistry.from_yaml(REGISTRY_PATH, environment=environment, storage_mode=storage_mode)
RUN_ID = str(uuid.uuid4())

if "spark" not in dir():
    raise RuntimeError("No Spark session - run this notebook attached to the lakehouse.")


def table_of(contract):
    """The physical table a contract writes, resolved as SLOValidator resolves it: the
    contract's own info.table_name under the layer root, else under the domain catalog."""
    info = (contract.contract_dict or {}).get("info") or {}
    table = str(info.get("table_name") or "").strip()
    if not table:
        return None
    base = getattr(registry.storage, f"{contract.layer}_root", None) or getattr(
        registry.storage, "domain_catalog", None
    )
    return f"{base}.{table}".replace("`", "") if base else None


def exists(table):
    try:
        return bool(spark.catalog.tableExists(table))  # noqa: F821 - Databricks injects spark
    except Exception:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evidence table
# MAGIC
# MAGIC The engine owns the evidence table: its name, schema and location beside the domain's <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_lakelogic_run_log</span>.

# COMMAND ----------

# DBTITLE 1,Resolve evidence table
from lakelogic.core.evidence_tables import resolve_evidence_target, write_evidence_rows

# The engine owns the evidence table: its name (`_lakelogic_erasure_evidence`), its schema,
# and its place - beside the run log this system declares in `_system.yaml`
# (`metadata.run_log_table`), i.e. in the domain schema next to `_lakelogic_run_log`.
_run_log_table = str((getattr(registry, "metadata", None) or {}).get("run_log_table") or "")
if not _run_log_table or "{" in _run_log_table:
    _run_log_table = f"{registry.storage.domain_catalog}._lakelogic_run_log"
EVIDENCE_METADATA = {"run_log_table": _run_log_table, "run_log_backend": "spark"}
_target = resolve_evidence_target("erasure_evidence", EVIDENCE_METADATA, engine_name="spark")
EVIDENCE_TABLE = _target[1] if _target else None
print(f"Erasure evidence: {EVIDENCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Plan or apply the erasure
# MAGIC
# MAGIC Changes data only with <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">dry_run = "false"</span> and <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">confirm = "ERASE"</span>. Evidence carries counts and a digest, never a subject ID.

# COMMAND ----------

# DBTITLE 1,Run erasure
import importlib.metadata

from lakelogic.core.gdpr import _get_pii_column_names
from lakelogic.core.models import DataContract
from lakelogic.pipeline import LakehousePipeline

DRY_RUN = str(dry_run).strip().lower() != "false"
CONFIRMED = str(confirm).strip() == "ERASE"
EXECUTE = (not DRY_RUN) and CONFIRMED
MODE = "executed" if EXECUTE else ("refused_unconfirmed" if not DRY_RUN else "dry_run")
# Changing data requires case_ref, so evidence links to the request. Dry runs do not.
CASE_REF = str(case_ref).strip()
if EXECUTE and not CASE_REF:
    raise Exception('dry_run is "false" and confirm is "ERASE", but case_ref is empty - '
                    "set the request reference this erasure answers. Nothing was changed.")
CASE_REF = CASE_REF or f"dry-run-{RUN_ID}"
COLUMN = str(subject_column).strip()
REQUESTED = str(strategy).strip().lower() or "nullify"

ids = [s.strip() for s in str(subject_ids).split(",") if s.strip()]
if str(subject_ids_file).strip():
    _path = str(subject_ids_file).strip()
    if not _path.startswith("/"):
        raise Exception(
            f"subject_ids_file must be an absolute path (a Unity Catalog Volume, e.g. "
            f"/Volumes/<catalog>/nondelta/lakelogic_logs/requests/r1.txt). Got: {_path}"
        )
    with open(_path, encoding="utf-8") as fh:
        ids += [line.strip() for line in fh if line.strip()]
ids = sorted(set(ids))
# Neither the evidence table nor LakeLogic receives the IDs: only the count and this digest
# (printed below), which lets the request's own ID list be matched to this run.
SUBJECTS_SHA256 = hashlib.sha256("\n".join(ids).encode("utf-8")).hexdigest() if ids else None
WANTED = {e.strip().lower() for e in str(entity_filter).split(",") if e.strip()}

print(f"Right to Erasure {domain}/{system}: column={COLUMN!r} subjects={len(ids)} mode={MODE}")
pipeline = LakehousePipeline(registry, engine="spark", spark=spark)  # noqa: F821
try:
    LAKELOGIC_VERSION = importlib.metadata.version("lakelogic")
except Exception:
    LAKELOGIC_VERSION = None


def matching(table, where=""):
    quoted = ", ".join("'" + i.replace("'", "''") + "'" for i in ids)
    sql = f"SELECT COUNT(*) AS n FROM {table} WHERE `{COLUMN}` IN ({quoted}){where}"
    return spark.sql(sql).first()["n"]  # noqa: F821


evidence, unresolved = [], []
if not (COLUMN and ids):
    print("Nothing to erase: set subject_column and subject_ids (or subject_ids_file).")
else:
    for c in registry.get_active_contracts():
        if WANTED and c.entity.lower() not in WANTED:
            continue
        dc = DataContract(**(c.contract_dict or {}))
        pii = sorted(_get_pii_column_names(dc) or [])
        fields = {f.name for f in (dc.model.fields if dc.model else [])}
        if not pii or COLUMN not in fields:
            continue
        table = table_of(c)
        effective = pipeline._resolve_erasure_strategy(c.contract_dict, REQUESTED)
        row = {
            "run_id": RUN_ID, "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "domain": domain, "system": system, "environment": environment, "entity": c.entity,
            "table": table, "subject_column": COLUMN, "subject_count": len(ids),
            "subjects_sha256": SUBJECTS_SHA256, "strategy": effective, "pii_columns": pii,
            "mode": MODE, "case_ref": CASE_REF, "lakelogic_version": LAKELOGIC_VERSION,
        }
        if not (table and exists(table)):
            row["outcome"] = "table_not_found"
            evidence.append(row)
            continue
        row["matched_rows"] = matching(table)
        if MODE == "refused_unconfirmed":
            row["outcome"] = "not_run"
        else:
            try:
                # Pass case_ref only if lakelogic supports it; older versions erase without reporting.
                import inspect as _inspect

                _gdpr_kwargs = (
                    {"case_ref": CASE_REF}
                    if "case_ref" in _inspect.signature(pipeline._execute_gdpr_pass).parameters else {}
                )
                if not _gdpr_kwargs:
                    print("  this lakelogic does not report privacy evidence - upgrade to send it to LakeLogic")
                pipeline._execute_gdpr_pass([c], COLUMN, ids, REQUESTED, str(hash_salt), not EXECUTE, **_gdpr_kwargs)
                row["engine_report_file"] = "written"
            except OSError as exc:
                # The report file path is Databricks-only; the change already ran and is recorded here.
                row["engine_report_file"] = f"not written ({type(exc).__name__})"
            row["outcome"] = "planned" if not EXECUTE else "applied"
            if EXECUTE and effective in ("nullify", "redact"):
                cols = [p for p in pii if p != COLUMN]
                if effective == "nullify":
                    still = " OR ".join(f"`{p}` IS NOT NULL" for p in cols)
                else:
                    still = " OR ".join(f"CAST(`{p}` AS STRING) <> '***REDACTED***'" for p in cols)
                remaining = matching(table, f" AND ({still})") if still else 0
                row["rows_still_holding_values"] = remaining
                if remaining:
                    row["outcome"] = "incomplete"
                    unresolved.append((c.entity, remaining))
        evidence.append(row)
        print(f"  {c.entity:<45} {row['outcome']:<16} matched={row.get('matched_rows')} strategy={effective}")

print(f"Subjects digest (sha256): {SUBJECTS_SHA256}  case_ref: {CASE_REF}  mode: {MODE}")
_now = datetime.datetime.now(datetime.timezone.utc)
_engine_rows = [{
    "run_id": RUN_ID, "timestamp": _now, "profile": "gdpr", "dataset": r["entity"],
    "table_name": r["table"], "subject_count": r["subject_count"],
    "rows_affected": r.get("matched_rows"), "reason": f"{MODE}:{CASE_REF}",
    "status": r["outcome"], "contract": r["entity"], "contract_version": None,
    "domain": domain, "system": system,
} for r in evidence]
written_to = write_evidence_rows("erasure_evidence", _engine_rows, EVIDENCE_METADATA, engine_name="spark")
if _engine_rows and not written_to:
    raise Exception(f"Erasure evidence was NOT recorded in {EVIDENCE_TABLE} - see the warning above.")
print(f"Recorded {len(_engine_rows) if written_to else 0} row(s) in {written_to or EVIDENCE_TABLE}.")
if MODE == "refused_unconfirmed":
    raise Exception('dry_run is "false" but confirm is not exactly "ERASE" - nothing was changed.')
if unresolved:
    raise Exception(f"Erasure incomplete - personal data still present: {unresolved}")
