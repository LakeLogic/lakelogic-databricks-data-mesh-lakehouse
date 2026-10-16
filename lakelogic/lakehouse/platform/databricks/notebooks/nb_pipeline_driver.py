# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # pipeline_driver — registry-driven medallion pipeline (Databricks)
# MAGIC
# MAGIC Runs one system's contracts bronze → silver → gold into its domain schema in the
# MAGIC target Unity Catalog. Edit the **contracts**, not this notebook.
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC 1. Installs LakeLogic (wheel, pinned version, or latest).
# MAGIC 2. Reads the run parameters and converts them to typed values.
# MAGIC 3. Loads the <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_system.yaml</span> registry, with an optional synthetic retarget.
# MAGIC 4. Runs the contracts through the requested layers.
# MAGIC 5. Prints the run summary and renders the pipeline lineage (DAG).
# MAGIC 6. Fails the task if any contract failed, with the tracebacks from the run log.
# MAGIC
# MAGIC **Parameters**
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">catalog</span> | Unity Catalog this run reads and writes |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">contracts_root</span> | workspace folder holding <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">contracts/</span> |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">domain</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">system</span> | which <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_system.yaml</span> registry to load |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">environment</span> | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">dev</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">staging</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">prod</span> |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">engine</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">storage_mode</span> | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">spark</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">polars</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">duckdb</span>; <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">catalog</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">path</span> |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">target_layers</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">entity_filter</span> | what to process |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">reset_layers</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">reload_layers</span> | DROP or TRUNCATE layers first |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">dry_run</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">ddl_only</span> | validate only; create tables only |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">retry_attempts</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">retry_wait_sec</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">max_failures</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">resume_run</span> | resilience |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">reprocess_*</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">lookback_days</span> | restate a date range or keys |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">parallel</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">max_workers</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">entity_timeout_minutes</span> | concurrency |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">landing_root_override</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">synthetic_schema_prefix</span> | synthetic retarget |
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
# Parameters cell — the deploy/orchestration layer overrides these per (domain, system)
# run. Values arrive as strings; the next cell converts them.
# Declare the parameters (the job passes values for these).
dbutils.widgets.text("catalog", "", "catalog")        # the Unity Catalog this run reads and writes
dbutils.widgets.text("contracts_root", "", "contracts_root")
dbutils.widgets.text("domain", "lakehouse", "domain")
dbutils.widgets.text("system", "lakehouse", "system")
dbutils.widgets.text("environment", "dev", "environment")                       # a key of _system.yaml environments
dbutils.widgets.text("engine", "spark", "engine")                               # spark | polars | duckdb
dbutils.widgets.text("storage_mode", "catalog", "storage_mode")                 # catalog (schema-enabled lakehouse) | path
dbutils.widgets.text("target_layers", "bronze, silver, gold", "target_layers")  # layers to process
dbutils.widgets.text("entity_filter", "", "entity_filter")                      # comma-separated entities to restrict to
dbutils.widgets.text("reset_layers", "", "reset_layers")                        # DROP these layers before processing
dbutils.widgets.text("reload_layers", "", "reload_layers")                      # TRUNCATE these layers before processing
dbutils.widgets.text("dry_run", "false", "dry_run")                             # validate contracts, write nothing
dbutils.widgets.text("ddl_only", "false", "ddl_only")                           # create tables only, no data
dbutils.widgets.text("retry_attempts", "1", "retry_attempts")                   # per-contract retries
dbutils.widgets.text("retry_wait_sec", "30", "retry_wait_sec")                  # base wait between retries (seconds)
dbutils.widgets.text("max_failures", "0", "max_failures")                       # stop after N consecutive failures (0 = unlimited)
dbutils.widgets.text("resume_run", "", "resume_run")                            # resume a previous pipeline run id
dbutils.widgets.text("created_by", "", "created_by")                            # stamped into lineage metadata
dbutils.widgets.text("landing_root_override", "", "landing_root_override")      # read bronze from THIS root instead of each contract's source.path (e.g. a staging copy)
dbutils.widgets.text("synthetic_schema_prefix", "", "synthetic_schema_prefix")  # prefix every output schema with this when retargeting, so it never mixes with prod
dbutils.widgets.text("landing_runtime", "", "landing_runtime")  # where landing files come from (Deliver data-source step); blank = the registry's own path
dbutils.widgets.text("telemetry_endpoint", "", "telemetry_endpoint")            # LakeLogic run-log ingest URL. Empty = do not report.
dbutils.widgets.text("telemetry_key", "", "telemetry_key")                      # LakeLogic API key. Supplied per run, never stored here.
dbutils.widgets.text("telemetry_key_scope", "", "telemetry_key_scope")          # OR: the Databricks secret SCOPE holding the key
dbutils.widgets.text("telemetry_key_secret", "", "telemetry_key_secret")        # OR: the secret NAME in that scope. Not a secret itself.
dbutils.widgets.text("reprocess_from", "", "reprocess_from")                    # restate from this date (YYYY-MM-DD)
dbutils.widgets.text("reprocess_to", "", "reprocess_to")                        # restate to this date, inclusive
dbutils.widgets.text("reprocess_column", "", "reprocess_column")                # the date column the range applies to
dbutils.widgets.text("reprocess_values", "", "reprocess_values")                # comma-separated keys to restate
dbutils.widgets.text("lookback_days", "", "lookback_days")                      # re-read this many days regardless of watermark
dbutils.widgets.text("parallel", "false", "parallel")                           # run independent contracts concurrently
dbutils.widgets.text("max_workers", "4", "max_workers")                         # threads when parallel = true
dbutils.widgets.text("entity_timeout_minutes", "", "entity_timeout_minutes")    # abandon one contract after this long

# Read them.
catalog = dbutils.widgets.get("catalog")
contracts_root = dbutils.widgets.get("contracts_root")
domain = dbutils.widgets.get("domain")
system = dbutils.widgets.get("system")
environment = dbutils.widgets.get("environment")
engine = dbutils.widgets.get("engine")
storage_mode = dbutils.widgets.get("storage_mode")
target_layers = dbutils.widgets.get("target_layers")
entity_filter = dbutils.widgets.get("entity_filter")
reset_layers = dbutils.widgets.get("reset_layers")
reload_layers = dbutils.widgets.get("reload_layers")
dry_run = dbutils.widgets.get("dry_run")
ddl_only = dbutils.widgets.get("ddl_only")
retry_attempts = dbutils.widgets.get("retry_attempts")
retry_wait_sec = dbutils.widgets.get("retry_wait_sec")
max_failures = dbutils.widgets.get("max_failures")
resume_run = dbutils.widgets.get("resume_run")
created_by = dbutils.widgets.get("created_by")
landing_root_override = dbutils.widgets.get("landing_root_override")
synthetic_schema_prefix = dbutils.widgets.get("synthetic_schema_prefix")
landing_runtime = dbutils.widgets.get("landing_runtime")
telemetry_endpoint = dbutils.widgets.get("telemetry_endpoint")
telemetry_key = dbutils.widgets.get("telemetry_key")
telemetry_key_scope = dbutils.widgets.get("telemetry_key_scope")
telemetry_key_secret = dbutils.widgets.get("telemetry_key_secret")
reprocess_from = dbutils.widgets.get("reprocess_from")
reprocess_to = dbutils.widgets.get("reprocess_to")
reprocess_column = dbutils.widgets.get("reprocess_column")
reprocess_values = dbutils.widgets.get("reprocess_values")
lookback_days = dbutils.widgets.get("lookback_days")
parallel = dbutils.widgets.get("parallel")
max_workers = dbutils.widgets.get("max_workers")
entity_timeout_minutes = dbutils.widgets.get("entity_timeout_minutes")


# COMMAND ----------

# DBTITLE 1,Landing source library
"""Where a delivered notebook reads its landing files from — run inside the delivered project.

This module imports only the standard library, and its source is pasted into the generated Databricks and Fabric notebooks in place of
their landing-runtime placeholder, so the pipeline it runs and the function tested here are the
same code. The Deliver data-source step (:mod:`deployment_target_data_source`) renders the
``landing_runtime`` spec it consumes:

* ``env``     ENV_NAME → the value of a per-environment parameter (already interpolated by the
  platform: a bundle variable on Databricks, a pipeline parameter on Fabric);
* ``secrets`` ENV_NAME → [scope, key] NAMES, read here through the platform's secret reader;
* ``auth``    for a direct connection, the service principal, each part a literal or ``$ENV_NAME``.

It fills ``os.environ`` before ``DomainRegistry.from_yaml`` (which resolves the ``ls_*`` keys
from it) and sets account-scoped Azure OAuth for a direct connection. The client secret is held
only in the Spark config: it is never exported to the environment and never printed.
"""
import json as _lr_json
import os as _lr_os


class LandingRuntimeError(RuntimeError):
    """The landing source cannot be set up; the message names the variable to set."""


def apply_landing_runtime(spec, get_secret, set_conf, environ=None):
    """Apply ``spec`` (a dict or its JSON) and return what was set, by NAME only.

    ``get_secret(scope, key)`` reads a secret (``dbutils.secrets.get`` on Databricks,
    ``notebookutils.credentials.getSecret`` on Fabric); ``set_conf(key, value)`` sets a Spark
    config (``spark.conf.set``)."""
    environ = _lr_os.environ if environ is None else environ
    if isinstance(spec, str):
        spec = _lr_json.loads(spec) if spec.strip() else {}
    spec = spec or {}
    auth = spec.get("auth") or None
    # The secret behind the client secret authenticates only: keep it out of the environment.
    private = {auth["client_secret"][1:]} if auth and str(auth.get("client_secret", "")).startswith("$") else set()
    values = {}
    for name, value in (spec.get("env") or {}).items():
        value = "" if value is None else str(value)
        if not value.strip() or value.startswith("${"):
            raise LandingRuntimeError(
                f"Landing parameter {name.lower()} has no value for this environment. "
                f"Set it on the deployment target (bundle variable / pipeline parameter) and re-run."
            )
        values[name] = value.strip()
    for name, ref in (spec.get("secrets") or {}).items():
        scope, key = (list(ref) + ["", ""])[:2]
        if not str(scope).strip() or not str(key).strip() or str(scope).startswith("${"):
            raise LandingRuntimeError(
                f"Secret for {name.lower()} has no scope or key name for this environment. "
                f"Set {name.lower()}_scope and {name.lower()}_key and re-run."
            )
        try:
            values[name] = get_secret(str(scope).strip(), str(key).strip())
        except Exception as exc:  # the platform's message names the scope; never the value
            raise LandingRuntimeError(
                f"Could not read secret {key!r} from {scope!r} for {name.lower()}: {type(exc).__name__}"
            ) from None
    for name, value in values.items():
        if name not in private:
            environ[name] = value

    configured = None
    if auth and auth.get("type") == "azure_service_principal":
        def _v(part):
            raw = str(auth.get(part) or "")
            return values.get(raw[1:], environ.get(raw[1:], "")) if raw.startswith("$") else raw

        account, tenant, client, secret = (_v("storage_account"), _v("tenant_id"),
                                           _v("client_id"), _v("client_secret"))
        missing = [p for p, v in (("storage_account", account), ("tenant_id", tenant),
                                  ("client_id", client), ("client_secret", secret)) if not v]
        if missing:
            raise LandingRuntimeError(f"Direct connection is missing {', '.join(missing)}.")
        host = f"{account}.dfs.core.windows.net"
        set_conf(f"fs.azure.account.auth.type.{host}", "OAuth")
        set_conf(f"fs.azure.account.oauth.provider.type.{host}",
                 "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        set_conf(f"fs.azure.account.oauth2.client.id.{host}", client)
        set_conf(f"fs.azure.account.oauth2.client.secret.{host}", secret)
        set_conf(f"fs.azure.account.oauth2.client.endpoint.{host}",
                 f"https://login.microsoftonline.com/{tenant}/oauth2/token")
        configured = host
    return {"environment": sorted(n for n in values if n not in private),
            "secrets": sorted((spec.get("secrets") or {}).keys()),
            "direct_connection": configured}


_FABRIC_API = "https://api.fabric.microsoft.com/v1"


def ensure_onelake_shortcut(spec, get_token, workspace_id, lakehouse_id, request, environ=None):
    """Create the OneLake shortcut ``Files/<name>`` → ADLS that a Fabric ``shortcut`` data
    source reads through, once. Run by setup after :func:`apply_landing_runtime`.

    An existing shortcut with the same target is left alone; one pointing anywhere else stops
    setup rather than read the wrong storage. ``request(method, url, headers, body)`` returns
    ``(status, json_or_None)``; ``get_token()`` returns a Fabric API bearer token. The
    connection's credentials stay in Fabric: only its ID is sent."""
    environ = _lr_os.environ if environ is None else environ
    if isinstance(spec, str):
        spec = _lr_json.loads(spec) if spec.strip() else {}
    sc = (spec or {}).get("shortcut")
    if not sc:
        return None

    def _v(part):
        raw = str(sc.get(part) or "")
        return environ.get(raw[1:], "") if raw.startswith("$") else raw

    name, account, container = _v("shortcut_name"), _v("storage_account"), _v("container")
    folder, connection = _v("folder").strip("/"), _v("connection_id")
    missing = [p for p, v in (("shortcut_name", name), ("storage_account", account),
                              ("container", container), ("connection_id", connection)) if not v]
    if missing:
        raise LandingRuntimeError(f"OneLake shortcut is missing {', '.join(missing)}.")
    location = f"https://{account}.dfs.core.windows.net"
    subpath = "/" + "/".join(p for p in (container, folder) if p)
    base = f"{_FABRIC_API}/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
    headers = {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

    status, body = request("GET", f"{base}/Files/{name}", headers, None)
    if status == 200:
        target = ((body or {}).get("target") or {}).get("adlsGen2") or {}
        same = (str(target.get("location", "")).rstrip("/").lower() == location.lower()
                and str(target.get("subpath", "")).rstrip("/") == subpath)
        if not same:
            raise LandingRuntimeError(
                f"Files/{name} already exists and points at {target.get('location')}{target.get('subpath')}, "
                f"not {location}{subpath}. Rename the shortcut or remove the old one."
            )
        return {"shortcut": f"Files/{name}", "target": location + subpath, "created": False}
    if status != 404:
        raise LandingRuntimeError(f"Could not read shortcut Files/{name}: HTTP {status}.")
    payload = {"path": "Files", "name": name,
               "target": {"adlsGen2": {"location": location, "subpath": subpath, "connectionId": connection}}}
    status, body = request("POST", base, headers, payload)
    if status not in (200, 201):
        detail = (body or {}).get("message") or (body or {}).get("errorCode") or ""
        raise LandingRuntimeError(f"Creating shortcut Files/{name} failed: HTTP {status} {detail}".rstrip())
    return {"shortcut": f"Files/{name}", "target": location + subpath, "created": True}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resolve configuration
# MAGIC
# MAGIC Points the contracts at this catalog, configures optional telemetry, and converts the string widgets to numbers, booleans and <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">None</span>.

# COMMAND ----------

# DBTITLE 1,Resolve configuration
import os
import traceback

# Contracts address tables as {catalog}.{domain}.<table>; this names the catalog.
os.environ[f"DATABRICKS_{environment.upper()}_CATALOG"] = catalog

# Where landing files come from (the Deliver data-source step): parameters, secret NAMES and,
# for a direct connection, the service principal's Spark config. Blank = the registry's own path.
_landing_source = apply_landing_runtime(landing_runtime, dbutils.secrets.get, spark.conf.set)
if str(landing_runtime).strip():
    print(f"Landing source: {_landing_source}")

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

REGISTRY_PATH = f"{contracts_root}/{domain}/{system}/_system.yaml"
print(f"Registry: {REGISTRY_PATH}  (env: {environment})")

# Convert string parameters to numbers and booleans.
DRY_RUN = str(dry_run).strip().lower() == "true"
DDL_ONLY = str(ddl_only).strip().lower() == "true"
RETRY_ATTEMPTS = int(str(retry_attempts).strip() or "1")
RETRY_WAIT = int(str(retry_wait_sec).strip() or "30")
MAX_FAILURES = int(str(max_failures).strip() or "0")
# An empty box means "not asked for", which the engine spells None -- "" is not None and
# would read as a real (empty) value.
REPROCESS_FROM = str(reprocess_from).strip() or None
REPROCESS_TO = str(reprocess_to).strip() or None
REPROCESS_COLUMN = str(reprocess_column).strip() or None
_reprocess_raw = str(reprocess_values).strip()
REPROCESS_VALUES = [v.strip() for v in _reprocess_raw.split(",") if v.strip()] or None
LOOKBACK_DAYS = int(str(lookback_days).strip()) if str(lookback_days).strip() else None
PARALLEL = str(parallel).strip().lower() == "true"
MAX_WORKERS = int(str(max_workers).strip() or "4")
ENTITY_TIMEOUT = int(str(entity_timeout_minutes).strip()) if str(entity_timeout_minutes).strip() else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load the registry
# MAGIC
# MAGIC Reads <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">{contracts_root}/{domain}/{system}/_system.yaml</span>. With <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">landing_root_override</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">synthetic_schema_prefix</span> set, bronze reads a staging copy and writes to a prefixed schema, apart from production.

# COMMAND ----------

# DBTITLE 1,Load registry
from lakelogic.core.registry import DomainRegistry
from lakelogic.pipeline import LakehousePipeline

registry = DomainRegistry.from_yaml(
    REGISTRY_PATH, environment=environment, storage_mode=storage_mode
)

# Optional retarget: read bronze from <landing_root_override>/<source.path> and write to
# `<synthetic_schema_prefix><domain>` schemas, kept apart from production tables.
_lro = str(landing_root_override).strip().strip('/')
_ssp = str(synthetic_schema_prefix).strip()
if _lro:
    for _c in registry.contracts:
        if _c.layer == 'bronze' and _c.contract_dict:
            _src = _c.contract_dict.get('source') or {}
            _p = _src.get('path')
            if _p and not str(_p).startswith(_lro + '/'):
                _src['path'] = f"{_lro}/{str(_p).lstrip('/')}"
                _c.contract_dict['source'] = _src
    print(f"Synthetic retarget: bronze landing <- {_lro}/<source.path>")
if _ssp:
    def _prefixed(root):
        # Prefix the SCHEMA, the last part of `catalog`.schema, so the catalog stays the one
        # that exists. Every root a table is written under is moved, layer roots included.
        head, dot, schema = str(root).rpartition(".")
        return f"{head}{dot}{_ssp}{schema}"

    for _key in ("domain_catalog", "quarantine_root", "bronze_root", "silver_root", "gold_root"):
        _root = getattr(registry.storage, _key, None)
        if _root:
            setattr(registry.storage, _key, _prefixed(_root))
    print(f"Synthetic retarget: output schema -> {registry.storage.domain_catalog}")

pipeline = LakehousePipeline(registry, engine=engine, spark=spark)  # noqa: F821 (Databricks global)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run the pipeline
# MAGIC
# MAGIC Processes every active contract through the requested layers. An exception prints its traceback and fails the task.

# COMMAND ----------

# DBTITLE 1,Run pipeline
try:
    summary = pipeline.run(
        target_layers=target_layers,
        reset_layers=reset_layers,
        reload_layers=reload_layers,
        dry_run=DRY_RUN,
        entity_filter=entity_filter,
        environment=environment,
        retry_attempts=RETRY_ATTEMPTS,
        retry_base_wait_seconds=RETRY_WAIT,
        max_consecutive_failures=MAX_FAILURES,
        resume_from_run=resume_run or None,
        ddl_only=DDL_ONLY,
        created_by=created_by or None,
        reprocess_from=REPROCESS_FROM,
        reprocess_to=REPROCESS_TO,
        reprocess_column=REPROCESS_COLUMN,
        reprocess_values=REPROCESS_VALUES,
        lookback_days=LOOKBACK_DAYS,
        parallel=PARALLEL,
        max_workers=MAX_WORKERS,
        entity_timeout_minutes=ENTITY_TIMEOUT,
    )
except Exception:
    print(traceback.format_exc())
    raise

# COMMAND ----------

# DBTITLE 1,Synthetic marker columns
# The synthetic seed marks every row it writes: `_synthetic` (always true) and
# `_test_case_types` (the edge cases a row exercises). The marks travel bronze -> silver -> gold
# so a synthetic row stays identifiable in every layer. No contract declares them, so the tables
# created above lack them, and Unity Catalog will not add a column on write when table ACLs are
# enabled: every silver write failed with DELTA_METADATA_MISMATCH (2026-10-14). Declared here,
# nullable, on every table this step created; real data leaves them empty.
#
# SCOPED TO THIS SYSTEM'S TABLES. The layer schemas are shared by every system of the domain and
# the setup tasks run in parallel; altering every table in the schema made two systems' tasks
# ALTER the same table at once (DELTA_METADATA_CHANGED, FIELD_ALREADY_EXISTS — 2026-10-14).
SYNTHETIC_MARKERS = {"_synthetic": "BOOLEAN", "_test_case_types": "STRING"}


def _system_table_names(contracts, quarantine_template):
    """Lower-case table names this system's contracts write (targets and their quarantine)."""
    names = set()
    for _c in contracts or []:
        _d = getattr(_c, "contract_dict", None) or {}
        _cands = []
        for _sect, _k in (("materialization", "target_path"), ("server", "path")):
            _v = (_d.get(_sect) or {}).get(_k)
            if isinstance(_v, str) and _v.startswith("table:"):
                _cands.append(_v[6:])
        _cands += [(_d.get("info") or {}).get("table_name"), _d.get("dataset")]
        for _v in _cands:
            if isinstance(_v, str) and _v.strip() and "{" not in _v:
                _t = _v.strip().rsplit(".", 1)[-1].strip("`").lower()
                names.add(_t)
                names.add(str(quarantine_template or "quarantine_{table}").replace("{table}", _t).lower())
    return names


if DDL_ONLY and not DRY_RUN:
    _mine = _system_table_names(
        registry.contracts, getattr(registry.storage, "quarantine_table_name", None))
    for _key in ("quarantine_root", "bronze_root", "silver_root", "gold_root"):
        _root = getattr(registry.storage, _key, None)
        if not _root:
            continue
        try:
            _tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {_root}").collect() if not r.isTemporary]  # noqa: F821
        except Exception as _exc:
            print(f"Synthetic markers: {_root} not listed ({_exc.__class__.__name__}); skipped")
            continue
        for _table in _tables:
            if _table.lower() not in _mine:
                continue  # another system's table: its own setup task declares the markers
            _name = f"{_root}.`{_table}`"
            _existing = {f.name for f in spark.table(_name).schema}  # noqa: F821
            _add = [f"`{c}` {t}" for c, t in SYNTHETIC_MARKERS.items() if c not in _existing]
            if _add:
                try:
                    spark.sql(f"ALTER TABLE {_name} ADD COLUMNS ({', '.join(_add)})")  # noqa: F821
                except Exception as _exc:
                    # Idempotent: a concurrent writer added it between the read and the ALTER.
                    if "FIELD_ALREADY_EXISTS" in str(_exc) or "already exists" in str(_exc).lower():
                        print(f"Synthetic markers already on {_name}")
                        continue
                    raise
                print(f"Synthetic markers added to {_name}: {', '.join(_add)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Run summary
# MAGIC
# MAGIC One line per contract: layer, status, rows and duration.

# COMMAND ----------

# DBTITLE 1,Run summary
print("\n" + "=" * 70)
print("PIPELINE RUN SUMMARY")
print("=" * 70)
print(f"  Run : {summary.run_id}   Env : {summary.environment}")
def _dur(sec):
    # Seconds the contract took (engine field `duration_seconds`); "-" when it never ran.
    if sec is None:
        return "-"
    return f"{sec:.1f}s" if sec < 60 else f"{int(sec) // 60}m {int(sec) % 60:02d}s"

for r in summary.results:
    print(f"  {r.get('contract','?'):<45} {r.get('layer','?'):<8} "
          f"{r.get('status','?'):<14} {str(r.get('rows','-')):>9} {_dur(r.get('duration_seconds')):>9}")
    if r.get('error'):
        print(f"    -> {r['error']}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lineage
# MAGIC
# MAGIC The engine's DAG for this system: which contracts feed which, across layers. A render failure is reported here and never fails the job.

# COMMAND ----------

# DBTITLE 1,Lineage
try:
    displayHTML(pipeline.visualize_dag(title=f"{domain} / {system}"))  # noqa: F821 (Databricks global)
except Exception as _dag_exc:
    print(f"Lineage: could not render the pipeline DAG ({type(_dag_exc).__name__}: {_dag_exc})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Fail on failed contracts
# MAGIC
# MAGIC Raises when any contract failed, naming each one and appending the traceback lakelogic recorded in <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_lakelogic_run_log</span>.

# COMMAND ----------

# DBTITLE 1,Fail on failed contracts
# Fail the Databricks task if any contract failed.
_failed = [r for r in summary.results if r.get('status') == 'failed']
if _failed:
    _details = ' | '.join(
        f"{r.get('contract') or r.get('table_name')} [{r.get('layer')}]: "
        f"{(str(r.get('error') or 'see logs').strip().splitlines() or ['see logs'])[0][:300]}"
        for r in _failed
    )
    # lakelogic records the frames in the run log. Read them back so the task error
    # names a line, instead of repeating a message that cannot locate anything.
    _tracebacks = []
    _log_table = f"{registry.storage.domain_catalog}._lakelogic_run_log"
    try:
        for _row in spark.sql(  # noqa: F821 (Databricks global)
            f"SELECT contract, error_traceback FROM {_log_table} "
            f"WHERE pipeline_run_id = '{summary.run_id}' AND status = 'failed'"
        ).collect():
            if _row["error_traceback"]:
                _tracebacks.append(_row["contract"] + chr(10) + _row["error_traceback"])
    except Exception as _log_exc:
        print(f"  ! no tracebacks from {_log_table}: {type(_log_exc).__name__}: {_log_exc}")
    if _tracebacks:
        print("=" * 70)
        print("FAILURE TRACEBACKS")
        print("=" * 70)
        for _tb in _tracebacks:
            print(_tb)
        _details = _details + (chr(10) * 2) + (chr(10) * 2).join(_tracebacks)
    raise RuntimeError(f"{len(_failed)} contract(s) failed — {_details}")
