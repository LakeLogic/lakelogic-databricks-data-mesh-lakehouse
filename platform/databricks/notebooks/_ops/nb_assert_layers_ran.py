# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : Assert Layers Ran — the orchestrator's closing gate
#
# WHY THIS EXISTS
# ---------------
# A Databricks job whose tasks were EXCLUDED still terminates SUCCESS. Observed on
# this very mesh:
#
#     TERMINATED SUCCESS | gold_aggregation=EXCLUDED silver_processing=EXCLUDED
#                          bronze_ingestion=SUCCESS
#
# Two of the three layers never ran, and the run reported success. The orchestrator
# YAML already carried a comment admitting it ("silver and gold are skipped too —
# and the run still reports SUCCESS") without anything enforcing otherwise.
#
# Exclusion happens when a task's dependency chain is unsatisfied — a gate resolved
# `false` further up, or an upstream task was itself excluded. That is exactly the
# case an operator needs to hear about, because it is indistinguishable from a
# healthy run on every dashboard.
#
# WHAT IT CHECKS
# --------------
# For each layer the caller ENABLED, at least one run-log row must exist for this
# pipeline run. It asserts against the run log rather than task states because the
# run log is what the platform and the dashboards read: if a layer left no trace
# there, it did not happen as far as anything downstream is concerned.
#
# A layer that was deliberately disabled (`enable_<layer>=false`) is not checked —
# skipping is legitimate; silently skipping is not.
# ═══════════════════════════════════════════════════════════════════════════════

dbutils.widgets.text("catalog", "governed_rideflow_lakehouse_demo", "Catalog")
dbutils.widgets.text("domain", "", "Domain (run-log schema)")
dbutils.widgets.text("enable_bronze", "true", "Bronze was enabled")
dbutils.widgets.text("enable_silver", "true", "Silver was enabled")
dbutils.widgets.text("enable_gold", "true", "Gold was enabled")
dbutils.widgets.text("window_minutes", "180", "How far back to look")

dbutils.widgets.text("environment", "dev", "Config - Environment")
CATALOG = dbutils.widgets.get("catalog").strip()
DOMAIN = dbutils.widgets.get("domain").strip()
WINDOW = int(dbutils.widgets.get("window_minutes").strip() or "180")

ENABLED = {
    layer: dbutils.widgets.get(f"enable_{layer}").strip().lower() == "true"
    for layer in ("bronze", "silver", "gold")
}


# ── Where a domain's tables live ──────────────────────────────────────────────
# A domain is either a SCHEMA of the shared catalog (`<catalog>`.`<domain>`, layer in the
# table name) or its OWN CATALOG with a schema per layer (`rideflow_dev_marketplace`.bronze /
# .silver / .gold / .quarantine / .lakelogic). The domain's system registry decides
# (storage.bronze_root / silver_root / gold_root, and the environment's
# domain_catalog_prefix); this reads it, so a notebook works for both while domains move
# across one at a time. Same function in every notebook that names a table itself — keep the
# copies identical.
def own_catalog(catalog: str, domain: str):
    """The domain's own catalog name, or None when it is a schema of the shared catalog.
    Read from the domain's system registry as staged in the contracts Volume: a registry that
    declares a schema per layer names the catalog `<domain_catalog_prefix>_<domain>`."""
    import glob

    import yaml

    try:
        env = dbutils.widgets.get("environment").strip() or "dev"
    except Exception:  # noqa: BLE001 - a notebook without the widget
        env = "dev"
    for reg in sorted(glob.glob(f"/Volumes/{catalog}/nondelta/_contracts/{domain}/*/_system.yaml")):
        try:
            with open(reg, encoding="utf-8") as fh:
                doc = yaml.safe_load(fh) or {}
        except Exception:  # noqa: BLE001
            continue
        storage = doc.get("storage") or {}
        prefix = ((doc.get("environments") or {}).get(env) or {}).get("domain_catalog_prefix")
        if prefix and any(storage.get(f"{layer}_root") for layer in ("bronze", "silver", "gold")):
            return f"{prefix}_{domain}"
    return None


def domain_layout(catalog: str, domain: str) -> dict:
    own = own_catalog(catalog, domain)
    if own:
        return {
            "per_domain_catalog": True, "catalog": own,
            "bronze": f"`{own}`.`bronze`", "silver": f"`{own}`.`silver`", "gold": f"`{own}`.`gold`",
            "quarantine": f"`{own}`.`quarantine`", "lakelogic": f"`{own}`.`lakelogic`",
            "landing": f"/Volumes/{own}/source/landing",
        }
    one = f"`{catalog}`.`{domain}`"
    return {
        "per_domain_catalog": False, "catalog": catalog,
        "bronze": one, "silver": one, "gold": one, "quarantine": one, "lakelogic": one,
        "landing": f"/Volumes/{catalog}/nondelta/landing_{domain}",
    }


def table_of(catalog: str, domain: str, table: str) -> str:
    """`bronze_x` / `silver_x` / `gold_x` / `quarantine_x` / `_lakelogic_x` -> its full name."""
    lay = domain_layout(catalog, domain)
    key = ("quarantine" if table.startswith("quarantine_") else "lakelogic" if table.startswith("_lakelogic")
           else table.split("_", 1)[0] if table.split("_", 1)[0] in ("bronze", "silver", "gold") else "lakelogic")
    return f"{lay[key]}.`{table}`"


RUN_LOG = table_of(CATALOG, DOMAIN, "_lakelogic_run_log")

print(f"Asserting layers ran | catalog={CATALOG} domain={DOMAIN} window={WINDOW}m")
print(f"Enabled: {', '.join(k for k, v in ENABLED.items() if v) or '(none)'}")

# COMMAND ----------

from pyspark.sql import functions as F

try:
    runs = (
        spark.table(RUN_LOG)
        .filter(F.col("timestamp") >= F.expr(f"current_timestamp() - interval {WINDOW} minutes"))
    )
except Exception as exc:
    # No run-log table at all means nothing ran anywhere. Fail loudly rather than
    # treating an absent table as "nothing to check".
    raise RuntimeError(
        f"Cannot read {RUN_LOG}: {exc}. If no layer has ever run for this domain, "
        f"that is itself the failure this gate exists to report."
    ) from exc

observed = {
    row["data_layer"]: row["n"]
    for row in runs.groupBy("data_layer").agg(F.count("*").alias("n")).collect()
}
print("Run-log rows in window by layer:", observed or "(none)")

missing = [layer for layer, enabled in ENABLED.items() if enabled and not observed.get(layer)]
skipped = [layer for layer, enabled in ENABLED.items() if not enabled]

if skipped:
    print(f"Not checked (deliberately disabled): {', '.join(skipped)}")

if missing:
    raise RuntimeError(
        "Orchestrator finished but these ENABLED layers produced no run-log rows: "
        f"{', '.join(missing)}. They were almost certainly EXCLUDED — a Databricks "
        "task whose dependency chain is unsatisfied is skipped, and the job still "
        "terminates SUCCESS. Open the run and check which gate resolved false."
    )

print("✅ Every enabled layer produced run-log rows.")
