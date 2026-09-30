# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : assert_mesh_complete — fail the run when the mesh silently did nothing
# Purpose   : A Databricks task that is SKIPPED does not fail its run. So when a
#             condition gate never reaches its "true" outcome, every downstream
#             stage is skipped and the orchestrator still reports SUCCESS.
#
#             That is exactly what happened: `check_run_silver` depended on a
#             `bronze_ingestion` task that the mesh disables, the dependency was
#             never satisfied, silver AND gold were skipped for every system — and
#             the whole mesh went green having produced no gold at all.
#
#             A green run must mean the work happened, not merely that nothing
#             errored. This asserts the estate actually contains what the run
#             claimed to build.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

dbutils.widgets.text("catalog", "rideflow_dev_demo", "Config - Unity Catalog name")
# Schemas that must hold at least one gold table once the mesh has run. Comma
# separated so a target with a different domain set can narrow it.
dbutils.widgets.text("gold_domains", "marketing,marketplace,operations,payments",
                     "Config - domains that must produce gold")
dbutils.widgets.text("min_gold_per_domain", "1", "Config - minimum gold tables per domain")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog").strip()
DOMAINS = [d.strip() for d in dbutils.widgets.get("gold_domains").split(",") if d.strip()]
MIN_GOLD = int(dbutils.widgets.get("min_gold_per_domain").strip() or "1")

print(f"🔎 Asserting mesh output in `{CATALOG}`")
print(f"   domains: {', '.join(DOMAINS)}  (≥ {MIN_GOLD} gold table each)")

# COMMAND ----------


def gold_tables(catalog: str, schema: str) -> list:
    """Gold tables in one schema. A missing schema counts as zero, not an error —
    the point is to report what is absent, not to stop at the first gap."""
    try:
        rows = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ {schema}: cannot list ({e})")
        return []
    return [r.tableName for r in rows if r.tableName.startswith("gold_")
            or "_gold_" in r.tableName]


shortfalls = []
for domain in DOMAINS:
    found = gold_tables(CATALOG, domain)
    ok = len(found) >= MIN_GOLD
    print(f"  {'✓' if ok else '✗'} {domain:<14} {len(found)} gold table(s)"
          f"{'' if ok else f' — expected at least {MIN_GOLD}'}")
    if not ok:
        shortfalls.append(f"{domain} ({len(found)}/{MIN_GOLD})")

if shortfalls:
    raise AssertionError(
        "Mesh run produced no gold for: " + ", ".join(shortfalls) + ".\n"
        "The tasks were almost certainly SKIPPED rather than failed — check the "
        "`check_run_silver` / `check_run_gold` condition gates in the per-system "
        "orchestrators. A skipped task does not fail a run, which is why the "
        "orchestrator can report SUCCESS while building nothing."
    )

print("\n✅ Every domain produced gold — the run did the work it reported.")
