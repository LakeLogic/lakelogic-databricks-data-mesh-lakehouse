# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : nb_assert_mesh_complete — fail the run when the mesh silently did nothing
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

dbutils.widgets.text("catalog", "governed_rideflow_lakehouse_demo", "Config - Unity Catalog name")
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



# ── Where a domain's tables live ──────────────────────────────────────────────
# A domain is either a SCHEMA of the shared catalog (`<catalog>`.`<domain>`, layer in the
# table name) or its OWN CATALOG with a schema per layer (`<catalog>_<domain>`.bronze /
# .silver / .gold / .quarantine / .lakelogic). The registry's `storage` block decides
# (bronze_root / silver_root / gold_root); this reads the result so a notebook works for
# both while domains move across one at a time. Same function in every notebook that
# names a table itself — keep the copies identical.
def domain_layout(catalog: str, domain: str) -> dict:
    own = f"{catalog}_{domain}"
    try:
        has_own = spark.sql(f"SHOW CATALOGS LIKE '{own}'").count() > 0
    except Exception:  # noqa: BLE001
        has_own = False
    if has_own:
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


def gold_tables(catalog: str, schema: str) -> list:
    """Gold tables of one domain. A missing schema counts as zero, not an error —
    the point is to report what is absent, not to stop at the first gap."""
    try:
        rows = spark.sql(f"SHOW TABLES IN {domain_layout(catalog, schema)['gold']}").collect()
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
