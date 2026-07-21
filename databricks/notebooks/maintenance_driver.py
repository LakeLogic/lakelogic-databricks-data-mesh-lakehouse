# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Maintenance Driver — OPTIMIZE + VACUUM across all medallion layers
# ══════════════════════════════════════════════════════════════════════════════
# Walks the contract registry for one (domain, system), discovers every
# Bronze/Silver/Gold Delta table it manages, then runs:
#
#   1. OPTIMIZE <table> [ZORDER BY (...)]   — coalesces small files,
#                                              optionally clusters by hot cols
#   2. VACUUM <table> RETAIN <hours>        — purges tombstoned files
#
# Designed to be scheduled weekly per domain. Doesn't touch contract logic,
# doesn't move data, doesn't change schema — just reclaims storage and
# improves read performance.
#
# Parameters
# ──────────
#   registry_path     — path to _system.yaml (same as other drivers)
#   environment       — dev | stage | prod
#   target_layers     — comma-separated subset of bronze,silver,gold (default: all)
#   vacuum_retain_hrs — VACUUM retention (default: 168 = 7d, Delta minimum is 168)
#   dry_run           — true → print SQL without executing
#   zorder_columns    — JSON map per-table of ZORDER cols, e.g.
#                       '{"silver_stripe_charges":["created_at","status"]}'
#

# MAGIC %pip install lakelogic pyyaml polars deltalake

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
from pathlib import Path

dbutils.widgets.text("registry_path", "")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("target_layers", "bronze,silver,gold")
dbutils.widgets.text("vacuum_retain_hrs", "168")
dbutils.widgets.text("dry_run", "false")
dbutils.widgets.text("zorder_columns", "{}")

registry_path = dbutils.widgets.get("registry_path")
environment = dbutils.widgets.get("environment")
target_layers = [s.strip() for s in dbutils.widgets.get("target_layers").split(",") if s.strip()]
vacuum_retain_hrs = int(dbutils.widgets.get("vacuum_retain_hrs"))
dry_run = dbutils.widgets.get("dry_run").lower() == "true"
zorder_map = json.loads(dbutils.widgets.get("zorder_columns") or "{}")

assert registry_path, "registry_path is required"
print(f"📋 Registry: {registry_path}")
print(f"📋 Env: {environment} · Layers: {target_layers} · Retain: {vacuum_retain_hrs}h · Dry-run: {dry_run}")

# COMMAND ----------

# Discover tables via the LakeLogic DomainRegistry so we always reflect the
# current contracts. No hardcoded table lists — anything added to the registry
# gets picked up next maintenance run for free.
from lakelogic.core.registry import DomainRegistry

registry = DomainRegistry.from_yaml(registry_path, environment=environment, storage_mode="uc")

discovered = []  # list of (layer, fully_qualified_table)
for contract in registry.contracts:
    layer = (getattr(contract, "info", None) and getattr(contract.info, "target_layer", None)) or "?"
    if layer not in target_layers:
        continue
    # Materialization path is the catalog-qualified table name for UC mode
    mat = getattr(contract, "materialization", None)
    target = getattr(mat, "target", None) if mat else None
    if not target:
        info_tbl = getattr(contract.info, "table_name", None) if getattr(contract, "info", None) else None
        target = info_tbl
    if not target:
        print(f"  ⚠ skip {contract.info.title if contract.info else '?'} — no target table")
        continue
    discovered.append((layer, target))

print(f"📋 Discovered {len(discovered)} tables to maintain")
for layer, tbl in discovered:
    print(f"   [{layer:6s}] {tbl}")

# COMMAND ----------

# Run OPTIMIZE then VACUUM per table. Failures on one table don't abort the run
# — we want maintenance to make as much progress as possible per window.
results = {"optimized": [], "vacuumed": [], "failed": []}

for layer, table in discovered:
    print(f"\n── {table} ─────────────────────────────────────────────")

    # OPTIMIZE (+ ZORDER if configured)
    zorder = zorder_map.get(table.split(".")[-1]) or zorder_map.get(table)
    if zorder:
        zorder_cols = ", ".join(zorder)
        sql_optimize = f"OPTIMIZE {table} ZORDER BY ({zorder_cols})"
    else:
        sql_optimize = f"OPTIMIZE {table}"
    print(f"  → {sql_optimize}")
    if not dry_run:
        try:
            spark.sql(sql_optimize)
            results["optimized"].append(table)
        except Exception as e:
            print(f"  ✗ OPTIMIZE failed: {e}")
            results["failed"].append((table, "optimize", str(e)))
            continue

    # VACUUM (Delta min retention = 168h / 7 days unless config relaxes it)
    sql_vacuum = f"VACUUM {table} RETAIN {vacuum_retain_hrs} HOURS"
    print(f"  → {sql_vacuum}")
    if not dry_run:
        try:
            spark.sql(sql_vacuum)
            results["vacuumed"].append(table)
        except Exception as e:
            print(f"  ✗ VACUUM failed: {e}")
            results["failed"].append((table, "vacuum", str(e)))

# COMMAND ----------

print("\n══════════════════════════════════════════════════════════════")
print(f"  Maintenance complete")
print(f"  OPTIMIZE:  ok={len(results['optimized'])}")
print(f"  VACUUM:    ok={len(results['vacuumed'])}")
print(f"  Failed:    {len(results['failed'])}")
for tbl, op, err in results["failed"]:
    print(f"    {op:8s} {tbl}: {err[:100]}")
print("══════════════════════════════════════════════════════════════")

if results["failed"] and not dry_run:
    # Fail the task so the on_failure email fires, but keep the partial wins.
    raise RuntimeError(f"{len(results['failed'])} maintenance operations failed; see logs above")
