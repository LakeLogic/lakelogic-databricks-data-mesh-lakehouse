# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 Table Maintenance
# MAGIC
# MAGIC Runs OPTIMIZE and VACUUM across all Delta tables in the Medallion lakehouse.
# MAGIC Keeps storage costs down and query performance up.
# MAGIC
# MAGIC **Operations:**
# MAGIC - `OPTIMIZE` — compacts small files, applies Z-ORDER
# MAGIC - `VACUUM` — removes files older than retention period
# MAGIC
# MAGIC Schedule this as a weekly job via Databricks Workflows.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Catalog")
dbutils.widgets.multiselect("layers", "bronze", ["bronze", "silver", "gold", "quarantine"], "Layers")
dbutils.widgets.text("vacuum_hours", "168", "Vacuum Retention (hours)")
dbutils.widgets.dropdown("dry_run", "yes", ["yes", "no"], "Dry Run")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
layers = dbutils.widgets.get("layers").split(",")
vacuum_hours = int(dbutils.widgets.get("vacuum_hours"))
dry_run = dbutils.widgets.get("dry_run") == "yes"

print(f"🧹 Table Maintenance")
print(f"   Catalog   : {catalog}")
print(f"   Layers    : {', '.join(layers)}")
print(f"   Vacuum    : {vacuum_hours}h retention")
print(f"   Dry run   : {dry_run}")

if dry_run:
    print(f"\n⚠️  DRY RUN — no changes will be made. Set 'Dry Run' to 'no' to execute.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Tables

# COMMAND ----------

import time

from pyspark.sql.functions import col

all_tables = []
for layer in layers:
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{layer}").collect()
        for t in tables:
            all_tables.append({"layer": layer, "table": t.tableName, "full_name": f"{catalog}.{layer}.{t.tableName}"})
    except Exception as e:
        print(f"⚠️ Could not list tables in {catalog}.{layer}: {e}")

print(f"📦 Found {len(all_tables)} table(s) across {len(layers)} layer(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Maintenance

# COMMAND ----------

results = []

for entry in all_tables:
    full_name = entry["full_name"]
    layer = entry["layer"]
    table = entry["table"]

    print(f"\n── {full_name} ──")

    try:
        # ── OPTIMIZE ─────────────────────────────────────────────────────
        if not dry_run:
            start = time.time()
            opt_result = spark.sql(f"OPTIMIZE {full_name}")
            elapsed = time.time() - start
            metrics = opt_result.collect()[0] if opt_result.count() > 0 else None
            print(f"  ✅ OPTIMIZE ({elapsed:.1f}s)")
        else:
            print(f"  ⏭️  OPTIMIZE (dry run)")

        # ── VACUUM ───────────────────────────────────────────────────────
        if not dry_run:
            start = time.time()
            spark.sql(f"VACUUM {full_name} RETAIN {vacuum_hours} HOURS")
            elapsed = time.time() - start
            print(f"  ✅ VACUUM ({elapsed:.1f}s, retain {vacuum_hours}h)")
        else:
            print(f"  ⏭️  VACUUM (dry run, would retain {vacuum_hours}h)")

        results.append({"table": full_name, "layer": layer, "status": "✅ Done" if not dry_run else "⏭️ Skipped"})

    except Exception as e:
        print(f"  ❌ Error: {e}")
        results.append({"table": full_name, "layer": layer, "status": f"❌ {e}"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import pandas as pd

summary_df = pd.DataFrame(results)

succeeded = sum(1 for r in results if "✅" in r["status"])
failed = sum(1 for r in results if "❌" in r["status"])

print(f"\n{'='*60}")
print(f"  MAINTENANCE SUMMARY")
print(f"{'='*60}")
print(f"  Tables processed : {len(results)}")
print(f"  Succeeded        : {succeeded}")
print(f"  Failed           : {failed}")
print(f"  Vacuum retention : {vacuum_hours}h")
print(f"{'='*60}")

if results:
    display(spark.createDataFrame(summary_df))
