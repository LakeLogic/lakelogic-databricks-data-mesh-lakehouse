# Databricks notebook source
# MAGIC %md
# MAGIC # 🔎 Inspect Quarantine
# MAGIC
# MAGIC Explores quarantined (rejected) rows across your Medallion lakehouse.
# MAGIC Helps data engineers understand:
# MAGIC - **Which** contracts are rejecting data
# MAGIC - **Why** rows are being quarantined (reject reasons)
# MAGIC - **Trends** in rejection rates over time
# MAGIC
# MAGIC Query the quarantine tables directly — no code changes needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Catalog")
dbutils.widgets.text("quarantine_schema", "quarantine", "Quarantine Schema")
dbutils.widgets.text("entity_filter", "", "Entity Filter (blank = all)")
dbutils.widgets.text("days_back", "7", "Days Back")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
quarantine_schema = dbutils.widgets.get("quarantine_schema")
entity_filter = dbutils.widgets.get("entity_filter")
days_back = int(dbutils.widgets.get("days_back"))

print(f"🔎 Quarantine Inspector")
print(f"   Catalog   : {catalog}")
print(f"   Schema    : {quarantine_schema}")
print(f"   Entity    : {entity_filter or 'all'}")
print(f"   Days back : {days_back}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Available Quarantine Tables

# COMMAND ----------

# ── List all quarantine tables ───────────────────────────────────────────────
quarantine_tables = spark.sql(
    f"""
    SHOW TABLES IN {catalog}.{quarantine_schema}
"""
)
display(quarantine_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rejection Summary

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
# ── Aggregate rejection counts across all quarantine tables ──────────────────
from pyspark.sql.functions import col, count, current_timestamp, datediff, lit

tables = [row.tableName for row in quarantine_tables.collect()]
if entity_filter:
    tables = [t for t in tables if entity_filter.lower() in t.lower()]

summaries = []
for table_name in tables:
    try:
        tbl = f"{catalog}.{quarantine_schema}.{table_name}"
        df = spark.table(tbl)

        # Try common timestamp columns
        ts_col = None
        for candidate in ["_ingested_at", "_quarantined_at", "_rejected_at", "_processed_at"]:
            if candidate in df.columns:
                ts_col = candidate
                break

        if ts_col:
            recent = df.filter(datediff(current_timestamp(), col(ts_col)) <= days_back)
        else:
            recent = df

        row_count = recent.count()
        if row_count > 0:
            summaries.append(spark.createDataFrame([(table_name, row_count)], ["entity", "quarantined_rows"]))
    except Exception as e:
        print(f"⚠️ Could not query {table_name}: {e}")

if summaries:
    summary_df = reduce(DataFrame.union, summaries).orderBy(col("quarantined_rows").desc())
    print(f"📊 Quarantine summary (last {days_back} days):\n")
    display(summary_df)
else:
    print(f"✅ No quarantined rows in the last {days_back} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reject Reasons Breakdown

# COMMAND ----------

# ── Show reject reasons if available ─────────────────────────────────────────
reason_summaries = []

for table_name in tables:
    try:
        tbl = f"{catalog}.{quarantine_schema}.{table_name}"
        df = spark.table(tbl)

        # Look for reject reason columns
        reason_col = None
        for candidate in ["_reject_reason", "_error_reason", "_quarantine_reason", "_rejection_reason"]:
            if candidate in df.columns:
                reason_col = candidate
                break

        if reason_col:
            reasons = (
                df.groupBy(lit(table_name).alias("entity"), col(reason_col).alias("reject_reason"))
                .agg(count("*").alias("occurrences"))
                .orderBy(col("occurrences").desc())
            )
            reason_summaries.append(reasons)
    except Exception:
        pass

if reason_summaries:
    all_reasons = reduce(DataFrame.union, reason_summaries)
    print("❌ Top reject reasons:\n")
    display(all_reasons.orderBy(col("occurrences").desc()).limit(50))
else:
    print("ℹ️ No reject reason columns found — enable quarantine.include_error_reason in contracts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Quarantined Rows

# COMMAND ----------

# ── Show sample quarantined rows for a specific entity ───────────────────────
if entity_filter:
    matching = [t for t in tables if entity_filter.lower() in t.lower()]
    if matching:
        target_table = f"{catalog}.{quarantine_schema}.{matching[0]}"
        print(f"📋 Sample quarantined rows from: {target_table}\n")
        display(spark.table(target_table).limit(20))
    else:
        print(f"⚠️ No quarantine table found matching '{entity_filter}'")
else:
    print("ℹ️ Set 'Entity Filter' widget to see detailed rows for a specific entity")
