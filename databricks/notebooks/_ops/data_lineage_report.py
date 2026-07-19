# Databricks notebook source
# MAGIC %md
# MAGIC # 🔗 Data Lineage Report
# MAGIC
# MAGIC Traces a record's journey through the Medallion architecture:
# MAGIC **Bronze → Silver → Gold**
# MAGIC
# MAGIC Shows how a specific entity flows through layers, including:
# MAGIC - Row counts per layer
# MAGIC - Schema evolution across layers
# MAGIC - Quality rule pass/fail rates
# MAGIC - Timestamp progression (ingestion → cleaning → aggregation)

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Catalog")
dbutils.widgets.text("entity", "customers", "Entity Name")
dbutils.widgets.text("domain", "crm", "Domain (schema prefix)")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
entity = dbutils.widgets.get("entity")
domain = dbutils.widgets.get("domain")

print(f"🔗 Data Lineage Report")
print(f"   Catalog : {catalog}")
print(f"   Entity  : {entity}")
print(f"   Domain  : {domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer Discovery

# COMMAND ----------

# ── Find matching tables across layers ───────────────────────────────────────
layers = ["bronze", "silver", "gold"]
found_tables = {}

for layer in layers:
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{layer}").collect()
        matching = [t.tableName for t in tables if entity.lower() in t.tableName.lower()]
        if matching:
            found_tables[layer] = matching
            print(f"  ✅ {layer}: {', '.join(matching)}")
        else:
            print(f"  ⏭️  {layer}: no matching tables")
    except Exception as e:
        print(f"  ⚠️  {layer}: {e}")

# ── Check quarantine ─────────────────────────────────────────────────────────
try:
    q_tables = spark.sql(f"SHOW TABLES IN {catalog}.quarantine").collect()
    q_matching = [t.tableName for t in q_tables if entity.lower() in t.tableName.lower()]
    if q_matching:
        found_tables["quarantine"] = q_matching
        print(f"  ❌ quarantine: {', '.join(q_matching)}")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Counts & Schema

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import count
from pyspark.sql.functions import max as spark_max

lineage_data = []

for layer, tables in found_tables.items():
    for table_name in tables:
        full_name = f"{catalog}.{layer}.{table_name}"
        try:
            df = spark.table(full_name)
            row_count = df.count()
            col_count = len(df.columns)

            # Find latest timestamp
            ts_cols = [c for c in df.columns if c.startswith("_") and "at" in c.lower()]
            latest_ts = None
            if ts_cols:
                latest_row = df.select(spark_max(ts_cols[0]).alias("latest")).collect()
                latest_ts = str(latest_row[0]["latest"]) if latest_row else None

            lineage_data.append(
                {
                    "Layer": layer.upper(),
                    "Table": table_name,
                    "Rows": row_count,
                    "Columns": col_count,
                    "Latest Record": latest_ts or "—",
                }
            )

            print(f"\n── {full_name} ──")
            print(f"   Rows   : {row_count:,}")
            print(f"   Columns: {col_count}")
            if latest_ts:
                print(f"   Latest : {latest_ts}")

        except Exception as e:
            print(f"  ❌ {full_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage Overview

# COMMAND ----------

if lineage_data:
    lineage_df = pd.DataFrame(lineage_data)
    display(spark.createDataFrame(lineage_df))
else:
    print("⚠️ No tables found matching the entity name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Comparison Across Layers

# COMMAND ----------

# ── Show how the schema evolves from bronze → silver → gold ──────────────────
for layer in ["bronze", "silver", "gold"]:
    if layer not in found_tables:
        continue

    for table_name in found_tables[layer]:
        full_name = f"{catalog}.{layer}.{table_name}"
        try:
            schema = spark.table(full_name).schema
            print(f"\n── {layer.upper()}: {table_name} ({len(schema.fields)} cols) ──")
            for field in schema.fields:
                print(f"   {field.name:<30} {str(field.dataType):<20} {'NOT NULL' if not field.nullable else ''}")
        except Exception as e:
            print(f"  ❌ {full_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flow Diagram

# COMMAND ----------

# ── ASCII flow diagram ───────────────────────────────────────────────────────
layer_info = {}
for entry in lineage_data:
    layer_info[entry["Layer"].lower()] = {"rows": entry["Rows"], "cols": entry["Columns"]}

b = layer_info.get("bronze", {})
s = layer_info.get("silver", {})
g = layer_info.get("gold", {})
q = layer_info.get("quarantine", {})

print(
    f"""
    ┌──────────────────────┐
    │   📥 BRONZE          │
    │   {b.get('rows', '—'):>10} rows     │
    │   {b.get('cols', '—'):>10} cols     │
    └──────────┬───────────┘
               │
    ┌──────────▼───────────┐
    │   🧹 SILVER          │
    │   {s.get('rows', '—'):>10} rows     │
    │   {s.get('cols', '—'):>10} cols     │
    └──────────┬───────────┘
               │
    ┌──────────▼───────────┐
    │   ⭐ GOLD            │
    │   {g.get('rows', '—'):>10} rows     │
    │   {g.get('cols', '—'):>10} cols     │
    └──────────────────────┘
"""
)

if q:
    print(f"    ❌ Quarantine: {q.get('rows', '—')} rows")
