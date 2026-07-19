# Databricks notebook source
# MAGIC %md
# MAGIC # 📐 Compare Schemas
# MAGIC
# MAGIC Detects schema drift by comparing a LakeLogic contract's expected fields
# MAGIC against the actual Delta table in Unity Catalog.
# MAGIC
# MAGIC Highlights:
# MAGIC - **Missing columns** — in contract but not in table
# MAGIC - **Extra columns** — in table but not in contract
# MAGIC - **Type mismatches** — contract says STRING but table has INT
# MAGIC
# MAGIC Run after deployments to catch drift before it causes pipeline failures.

# COMMAND ----------

# MAGIC %pip install lakelogic==1.40.0 pyyaml --quiet

# COMMAND ----------

dbutils.widgets.text(
    "contract_path", "/Volumes/catalog/schema/contracts/crm/olist/contracts/bronze/customers.yaml", "Contract Path"
)
dbutils.widgets.text("table_name", "dev.bronze.customers", "Table Name (catalog.schema.table)")

# COMMAND ----------

from pathlib import Path

from lakelogic import DataContract

contract_path = dbutils.widgets.get("contract_path")
table_name = dbutils.widgets.get("table_name")

print(f"📄 Contract : {Path(contract_path).name}")
print(f"🗄️  Table    : {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Fields

# COMMAND ----------

# ── Parse contract schema ────────────────────────────────────────────────────
contract = DataContract.from_yaml(contract_path)

contract_fields = {}
for field in contract.model.fields:
    contract_fields[field.name.lower()] = {
        "name": field.name,
        "type": str(field.type).upper(),
        "required": getattr(field, "not_null", False) or getattr(field, "required", False),
    }

print(f"📋 Contract defines {len(contract_fields)} field(s):\n")
for name, info in contract_fields.items():
    req = " (required)" if info["required"] else ""
    print(f"   {info['name']:<30} {info['type']:<15}{req}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Schema

# COMMAND ----------

# ── Read actual table schema ─────────────────────────────────────────────────
try:
    table_df = spark.table(table_name)
    table_schema = {f.name.lower(): {"name": f.name, "type": str(f.dataType)} for f in table_df.schema.fields}

    print(f"🗄️  Table has {len(table_schema)} column(s):\n")
    for name, info in table_schema.items():
        print(f"   {info['name']:<30} {info['type']}")
except Exception as e:
    print(f"❌ Could not read table: {e}")
    print(f"   Make sure '{table_name}' exists and you have access.")
    dbutils.notebook.exit(f"Table not found: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Comparison

# COMMAND ----------

# ── Compare ──────────────────────────────────────────────────────────────────
contract_names = set(contract_fields.keys())
table_names = set(table_schema.keys())

missing_in_table = contract_names - table_names
extra_in_table = table_names - contract_names
common = contract_names & table_names

# Type normalization for comparison
TYPE_MAP = {
    "StringType()": "STRING",
    "LongType()": "BIGINT",
    "IntegerType()": "INTEGER",
    "DoubleType()": "DOUBLE",
    "FloatType()": "FLOAT",
    "BooleanType()": "BOOLEAN",
    "TimestampType()": "TIMESTAMP",
    "DateType()": "DATE",
}

type_mismatches = []
for col_name in sorted(common):
    contract_type = contract_fields[col_name]["type"]
    table_type_raw = table_schema[col_name]["type"]
    table_type = TYPE_MAP.get(table_type_raw, table_type_raw.upper())

    # Flexible comparison (ignore precision differences)
    contract_base = contract_type.split("(")[0]
    table_base = table_type.split("(")[0]

    if contract_base != table_base:
        type_mismatches.append(
            {
                "column": col_name,
                "contract_type": contract_type,
                "table_type": table_type,
            }
        )

# ── Results ──────────────────────────────────────────────────────────────────
drift_found = False

print(f"{'='*60}")
print(f"  SCHEMA COMPARISON: {Path(contract_path).stem} vs {table_name}")
print(f"{'='*60}\n")

if missing_in_table:
    drift_found = True
    print(f"⚠️  Missing in table ({len(missing_in_table)}):")
    for col_name in sorted(missing_in_table):
        info = contract_fields[col_name]
        print(f"   ➕ {info['name']} ({info['type']})")
    print()

if extra_in_table:
    print(f"ℹ️  Extra in table ({len(extra_in_table)}) — not in contract:")
    for col_name in sorted(extra_in_table):
        info = table_schema[col_name]
        print(f"   ➕ {info['name']} ({info['type']})")
    print()

if type_mismatches:
    drift_found = True
    print(f"❌ Type mismatches ({len(type_mismatches)}):")
    for m in type_mismatches:
        print(f"   🔀 {m['column']}: contract={m['contract_type']}, table={m['table_type']}")
    print()

matched = len(common) - len(type_mismatches)
print(f"✅ Matched columns: {matched}/{len(contract_fields)}")
print(f"   Contract fields: {len(contract_fields)}")
print(f"   Table columns  : {len(table_schema)}")

if not drift_found:
    print(f"\n✅ No schema drift detected — contract and table are aligned")
else:
    print(f"\n⚠️  Schema drift detected — review mismatches above")
