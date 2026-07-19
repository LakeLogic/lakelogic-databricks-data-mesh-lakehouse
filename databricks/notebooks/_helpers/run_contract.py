# Databricks notebook source
# MAGIC %md
# MAGIC # ▶️ Run Contract
# MAGIC
# MAGIC End-to-end execution of a single LakeLogic contract:
# MAGIC 1. **Parse** the contract YAML
# MAGIC 2. **Generate** realistic test data
# MAGIC 3. **Process** through DataProcessor (apply quality rules)
# MAGIC 4. **Show** good rows, quarantined rows, and reject reasons
# MAGIC
# MAGIC Use this to smoke-test a contract before deploying it to production.

# COMMAND ----------

# MAGIC %pip install lakelogic==1.40.0 pyyaml polars deltalake --quiet

# COMMAND ----------

dbutils.widgets.text(
    "contract_path", "/Volumes/catalog/schema/contracts/crm/olist/contracts/bronze/customers.yaml", "Contract Path"
)
dbutils.widgets.text("num_rows", "100", "Test Rows")
dbutils.widgets.dropdown("engine", "polars", ["polars", "spark", "pandas"], "Engine")

# COMMAND ----------

import time
from pathlib import Path

from lakelogic import DataContract, DataGenerator, DataProcessor

contract_path = dbutils.widgets.get("contract_path")
num_rows = int(dbutils.widgets.get("num_rows"))
engine = dbutils.widgets.get("engine")
contract_name = Path(contract_path).stem

print(f"📄 Contract : {contract_name}")
print(f"🔢 Rows     : {num_rows}")
print(f"⚙️  Engine   : {engine}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Parse Contract

# COMMAND ----------

contract = DataContract.from_yaml(contract_path)

field_count = len(contract.model.fields)
print(f"✅ Contract parsed successfully")
print(f"   Fields: {field_count}")
print(f"   Version: {contract.version if hasattr(contract, 'version') else 'N/A'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Generate Test Data

# COMMAND ----------

gen = DataGenerator(contract_path, seed=42)
test_df = gen.generate(rows=num_rows)

print(f"✅ Generated {len(test_df)} rows × {len(test_df.columns)} columns")
print(f"\n📋 Sample (first 5 rows):")
display(spark.createDataFrame(test_df.to_pandas() if hasattr(test_df, "to_pandas") else test_df).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Process Through DataProcessor

# COMMAND ----------

start = time.time()

processor = DataProcessor(
    contract=contract_path,
    engine=engine,
)
result = processor.run(test_df)

elapsed = time.time() - start

good_count = result.good_count if hasattr(result, "good_count") else len(result.good) if result.good is not None else 0
bad_count = result.bad_count if hasattr(result, "bad_count") else len(result.bad) if result.bad is not None else 0
total = good_count + bad_count

print(f"✅ Processing complete in {elapsed:.2f}s")
print()
print(f"   📊 Results:")
print(f"   ├── Source rows : {num_rows}")
print(
    f"   ├── Good rows   : {good_count}  ({good_count/total*100:.1f}%)"
    if total > 0
    else f"   ├── Good rows   : {good_count}"
)
print(
    f"   ├── Quarantined : {bad_count}  ({bad_count/total*100:.1f}%)"
    if total > 0
    else f"   ├── Quarantined : {bad_count}"
)
print(f"   └── Recon check : {'✅ PASS' if total == num_rows else '⚠️ MISMATCH'} ({total}/{num_rows})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Good Rows

# COMMAND ----------

if result.good is not None and good_count > 0:
    good_df = result.good
    if hasattr(good_df, "to_pandas"):
        good_df = spark.createDataFrame(good_df.to_pandas())
    display(good_df)
else:
    print("⚠️ No good rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Quarantined Rows

# COMMAND ----------

if result.bad is not None and bad_count > 0:
    bad_df = result.bad
    if hasattr(bad_df, "to_pandas"):
        bad_df = spark.createDataFrame(bad_df.to_pandas())

    print(f"❌ {bad_count} row(s) quarantined:\n")
    display(bad_df)
else:
    print("✅ No quarantined rows — all data passed quality rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(
    f"""
{'='*60}
  CONTRACT EXECUTION SUMMARY
{'='*60}
  Contract    : {contract_name}
  Engine      : {engine}
  Source rows : {num_rows}
  Good rows   : {good_count}
  Quarantined : {bad_count}
  Pass rate   : {good_count/total*100:.1f}%
  Duration    : {elapsed:.2f}s
{'='*60}
"""
    if total > 0
    else f"No rows processed for {contract_name}"
)
