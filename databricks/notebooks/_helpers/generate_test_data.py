# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Generate Test Data
# MAGIC
# MAGIC Generates realistic fake data from any LakeLogic contract using `DataGenerator`.
# MAGIC Useful for:
# MAGIC - Previewing what a contract's data looks like before building the pipeline
# MAGIC - Testing transformations with realistic shapes
# MAGIC - Smoke-testing quality rules
# MAGIC
# MAGIC Select a contract from the Volume, choose row count, and preview the output.

# COMMAND ----------

# MAGIC %pip install lakelogic==1.40.0 pyyaml polars deltalake --quiet

# COMMAND ----------

dbutils.widgets.text(
    "contract_path", "/Volumes/catalog/schema/contracts/crm/olist/contracts/bronze/customers.yaml", "Contract Path"
)
dbutils.widgets.text("num_rows", "100", "Number of Rows")
dbutils.widgets.dropdown("output_format", "display", ["display", "temp_view", "both"], "Output")

dbutils.widgets.dropdown("ai", "false", ["false", "true"], "AI Edges")
dbutils.widgets.dropdown("ai_provider", "openai", ["openai", "azure", "anthropic", "gemini", "ollama"], "AI Provider")
dbutils.widgets.text("ai_model", "gpt-4o-mini", "AI Model")
dbutils.widgets.text("ai_api_key", "", "AI Key")

# COMMAND ----------

from pathlib import Path

from lakelogic import DataContract, DataGenerator

contract_path = dbutils.widgets.get("contract_path")
num_rows = int(dbutils.widgets.get("num_rows"))
output_format = dbutils.widgets.get("output_format")

ai = dbutils.widgets.get("ai") == "true"
ai_provider = dbutils.widgets.get("ai_provider")
ai_model = dbutils.widgets.get("ai_model")
ai_api_key = dbutils.widgets.get("ai_api_key")

if ai_api_key:
    import os
    os.environ[f"{ai_provider.upper()}_API_KEY"] = ai_api_key

print(f"📄 Contract : {contract_path}")
print(f"🔢 Rows     : {num_rows}")
print(f"📊 Output   : {output_format}")
print(f"🤖 AI Edges : {'Enabled' if ai else 'Disabled'} ({ai_provider})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Summary

# COMMAND ----------

# ── Parse the contract ───────────────────────────────────────────────────────
contract = DataContract.from_yaml(contract_path)

print(f"📋 Name    : {contract.info.title if hasattr(contract, 'info') else contract_path}")
print(f"📦 Version : {contract.version if hasattr(contract, 'version') else 'N/A'}")
print(f"🏗️  Fields  : {len(contract.model.fields)}")
print()

# Show field definitions
print(f"{'Field':<30} {'Type':<15} {'Nullable':<10}")
print("─" * 55)
for field in contract.model.fields:
    nullable = "no" if getattr(field, "not_null", False) or getattr(field, "required", False) else "yes"
    print(f"{field.name:<30} {field.type:<15} {nullable:<10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generated Data

# COMMAND ----------

# ── Generate test data ───────────────────────────────────────────────────────
gen = DataGenerator(contract_path, seed=42)
test_df = gen.generate(
    rows=num_rows,
    ai=ai,
    ai_provider=ai_provider,
    ai_model=ai_model
)

print(f"✅ Generated {len(test_df)} rows × {len(test_df.columns)} columns")
print(f"   Columns: {', '.join(test_df.columns)}")

# COMMAND ----------

# ── Output ───────────────────────────────────────────────────────────────────
# Convert to Spark DataFrame for display
if hasattr(test_df, "to_pandas"):
    # Polars DataFrame
    spark_df = spark.createDataFrame(test_df.to_pandas())
elif hasattr(test_df, "toPandas"):
    # Already Spark
    spark_df = test_df
else:
    # Pandas
    spark_df = spark.createDataFrame(test_df)

if output_format in ("display", "both"):
    display(spark_df)

if output_format in ("temp_view", "both"):
    view_name = Path(contract_path).stem.replace("-", "_")
    spark_df.createOrReplaceTempView(f"test_{view_name}")
    print(f"\n📊 Temp view created: test_{view_name}")
    print(f"   Query with: SELECT * FROM test_{view_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Profile

# COMMAND ----------

# ── Quick stats ──────────────────────────────────────────────────────────────
print(f"📊 Data Profile")
print(f"{'='*50}")

display(spark_df.describe())

# ── Null analysis ────────────────────────────────────────────────────────────
from pyspark.sql.functions import col, count
from pyspark.sql.functions import sum as spark_sum

null_counts = spark_df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in spark_df.columns])

print("\n🔍 Null counts per column:")
display(null_counts)
