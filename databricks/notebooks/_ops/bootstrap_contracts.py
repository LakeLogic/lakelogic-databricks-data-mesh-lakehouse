# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap Contracts
# MAGIC Automatically scans a landing zone and generates Data Contracts and a Registry YAML.
# MAGIC 
# MAGIC Supports AI enrichment (`--ai`) to automatically infer and generate field descriptions, 
# MAGIC identify PII with masked remediation strategies, and define semantic SQL quality rules.

# COMMAND ----------

# MAGIC %pip install lakelogic==1.40.0 pyyaml polars deltalake "typing-extensions>=4.12.0" typer --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("landing_zone", "/Volumes/dev/nondelta/landing_marketing/google_analytics", "Source - Landing Zone")
dbutils.widgets.text("output_dir", "/Workspace/Shared/data_platform/domains_retail/marketing/google_analytics/contracts/new", "Output - Contracts Dir")
dbutils.widgets.text("registry", "/Workspace/Shared/data_platform/domains_retail/marketing/google_analytics/_system.yaml", "Output - Registry YAML")
dbutils.widgets.dropdown("format", "json", ["json", "csv", "parquet"], "Config - Format")
dbutils.widgets.text("pattern", "*.json", "Config - File Pattern")
dbutils.widgets.text("layer", "bronze", "Config - Layer Name")
dbutils.widgets.text("sample_rows", "1000", "Config - Sample Rows")

# AI Enrichment Options
dbutils.widgets.dropdown("ai", "false", ["false", "true"], "AI - Enrichment")
dbutils.widgets.dropdown("ai_provider", "gemini", ["openai", "azure", "anthropic", "gemini", "ollama"], "AI - Provider")
dbutils.widgets.text("ai_model", "gemini-3-flash-preview", "AI - Model")
dbutils.widgets.text("ai_api_key", "", "AI - Optional API Key")

# COMMAND ----------

import os
from pathlib import Path
from lakelogic.cli.main import bootstrap

# Retrieve parameters
landing_zone = dbutils.widgets.get("landing_zone").strip()
output_dir = dbutils.widgets.get("output_dir").strip()
registry = dbutils.widgets.get("registry").strip()
fmt = dbutils.widgets.get("format").strip()
pattern = dbutils.widgets.get("pattern").strip()
layer = dbutils.widgets.get("layer").strip()
sample_rows = int(dbutils.widgets.get("sample_rows") or 1000)

ai = dbutils.widgets.get("ai") == "true"
ai_provider = dbutils.widgets.get("ai_provider").strip() or None
ai_model = dbutils.widgets.get("ai_model").strip() or None
ai_api_key = dbutils.widgets.get("ai_api_key").strip()

if ai_api_key:
    os.environ[f"{ai_provider.upper()}_API_KEY"] = ai_api_key

print(f"🚀 Bootstrapping Contracts")
print(f"   Landing Zone  : {landing_zone}")
print(f"   Output        : {output_dir}")
print(f"   Registry      : {registry}")
print(f"   Format/Match  : {fmt} / {pattern}")
print(f"   AI Enrichment : {'Enabled' if ai else 'Disabled'} ({ai_provider})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Bootstrap Process
# MAGIC This will scan the `landing_zone` for your tables/folders, infer the schemas, build the initial contracts, and update the Central Registry.

# COMMAND ----------

try:
    bootstrap(
        landing=Path(landing_zone),
        output_dir=Path(output_dir),
        registry=Path(registry),
        format=fmt,
        pattern=pattern,
        layer=layer,
        sample_rows=sample_rows,
        # Syncing args
        sync=False,
        sync_update_schema=False,
        sync_overwrite=False,
        # Profiling args
        profile=False,
        detect_pii=False,
        suggest_rules=True,        # Automatically apply pandas deterministic rules
        profile_output_dir=None,
        pii_sample_size=50,
        # AI arguments
        ai=ai,
        ai_provider=ai_provider,
        ai_model=ai_model
    )
    print("\n✅ Generation Complete!")
except SystemExit as e:
    # Typer uses SystemExit(0) on success and (1) on failure
    if e.code == 0:
        print("\n✅ Generation Complete!")
    else:
        print(f"\n❌ Generation failed with exit code: {e.code}")
        raise
except Exception as e:
    print(f"\n❌ Unexpected error: {e}")
    raise
