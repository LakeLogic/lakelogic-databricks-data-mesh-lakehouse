# Databricks notebook source
"""Fail the release bootstrap when its promised outputs are missing."""

dbutils.widgets.text("catalog", "rideflow_dev_demo", "Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

required_tables = [
    ("marketplace", "bronze_rideflow_trip_completed"),
    ("marketplace", "silver_rideflow_trips"),
    ("marketplace", "gold_rideflow_fact_trip_daily_kpis"),
    ("marketplace", "gold_rideflow_dim_rider"),
]

available = {
    (row.table_schema, row.table_name)
    for row in spark.sql(
        f"SELECT table_schema, table_name FROM `{CATALOG}`.information_schema.tables"
    ).collect()
}
missing = [f"{schema}.{table}" for schema, table in required_tables if (schema, table) not in available]
if missing:
    raise AssertionError(f"Missing expected tables: {', '.join(missing)}")

gold_rows = spark.table(f"`{CATALOG}`.marketplace.gold_rideflow_fact_trip_daily_kpis").count()
if gold_rows < 1:
    raise AssertionError("Gold trip KPI table exists but contains no rows")

quarantine_tables = [
    table for schema, table in available
    if schema == "quarantine" and table.startswith("marketplace_")
]
if not quarantine_tables:
    raise AssertionError("No Marketplace quarantine tables were created")

scd2_columns = {
    row.column_name
    for row in spark.sql(
        f"""
        SELECT column_name
        FROM `{CATALOG}`.information_schema.columns
        WHERE table_schema = 'marketplace'
          AND table_name = 'gold_rideflow_dim_rider'
        """
    ).collect()
}
required_scd2 = {"rider_sk", "effective_from", "effective_to", "is_current"}
if not required_scd2.issubset(scd2_columns):
    raise AssertionError(f"Missing SCD2 columns: {sorted(required_scd2 - scd2_columns)}")

dbutils.fs.ls(f"/Volumes/{CATALOG}/nondelta/_logs")
print(
    f"Release smoke test passed: gold_rows={gold_rows}, "
    f"quarantine_tables={len(quarantine_tables)}, scd2_columns=present, logs=present"
)
