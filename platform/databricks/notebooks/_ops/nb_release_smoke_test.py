# Databricks notebook source
"""Fail the release bootstrap when its promised outputs are missing."""

dbutils.widgets.text("catalog", "governed_rideflow_lakehouse_demo", "Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

required_tables = [
    ("marketplace", "bronze_rideflow_trip_completed"),
    ("marketplace", "silver_rideflow_trips"),
    ("marketplace", "gold_fact_trip_completed"),
    ("marketplace", "gold_dim_rider"),
]


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


_mkt = domain_layout(CATALOG, "marketplace")
# Keyed by DOMAIN, whichever catalog and schema the table sits in.
available = {
    ("marketplace", row.table_name)
    for row in spark.sql(
        f"SELECT table_name FROM `{_mkt['catalog']}`.information_schema.tables"
        + ("" if _mkt["per_domain_catalog"] else " WHERE table_schema = 'marketplace'")
    ).collect()
}
missing = [f"{schema}.{table}" for schema, table in required_tables if (schema, table) not in available]
if missing:
    raise AssertionError(f"Missing expected tables: {', '.join(missing)}")

gold_rows = spark.table(table_of(CATALOG, "marketplace", "gold_fact_trip_completed")).count()
if gold_rows < 1:
    raise AssertionError("Gold trip fact table exists but contains no rows")

quarantine_tables = [
    table for schema, table in available
    if schema == "marketplace" and table.startswith("quarantine_")
]
if not quarantine_tables:
    raise AssertionError("No Marketplace quarantine tables were created")

scd2_columns = {
    row.column_name
    for row in spark.sql(
        f"""
        SELECT column_name
        FROM `{_mkt['catalog']}`.information_schema.columns
        WHERE table_schema = '{'gold' if _mkt['per_domain_catalog'] else 'marketplace'}'
          AND table_name = 'gold_dim_rider'
        """
    ).collect()
}
required_scd2 = {"rider_sk", "effective_from", "effective_to", "is_current", "version_number"}
if not required_scd2.issubset(scd2_columns):
    raise AssertionError(f"Missing SCD2 columns: {sorted(required_scd2 - scd2_columns)}")

dbutils.fs.ls(f"/Volumes/{CATALOG}/nondelta/_logs")
print(
    f"Release smoke test passed: gold_rows={gold_rows}, "
    f"quarantine_tables={len(quarantine_tables)}, scd2_columns=present, logs=present"
)
