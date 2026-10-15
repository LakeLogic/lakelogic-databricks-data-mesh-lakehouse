# Databricks notebook source
# MAGIC %md
# MAGIC # Marketplace tour — what LakeLogic produced
# MAGIC
# MAGIC A read-only walk through the **marketplace** domain: the engine's own tables
# MAGIC (run log, service levels, retention, erasure), and the data features the contracts
# MAGIC declare (quarantine, masking, SCD2, fact → dimension).
# MAGIC
# MAGIC Every cell only reads. Run top to bottom after the mesh orchestrator has run once.
# MAGIC Each section says **what to look for**.

# COMMAND ----------

dbutils.widgets.text("catalog", "rideflow_dev_platform", "Catalog")
dbutils.widgets.text("environment", "dev", "Environment")
CATALOG = dbutils.widgets.get("catalog").strip()
DOMAIN = "marketplace"
# ── Where a domain's tables live ──────────────────────────────────────────────
# A domain is either a SCHEMA of the shared catalog (`<catalog>`.`<domain>`, layer in the
# table name) or its OWN CATALOG with a schema per layer (`rideflow_dev_marketplace`.bronze /
# .silver / .gold / .quarantine / .lakelogic). The domain's system registry decides
# (storage.bronze_root / silver_root / gold_root, and the environment's
# domain_catalog_prefix); this reads it, so a notebook works for both while domains move
# across one at a time. Same function in every notebook that names a table itself — keep the
# copies identical.
def own_catalog(catalog: str, domain: str):
    """The domain's own catalog name, or None when it is a schema of the shared catalog.
    Read from the domain's system registry as staged in the contracts Volume: a registry that
    declares a schema per layer names the catalog `<domain_catalog_prefix>_<domain>`."""
    import glob

    import yaml

    try:
        env = dbutils.widgets.get("environment").strip() or "dev"
    except Exception:  # noqa: BLE001 - a notebook without the widget
        env = "dev"
    for reg in sorted(glob.glob(f"/Volumes/{catalog}/nondelta/_contracts/{domain}/*/_system.yaml")):
        try:
            with open(reg, encoding="utf-8") as fh:
                doc = yaml.safe_load(fh) or {}
        except Exception:  # noqa: BLE001
            continue
        storage = doc.get("storage") or {}
        prefix = ((doc.get("environments") or {}).get(env) or {}).get("domain_catalog_prefix")
        if prefix and any(storage.get(f"{layer}_root") for layer in ("bronze", "silver", "gold")):
            return f"{prefix}_{domain}"
    return None


def domain_layout(catalog: str, domain: str) -> dict:
    own = own_catalog(catalog, domain)
    if own:
        return {
            "per_domain_catalog": True, "catalog": own,
            "bronze": f"`{own}`.`bronze`", "silver": f"`{own}`.`silver`", "gold": f"`{own}`.`gold`",
            "quarantine": f"`{own}`.`quarantine`", "lakelogic": f"`{own}`.`lakelogic`",
            "landing": f"/Volumes/{own}/files/landing",
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


LAY = domain_layout(CATALOG, DOMAIN)
# One name per kind of table, so a query reads the same whichever layout the domain has.
BRONZE, SILVER, GOLD = LAY["bronze"], LAY["silver"], LAY["gold"]
QUARANTINE, META = LAY["quarantine"], LAY["lakelogic"]


def q(sql: str):
    """Run a query against the marketplace domain and show the result."""
    display(spark.sql(sql.format(BRONZE=BRONZE, SILVER=SILVER, GOLD=GOLD, QUARANTINE=QUARANTINE, META=META)))


def exists(table: str) -> bool:
    return spark.catalog.tableExists(table_of(CATALOG, DOMAIN, table).replace("`", ""))


print(f"Reading {DOMAIN} from {LAY['catalog']}" + (" (its own catalog)" if LAY["per_domain_catalog"] else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is in the domain
# MAGIC The medallion tables, plus LakeLogic's own tables — every engine-owned table starts
# MAGIC with `_lakelogic_`, and quarantine tables sit beside the table they reject for.

# COMMAND ----------

q("""
SELECT table_name,
       CASE WHEN table_name LIKE '\\_lakelogic\\_%' THEN 'lakelogic'
            WHEN table_name LIKE 'quarantine\\_%'     THEN 'quarantine'
            ELSE split(table_name, '_')[0] END AS kind
FROM `{catalog}`.information_schema.tables
WHERE table_schema = '{domain}'
ORDER BY kind, table_name
""".replace("{catalog}", CATALOG).replace("{domain}", DOMAIN))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run log — `_lakelogic_run_log`
# MAGIC One row per dataset per run. **Look for:** every layer present, `status` = success,
# MAGIC and `counts_good + counts_quarantined = counts_total`.

# COMMAND ----------

q("""
SELECT data_layer, dataset, status,
       counts_total, counts_good, counts_quarantined,
       round(quarantine_ratio * 100, 1) AS quarantined_pct,
       round(run_duration_seconds, 1)   AS seconds,
       end_time
FROM {META}._lakelogic_run_log
QUALIFY row_number() OVER (PARTITION BY dataset ORDER BY end_time DESC) = 1
ORDER BY CASE data_layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 ELSE 3 END, dataset
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Service levels — `_lakelogic_slo_checks`
# MAGIC The latest verdict per table and check (freshness, volume, quality).
# MAGIC **Look for:** any `passed = false` row and its `message`.

# COMMAND ----------

if exists("_lakelogic_slo_checks"):
    q("""
    SELECT entity, check_type, passed, severity, message, checked_at
    FROM {META}._lakelogic_slo_checks
    QUALIFY row_number() OVER (PARTITION BY entity, check_type ORDER BY checked_at DESC) = 1
    ORDER BY passed, entity, check_type
    """)
else:
    print("No service-level checks yet — run pl_monitor_slo_marketplace_rideflow.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantine — rejected rows, with the reason
# MAGIC Rows that broke a contract rule are kept, not dropped, with `_lakelogic_errors`
# MAGIC naming the rule. **Look for:** the reasons, and which table they came from.

# COMMAND ----------

q_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {QUARANTINE} LIKE 'quarantine_*'").collect()]
if not q_tables:
    print("No quarantine tables — nothing has been rejected.")
else:
    union = " UNION ALL ".join(
        f"SELECT '{t.removeprefix('quarantine_')}' AS source_table, _lakelogic_errors FROM {QUARANTINE}.`{t}`"
        for t in q_tables
    )
    display(spark.sql(f"""
        SELECT source_table, _lakelogic_errors AS reason, count(*) AS rows
        FROM ({union}) GROUP BY ALL ORDER BY rows DESC LIMIT 25
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Masking — bronze (raw) vs silver (masked)
# MAGIC Bronze keeps the source value; silver applies the contract's masking
# MAGIC (`name`, `home_address` redacted; `email`, `phone`, `date_of_birth` hashed).
# MAGIC **Look for:** the same rider, readable in bronze and masked in silver.
# MAGIC *Bronze holds personal data — it is access-controlled, not masked.*

# COMMAND ----------

q("""
SELECT b.rider_id,
       b.name  AS bronze_name,  s.name  AS silver_name,
       b.email AS bronze_email, s.email AS silver_email,
       b.phone AS bronze_phone, s.phone AS silver_phone
FROM {BRONZE}.bronze_rideflow_rider_profiles b
JOIN {SILVER}.silver_rideflow_rider_profiles s USING (rider_id)
LIMIT 5
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. SCD2 dimension — `gold_dim_rider`
# MAGIC History is kept for the tracked columns (`city_code`, `status`,
# MAGIC `preferred_payment_method`). **Look for:** riders with more than one version —
# MAGIC one `is_current = true` row, older rows closed with `effective_to` — and the
# MAGIC `-1` unknown member that late or missing keys resolve to.

# COMMAND ----------

q("""
SELECT rider_id, rider_sk, version_number, status, city_code,
       effective_from, effective_to, is_current
FROM {GOLD}.gold_dim_rider
WHERE rider_id IN (
  SELECT rider_id FROM {GOLD}.gold_dim_rider GROUP BY rider_id HAVING count(*) > 1 LIMIT 5
)
ORDER BY rider_id, version_number
""")

# COMMAND ----------

q("""
SELECT count(*)                                   AS versions,
       count_if(is_current)                       AS current_rows,
       count(DISTINCT rider_id)                   AS riders,
       count_if(rider_sk = '-1')         AS unknown_member_rows
FROM {GOLD}.gold_dim_rider
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fact → dimension — `gold_fact_trip_completed` to `gold_dim_rider`
# MAGIC Each trip carries the rider version that was current at drop-off (`rider_sk`, resolved
# MAGIC as-of `dropoff_at`). **Look for:** every fact row finding its rider version (`orphans`
# MAGIC should be 0; unmatched keys land on the `-1` member).

# COMMAND ----------

q("""
SELECT f.trip_id, f.dropoff_at, d.rider_id, d.version_number, d.city_code, d.status,
       f.total_fare_amount, f.total_tip_amount
FROM {GOLD}.gold_fact_trip_completed f
JOIN {GOLD}.gold_dim_rider d ON d.rider_sk = f.rider_sk
ORDER BY f.dropoff_at DESC
LIMIT 10
""")

# COMMAND ----------

q("""
SELECT count(*)                                  AS fact_rows,
       count_if(d.rider_sk IS NULL)     AS orphans,
       count_if(f.rider_sk = '-1')               AS on_unknown_member
FROM {GOLD}.gold_fact_trip_completed f
LEFT JOIN (SELECT DISTINCT rider_sk FROM {GOLD}.gold_dim_rider) d
  ON d.rider_sk = f.rider_sk
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Retention — `_lakelogic_retention_evidence`
# MAGIC What the retention job checked, the policy window, and the outcome.

# COMMAND ----------

if exists("_lakelogic_retention_evidence"):
    q("""
    SELECT table_name, policy, cutoff, rows_expired, status, detail, timestamp
    FROM {META}._lakelogic_retention_evidence
    QUALIFY row_number() OVER (PARTITION BY table_name ORDER BY timestamp DESC) = 1
    ORDER BY status, table_name
    """)
else:
    print("No retention evidence yet — run pl_privacy_retention.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Erasure — requests and evidence
# MAGIC Requests are inserted into `_lakelogic_erasure_requests`; the erasure job processes
# MAGIC them and writes one `_lakelogic_erasure_evidence` row per table it touched, with
# MAGIC `reason` = the request id. **Look for:** each request's status, and its evidence.
# MAGIC Evidence carries counts only — never the subject's ID.

# COMMAND ----------

if exists("_lakelogic_erasure_requests"):
    q("""
    SELECT request_id, framework, system, status, requested_at, processed_at
    FROM {META}._lakelogic_erasure_requests
    ORDER BY requested_at DESC
    """)
else:
    print("No erasure requests table yet — run pl_admin_setup.")

# COMMAND ----------

if exists("_lakelogic_erasure_evidence"):
    q("""
    SELECT reason AS request_id, profile, table_name, subject_count, rows_affected, status, timestamp
    FROM {META}._lakelogic_erasure_evidence
    ORDER BY timestamp DESC, table_name
    """)
else:
    print("No erasure evidence yet — run pl_privacy_erasure.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ungoverned table — the Estate view
# MAGIC
# MAGIC A table no contract governs. LakeLogic's scan finds it, and **Understand → Estate**
# MAGIC lists it as ungoverned (no contract, no rules, no owner). **Look for:**
# MAGIC `bronze_rideflow_taxi_ungoverned` in the Estate view after the next scan.
# MAGIC Drop it with the last cell when you are done.

# COMMAND ----------

# CREATE OR REPLACE, so re-running the tour does not pile up duplicate rows.
spark.sql(f"""
CREATE OR REPLACE TABLE {BRONZE}.bronze_rideflow_taxi_ungoverned AS
SELECT * FROM VALUES
  (1, 'alpha', current_timestamp()),
  (2, 'beta',  current_timestamp())
AS t(id, name, created_at)""")
q("SELECT * FROM {BRONZE}.bronze_rideflow_taxi_ungoverned")

# COMMAND ----------

# Clean-up: uncomment to remove the test table.
# spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.bronze_rideflow_taxi_ungoverned")
