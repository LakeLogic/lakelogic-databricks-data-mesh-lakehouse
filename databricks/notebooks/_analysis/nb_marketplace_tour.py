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

dbutils.widgets.text("catalog", "governed_rideflow_lakehouse_demo", "Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()
DOMAIN = "marketplace"
S = f"`{CATALOG}`.`{DOMAIN}`"


def q(sql: str):
    """Run a query against the marketplace schema and show the result."""
    display(spark.sql(sql.format(S=S)))


def exists(table: str) -> bool:
    return spark.catalog.tableExists(f"{CATALOG}.{DOMAIN}.{table}")


print(f"Reading {CATALOG}.{DOMAIN}")

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
FROM {S}._lakelogic_run_log
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
    FROM {S}._lakelogic_slo_checks
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

q_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {S} LIKE 'quarantine_*'").collect()]
if not q_tables:
    print("No quarantine tables — nothing has been rejected.")
else:
    union = " UNION ALL ".join(
        f"SELECT '{t.removeprefix('quarantine_')}' AS source_table, _lakelogic_errors FROM {S}.`{t}`"
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
FROM {S}.bronze_rideflow_rider_profiles b
JOIN {S}.silver_rideflow_rider_profiles s USING (rider_id)
LIMIT 5
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. SCD2 dimension — `gold_dim_rider_profiles`
# MAGIC History is kept for the tracked columns (`city_code`, `status`,
# MAGIC `preferred_payment_method`). **Look for:** riders with more than one version —
# MAGIC one `is_current = true` row, older rows closed with `effective_to` — and the
# MAGIC `-1` unknown member that late or missing keys resolve to.

# COMMAND ----------

q("""
SELECT rider_id, rider_profiles_sk, version_number, status, city_code,
       effective_from, effective_to, is_current
FROM {S}.gold_dim_rider_profiles
WHERE rider_id IN (
  SELECT rider_id FROM {S}.gold_dim_rider_profiles GROUP BY rider_id HAVING count(*) > 1 LIMIT 5
)
ORDER BY rider_id, version_number
""")

# COMMAND ----------

q("""
SELECT count(*)                                   AS versions,
       count_if(is_current)                       AS current_rows,
       count(DISTINCT rider_id)                   AS riders,
       count_if(rider_profiles_sk = '-1')         AS unknown_member_rows
FROM {S}.gold_dim_rider_profiles
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fact → dimension — `gold_fact_trip_completed` to `gold_dim_rider_profiles`
# MAGIC Each trip carries the rider version that was current at drop-off (`rider_sk`, resolved
# MAGIC as-of `dropoff_at`). **Look for:** every fact row finding its rider version (`orphans`
# MAGIC should be 0; unmatched keys land on the `-1` member).

# COMMAND ----------

q("""
SELECT f.trip_id, f.dropoff_at, d.rider_id, d.version_number, d.city_code, d.status,
       f.total_fare_amount, f.total_tip_amount
FROM {S}.gold_fact_trip_completed f
JOIN {S}.gold_dim_rider_profiles d ON d.rider_profiles_sk = f.rider_sk
ORDER BY f.dropoff_at DESC
LIMIT 10
""")

# COMMAND ----------

q("""
SELECT count(*)                                  AS fact_rows,
       count_if(d.rider_profiles_sk IS NULL)     AS orphans,
       count_if(f.rider_sk = '-1')               AS on_unknown_member
FROM {S}.gold_fact_trip_completed f
LEFT JOIN (SELECT DISTINCT rider_profiles_sk FROM {S}.gold_dim_rider_profiles) d
  ON d.rider_profiles_sk = f.rider_sk
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Retention — `_lakelogic_retention_evidence`
# MAGIC What the retention job checked, the policy window, and the outcome.

# COMMAND ----------

if exists("_lakelogic_retention_evidence"):
    q("""
    SELECT table_name, policy, cutoff, rows_expired, status, detail, timestamp
    FROM {S}._lakelogic_retention_evidence
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
    FROM {S}._lakelogic_erasure_requests
    ORDER BY requested_at DESC
    """)
else:
    print("No erasure requests table yet — run pl_admin_setup.")

# COMMAND ----------

if exists("_lakelogic_erasure_evidence"):
    q("""
    SELECT reason AS request_id, profile, table_name, subject_count, rows_affected, status, timestamp
    FROM {S}._lakelogic_erasure_evidence
    ORDER BY timestamp DESC, table_name
    """)
else:
    print("No erasure evidence yet — run pl_privacy_erasure.")
