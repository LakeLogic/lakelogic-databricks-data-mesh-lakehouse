"""
Shared – Dim User SCD Type 2 Generator
=========================================
Builds a slowly-changing dimension (Type 2) for GA4 users,
tracking attribute changes over time with versioning.

Called by LakeLogic via external_logic in:
  gold_shared_dim_user_v1.0.yaml

Input:  silver_events table (user-level attributes from GA4)
Output: SCD Type 2 dimension — one row per user per version.

SCD Type 2 columns:
  - user_key         (surrogate key — hash of natural key + version)
  - user_pseudo_id   (natural/business key)
  - effective_from   (when this version became active)
  - effective_to     (when replaced — NULL if current)
  - is_current       (True for the latest version)
"""

from __future__ import annotations


def run(
    good_df=None,
    *,
    contract=None,
    engine: str = "spark",
    existing_dim_table: str = "",
    **_kwargs,
):
    """
    LakeLogic external_logic entrypoint for dim_user SCD Type 2.

    Compares incoming user attributes against existing dimension records
    and creates new versions for users whose tracked attributes have changed.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    spark = SparkSession.builder.getOrCreate()

    if good_df is None:
        raise ValueError("No source data — silver events table is empty or missing.")

    # ── Step 1: Extract latest user attributes from events ───────────────
    # Take the most recent event per user to get their current state
    window_latest = Window.partitionBy("user_pseudo_id").orderBy(F.col("event_timestamp").desc())

    user_attrs = (
        good_df.withColumn("_row_num", F.row_number().over(window_latest))
        .filter(F.col("_row_num") == 1)
        .select(
            F.col("user_pseudo_id"),
            F.coalesce(F.col("device_category"), F.lit("unknown")).alias("primary_device"),
            F.coalesce(F.col("device_operating_system"), F.lit("unknown")).alias("primary_os"),
            F.coalesce(F.col("geo_country"), F.lit("unknown")).alias("primary_country"),
            F.coalesce(F.col("geo_city"), F.lit("unknown")).alias("primary_city"),
            F.coalesce(F.col("traffic_source_source"), F.lit("direct")).alias("primary_traffic_source"),
            F.coalesce(F.col("traffic_source_medium"), F.lit("(none)")).alias("primary_traffic_medium"),
            F.current_timestamp().alias("_snapshot_at"),
        )
        .filter(F.col("user_pseudo_id").isNotNull())
    )

    # Columns that trigger a new SCD2 version when changed
    tracked_cols = [
        "primary_device",
        "primary_os",
        "primary_country",
        "primary_city",
        "primary_traffic_source",
        "primary_traffic_medium",
    ]

    # ── Step 2: Load existing dimension (if available) ───────────────────
    existing_dim = None
    if existing_dim_table:
        try:
            existing_dim = spark.table(existing_dim_table).filter(F.col("is_current") == True)
        except Exception:
            existing_dim = None

    # ── Step 3: SCD Type 2 merge logic ───────────────────────────────────
    if existing_dim is not None and existing_dim.count() > 0:
        # Join incoming attributes with existing current records
        joined = user_attrs.alias("new").join(
            existing_dim.alias("old"),
            F.col("new.user_pseudo_id") == F.col("old.user_pseudo_id"),
            "full_outer",
        )

        # Detect changes: build a change flag from tracked columns
        change_condition = F.lit(False)
        for col_name in tracked_cols:
            change_condition = change_condition | (
                F.coalesce(F.col(f"new.{col_name}"), F.lit("__NULL__"))
                != F.coalesce(F.col(f"old.{col_name}"), F.lit("__NULL__"))
            )

        # New users (no match in existing dim)
        new_users = (
            joined.filter(F.col("old.user_pseudo_id").isNull())
            .select(
                F.col("new.user_pseudo_id"),
                *[F.col(f"new.{c}") for c in tracked_cols],
                F.col("new._snapshot_at").alias("effective_from"),
                F.lit(None).cast("timestamp").alias("effective_to"),
                F.lit(True).alias("is_current"),
                F.lit(1).alias("version"),
            )
        )

        # Changed users — close old record + create new version
        changed = joined.filter(
            F.col("old.user_pseudo_id").isNotNull()
            & F.col("new.user_pseudo_id").isNotNull()
            & change_condition
        )

        # Close old records (set effective_to and is_current = False)
        closed_records = changed.select(
            F.col("old.user_pseudo_id"),
            *[F.col(f"old.{c}") for c in tracked_cols],
            F.col("old.effective_from"),
            F.col("new._snapshot_at").alias("effective_to"),
            F.lit(False).alias("is_current"),
            F.col("old.version"),
        )

        # New version records
        new_versions = changed.select(
            F.col("new.user_pseudo_id"),
            *[F.col(f"new.{c}") for c in tracked_cols],
            F.col("new._snapshot_at").alias("effective_from"),
            F.lit(None).cast("timestamp").alias("effective_to"),
            F.lit(True).alias("is_current"),
            (F.col("old.version") + 1).alias("version"),
        )

        # Unchanged users — keep existing records as-is
        unchanged = joined.filter(
            F.col("old.user_pseudo_id").isNotNull()
            & (F.col("new.user_pseudo_id").isNotNull())
            & (~change_condition)
        ).select(
            F.col("old.user_pseudo_id"),
            *[F.col(f"old.{c}") for c in tracked_cols],
            F.col("old.effective_from"),
            F.col("old.effective_to"),
            F.col("old.is_current"),
            F.col("old.version"),
        )

        # Get all historical non-current records from existing dim
        historical = spark.table(existing_dim_table).filter(F.col("is_current") == False)

        # Union everything
        result = historical.unionByName(unchanged).unionByName(closed_records).unionByName(new_versions).unionByName(new_users)

    else:
        # First load — all users are new, version 1
        result = user_attrs.select(
            F.col("user_pseudo_id"),
            *[F.col(c) for c in tracked_cols],
            F.col("_snapshot_at").alias("effective_from"),
            F.lit(None).cast("timestamp").alias("effective_to"),
            F.lit(True).alias("is_current"),
            F.lit(1).alias("version"),
        )

    # ── Step 4: Generate surrogate key ───────────────────────────────────
    result = result.withColumn(
        "user_key",
        F.sha2(F.concat_ws("||", F.col("user_pseudo_id"), F.col("version").cast("string")), 256),
    )

    # ── Step 5: Add metadata ────────────────────────────────────────────
    result = result.withColumn("dim_processed_at", F.current_timestamp())

    # ── Final select ─────────────────────────────────────────────────────
    final_cols = [
        "user_key",
        "user_pseudo_id",
        "primary_device",
        "primary_os",
        "primary_country",
        "primary_city",
        "primary_traffic_source",
        "primary_traffic_medium",
        "effective_from",
        "effective_to",
        "is_current",
        "version",
        "dim_processed_at",
    ]

    return result.select(final_cols)
