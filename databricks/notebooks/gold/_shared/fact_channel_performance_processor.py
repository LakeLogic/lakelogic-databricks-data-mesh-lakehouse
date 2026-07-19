"""
Gold – GA4 Channel Performance Processor
=========================================
Aggregates silver sessions and joins with silver conversions to produce 
the gold performance report by channel group and date.

Logic:
  - Aggregate sessions by (channel_group, session_date)
  - Join with aggregated conversions by (channel_group, conversion_date)
  - Derive engagement, bounce, and conversion rates
  - Derive revenue per session and average order value
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import polars as pl


def run(
    good_df,
    *,
    contract=None,
    engine: str = "polars",
    **_kwargs,
):
    """
    LakeLogic external_logic entrypoint.

    Parameters
    ----------
    good_df : DataFrame
        Silver sessions that have passed quality rules.
    """
    # ── 1. Coerce sessions to Polars ─────────────────────────────────────
    sessions_df = _to_polars(good_df)

    # ── 2. Aggregate Sessions → Daily Channel Totals ─────────────────────
    gold_df = (
        sessions_df
        .group_by(["channel_group", "session_date"])
        .agg(
            pl.count("session_id").alias("sessions"),
            pl.sum("is_engaged").cast(pl.Int64).alias("engaged_sessions"),
            pl.sum("is_bounce").cast(pl.Int64).alias("bounce_sessions"),
            pl.mean("session_duration_seconds").alias("avg_session_duration_seconds"),
            pl.n_unique("user_pseudo_id").alias("unique_users"),
            pl.col("ga_session_number").filter(pl.col("ga_session_number") == 1).count().alias("new_users"),
            pl.col("ga_session_number").filter(pl.col("ga_session_number") > 1).count().alias("returning_users"),
            pl.sum("has_add_to_cart").cast(pl.Int64).alias("add_to_cart_sessions"),
            pl.sum("has_checkout").cast(pl.Int64).alias("checkout_sessions"),
            pl.sum("has_purchase").cast(pl.Int64).alias("purchase_sessions"),
            pl.col("device_category").filter(pl.col("device_category") == "desktop").count().alias("_desktop_count"),
            pl.col("device_category").filter(pl.col("device_category") == "mobile").count().alias("_mobile_count"),
            
            # E-commerce Outcomes
            pl.sum("revenue").alias("revenue"),
            pl.col("transaction_id").drop_nulls().n_unique().alias("transactions")
        )
        .rename({"session_date": "performance_date"})
    )

    # ── 3. Derive Final KPIs ─────────────────────────────────────────────
    gold_df = (
        gold_df
        .with_columns(
            # Rates
            (pl.col("engaged_sessions") / pl.col("sessions")).alias("engagement_rate"),
            (pl.col("bounce_sessions") / pl.col("sessions")).alias("bounce_rate"),
            (pl.col("purchase_sessions") / pl.col("sessions")).alias("conversion_rate"),
            # Values
            (pl.col("revenue") / pl.col("transactions").replace(0, None)).alias("avg_order_value"),
            (pl.col("revenue") / pl.col("sessions")).alias("revenue_per_session"),
            # Device Mix
            (pl.col("_desktop_count") / pl.col("sessions")).alias("desktop_sessions_pct")
            #(pl.col("_mobile_count") / pl.col("sessions")).alias("mobile_sessions_pct"),
        )
        .with_columns(
            pl.lit(datetime.now(timezone.utc).replace(microsecond=0)).alias("gold_processed_at"),
        )
        .drop(["_desktop_count", "_mobile_count", "bounce_sessions"])
    )

    # Final cleanup: fill nulls for metrics
    metrics = [
        "revenue", "transactions", "avg_order_value", "revenue_per_session",
        "engagement_rate", "bounce_rate", "conversion_rate",
        "desktop_sessions_pct"#, "mobile_sessions_pct"
    ]
    gold_df = gold_df.with_columns([pl.col(c).fill_null(0.0) for c in metrics])

    return _maybe_to_spark(gold_df, engine)


# ── Helpers (Standardized across processors) ──────────────────────────────────

def _to_polars(df) -> pl.DataFrame:
    """Convert any DataFrame to Polars."""
    if isinstance(df, pl.DataFrame):
        return df
    if hasattr(df, "toPandas"):
        return pl.from_pandas(df.toPandas())
    return pl.from_pandas(df)


def _try_load_table(table_name: str, engine: str) -> Optional[pl.DataFrame]:
    """Try to load a table. Returns None if unavailable (e.g. local testing)."""
    if not table_name:
        return None
    try:
        if engine == "spark":
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            return pl.from_pandas(spark.table(table_name).toPandas())
        else:
            # Delta table path or name
            path = table_name.replace("table:", "")
            return pl.read_delta(path)
    except Exception:
        return None


def _maybe_to_spark(df: pl.DataFrame, engine: str):
    """Convert back to Spark if needed."""
    if engine == "spark":
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            return spark.createDataFrame(df.to_pandas())
        except Exception:
            pass
    return df
