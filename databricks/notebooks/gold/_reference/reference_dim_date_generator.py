"""
Gold – Date Dimension Generator
=================================
Uses LakeLogic's built-in generate_date_dimension() to produce
a complete date dimension table covering the Olist dataset range.

Called by lakelogic via external_logic in gold/dim_date.yaml.

Note: This contract has no source table — the data is entirely generated.
"""

from __future__ import annotations


def run(
    good_df=None,
    *,
    contract=None,
    engine: str = "polars",
    **_kwargs,
):
    """
    LakeLogic external_logic entrypoint for dim_date.

    Generates a date dimension covering the last 30 years through the end
    of the current year.  The range is computed dynamically so the table
    stays current without manual edits.
    The good_df parameter is ignored (no source table), but the signature
    must match lakelogic's external_logic protocol.
    """
    from datetime import date

    from lakelogic.core.dim_date import generate_date_dimension

    today = date.today()
    start_date = f"{today.year - 30}-01-01"
    end_date = f"{today.year}-12-31"

    # generate_date_dimension outputs columns:
    #   date_key, full_date, year, month, day, day_of_week, day_of_year,
    #   day_name, day_abbrev, month_name, month_abbrev, quarter, year_quarter,
    #   year_month, iso_year, iso_week, iso_weekday, is_weekend, is_business_day,
    #   is_holiday, holiday_name, is_month_start, is_month_end, is_quarter_start,
    #   is_quarter_end, is_year_start, is_year_end, fiscal_year, fiscal_quarter,
    #   fiscal_month, fiscal_year_quarter
    date_df = generate_date_dimension(
        start_date=start_date,
        end_date=end_date,
        fiscal_year_start_month=4,  # April fiscal year
    )

    # Select only the columns defined in the contract schema
    contract_cols = [
        "date_key",
        "year",
        "quarter",
        "month",
        "month_name",
        "week_of_year",
        "day_of_month",
        "day_of_week",
        "day_name",
        "is_weekend",
        "is_month_start",
        "is_month_end",
        "fiscal_year",
        "fiscal_quarter",
    ]

    # Map generator columns to contract columns where names differ
    rename_map = {
        "iso_week": "week_of_year",
        "day": "day_of_month",
    }
    existing_renames = {k: v for k, v in rename_map.items() if k in date_df.columns}
    if existing_renames:
        date_df = date_df.rename(existing_renames)

    available = [c for c in contract_cols if c in date_df.columns]
    result = date_df.select(available)

    # Convert to Spark if running on Databricks
    if engine == "spark":
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(result.to_pandas())

    return result
