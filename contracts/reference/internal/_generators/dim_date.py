"""
External-logic hook for gold_dim_date.

Called by the LakeLogic pipeline at gold materialization time. Delegates to
the OSS calendar generator and returns the result as a Polars DataFrame —
the pipeline writes it to the gold Delta table (replacing the empty seed).
"""
from __future__ import annotations

from lakelogic.core.dim_date import generate_date_dimension


def run(good_df=None, contract=None, **kwargs):
    """
    LakeLogic external_logic entrypoint.

    Args defined in the contract's `external_logic.args` block are forwarded
    here as kwargs (start_date, end_date, fiscal_year_start_month).

    Returns
    -------
    polars.DataFrame
        Fully populated date dimension.
    """
    import polars as pl

    def calendar(holidays: str):
        return generate_date_dimension(
            start_date=kwargs.get("start_date", "2020-01-01"),
            end_date=kwargs.get("end_date", "2030-12-31"),
            fiscal_year_start_month=kwargs.get("fiscal_year_start_month", 4),
            holiday_calendar=holidays,
            engine="polars",
        )

    # The contract declares a holiday flag per market (is_holiday_us, is_holiday_uk); the
    # calendar flags one calendar at a time, so build each and join them on the date.
    frame = calendar("us").with_columns(pl.col("is_holiday").alias("is_holiday_us"))
    return frame.join(
        calendar("uk").select("date_key", pl.col("is_holiday").alias("is_holiday_uk")), on="date_key")
