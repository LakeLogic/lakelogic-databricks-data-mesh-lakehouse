"""Marketplace data is partitioned by country, from bronze to gold.

    materialization._all.partition_by: [country_code]   (marketplace/rideflow/_system.yaml)

The simulator stamps `country_code` on every entity from its city, and one country follows a
record through every relationship, so each country lands in its own `country_code=GB/` folder.
That only holds if every table that should carry the column declares it: a contract without
the field silently writes unpartitioned (the writer prunes a partition column the data lacks).

WHAT THIS WOULD CATCH
    A new bronze or silver contract added without `country_code`; a gold aggregate that drops
    it from its SELECT; or the driver's derived trip events (requests, cancellations) losing it,
    which would land every derived row in the null partition.

    python -m pytest databricks/tests
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SYSTEM = ROOT / "domains_rideflow" / "marketplace" / "rideflow"

#: Gold tables whose grain is one rider or driver — no single country column to carry, so they
#: are written unpartitioned (with a warning). Add one here only with that reason.
UNPARTITIONED_GOLD = {
    "gold_dim_driver_scorecard_v1.0.yaml",
    "gold_fact_rider_daily_metrics_v1.0.yaml",
    "gold_fact_surge_pricing_inference_v1.0.yaml",  # fed by its own landing, not the simulator
}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _fields(path: Path) -> list[str]:
    return [f["name"] for f in (_load(path).get("model") or {}).get("fields") or []]


def test_every_marketplace_layer_partitions_by_country():
    mat = _load(SYSTEM / "_system.yaml").get("materialization") or {}
    assert (mat.get("_all") or {}).get("partition_by") == ["country_code"]


@pytest.mark.parametrize(
    "contract",
    sorted((SYSTEM / "contracts").glob("*/*.yaml")),
    ids=lambda p: p.name,
)
def test_every_contract_carries_the_partition_column(contract: Path):
    if contract.name in UNPARTITIONED_GOLD:
        pytest.skip("per-entity grain; written unpartitioned by design")
    assert "country_code" in _fields(contract), f"{contract.name} would write unpartitioned"


def test_the_daily_kpi_aggregate_keeps_country():
    sql = " ".join(
        t.get("sql", "") for t in _load(SYSTEM / "contracts" / "gold" / "gold_fact_trip_daily_kpis_v1.0.yaml")["transformations"]
    )
    assert "country_code," in sql and "city_code, country_code" in sql


def test_derived_trip_events_inherit_the_trips_country():
    driver = (ROOT / "databricks" / "notebooks" / "test_data_driver.py").read_text(encoding="utf-8")
    body = driver.split("def _gen_trip_events", 1)[1].split("\ndef ", 1)[0]
    assert body.count('"country_code": t.get("country_code"') == 2, "requests and cancellations both carry it"
