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

#: Gold is the Build Centre-generated star (gold_rideflow_*). Its facts declare their own
#: `materialization.partition_by` (the event date), which overrides `_all`; its dims are keyed
#: per rider / driver / code, with no single country to carry, so they are written
#: unpartitioned (with a warning). Bronze and silver still carry country_code end to end.
def _gold_partitions_itself(contract: Path) -> bool:
    return contract.parent.name == "gold" and contract.name.startswith("gold_rideflow_")


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
    if _gold_partitions_itself(contract):
        pytest.skip("BC gold: facts partition by event date, dims are per-entity")
    assert "country_code" in _fields(contract), f"{contract.name} would write unpartitioned"


def test_gold_facts_declare_their_own_partition():
    facts = sorted((SYSTEM / "contracts" / "gold").glob("gold_rideflow_fact_*.yaml"))
    assert facts, "no BC gold facts found"
    unpartitioned = [
        f.name for f in facts
        if not ((_load(f).get("materialization") or {}).get("partition_by"))
    ]
    assert not unpartitioned, f"facts with no partition_by: {unpartitioned}"


def test_derived_trip_events_inherit_the_trips_country():
    driver = (ROOT / "databricks" / "notebooks" / "nb_test_data_driver.py").read_text(encoding="utf-8")
    body = driver.split("def _gen_trip_events", 1)[1].split("\ndef ", 1)[0]
    assert body.count('"country_code": t.get("country_code"') == 2, "requests and cancellations both carry it"
