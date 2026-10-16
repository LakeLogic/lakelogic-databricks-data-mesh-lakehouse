#!/usr/bin/env python3
"""Standalone contract-derived synthetic data generator (LakeLogic Synthetic v2).

Reads the bundled contracts + config and generates a referentially-consistent synthetic
estate with the OSS `lakelogic` DataGenerator. NO production rows were ever read to build
this bundle. Values are contract-derived (Faker semantic generation for recognisable column
names; type-correct otherwise). Masked/PII columns carry masking flags in the contracts, so
run the generated frames through `lakelogic.DataProcessor` to mask them by construction.

Usage:
    pip install lakelogic
    python generate.py            # writes ./out/<dataset>.parquet for every contract
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import yaml
from lakelogic import DataGenerator

HERE = Path(__file__).resolve().parent
CONFIG = json.loads((HERE / "config.json").read_text(encoding="utf-8"))


def _contract_dir() -> Path:
    """Where the contracts live.

    Beside this script when the bundle was downloaded on its own, or one level up when it
    ships inside an exported project - which already carries the contracts, and would
    otherwise carry them twice.
    """
    for candidate in (HERE / "contracts", HERE.parent / "contracts"):
        if candidate.is_dir():
            return candidate
    raise SystemExit(
        "No contracts/ directory found beside generate.py or in its parent. "
        "Run this from inside the exported project, or from the standalone bundle."
    )


def _load_contracts() -> dict:
    contracts = {}
    # rglob, not glob: a standalone bundle keeps them flat, an exported project lays them
    # out by layer (contracts/bronze/..., contracts/silver/...).
    for path in sorted(_contract_dir().rglob("*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        name = doc.get("dataset") or doc["info"]["title"]
        contracts[name] = str(path)
    return contracts


def main() -> None:
    contracts = _load_contracts()
    window_start = datetime.fromisoformat(CONFIG["window_start"])
    window_end = datetime.fromisoformat(CONFIG["window_end"])
    frames = DataGenerator.generate_related(
        contracts,
        rows=CONFIG["rows_per_table"],
        seed=CONFIG["seed"],
        invalid_ratio=CONFIG["invalid_ratio"],
        use_faker=True,
        output_format="pandas",
        relationships=CONFIG["relationships"],
        window_start=window_start,
        window_end=window_end,
        unique_entity_seeds=True,
    )
    out = HERE / "out"
    out.mkdir(exist_ok=True)
    for name, df in frames.items():
        df.to_parquet(out / f"{name}.parquet")
        print(f"  {name}: {len(df)} rows -> out/{name}.parquet")


if __name__ == "__main__":
    main()
