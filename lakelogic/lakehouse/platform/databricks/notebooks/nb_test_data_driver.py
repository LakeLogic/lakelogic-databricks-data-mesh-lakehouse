# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # test_data_driver — write a synthetic BRONZE estate (Databricks)
# MAGIC
# MAGIC Generates synthetic bronze data from the contract schemas, with consistent foreign keys.
# MAGIC **No production rows are read.** Every row has <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_synthetic=true</span> and is **permanently
# MAGIC synthetic** — never re-classify it as production.
# MAGIC
# MAGIC Use it for development, testing, demos and CI. Set <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">generative=true</span> (needs an LLM key in
# MAGIC THIS environment) for AI-enriched values. Running the pipeline for silver/gold is optional.
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC 1. Installs LakeLogic.
# MAGIC 2. Defines the synthetic seed library (schema-driven rows, consistent foreign keys, documents).
# MAGIC 3. Reads the parameters.
# MAGIC 4. Writes synthetic bronze files into each contract's landing zone (<span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">seed</span> or <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">extend</span> mode).
# MAGIC
# MAGIC **Parameters**
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">catalog</span> | Unity Catalog this run reads and writes |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">contracts_root</span> | workspace folder holding <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">contracts/</span> |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">domain</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">system</span> | which <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">_system.yaml</span> registry to load |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">environment</span> | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">dev</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">staging</span> \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">prod</span> |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">rows_per_table</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">rows_per_entity</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">entities</span> | how many rows, for which entities |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">mode</span> | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">seed</span> (one window) \| <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">extend</span> (catch up to now) |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">windows_on_empty</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">history_minutes</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">max_windows</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">new_parent_ratio</span> | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">extend</span> windowing |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">loop_interval_minutes</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">max_run_minutes</span> | optional repeat loop |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">batches</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">seed</span> | files per table; random seed |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">invalid_ratio</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">duplicate_ratio</span> | deliberate bad rows for the quality rules |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">synthetic_schema</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">generative</span> | output schema label; AI-enriched values |
# MAGIC | <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">lakelogic_version</span> / <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">lakelogic_wheel</span> | install source (blank = latest from PyPI; a wheel wins) |
# MAGIC
# MAGIC Generated by the LakeLogic Medallion Builder — regenerate, do not hand-edit.
# MAGIC generated by: LakeLogic Medallion Builder
# MAGIC revision: chv1:e7b0c49f07bce8fda7e01b76564ac41c80ddcbb956e46bec30c967061acb4e1a
# MAGIC governance seal: sha256:5dec3512859b80333097a50f1f3bd620ff4ec644a86a89f7ebd2a821ba9cc108
# MAGIC source: estate snapshot snap-d91da9d87d0e48ba
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install LakeLogic
# MAGIC
# MAGIC Databricks pre-installs the packages named in a <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">%pip</span> cell at session start, so a
# MAGIC bare <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">lakelogic</span> can report "already satisfied" and silently keep an older build --
# MAGIC the run then tests the wrong code while looking correct. <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">--upgrade</span> (or an explicit
# MAGIC <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">==</span>) forces pip to act. <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">restartPython()</span> is required after any <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">%pip</span> install
# MAGIC before the new version is importable.

# COMMAND ----------

# DBTITLE 1,Resolve install source
# Declare the parameters (the job passes values for these).
dbutils.widgets.text("lakelogic_version", "", "LakeLogic version (blank = latest)")
dbutils.widgets.text("lakelogic_wheel", "", "LakeLogic wheel (blank = PyPI)")

# Read them.
_version = dbutils.widgets.get("lakelogic_version").strip()
_wheel = dbutils.widgets.get("lakelogic_wheel").strip()
# [synthetic] brings Faker for realistic values; [pdf] reads PDFs for document contracts.
# A wheel wins over a version. `--force-reinstall` because Databricks pre-installs the
# packages named in this cell at session start: without it a same-version wheel is
# "already satisfied" and the run silently executes the PUBLISHED build instead.
if _wheel:
    # `[synthetic,pdf]` on the wheel path too: a bare wheel installs no extras, and the
    # synthetic seed needs reportlab to write documents. pip accepts extras on a local
    # file exactly as it does on a package name.
    lakelogic_pkg = f"{_wheel}[synthetic,pdf] --force-reinstall"
elif _version:
    lakelogic_pkg = f"lakelogic[synthetic,pdf]=={_version}"
else:
    lakelogic_pkg = "lakelogic[synthetic,pdf] --upgrade"
print(f"Installing LakeLogic from: {lakelogic_pkg}")

# COMMAND ----------

# DBTITLE 1,Install LakeLogic
# MAGIC # pyarrow<25: DBR ships an older pyarrow than lakelogic needs, and
# MAGIC # databricks-connect caps it below 25. pyyaml/polars/deltalake arrive as
# MAGIC # lakelogic's own dependencies and are not named here.
# MAGIC %pip install $lakelogic_pkg "pyarrow<25"

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Synthetic seed library
# MAGIC
# MAGIC Pure functions: nothing runs until section 4 calls them.

# COMMAND ----------

# DBTITLE 1,Synthetic seed library
"""Synthetic bronze seed: fake rows for every contract that reads a landing zone.

- Rows come from each contract's schema; no production data is read.
- Each entity is written to its contract's landing zone (`source.path`), in the
  contract's format and partition (see `landing_layout`).
- Foreign keys are consistent (lakelogic `DataGenerator.generate_related`).
- Same seed => same rows. Every row has `_synthetic=True`.
- Self-contained (stdlib, lakelogic, polars) so it can be pasted into a notebook.
  Do not add `api.*` imports or `from __future__` (it must run mid-notebook).
"""
import argparse
import hashlib
import os
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# A fixed generation window keeps temporal columns reproducible from the seed.
_DEFAULT_WINDOW_START = datetime(2020, 1, 1)
_DEFAULT_WINDOW_END = datetime(2025, 1, 1)
_MARKER = "_synthetic"


def reads_landing(layer: Any, contract_dict: Any) -> bool:
    """True if the contract reads a landing zone, so it gets synthetic data.

    - Every bronze contract.
    - Any contract with `source.type: landing`, e.g. gold seed dimensions (countries,
      a date spine) that read a CSV from landing with no bronze in front.
    """
    if layer == "bronze":
        return True
    source = (contract_dict or {}).get("source") if isinstance(contract_dict, dict) else None
    return isinstance(source, dict) and str(source.get("type") or "").lower() == "landing"


def _fields_of(contract_dict: Dict[str, Any]) -> List[str]:
    """Column names in a contract's `model.fields`."""
    model = contract_dict.get("model") if isinstance(contract_dict, dict) else None
    fields = (model or {}).get("fields") or []
    return [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]


def _resolve_name(name: Any, *, system: str = "", domain: str = "") -> str:
    """A name with layer, system and domain placeholders filled in, lowercased."""
    out = _resolve_layers(str(name or ""))
    if system:
        out = out.replace("{system}", system)
    if domain:
        out = out.replace("{domain}", domain)
    return out.strip().lower()


def declared_relationships(
    docs: Dict[str, Dict[str, Any]],
    *,
    aliases: Optional[Dict[str, str]] = None,
    system: str = "",
    domain: str = "",
    unresolved: Optional[List[Dict[str, str]]] = None,
) -> List[Dict[str, str]]:
    """FK edges from the contracts' declared `foreign_key` blocks.

    - `docs` is `{name: contract doc}`.
    - `foreign_key.contract` is matched to a doc's name, `dataset`, `info.table_name`
      or an `aliases` entry (e.g. the silver table a bronze entity feeds).
    - Kept only if the parent is in `docs` and has the referenced column.
    - Otherwise (usually a parent in another system) it goes to `unresolved`,
      never to a wrong table.
    """
    index: Dict[str, str] = {}
    for name, doc in docs.items():
        info = doc.get("info") or {}
        for key in (name, doc.get("dataset"), info.get("table_name")):
            resolved = _resolve_name(key, system=system, domain=domain)
            if resolved and "{" not in resolved:
                index.setdefault(resolved, name)
    for alias, name in (aliases or {}).items():
        if name in docs:
            index.setdefault(_resolve_name(alias, system=system, domain=domain), name)

    rels: List[Dict[str, str]] = []
    seen = set()
    for child in sorted(docs):
        for field in ((docs[child].get("model") or {}).get("fields") or []):
            fk = field.get("foreign_key") if isinstance(field, dict) else None
            if not isinstance(fk, dict) or not field.get("name"):
                continue
            column = str(field["name"])
            target = _resolve_name(fk.get("contract"), system=system, domain=domain)
            parent = index.get(target)
            parent_column = str(fk.get("column") or column)
            if (child, column) in seen:
                continue
            if not parent or parent == child or parent_column not in _fields_of(docs[parent]):
                if unresolved is not None:
                    unresolved.append({"child": child, "child_column": column,
                                       "parent_contract": target, "parent_column": parent_column})
                continue
            seen.add((child, column))
            rels.append({"child": child, "child_column": column,
                         "parent": parent, "parent_column": parent_column})
    return rels


def _declares_foreign_key(doc: Dict[str, Any], column: str) -> bool:
    return any(
        isinstance(f, dict) and f.get("name") == column and isinstance(f.get("foreign_key"), dict)
        for f in ((doc.get("model") or {}).get("fields") or [])
    )


def contract_relationships(
    docs: Dict[str, Dict[str, Any]],
    *,
    aliases: Optional[Dict[str, str]] = None,
    system: str = "",
    domain: str = "",
    unresolved: Optional[List[Dict[str, str]]] = None,
) -> List[Dict[str, str]]:
    """Declared `foreign_key` edges, plus name-based edges for columns that declare none.

    A column whose declared parent is outside `docs` is never re-wired by name.
    """
    declared = declared_relationships(
        docs, aliases=aliases, system=system, domain=domain, unresolved=unresolved,
    )
    inferred = [
        r for r in _infer_relationships({e: _fields_of(d) for e, d in docs.items()})
        if not _declares_foreign_key(docs[r["child"]], r["child_column"])
    ]
    return declared + inferred


def _infer_relationships(entity_columns: Dict[str, List[str]]) -> List[Dict[str, str]]:
    """Fallback FK edges guessed from column names (declared `foreign_key` wins).

    - `<base>_id` on a child points to the entity named `<base>` (singular or plural)
      that also has `<base>_id`.
    - Unmatched `*_id` columns stay plain fake ids.
    """
    # Which entity owns each `<name>_id` column.
    owner_of: Dict[str, str] = {}
    for ent in sorted(entity_columns):
        singular = ent[:-1] if ent.endswith("s") else ent
        for cand in (f"{ent}_id", f"{singular}_id"):
            if cand in entity_columns[ent]:
                owner_of.setdefault(cand, ent)
    rels: List[Dict[str, str]] = []
    seen = set()
    for child in sorted(entity_columns):
        for col in entity_columns[child]:
            if not (isinstance(col, str) and col.endswith("_id")):
                continue
            parent = owner_of.get(col)
            if parent and parent != child and (child, col) not in seen:
                seen.add((child, col))
                rels.append({"child": child, "child_column": col,
                             "parent": parent, "parent_column": col})
    return rels


#: How many placeholder values a code column gets while its allowed values are unanswered.
CODE_PLACEHOLDER_COUNT = 6
_CODE_LIST_SQL = None


def code_columns(contracts: Any) -> set:
    """Columns a GOLD CODE DIMENSION lists: its step is `SELECT DISTINCT <col> FROM source`
    (``code_list_sql``) and its key is that one column.

    The scan is metadata-only, so nobody knows these columns' values. Unanswered, the
    generator invented random `CAN-4821` codes and a code dimension got ~5,500 rows
    (Databricks, 2026-09-29). :func:`with_code_placeholders` keeps them to a handful.
    """
    import re

    global _CODE_LIST_SQL
    if _CODE_LIST_SQL is None:
        _CODE_LIST_SQL = re.compile(r"SELECT\s+DISTINCT\s+([A-Za-z_][A-Za-z0-9_]*)\s+FROM\s+source\b", re.IGNORECASE)
    out = set()
    for c in contracts or ():
        doc = getattr(c, "contract_dict", c) or {}
        if str(getattr(c, "layer", "") or (doc.get("info") or {}).get("target_layer") or "").lower() != "gold":
            continue
        pk = doc.get("primary_key")
        pk = [pk] if isinstance(pk, str) else list(pk or [])
        for step in doc.get("transformations") or []:
            m = _CODE_LIST_SQL.search(str((step or {}).get("sql") or "")) if isinstance(step, dict) else None
            if m and pk == [m.group(1)]:
                out.add(m.group(1))
    return out


def _has_allowed_values(doc: Dict[str, Any], column: str) -> bool:
    """Whether the (typed) bronze doc already states this column's values."""
    for f in ((doc.get("model") or {}).get("fields") or []):
        if isinstance(f, dict) and f.get("name") == column and f.get("accepted_values"):
            return True
    for rule in ((doc.get("quality") or {}).get("row_rules") or []):
        if not isinstance(rule, dict):
            continue
        sql = str(rule.get("sql") or "")
        if column in _sql_rule_columns(sql) and " in (" in sql.lower():
            return True
        av = rule.get("accepted_values")
        if isinstance(av, dict) and str(av.get("field")) == column:
            return True
    return False


def with_code_placeholders(doc: Dict[str, Any], codes: set) -> Dict[str, Any]:
    """``doc`` with a small placeholder set on each code column that has no allowed values.

    `cancelled_by_1` … `cancelled_by_6`: honest placeholders, not guesses. An answered
    `q:codevalues` question becomes an accepted_values rule on silver, which
    :func:`typed_bronze_doc` copies in, and then the real values are used instead.
    """
    for f in ((doc.get("model") or {}).get("fields") or []):
        name = f.get("name") if isinstance(f, dict) else None
        if name in codes and not _has_allowed_values(doc, name):
            f["accepted_values"] = [f"{name}_{i}" for i in range(1, CODE_PLACEHOLDER_COUNT + 1)]
    return doc


def generate_bronze_frames(
    registry_path: str,
    *,
    environment: str = "dev",
    storage_mode: str = "catalog",
    rows: int = 200,
    seed: int = 7,
    invalid_ratio: float = 0.0,
    #: Per-entity row counts, e.g. `{"charges": 5000}`; others use `rows`.
    rows_by_entity: Optional[Dict[str, int]] = None,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
    generative: bool = False,
    ai_provider: Optional[str] = None,
    ai_model: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate `{entity: polars.DataFrame}` for every landing contract. Does not write.

    - Reads contract schemas only, typed from the silver contract that reads each one.
    - FK edges come from `seed_relationships`.
    - Skips `extraction` contracts; `external_logic` contracts get an empty frame.
    - Every frame gets `_synthetic=True`.
    - Raises `RuntimeError` if no contract reads a landing zone.
    """
    from lakelogic.core.generator import DataGenerator
    from lakelogic.core.registry import DomainRegistry
    import polars as pl

    rows = max(1, int(rows))
    window_start = window_start or _DEFAULT_WINDOW_START
    window_end = window_end or _DEFAULT_WINDOW_END

    registry = DomainRegistry.from_yaml(
        registry_path, environment=environment, storage_mode=storage_mode
    )
    # Every contract that reads a landing zone (bronze and gold seed dims).
    bronze_contracts = [
        c for c in registry.contracts
        if reads_landing(c.layer, c.contract_dict) and c.resolved_path
    ]
    if not bronze_contracts:
        raise RuntimeError(
            f"No bronze contracts in {registry_path} (nor any other contract reading a "
            "landing zone) — nothing to seed."
        )
    # Skip `extraction` contracts: the generator rejects them and fails the whole batch.
    extracted = sorted(c.entity for c in bronze_contracts if (c.contract_dict or {}).get("extraction"))
    for entity in extracted:
        print(f"[synthetic_seed] {entity}: skipped - it extracts from unstructured documents "
              "(`extraction`), which cannot be synthesised; land real sample files for it")
    bronze_contracts = [c for c in bronze_contracts if c.entity not in extracted]
    # `external_logic` builds its own rows (e.g. date dim): header-only file.
    bootstrap: Dict[str, List[str]] = {
        c.entity: _fields_of(c.contract_dict or {})
        for c in bronze_contracts if (c.contract_dict or {}).get("external_logic")
    }
    for entity in sorted(bootstrap):
        print(f"[synthetic_seed] {entity}: header-only bootstrap - its rows come from its "
              "`external_logic`, not from synthetic generation")
    bronze_contracts = [c for c in bronze_contracts if c.entity not in bootstrap]
    if not bronze_contracts and not bootstrap:
        print(f"[synthetic_seed] every bronze contract in {registry_path} is extraction-based; nothing to generate")
        return {}
    # Use silver's types, not bronze's all-string ones.
    consumers = silver_consumers(registry)
    codes = code_columns(registry.contracts)
    import yaml as _yaml

    bronze = {
        c.entity: _yaml.safe_dump(
            with_code_placeholders(typed_bronze_doc(c.contract_dict or {}, consumers.get(c.entity)), codes),
            sort_keys=False,
        )
        for c in bronze_contracts
    }
    # Only edges between generated tables.
    relationships = [
        r for r in seed_relationships(registry, quiet=True)
        if r["child"] in bronze and r["parent"] in bronze
    ]

    def _generate(ai_on: bool) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = dict(
            rows=(
                {e: max(1, int((rows_by_entity or {}).get(e, rows))) for e in bronze}
                if rows_by_entity else rows
            ),
            invalid_ratio=invalid_ratio, seed=seed, output_format="polars",
            window_start=window_start, window_end=window_end,
            relationships=relationships or None,
            # Same options as the Deliver bundle's `generate.py`, so both give the same data.
            # - use_faker: a missing extra fails loudly.
            # - unique_entity_seeds: same-shaped tables don't share keys.
            use_faker=True,
            unique_entity_seeds=True,
            # A child row copies its parent's partition columns.
            inherit_partition_columns=True,
        )
        if ai_on:
            kwargs.update(ai=True, ai_provider=ai_provider, ai_model=ai_model)
        return DataGenerator.generate_related(bronze, **kwargs)

    # Generative mode needs an LLM key (OPENAI_API_KEY / ANTHROPIC_API_KEY /
    # LAKELOGIC_AI_PROVIDER). If it fails, warn and fall back to deterministic.
    if not bronze:
        frames = {}  # only bootstrap contracts in this system
    elif generative:
        try:
            frames = _generate(True)
        except Exception as exc:  # noqa: BLE001 - fall back, don't fail
            print(
                f"[synthetic_seed] generative mode unavailable ({type(exc).__name__}: {exc}); "
                "falling back to deterministic generation. Configure an LLM provider "
                "(e.g. OPENAI_API_KEY) in this environment to enable it."
            )
            frames = _generate(False)
    else:
        frames = _generate(False)

    out: Dict[str, Any] = {}
    for entity in sorted(frames):  # deterministic order
        df = frames[entity]
        # Drop `_is_invalid`; add the synthetic marker.
        if "_is_invalid" in df.columns:
            df = df.drop("_is_invalid")
        out[entity] = df.with_columns(pl.lit(True).alias(_MARKER))
    for entity, columns in sorted(bootstrap.items()):
        out[entity] = pl.DataFrame(schema={**{col: pl.Utf8 for col in columns}, _MARKER: pl.Boolean})
    return out


def document_folder(contract_dict: Dict[str, Any], *, landing_root: str, out_root: str) -> Optional[str]:
    """Folder for a document contract's files: `source.path` up to the first glob, under `out_root`.

    E.g. `{landing_root}/driver_licences/**/*.pdf` -> `<out_root>/driver_licences`.
    """
    declared = str(((contract_dict or {}).get("source") or {}).get("path") or "").replace("\\", "/")
    if not declared:
        return None
    root = str(landing_root or "").strip("/")
    declared = declared.replace("{landing_root}", root)
    for mount in ("/lakehouse/default/", "lakehouse/default/"):
        if declared.startswith(mount):
            declared = declared[len(mount):]
    parts = []
    for part in declared.strip("/").split("/"):
        if any(ch in part for ch in "*?["):
            break
        parts.append(part)
    folder = "/".join(parts)
    base = out_root.rstrip("/")
    if root and (folder == root or folder.startswith(root + "/")):
        tail = folder[len(root):].strip("/")
        return f"{base}/{tail}" if tail else base
    return f"{base}/{parts[-1]}" if parts else base


def _write_documents(
    registry_path: str, out_root: str, *, environment: str, storage_mode: str, rows: int,
    rows_by_entity: Optional[Dict[str, int]], seed: int, invalid_ratio: float, wanted: set,
) -> List[Dict[str, Any]]:
    """Write synthetic documents for this system's extraction contracts (lakelogic core)."""
    from lakelogic.core.registry import DomainRegistry as _Reg

    registry = _Reg.from_yaml(registry_path, environment=environment, storage_mode=storage_mode)
    landing_root = str(getattr(getattr(registry, "storage", None), "landing_root", "") or "")
    candidates = [
        c for c in getattr(registry, "contracts", []) or []
        if (getattr(c, "contract_dict", None) or {}).get("extraction")
        and reads_landing(getattr(c, "layer", None), getattr(c, "contract_dict", None))
        and (not wanted or c.entity.lower() in wanted)
    ]
    if not candidates:
        return []
    try:
        from lakelogic.core.synthetic_documents import generate_documents, supports_documents
    except ImportError:
        print("[synthetic_seed] this lakelogic has no `lakelogic.core.synthetic_documents`; "
              f"documents not written for {sorted(c.entity for c in candidates)} - upgrade lakelogic")
        return []
    written: List[Dict[str, Any]] = []
    for c in candidates:
        doc = c.contract_dict or {}
        if not supports_documents(doc):
            continue  # LLM extraction: nothing to render
        dest = document_folder(doc, landing_root=landing_root, out_root=out_root)
        if not dest:
            print(f"[synthetic_seed] {c.entity}: no source.path to write documents to")
            continue
        count = max(1, int((rows_by_entity or {}).get(c.entity, rows)))
        paths = generate_documents(doc, dest, rows=count, seed=seed, invalid_ratio=invalid_ratio)
        print(f"[synthetic_seed] {c.entity}: {len(paths)} synthetic document(s) -> {dest}")
        written.append({"entity": c.entity, "rows": len(paths), "path": dest, "format": "pdf",
                        "partition": None, "declared": True, "batches": len(paths)})
    return written


#: File extension per format.
_EXT = {"csv": "csv", "json": "json", "parquet": "parquet"}


def batch_filename(entity: str, ext: str, *, seed: int = 7, index: int = 0) -> str:
    """A landing file name like `batch_00_e7cec9.csv`.

    - Same `seed` and `index` => same name, so a re-run with the same seed rewrites its files.
    - A different seed or index => a new file, so batches accumulate.
    """
    import hashlib

    digest = hashlib.sha256(f"{entity}:{seed}:{index}".encode()).hexdigest()[:6]
    return f"batch_{index:02d}_{digest}.{ext}"


def landing_layout(
    contract_dict: Dict[str, Any], *, landing_root: str, out_root: str, entity: str,
    at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Where and how an entity's landing files are written, read from its contract.

    Returns:
    - `path`: `source.path` moved from `landing_root` onto `out_root`, plus the partition.
      `<out_root>/<entity>` only if no `source.path` is declared.
    - `format`: `source.format`, per contract (one design can mix json and parquet).
    - `partition`: `source.partition.format` filled in with `at` (default now, UTC).
    - `multiline`: the JSON `multiLine` option (default true).
    """
    src = ((contract_dict or {}).get("source") or {}) if isinstance(contract_dict, dict) else {}
    declared = str(src.get("path") or "").strip().rstrip("/")
    root = str(landing_root or "").strip().rstrip("/")
    base = out_root.rstrip("/")
    if declared and root and (declared == root or declared.startswith(root + "/")):
        tail = declared[len(root):].lstrip("/")
        dest = f"{base}/{tail}" if tail else base
    elif declared:
        # Not under landing_root: use the last segment, to stay inside the zone.
        dest = f"{base}/{declared.rsplit('/', 1)[-1]}"
    else:
        dest = f"{base}/{entity}"

    fmt = str(src.get("format") or "").strip().lower() or None
    # Match the reader's JSON shape. `multiLine` defaults to true (one JSON value per file);
    # JSON Lines would then load only the first row of each file.
    json_opts = (src.get("options") or {}) if isinstance(src.get("options"), dict) else {}
    multiline = bool(json_opts.get("multiLine", json_opts.get("multiline", True)))
    part_fmt = str(((src.get("partition") or {}) or {}).get("format") or "").strip()
    partition = None
    if part_fmt:
        stamp = at or datetime.now(timezone.utc)
        partition = stamp.strftime(part_fmt).strip("/")
        if partition:
            dest = f"{dest}/{partition}"
    return {"path": dest, "format": fmt, "partition": partition, "multiline": multiline}


def typed_bronze_schema(bronze_contract: Dict[str, Any], silver_contract: Optional[Dict[str, Any]]) -> str:
    """The bronze contract as YAML, with silver's types and constraints (`typed_bronze_doc`).

    - Bronze is all-string, so generating from it gives values like `"DIS-9445"` for a
      number, and silver quarantines every row.
    - YAML (not a type map) keeps pii, masking and other settings.
    - No silver consumer: the bronze contract as is.
    """
    import yaml as _yaml

    return _yaml.safe_dump(typed_bronze_doc(bronze_contract, silver_contract), sort_keys=False)


#: Field keys copied from silver, so rows pass silver's checks.
_SILVER_FIELD_KEYS = (
    "type", "required", "accepted_values", "min", "max", "min_length", "max_length",
    "regex_match", "pattern",
)  # not `primary_key`: the key is a top-level list (copied below), never a field flag
#: Structured row rules the generator honours; each names its `field`.
_STRUCTURED_ROW_RULES = ("accepted_values", "range", "regex_match", "min_length", "max_length")
#: SQL words that are not column names.
_SQL_WORDS = frozenset({
    "and", "or", "not", "is", "null", "in", "like", "ilike", "between", "true", "false",
})


def _sql_rule_columns(sql: str) -> set:
    """Lower-cased identifiers a SQL row rule reads (string literals ignored).

    Function names and keywords like CASE/CAST also count, so such a rule is never carried.
    """
    import re

    without_literals = re.sub(r"'(?:[^']|'')*'", "''", str(sql or ""))
    names = {m.lower() for m in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", without_literals)}
    return names - _SQL_WORDS


_UNIQUE_COLUMN_KEYS = ("field", "fields", "column", "columns")


def _unique_names(value: Any) -> List[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [v for v in value if isinstance(v, str)]
    if isinstance(value, dict):
        out: List[str] = []
        for key in _UNIQUE_COLUMN_KEYS:
            if key in value:
                out.extend(_unique_names(value.get(key)))
        return out
    return []


def _unique_rule_columns(rule: Any) -> List[str]:
    """Column(s) a ``unique`` dataset rule covers (``unique: col | [a, b] | {field|columns: ...}``).

    Mirrors ``api.services.check_coverage.unique_rule_columns`` — inlined because this runner is
    exported verbatim into the customer's notebooks, where the SaaS ``api`` package does not exist."""
    if not isinstance(rule, dict) or not rule.get("unique"):
        return []
    return _unique_names(rule["unique"])


def typed_bronze_doc(
    bronze_contract: Dict[str, Any], silver_contract: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """The bronze contract with silver's field constraints and quality rules copied in.

    - Per shared column: silver's type, `required` and value limits;
      `foreign_key` only if bronze has none.
    - Row rules (structured or SQL) and `unique` dataset rules, only if every column
      they read exists in bronze (a rule on a derived column can't hold at landing).
    - Silver's table `primary_key` if bronze has none.
    """
    import copy

    doc = copy.deepcopy(bronze_contract or {})
    doc.setdefault("version", "1.0.0")
    if not silver_contract:
        return doc
    silver_fields = {
        str(f.get("name")): f
        for f in ((silver_contract.get("model") or {}).get("fields") or [])
        if isinstance(f, dict) and f.get("name")
    }
    columns = set()
    for field in ((doc.get("model") or {}).get("fields") or []):
        if not isinstance(field, dict):
            continue
        columns.add(str(field.get("name")))
        declared = silver_fields.get(str(field.get("name")))
        if not declared:
            continue
        for key in _SILVER_FIELD_KEYS:
            if key in declared and declared[key] is not None:
                field[key] = copy.deepcopy(declared[key])
        if not isinstance(field.get("foreign_key"), dict) and isinstance(declared.get("foreign_key"), dict):
            field["foreign_key"] = copy.deepcopy(declared["foreign_key"])

    silver_quality = silver_contract.get("quality") or {}
    quality = doc.setdefault("quality", {}) if silver_quality else doc.get("quality")
    for rule in silver_quality.get("row_rules") or []:
        if not isinstance(rule, dict):
            continue
        names = [k for k in _STRUCTURED_ROW_RULES if isinstance(rule.get(k), dict)]
        sql = rule.get("sql") if isinstance(rule.get("sql"), str) else None
        read = _sql_rule_columns(sql) if sql else set()
        if names and all(str(rule[k].get("field")) in columns for k in names):
            quality.setdefault("row_rules", []).append(copy.deepcopy(rule))
        elif sql and read and read <= {c.lower() for c in columns}:
            quality.setdefault("row_rules", []).append(copy.deepcopy(rule))
    for rule in silver_quality.get("dataset_rules") or []:
        wanted = _unique_rule_columns(rule)
        if wanted and all(str(c) in columns for c in wanted):
            quality.setdefault("dataset_rules", []).append(copy.deepcopy(rule))

    pk = silver_contract.get("primary_key")
    pk = [pk] if isinstance(pk, str) else list(pk or [])
    if pk and all(str(c) in columns for c in pk) and not doc.get("primary_key"):
        doc["primary_key"] = pk
    return doc


def silver_consumers(registry) -> Dict[str, Dict[str, Any]]:
    """`{bronze entity: silver contract that reads it}`, matched on silver's `source.path` (`table:...`)."""
    out: Dict[str, Dict[str, Any]] = {}
    by_table: Dict[str, str] = {}
    # Fill `{system}`/`{domain}` too, or paths never match.
    names = dict(system=str(getattr(registry, "system", "") or ""),
                 domain=str(getattr(registry, "domain", "") or ""))
    for c in getattr(registry, "contracts", []) or []:
        if getattr(c, "layer", None) != "bronze":
            continue
        doc = getattr(c, "contract_dict", None) or {}
        table = str(((doc.get("info") or {}).get("table_name")) or c.entity or "")
        by_table.setdefault(_resolve_name(table, **names), c.entity)
        by_table.setdefault(_resolve_name(c.entity, **names), c.entity)

    for c in getattr(registry, "contracts", []) or []:
        if getattr(c, "layer", None) != "silver":
            continue
        doc = getattr(c, "contract_dict", None) or {}
        path = str(((doc.get("source") or {}) or {}).get("path") or "")
        if not path.startswith("table:"):
            continue
        table = path[len("table:"):].split(".")[-1].strip()
        entity = by_table.get(_resolve_name(table, **names))
        if entity:
            out[entity] = doc
    return out


def _landing_prefix(out_root: str, landing_root: str) -> Optional[str]:
    """The prefix `out_root` adds before `landing_root` (e.g. `/lakehouse/default/`), or None."""
    out = str(out_root or "").replace("\\", "/").rstrip("/")
    root = str(landing_root or "").replace("\\", "/").strip("/")
    if not root:
        return None
    if out == root:
        return ""
    if out.endswith("/" + root):
        return out[: -len(root)]
    return None


def foreign_key_pools(
    registry_path: str,
    out_root: str,
    missing: List[Dict[str, str]],
    *,
    landing_root: str,
    environment: str = "dev",
    storage_mode: str = "catalog",
) -> Dict[Tuple[str, str], List[str]]:
    """`{(child, child_column): [parent values]}` for FKs whose parent is in another system.

    - Other systems: sibling `<mesh>/<domain>/<system>/_system.yaml` registries.
    - Values are read from the parent's landing zone, per its own contract.
    - A parent that has not landed yet is reported and skipped; seed parent systems first.
    """
    if not missing:
        return {}
    prefix = _landing_prefix(out_root, landing_root)
    if prefix is None:
        print(f"[synthetic_seed] {out_root} does not end with landing_root {landing_root!r}; "
              "cannot locate other systems' landing zones - cross-system keys stay generated")
        return {}
    import glob

    from lakelogic.core.registry import DomainRegistry as _Reg

    here = os.path.normcase(os.path.abspath(registry_path))
    mesh_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(registry_path))))
    owners: Dict[str, Tuple[Dict[str, Any], str, str]] = {}
    for path in sorted(glob.glob(os.path.join(mesh_root, "*", "*", "_system.yaml"))):
        if os.path.normcase(os.path.abspath(path)) == here:
            continue
        try:
            reg = _Reg.from_yaml(path, environment=environment, storage_mode=storage_mode)
        except Exception as exc:  # noqa: BLE001 - skip an unreadable registry
            print(f"[synthetic_seed] skipped registry {path}: {exc}")
            continue
        names = dict(system=str(getattr(reg, "system", "") or ""), domain=str(getattr(reg, "domain", "") or ""))
        root = str(getattr(getattr(reg, "storage", None), "landing_root", "") or "")
        consumers = silver_consumers(reg)
        for c in getattr(reg, "contracts", []) or []:
            if not reads_landing(getattr(c, "layer", None), getattr(c, "contract_dict", None)):
                continue
            doc = getattr(c, "contract_dict", None) or {}
            keys = [c.entity, doc.get("dataset"), (doc.get("info") or {}).get("table_name")]
            silver = consumers.get(c.entity)
            if silver:  # FKs declared on silver name the silver table
                keys += [silver.get("dataset"), (silver.get("info") or {}).get("table_name")]
            for key in keys:
                resolved = _resolve_name(key, **names)
                if resolved and "{" not in resolved:
                    owners.setdefault(resolved, (doc, root, c.entity))

    pools: Dict[Tuple[str, str], List[str]] = {}
    for miss in missing:
        hit = owners.get(miss["parent_contract"])
        if hit is None:
            print(f"[synthetic_seed] {miss['parent_contract']} is in no system of this mesh")
            continue
        doc, root, entity = hit
        layout = landing_layout(doc, landing_root=root, out_root=prefix + root.strip("/"), entity=entity, at=None)
        path, partition = layout["path"], layout.get("partition")
        base = path[: -(len(partition) + 1)] if partition and path.endswith("/" + partition) else path
        values = _existing_values(base, (layout.get("format") or "delta").lower(),
                                  miss["parent_column"], layout.get("multiline", True))
        if values:
            pools[(miss["child"], miss["child_column"])] = values
            print(f"[synthetic_seed] {miss['child']}.{miss['child_column']} -> {len(values)} landed "
                  f"{entity}.{miss['parent_column']} value(s)")
        else:
            print(f"[synthetic_seed] {entity} has not landed yet; {miss['child']}.{miss['child_column']} "
                  "stays generated - seed that system first")
    return pools


def _apply_foreign_pools(frames: Dict[str, Any], pools: Dict[Tuple[str, str], List[str]], seed: int) -> Dict[str, Any]:
    """Set each cross-system FK to a random landed parent value."""
    if not pools:
        return frames
    import polars as pl

    rng = random.Random(seed)
    for (child, column), pool in sorted(pools.items()):
        df = frames.get(child)
        if df is None or column not in df.columns or not pool:
            continue
        values = [pool[rng.randrange(len(pool))] for _ in range(df.height)]
        frames[child] = df.with_columns(pl.Series(column, values).cast(df.schema[column], strict=False))
    return frames


def seed_relationships(
    registry, *, unresolved: Optional[List[Dict[str, str]]] = None, quiet: bool = False,
) -> List[Dict[str, str]]:
    """FK edges for a system's synthetic data. Used by both seed and extend.

    - Read from contracts with silver's constraints applied, so silver-only FKs count.
    - A silver table name is an alias for the bronze entity that feeds it.
    - Parents in another system go to `unresolved` and are printed unless `quiet`.
    """
    system = str(getattr(registry, "system", "") or "")
    domain = str(getattr(registry, "domain", "") or "")
    consumers = silver_consumers(registry)
    docs: Dict[str, Dict[str, Any]] = {}
    aliases: Dict[str, str] = {}
    for c in getattr(registry, "contracts", []) or []:
        if not reads_landing(getattr(c, "layer", None), getattr(c, "contract_dict", None)):
            continue
        silver = consumers.get(c.entity)
        docs[c.entity] = typed_bronze_doc(getattr(c, "contract_dict", None) or {}, silver)
        if silver:
            for key in (silver.get("dataset"), (silver.get("info") or {}).get("table_name")):
                if key:
                    aliases[str(key)] = c.entity
    missing: List[Dict[str, str]] = [] if unresolved is None else unresolved
    rels = contract_relationships(docs, aliases=aliases, system=system, domain=domain, unresolved=missing)
    for miss in [] if quiet else missing:
        print(f"[synthetic_seed] {miss['child']}.{miss['child_column']} -> {miss['parent_contract']} "
              "is in another system; values come from its landing zone")
    return rels


def _resolve_layers(name: str) -> str:
    """Replace `{bronze_layer}`-style placeholders with the layer name."""
    out = str(name or "")
    for token, word in (("bronze_layer", "bronze"), ("silver_layer", "silver"), ("gold_layer", "gold")):
        out = out.replace("{" + token + "}", word)
    return out


def dedup_keys_declared(contract_dict: Dict[str, Any]) -> List[str]:
    """Key columns of the contract's `deduplicate` transformation (`["*"]` if none named), or `[]`."""
    for xf in (contract_dict or {}).get("transformations") or ():
        dedup = xf.get("deduplicate") if isinstance(xf, dict) else None
        if isinstance(dedup, dict):
            keys = dedup.get("keys") or dedup.get("subset") or dedup.get("columns")
            if isinstance(keys, str):
                return [keys]
            if isinstance(keys, (list, tuple)):
                return [str(k) for k in keys]
            return ["*"]
    return []


def inject_duplicates(df, ratio: float, seed: int):
    """Re-append `ratio` of the rows (sampled by `seed`) so dedup has work to do."""
    if ratio <= 0 or df.height == 0:
        return df
    n = max(1, int(round(df.height * ratio)))
    return df.vstack(df.sample(n=min(n, df.height), seed=seed, with_replacement=False))


def _write_entity(
    df, dest: str, entity: str, entity_fmt: str, layout: Dict[str, Any],
    *, batches: int, seed: int, write_mode: str,
) -> List[str]:
    """Write one entity to `dest` in its format; return the paths written.

    - Shared by seed and extend, so both lay out files the same way.
    - File names come from `seed`; extend passes a per-window seed, so a replayed
      window rewrites its own files instead of adding duplicates.
    """
    if entity_fmt == "delta":
        # Delta is one table: no batches.
        df.write_delta(dest, mode=write_mode)
        written = [dest]
    else:
        # Batch files, like a real landing zone.
        os.makedirs(dest, exist_ok=True)
        if df.height == 0:
            # Header-only file: the reader needs the folder; `external_logic` makes the rows.
            target = f"{dest}/{batch_filename(entity, _EXT[entity_fmt], seed=seed, index=0)}"
            if entity_fmt == "json":
                with open(target, "w", encoding="utf-8") as fh:
                    fh.write("[]" if layout.get("multiline", True) else "")
            elif entity_fmt == "csv":
                _single_line_text(df).write_csv(target)
            else:
                df.write_parquet(target)
            return [target]
        n = max(1, int(batches))
        size = max(1, -(-df.height // n))  # ceil: every row in one batch
        written = []
        for i in range(n):
            part = df.slice(i * size, size)
            if part.height == 0:
                break
            target = f"{dest}/{batch_filename(entity, _EXT[entity_fmt], seed=seed, index=i)}"
            if entity_fmt == "json":
                # `multiLine` (default): one JSON array per file.
                if layout.get("multiline", True):
                    with open(target, "w", encoding="utf-8") as fh:
                        fh.write(part.write_json())
                else:
                    part.write_ndjson(target)
            elif entity_fmt == "csv":
                _single_line_text(part).write_csv(target)
            else:
                part.write_parquet(target)
            written.append(target)
    return written


# ── Stream extension ─────────────────────────────────────────────────────────────────────
#
# Each run appends time windows from the latest landed partition up to now.
# - Parents only gain new rows.
# - Children point at parents that have landed.

_PARTITION_TOKENS = {
    "%Y": r"(?P<Y>\d{4})",
    "%m": r"(?P<m>\d{2})",
    "%d": r"(?P<d>\d{2})",
    "%H": r"(?P<H>\d{2})",
    "%M": r"(?P<M>\d{2})",
}


def _partition_regex(part_fmt: str):
    """A strftime partition format (e.g. `y_%Y/m_%m/d_%d`) as a regex."""
    out, i = [], 0
    while i < len(part_fmt):
        token = part_fmt[i:i + 2]
        if token in _PARTITION_TOKENS:
            out.append(_PARTITION_TOKENS[token])
            i += 2
        else:
            out.append(re.escape(part_fmt[i]))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def window_minutes_of(part_fmt: str) -> Optional[int]:
    """Window size in minutes from the partition format's finest unit, or None."""
    if not part_fmt:
        return None
    if "%M" in part_fmt:
        return 1
    if "%H" in part_fmt:
        return 60
    if "%d" in part_fmt:
        return 1440
    return None


def latest_partition(base: str, part_fmt: str) -> Optional[datetime]:
    """Time of the newest non-empty partition under `base`, or None."""
    part_fmt = (part_fmt or "").strip("/")
    if not part_fmt or not os.path.isdir(base):
        return None
    regex = _partition_regex(part_fmt)
    depth = part_fmt.count("/") + 1
    latest = None
    for dirpath, _dirs, files in os.walk(base):
        rel = os.path.relpath(dirpath, base).replace("\\", "/")
        if rel == "." or rel.count("/") + 1 != depth:
            continue
        if not any(not f.startswith((".", "_")) for f in files):
            continue  # empty partition: not landed
        match = regex.match(rel)
        if not match:
            continue
        g = match.groupdict()
        ts = datetime(
            int(g.get("Y") or 1970), int(g.get("m") or 1), int(g.get("d") or 1),
            int(g.get("H") or 0), int(g.get("M") or 0), tzinfo=timezone.utc,
        )
        if latest is None or ts > latest:
            latest = ts
    return latest


def _single_line_text(df):
    """Replace line breaks in text columns with ", " before writing CSV.

    Faker addresses contain a newline (street, then city). A CSV reader without `multiLine`
    splits that record in two, shifting every later column (dates in city_code).
    """
    import polars as pl

    text = [c for c, t in df.schema.items() if t == pl.Utf8]
    if not text:
        return df
    return df.with_columns(
        pl.col(c).str.replace_all(r"\r\n|\r|\n", ", ") for c in text
    )


def _existing_values(base: str, fmt: str, column: str, multiline: bool) -> List[str]:
    """Sorted values of `column` already landed under `base`."""
    import polars as pl

    if not os.path.isdir(base):
        return []
    values = set()
    if os.path.isdir(os.path.join(base, "_delta_log")):
        try:
            values.update(str(v) for v in pl.read_delta(base, columns=[column])[column].drop_nulls().to_list())
        except Exception:  # noqa: BLE001 - unreadable: no keys
            pass
        return sorted(values)
    for dirpath, _dirs, files in os.walk(base):
        for name in files:
            if name.startswith((".", "_")):
                continue
            path = os.path.join(dirpath, name)
            try:
                if name.endswith(".csv"):
                    frame = pl.read_csv(path, columns=[column], infer_schema_length=0)
                elif name.endswith(".parquet"):
                    frame = pl.read_parquet(path, columns=[column])
                elif name.endswith(".json"):
                    frame = pl.read_json(path) if multiline else pl.read_ndjson(path)
                else:
                    continue
                if column in frame.columns:
                    values.update(str(v) for v in frame[column].drop_nulls().to_list())
            except Exception:  # noqa: BLE001
                continue
    return sorted(values)


def _floor(ts: datetime, minutes: int) -> datetime:
    step = minutes * 60
    return datetime.fromtimestamp((int(ts.timestamp()) // step) * step, tz=timezone.utc)


def _window_seed(seed: int, window_start: datetime) -> int:
    return int(hashlib.sha256(f"{seed}:{window_start.isoformat()}".encode()).hexdigest()[:8], 16)


def extend_bronze_estate(
    registry_path: str,
    out_root: str,
    *,
    environment: str = "dev",
    storage_mode: str = "catalog",
    rows: int = 200,
    rows_by_entity: Optional[Dict[str, int]] = None,
    entities: Optional[List[str]] = None,
    seed: int = 7,
    invalid_ratio: float = 0.0,
    duplicate_ratio: float = 0.0,
    batches: int = 1,
    #: Windows of history for an empty landing zone.
    windows_on_empty: int = 24,
    #: History in minutes (1440 = 24 hours). Overrides `windows_on_empty`; also caps a stale zone.
    history_minutes: Optional[int] = None,
    #: Max windows written per run.
    max_windows: int = 48,
    #: Share of `rows` added to parent tables per window, once they have rows.
    new_parent_ratio: float = 0.1,
    fmt: str = "delta",
    generative: bool = False,
    ai_provider: Optional[str] = None,
    ai_model: Optional[str] = None,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Add the missing time windows to a synthetic landing zone, up to now.

    - Only complete windows are written; a re-run in the same window adds nothing.
    - Unpartitioned full-load contracts are written once.
    - Returns one report row per entity per window (same shape as `seed_bronze_estate`).
    """
    from lakelogic.core.registry import DomainRegistry as _Reg
    import polars as pl

    registry = _Reg.from_yaml(registry_path, environment=environment, storage_mode=storage_mode)
    landing_root = str(getattr(getattr(registry, "storage", None), "landing_root", "") or "")
    bronze = [c for c in registry.contracts if reads_landing(c.layer, c.contract_dict)]
    if not bronze:
        raise RuntimeError(f"No bronze contracts in {registry_path} - nothing to extend.")

    # Each entity's landing path (without partition) and partition format.
    info: Dict[str, Dict[str, Any]] = {}
    for c in bronze:
        doc = c.contract_dict or {}
        layout = landing_layout(doc, landing_root=landing_root, out_root=out_root, entity=c.entity, at=None)
        path, partition = layout["path"], layout.get("partition")
        base = path[: -(len(partition) + 1)] if partition and path.endswith("/" + partition) else path
        part_fmt = str(((doc.get("source") or {}).get("partition") or {}).get("format") or "").strip()
        info[c.entity] = {
            "doc": doc, "base": base, "part_fmt": part_fmt,
            "fmt": (layout.get("format") or fmt or "delta").strip().lower(),
            "multiline": layout.get("multiline", True),
            "dedup": dedup_keys_declared(doc),
            # Unpartitioned full load: write once, or keys duplicate.
            "static": not part_fmt and str((doc.get("source") or {}).get("load_mode") or "").lower() == "full",
        }
    landed_static = {
        e for e, v in info.items()
        if v["static"] and os.path.isdir(v["base"])
        and any(files for _d, _s, files in os.walk(v["base"]))
    }

    # Window size: the finest partition any entity declares.
    granularities = [m for m in (window_minutes_of(v["part_fmt"]) for v in info.values()) if m]
    window = min(granularities) if granularities else 60
    delta = timedelta(minutes=window)
    now = now or datetime.now(timezone.utc)
    limit = _floor(now, window)  # complete windows only

    finest = [e for e, v in info.items() if window_minutes_of(v["part_fmt"]) == window]
    landed = [t for t in (latest_partition(info[e]["base"], info[e]["part_fmt"]) for e in finest) if t]
    history_windows = max(1, int(windows_on_empty))
    if history_minutes:
        history_windows = max(1, -(-int(history_minutes) // window))  # ceil
    history_start = limit - delta * history_windows
    if landed:
        cursor = max(landed) + delta
        if cursor < history_start:
            # Stale zone: restart at the history start instead of replaying the gap.
            print(f"[synthetic_seed] latest landed window {max(landed).isoformat()} is older than the "
                  f"{history_windows}-window history; resuming at {history_start.isoformat()}")
            cursor = history_start
        else:
            print(f"[synthetic_seed] extending from {cursor.isoformat()} (latest window already landed)")
    else:
        cursor = history_start
        print(f"[synthetic_seed] empty landing zone: writing {history_windows} window(s) of history")

    missing: List[Dict[str, str]] = []
    relationships = seed_relationships(registry, unresolved=missing)
    # Parent values from other systems, read once.
    foreign = foreign_key_pools(registry_path, out_root, missing, landing_root=landing_root,
                                environment=environment, storage_mode=storage_mode)
    parent_keys = sorted({(r["parent"], r["parent_column"]) for r in relationships})
    pools: Dict[Tuple[str, str], List[str]] = {
        (p, col): _existing_values(info[p]["base"], info[p]["fmt"], col, info[p]["multiline"])
        for p, col in parent_keys if p in info
    }
    wanted = {str(e).strip().lower() for e in (entities or []) if str(e).strip()}

    report: List[Dict[str, Any]] = []
    written_windows = 0
    while cursor + delta <= limit and written_windows < max(1, int(max_windows)):
        w_seed = _window_seed(seed, cursor)
        rng = random.Random(w_seed)
        frames = generate_bronze_frames(
            registry_path, environment=environment, storage_mode=storage_mode,
            rows=rows, rows_by_entity=rows_by_entity, seed=w_seed, invalid_ratio=invalid_ratio,
            window_start=cursor.replace(tzinfo=None), window_end=(cursor + delta).replace(tzinfo=None),
            generative=generative, ai_provider=ai_provider, ai_model=ai_model,
        )

        # Parents: drop keys already landed, keep a small share.
        for parent, col in parent_keys:
            df = frames.get(parent)
            if df is None or col not in df.columns:
                continue
            existing = set(pools.get((parent, col), []))
            if existing:
                df = df.filter(~pl.col(col).cast(pl.Utf8).is_in(list(existing)))
                keep = max(1, round((rows_by_entity or {}).get(parent, rows) * float(new_parent_ratio)))
                df = df.head(keep)
            frames[parent] = df

        # Children: point FKs at parents landed before or in this window.
        for rel in relationships:
            child, parent, col = rel["child"], rel["parent"], rel["child_column"]
            df = frames.get(child)
            if df is None or col not in df.columns:
                continue
            landing_now = []
            if not wanted or parent.lower() in wanted:
                pframe = frames.get(parent)
                if pframe is not None and rel["parent_column"] in pframe.columns:
                    landing_now = [str(v) for v in pframe[rel["parent_column"]].drop_nulls().to_list()]
            pool = pools.get((parent, rel["parent_column"]), []) + landing_now
            if pool:
                values = [pool[rng.randrange(len(pool))] for _ in range(df.height)]
                frames[child] = df.with_columns(pl.Series(col, values).cast(df.schema[col], strict=False))
        frames = _apply_foreign_pools(frames, foreign, w_seed)

        for entity, df in frames.items():
            if wanted and entity.lower() not in wanted:
                continue
            meta = info.get(entity)
            if meta is None:
                continue
            if meta["static"]:
                if entity in landed_static:
                    continue
                landed_static.add(entity)
            if duplicate_ratio > 0 and meta["dedup"]:
                df = inject_duplicates(df, duplicate_ratio, w_seed)
            layout = landing_layout(
                meta["doc"], landing_root=landing_root, out_root=out_root, entity=entity, at=cursor,
            )
            dest = layout.get("path") or f"{out_root.rstrip('/')}/{entity}"
            entity_fmt = meta["fmt"] if meta["fmt"] in ("delta", "parquet", "csv", "json") else "csv"
            written = _write_entity(
                df, dest, entity, entity_fmt, layout,
                batches=batches, seed=w_seed, write_mode="append",
            )
            report.append({
                "entity": entity, "rows": df.height, "path": dest, "format": entity_fmt,
                "partition": layout.get("partition"), "declared": True,
                "batches": len(written), "window_start": cursor.isoformat(),
            })
            for p, col in parent_keys:
                if p == entity and col in df.columns:
                    pools.setdefault((p, col), []).extend(str(v) for v in df[col].drop_nulls().to_list())

        cursor += delta
        written_windows += 1

    if written_windows == 0:
        print("[synthetic_seed] already caught up - no complete window to add.")
    elif cursor + delta <= limit:
        print(f"[synthetic_seed] wrote {written_windows} window(s); more remain - run again to catch up.")
    else:
        print(f"[synthetic_seed] wrote {written_windows} window(s); caught up to {limit.isoformat()}.")
    return report


def seed_bronze_estate(
    registry_path: str,
    out_root: str,
    *,
    environment: str = "dev",
    storage_mode: str = "catalog",
    rows: int = 200,
    seed: int = 7,
    invalid_ratio: float = 0.0,
    #: Share of rows duplicated, only for contracts with `deduplicate`.
    duplicate_ratio: float = 0.0,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
    fmt: str = "delta",
    write_mode: str = "overwrite",
    #: `batch_NN_*` files per partition.
    batches: int = 1,
    generative: bool = False,
    ai_provider: Optional[str] = None,
    ai_model: Optional[str] = None,
    #: Write only these entities. Parents are still generated for their ids, not written.
    entities: Optional[List[str]] = None,
    rows_by_entity: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """Generate synthetic data and write each entity to its landing zone under `out_root`.

    - Path, format and partition come from each contract (`landing_layout`); `fmt` is used
      only when a contract declares no format.
    - Also writes synthetic documents for extraction contracts, and applies cross-system FKs.
    - Returns one report row per entity: entity, rows, path, format, partition, batches.
    """
    fmt = (fmt or "delta").strip().lower()
    if fmt not in ("delta", "parquet", "csv"):
        raise ValueError(f"fmt must be 'delta', 'parquet' or 'csv', got {fmt!r}")

    frames = generate_bronze_frames(
        registry_path, environment=environment, storage_mode=storage_mode, rows=rows, seed=seed,
        rows_by_entity=rows_by_entity,
        invalid_ratio=invalid_ratio, window_start=window_start, window_end=window_end,
        generative=generative, ai_provider=ai_provider, ai_model=ai_model,
    )
    # Landing layout per contract.
    layouts: Dict[str, Dict[str, Any]] = {}
    dedup_targets: Dict[str, List[str]] = {}
    try:
        from lakelogic.core.registry import DomainRegistry as _Reg

        _r = _Reg.from_yaml(registry_path, environment=environment, storage_mode=storage_mode)
        _root = str(getattr(getattr(_r, "storage", None), "landing_root", "") or "")
        for _c in getattr(_r, "contracts", []) or []:
            if not reads_landing(getattr(_c, "layer", None), getattr(_c, "contract_dict", None)):
                continue
            _doc = getattr(_c, "contract_dict", None) or {}
            layouts[_c.entity] = landing_layout(
                _doc, landing_root=_root, out_root=out_root, entity=_c.entity, at=window_end,
            )
            # Dedup keys, if declared.
            dedup_targets[_c.entity] = dedup_keys_declared(_doc)
    except Exception as exc:  # noqa: BLE001 - fall back, keep seeding
        print(f"[synthetic_seed] could not read declared landing layout ({exc}); "
              "falling back to <out_root>/<entity>")

    try:
        _missing: List[Dict[str, str]] = []
        seed_relationships(_r, unresolved=_missing)
        frames = _apply_foreign_pools(frames, foreign_key_pools(
            registry_path, out_root, _missing, landing_root=_root,
            environment=environment, storage_mode=storage_mode,
        ), seed)
    except Exception as exc:  # noqa: BLE001 - best effort
        print(f"[synthetic_seed] cross-system keys not applied ({exc})")

    wanted = {str(e).strip().lower() for e in (entities or []) if str(e).strip()}
    unknown = sorted(wanted - {e.lower() for e in frames})
    if unknown:
        print(f"[synthetic_seed] entities not in this system, ignored: {unknown}")
    report: List[Dict[str, Any]] = []
    # Documents for extraction contracts (LLM extraction is skipped).
    report.extend(_write_documents(
        registry_path, out_root, environment=environment, storage_mode=storage_mode,
        rows=rows, rows_by_entity=rows_by_entity, seed=seed, invalid_ratio=invalid_ratio, wanted=wanted,
    ))
    for entity, df in frames.items():
        if wanted and entity.lower() not in wanted:
            continue
        if duplicate_ratio > 0 and dedup_targets.get(entity):
            before = df.height
            df = inject_duplicates(df, duplicate_ratio, seed)
            if df.height != before:
                print(f"[synthetic_seed] {entity}: +{df.height - before} duplicate row(s) "
                      f"for the declared dedup keys {dedup_targets[entity]}")
        layout = layouts.get(entity) or {}
        dest = layout.get("path") or f"{out_root.rstrip('/')}/{entity}"
        # Per-contract format.
        entity_fmt = (layout.get("format") or fmt or "delta").strip().lower()
        if entity_fmt not in ("delta", "parquet", "csv", "json"):
            print(f"[synthetic_seed] {entity}: unsupported declared format {entity_fmt!r}; using csv")
            entity_fmt = "csv"

        written = _write_entity(
            df, dest, entity, entity_fmt, layout,
            batches=batches, seed=seed, write_mode=write_mode,
        )

        report.append({
            "entity": entity, "rows": df.height, "path": dest, "format": entity_fmt,
            # True if the layout came from the contract.
            "partition": layout.get("partition"),
            "declared": bool(layout),
            "batches": len(written),
        })
    return report


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Args come from flags or `SYNTH_*` env vars."""
    p = argparse.ArgumentParser(description="Write a synthetic bronze estate to the lake.")
    p.add_argument("--registry", default=os.getenv("SYNTH_REGISTRY_PATH"),
                   help="Path to the system's _system.yaml registry.")
    p.add_argument("--out-root", default=os.getenv("SYNTH_OUT_ROOT"),
                   help="Lake root the synthetic bronze tables are written under.")
    p.add_argument("--rows", type=int, default=int(os.getenv("SYNTH_ROWS", "200")))
    p.add_argument("--seed", type=int, default=int(os.getenv("SYNTH_SEED", "7")))
    p.add_argument("--invalid-ratio", type=float, default=float(os.getenv("SYNTH_INVALID_RATIO", "0.0")))
    p.add_argument("--environment", default=os.getenv("SYNTH_ENVIRONMENT", "dev"))
    p.add_argument("--fmt", default=os.getenv("SYNTH_FORMAT", "delta"), choices=["delta", "parquet", "csv"])
    p.add_argument("--generative", action="store_true",
                   default=os.getenv("SYNTH_GENERATIVE", "").strip().lower() in ("1", "true", "yes"),
                   help="Enable AI-enriched realistic values (needs an LLM key in this env).")
    p.add_argument("--ai-provider", default=os.getenv("SYNTH_AI_PROVIDER") or os.getenv("LAKELOGIC_AI_PROVIDER"))
    p.add_argument("--ai-model", default=os.getenv("SYNTH_AI_MODEL") or os.getenv("LAKELOGIC_AI_MODEL"))
    args = p.parse_args(argv)

    if not args.registry or not args.out_root:
        p.error("--registry and --out-root (or SYNTH_REGISTRY_PATH / SYNTH_OUT_ROOT) are required")

    report = seed_bronze_estate(
        args.registry, args.out_root,
        environment=args.environment, rows=args.rows, seed=args.seed,
        invalid_ratio=args.invalid_ratio, fmt=args.fmt,
        generative=args.generative, ai_provider=args.ai_provider, ai_model=args.ai_model,
    )
    total = sum(r["rows"] for r in report)
    for r in report:
        print(f"  {r['entity']}: {r['rows']} rows -> {r['path']} ({r['format']})")
    print(f"Synthetic bronze estate: {len(report)} table(s), {total} rows.")
    return 0
# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parameters
# MAGIC
# MAGIC Widgets arrive as strings; a job run overrides them per run. One widget per line.

# COMMAND ----------

# DBTITLE 1,Parameters
# Parameters — the pipeline overrides these per (domain, system). Values arrive as strings.
# Declare the parameters (the job passes values for these).
dbutils.widgets.text("catalog", "", "catalog")     # the Unity Catalog this run reads and writes
dbutils.widgets.text("contracts_root", "", "contracts_root")
dbutils.widgets.text("domain", "lakehouse", "domain")
dbutils.widgets.text("system", "lakehouse", "system")
dbutils.widgets.text("environment", "dev", "environment")                    # a key of _system.yaml environments
dbutils.widgets.text("rows_per_table", "100", "rows_per_table")               # synthetic rows per bronze entity, per window
dbutils.widgets.text("entities", "", "entities")                             # comma-separated bronze entities to write; blank => all
dbutils.widgets.text("rows_per_entity", "", "rows_per_entity")               # JSON, e.g. {"charges": 5000}; overrides rows_per_table
dbutils.widgets.text("mode", "extend", "mode")                               # seed = one window | extend = add the windows the landing zone is missing, up to now
dbutils.widgets.text("windows_on_empty", "48", "windows_on_empty")           # extend: history an EMPTY landing zone gets on its first run (2 days, hourly)
dbutils.widgets.text("history_minutes", "2880", "history_minutes")           # extend: history as a duration (2 days); overrides windows_on_empty
dbutils.widgets.text("max_windows", "96", "max_windows")                     # extend: most windows one run writes; MUST exceed the history above
dbutils.widgets.text("new_parent_ratio", "0.1", "new_parent_ratio")          # extend: parent tables grow by this share of rows per window
dbutils.widgets.text("loop_interval_minutes", "0", "loop_interval_minutes")  # extend: 0 = catch up and stop (for a schedule); N = repeat every N minutes
dbutils.widgets.text("max_run_minutes", "60", "max_run_minutes")             # extend: stop looping after this long
dbutils.widgets.text("batches", "10", "batches")                             # batch files per partition (a landing zone holds several)
dbutils.widgets.text("seed", "7", "seed")                                    # deterministic — same seed => same estate
dbutils.widgets.text("invalid_ratio", "0.1", "invalid_ratio")                # inject bad rows (0..1) to exercise quality gates
dbutils.widgets.text("duplicate_ratio", "0.05", "duplicate_ratio")           # re-append this fraction as duplicates, for contracts that declare dedup
dbutils.widgets.text("synthetic_schema", "", "synthetic_schema")             # target lakehouse schema; blank => synthetic_<domain>
dbutils.widgets.text("generative", "false", "generative")                    # true => AI-enriched realistic values (needs an LLM key in THIS runtime env)
dbutils.widgets.text("landing_runtime", "", "landing_runtime")  # where landing files come from (Deliver data-source step); blank = the registry's own path

# Read them.
catalog = dbutils.widgets.get("catalog")
contracts_root = dbutils.widgets.get("contracts_root")
domain = dbutils.widgets.get("domain")
system = dbutils.widgets.get("system")
environment = dbutils.widgets.get("environment")
rows_per_table = dbutils.widgets.get("rows_per_table")
entities = dbutils.widgets.get("entities")
rows_per_entity = dbutils.widgets.get("rows_per_entity")
mode = dbutils.widgets.get("mode")
windows_on_empty = dbutils.widgets.get("windows_on_empty")
history_minutes = dbutils.widgets.get("history_minutes")
max_windows = dbutils.widgets.get("max_windows")
new_parent_ratio = dbutils.widgets.get("new_parent_ratio")
loop_interval_minutes = dbutils.widgets.get("loop_interval_minutes")
max_run_minutes = dbutils.widgets.get("max_run_minutes")
batches = dbutils.widgets.get("batches")
seed = dbutils.widgets.get("seed")
invalid_ratio = dbutils.widgets.get("invalid_ratio")
duplicate_ratio = dbutils.widgets.get("duplicate_ratio")
synthetic_schema = dbutils.widgets.get("synthetic_schema")
generative = dbutils.widgets.get("generative")
landing_runtime = dbutils.widgets.get("landing_runtime")

                                       # extend is the default so a FIRST load lands real history, not one window

# COMMAND ----------

# DBTITLE 1,Landing source library
"""Where a delivered notebook reads its landing files from — run inside the delivered project.

This module imports only the standard library, and its source is pasted into the generated Databricks and Fabric notebooks in place of
their landing-runtime placeholder, so the pipeline it runs and the function tested here are the
same code. The Deliver data-source step (:mod:`deployment_target_data_source`) renders the
``landing_runtime`` spec it consumes:

* ``env``     ENV_NAME → the value of a per-environment parameter (already interpolated by the
  platform: a bundle variable on Databricks, a pipeline parameter on Fabric);
* ``secrets`` ENV_NAME → [scope, key] NAMES, read here through the platform's secret reader;
* ``auth``    for a direct connection, the service principal, each part a literal or ``$ENV_NAME``.

It fills ``os.environ`` before ``DomainRegistry.from_yaml`` (which resolves the ``ls_*`` keys
from it) and sets account-scoped Azure OAuth for a direct connection. The client secret is held
only in the Spark config: it is never exported to the environment and never printed.
"""
import json as _lr_json
import os as _lr_os


class LandingRuntimeError(RuntimeError):
    """The landing source cannot be set up; the message names the variable to set."""


def apply_landing_runtime(spec, get_secret, set_conf, environ=None):
    """Apply ``spec`` (a dict or its JSON) and return what was set, by NAME only.

    ``get_secret(scope, key)`` reads a secret (``dbutils.secrets.get`` on Databricks,
    ``notebookutils.credentials.getSecret`` on Fabric); ``set_conf(key, value)`` sets a Spark
    config (``spark.conf.set``)."""
    environ = _lr_os.environ if environ is None else environ
    if isinstance(spec, str):
        spec = _lr_json.loads(spec) if spec.strip() else {}
    spec = spec or {}
    auth = spec.get("auth") or None
    # The secret behind the client secret authenticates only: keep it out of the environment.
    private = {auth["client_secret"][1:]} if auth and str(auth.get("client_secret", "")).startswith("$") else set()
    values = {}
    for name, value in (spec.get("env") or {}).items():
        value = "" if value is None else str(value)
        if not value.strip() or value.startswith("${"):
            raise LandingRuntimeError(
                f"Landing parameter {name.lower()} has no value for this environment. "
                f"Set it on the deployment target (bundle variable / pipeline parameter) and re-run."
            )
        values[name] = value.strip()
    for name, ref in (spec.get("secrets") or {}).items():
        scope, key = (list(ref) + ["", ""])[:2]
        if not str(scope).strip() or not str(key).strip() or str(scope).startswith("${"):
            raise LandingRuntimeError(
                f"Secret for {name.lower()} has no scope or key name for this environment. "
                f"Set {name.lower()}_scope and {name.lower()}_key and re-run."
            )
        try:
            values[name] = get_secret(str(scope).strip(), str(key).strip())
        except Exception as exc:  # the platform's message names the scope; never the value
            raise LandingRuntimeError(
                f"Could not read secret {key!r} from {scope!r} for {name.lower()}: {type(exc).__name__}"
            ) from None
    for name, value in values.items():
        if name not in private:
            environ[name] = value

    configured = None
    if auth and auth.get("type") == "azure_service_principal":
        def _v(part):
            raw = str(auth.get(part) or "")
            return values.get(raw[1:], environ.get(raw[1:], "")) if raw.startswith("$") else raw

        account, tenant, client, secret = (_v("storage_account"), _v("tenant_id"),
                                           _v("client_id"), _v("client_secret"))
        missing = [p for p, v in (("storage_account", account), ("tenant_id", tenant),
                                  ("client_id", client), ("client_secret", secret)) if not v]
        if missing:
            raise LandingRuntimeError(f"Direct connection is missing {', '.join(missing)}.")
        host = f"{account}.dfs.core.windows.net"
        set_conf(f"fs.azure.account.auth.type.{host}", "OAuth")
        set_conf(f"fs.azure.account.oauth.provider.type.{host}",
                 "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        set_conf(f"fs.azure.account.oauth2.client.id.{host}", client)
        set_conf(f"fs.azure.account.oauth2.client.secret.{host}", secret)
        set_conf(f"fs.azure.account.oauth2.client.endpoint.{host}",
                 f"https://login.microsoftonline.com/{tenant}/oauth2/token")
        configured = host
    return {"environment": sorted(n for n in values if n not in private),
            "secrets": sorted((spec.get("secrets") or {}).keys()),
            "direct_connection": configured}


_FABRIC_API = "https://api.fabric.microsoft.com/v1"


def ensure_onelake_shortcut(spec, get_token, workspace_id, lakehouse_id, request, environ=None):
    """Create the OneLake shortcut ``Files/<name>`` → ADLS that a Fabric ``shortcut`` data
    source reads through, once. Run by setup after :func:`apply_landing_runtime`.

    An existing shortcut with the same target is left alone; one pointing anywhere else stops
    setup rather than read the wrong storage. ``request(method, url, headers, body)`` returns
    ``(status, json_or_None)``; ``get_token()`` returns a Fabric API bearer token. The
    connection's credentials stay in Fabric: only its ID is sent."""
    environ = _lr_os.environ if environ is None else environ
    if isinstance(spec, str):
        spec = _lr_json.loads(spec) if spec.strip() else {}
    sc = (spec or {}).get("shortcut")
    if not sc:
        return None

    def _v(part):
        raw = str(sc.get(part) or "")
        return environ.get(raw[1:], "") if raw.startswith("$") else raw

    name, account, container = _v("shortcut_name"), _v("storage_account"), _v("container")
    folder, connection = _v("folder").strip("/"), _v("connection_id")
    missing = [p for p, v in (("shortcut_name", name), ("storage_account", account),
                              ("container", container), ("connection_id", connection)) if not v]
    if missing:
        raise LandingRuntimeError(f"OneLake shortcut is missing {', '.join(missing)}.")
    location = f"https://{account}.dfs.core.windows.net"
    subpath = "/" + "/".join(p for p in (container, folder) if p)
    base = f"{_FABRIC_API}/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
    headers = {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

    status, body = request("GET", f"{base}/Files/{name}", headers, None)
    if status == 200:
        target = ((body or {}).get("target") or {}).get("adlsGen2") or {}
        same = (str(target.get("location", "")).rstrip("/").lower() == location.lower()
                and str(target.get("subpath", "")).rstrip("/") == subpath)
        if not same:
            raise LandingRuntimeError(
                f"Files/{name} already exists and points at {target.get('location')}{target.get('subpath')}, "
                f"not {location}{subpath}. Rename the shortcut or remove the old one."
            )
        return {"shortcut": f"Files/{name}", "target": location + subpath, "created": False}
    if status != 404:
        raise LandingRuntimeError(f"Could not read shortcut Files/{name}: HTTP {status}.")
    payload = {"path": "Files", "name": name,
               "target": {"adlsGen2": {"location": location, "subpath": subpath, "connectionId": connection}}}
    status, body = request("POST", base, headers, payload)
    if status not in (200, 201):
        detail = (body or {}).get("message") or (body or {}).get("errorCode") or ""
        raise LandingRuntimeError(f"Creating shortcut Files/{name} failed: HTTP {status} {detail}".rstrip())
    return {"shortcut": f"Files/{name}", "target": location + subpath, "created": True}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write the bronze estate
# MAGIC
# MAGIC Fails the task on any error and leaves the traceback in <span style="background:#7f1d1d;color:#ffffff;padding:1px 6px;border-radius:4px;font-family:monospace;font-size:0.9em">lakelogic_logs/</span>.

# COMMAND ----------

# DBTITLE 1,Write synthetic bronze
import os, traceback
# Convert string parameters to numbers and booleans.
ROWS = max(1, int(str(rows_per_table).strip() or "100"))
import json as _json
ENTITIES = [e.strip() for e in str(entities).split(",") if e.strip()]
_rpe = str(rows_per_entity).strip()
ROWS_BY_ENTITY = {str(k): max(1, int(v)) for k, v in _json.loads(_rpe).items()} if _rpe else None
MODE = str(mode).strip().lower() or "seed"
WINDOWS_ON_EMPTY = max(1, int(str(windows_on_empty).strip() or "48"))
HISTORY_MINUTES = int(str(history_minutes).strip() or "0") or None
MAX_WINDOWS = max(1, int(str(max_windows).strip() or "96"))
NEW_PARENT_RATIO = float(str(new_parent_ratio).strip() or "0.1")
BATCHES = max(1, int(str(batches).strip() or "10"))
SEED = int(str(seed).strip() or "7")
INVALID_RATIO = float(str(invalid_ratio).strip() or "0.1")
DUPLICATE_RATIO = float(str(duplicate_ratio).strip() or "0.05")
GENERATIVE = str(generative).strip().lower() in ("1", "true", "yes")
SCHEMA = str(synthetic_schema).strip() or f"synthetic_{domain}"

os.environ[f"DATABRICKS_{environment.upper()}_CATALOG"] = catalog

# Where landing files come from (the Deliver data-source step): parameters, secret NAMES and,
# for a direct connection, the service principal's Spark config. Blank = the registry's own path.
_landing_source = apply_landing_runtime(landing_runtime, dbutils.secrets.get, spark.conf.set)
if str(landing_runtime).strip():
    print(f"Landing source: {_landing_source}")
REGISTRY_PATH = f"{contracts_root}/{domain}/{system}/_system.yaml"
# Write files into the landing zone for bronze to read (Delta can't be written through the
# driver's file mount). The landing root comes from the contracts' own registry.
from lakelogic.core.registry import DomainRegistry as _DomainRegistry
_registry = _DomainRegistry.from_yaml(
    path=REGISTRY_PATH, environment=environment, storage_mode="uc",
)
_landing = _registry.storage.landing_root
# Path, format and partitioning are read from each contract.
# Unity Catalog landing roots are absolute Volume paths. A relative one would mean
# the registry was written for a different host, so fail rather than guess a prefix.
OUT_ROOT = str(_landing)
if not OUT_ROOT.startswith("/"):
    raise RuntimeError(
        f"Synthetic data is written to a Volume only, and this system reads landing files from "
        f"{OUT_ROOT}. Seeding never writes into remote source storage."
    )
print(f"Registry : {REGISTRY_PATH}")
print(f"Landing  -> {OUT_ROOT}  (rows={ROWS}, seed={SEED}, batches={BATCHES}, generative={GENERATIVE})")

def _extend_report():
    # Catch up to now. If loop_interval_minutes > 0, repeat until max_run_minutes.
    import time as _time
    _interval = max(0, int(str(loop_interval_minutes).strip() or "0"))
    _deadline = _time.time() + max(1, int(str(max_run_minutes).strip() or "60")) * 60
    _all = []
    while True:
        _all.extend(extend_bronze_estate(
            REGISTRY_PATH, OUT_ROOT, environment=environment, rows=ROWS,
            rows_by_entity=ROWS_BY_ENTITY, entities=ENTITIES or None, seed=SEED,
            invalid_ratio=INVALID_RATIO, duplicate_ratio=DUPLICATE_RATIO, batches=BATCHES,
            windows_on_empty=WINDOWS_ON_EMPTY, history_minutes=HISTORY_MINUTES, max_windows=MAX_WINDOWS,
            new_parent_ratio=NEW_PARENT_RATIO, generative=GENERATIVE,
        ))
        if _interval <= 0 or _time.time() + _interval * 60 > _deadline:
            break
        _time.sleep(_interval * 60)
    return _all

try:
    if MODE == "extend":
        report = _extend_report()
    else:
      report = seed_bronze_estate(
        REGISTRY_PATH, OUT_ROOT,
        environment=environment, rows=ROWS, seed=SEED, invalid_ratio=INVALID_RATIO,
        duplicate_ratio=DUPLICATE_RATIO,
        entities=ENTITIES or None, rows_by_entity=ROWS_BY_ENTITY,
        batches=BATCHES, generative=GENERATIVE,
    )
    print("=" * 70)
    for r in report:
        print(f"  {r['entity']}: {r['rows']} rows -> {r['path']} "
              f"[{r['format']}, {r.get('batches', 1)} batch file(s)]")
    print(f"  Synthetic bronze estate: {len(report)} table(s) in schema '{SCHEMA}'.")
    print("=" * 70)
except Exception:
    tb = traceback.format_exc()
    print(tb)
    try:
        import os
        _log_dir = f"{OUT_ROOT.rsplit('/landing_', 1)[0]}/lakelogic_logs"
        os.makedirs(_log_dir, exist_ok=True)
        with open(f"{_log_dir}/synthetic_seed_error.txt", "w", encoding="utf-8") as _fh:
            _fh.write(tb)
    except Exception:
        pass
    raise
