"""Every notebook that builds a registry must resolve `{catalog}` the same way.

THE FAILURE
    `service_level_checks` built its registry as

        DomainRegistry.from_yaml(REGISTRY_PATH, environment=ENVIRONMENT)

    while `pipeline_driver` first derives the catalog from the registry Volume path
    into `RIDEFLOW_<ENV>_CATALOG` and passes `storage_mode="uc"`. Without the env var
    the `{catalog}` placeholder in

        domain_catalog: "`{catalog}`.{domain}"
        run_log_table:  "{domain_catalog}._pipeline_run_log"

    resolved to an empty string, so every run-log query was built as

        FROM .marketplace._pipeline_run_log

    and Databricks answered [PARSE_SYNTAX_ERROR] Syntax error at or near '.'.
    The job "succeeded" — the checks simply all failed to parse.

These are source-level assertions on purpose: the notebooks only import inside
Databricks, so the cheapest guard that would have caught this is a read of the text.

    python -m pytest databricks/tests
"""
from __future__ import annotations

from pathlib import Path

import pytest

NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"

# Notebooks that construct a DomainRegistry and therefore need the catalog resolved.
REGISTRY_NOTEBOOKS = ["pipeline_driver.py", "service_level_checks.py"]


def _src(name: str) -> str:
    return (NOTEBOOKS / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("name", REGISTRY_NOTEBOOKS)
def test_the_notebook_exists(name):
    assert (NOTEBOOKS / name).exists(), f"{name} was renamed without updating this guard"


@pytest.mark.parametrize("name", REGISTRY_NOTEBOOKS)
def test_the_catalog_is_derived_from_the_registry_path(name):
    """The one source of truth: /Volumes/<catalog>/... names the catalog."""
    src = _src(name)
    assert "_derived_catalog" in src, f"{name} never derives a catalog"
    assert '_CATALOG"' in src or "_CATALOG'" in src, f"{name} never exports the catalog env var"


@pytest.mark.parametrize("name", REGISTRY_NOTEBOOKS)
def test_the_registry_is_built_in_uc_mode(name):
    """`from_yaml` defaults to storage_mode="catalog"; the mesh is Unity Catalog.

    Taking the default resolves the layer roots against a different target than the
    pipeline writes, so the checks would read tables that do not exist.
    """
    src = _src(name)
    assert 'storage_mode="uc"' in src or "storage_mode=STORAGE_MODE" in src, (
        f"{name} builds its registry without an explicit storage_mode"
    )


@pytest.mark.parametrize("name", REGISTRY_NOTEBOOKS)
def test_the_catalog_is_set_before_the_registry_is_built(name):
    """Order matters: `from_yaml` reads the env var while resolving placeholders."""
    src = _src(name)
    assert src.index("_derived_catalog") < src.index("DomainRegistry.from_yaml"), (
        f"{name} derives the catalog after building the registry — too late to matter"
    )
