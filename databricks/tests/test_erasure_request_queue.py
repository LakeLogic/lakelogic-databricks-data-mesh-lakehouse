"""The erasure job reads `_lakelogic_erasure_requests` when no IDs are given.

Setup pre-creates the table; its columns must be the engine's own schema, or the
engine's status updates would target columns that do not exist.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"
SETUP = (NOTEBOOKS / "nb_00_setup.py").read_text(encoding="utf-8")
ERASURE = (NOTEBOOKS / "nb_compliance_erasure.py").read_text(encoding="utf-8")


def test_setup_creates_the_request_table_with_the_engine_schema():
    er = pytest.importorskip("lakelogic.core.erasure_requests")
    ddl = re.search(r'_ERASURE_REQUEST_COLS = """\((.*?)\)"""', SETUP, re.S).group(1)
    cols = [part.split()[0] for part in ddl.split(",")]
    assert cols == list(er.REQUEST_COLUMNS)
    assert "`_lakelogic_erasure_requests` " in SETUP


def test_blank_ids_process_the_queue_and_explicit_ids_still_work():
    assert "QUEUE_MODE = not GDPR_IDS and not HIPAA_IDS" in ERASURE
    assert "pipeline.process_erasure_requests(" in ERASURE
    assert "if not QUEUE_MODE and GDPR_COLUMN and GDPR_IDS:" in ERASURE
    assert "if not QUEUE_MODE and HIPAA_COLUMN and HIPAA_IDS:" in ERASURE


def test_the_notebook_never_prints_subject_ids():
    assert "{GDPR_IDS}" not in ERASURE and "{HIPAA_IDS}" not in ERASURE


def test_interactive_runs_are_dry_by_default():
    assert 'dbutils.widgets.dropdown("dry_run", "true"' in ERASURE
