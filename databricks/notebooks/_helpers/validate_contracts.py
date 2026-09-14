# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Validate Contracts
# MAGIC
# MAGIC Validates all LakeLogic contract YAML files in a Unity Catalog Volume.
# MAGIC Same checks as CI but runnable inside Databricks for quick verification.
# MAGIC
# MAGIC **Checks performed:**
# MAGIC 1. YAML syntax — can the file be parsed?
# MAGIC 2. Schema validation — does it pass `validate_contract()`?
# MAGIC 3. Naming convention — `lowercase_snake_case.yaml`
# MAGIC 4. Required fields — `version`, `model.fields`, etc.

# COMMAND ----------

# ── LakeLogic source ──────────────────────────────────────────────────
# Blank widget = install the published package from PyPI (what the demo does).
# Set it to a wheel in the catalog's _wheels Volume to run an UNRELEASED build:
#   scripts/upload_lakelogic_wheel.sh   ->  prints the path to paste here
# That is the only way to exercise a LakeLogic change on Databricks BEFORE it
# is published, rather than discovering a bad release from the demo breaking.
dbutils.widgets.text("lakelogic_wheel", "", "LakeLogic wheel (blank = PyPI)")
dbutils.widgets.text("lakelogic_version", "", "LakeLogic version (blank = latest)")
_wheel = dbutils.widgets.get("lakelogic_wheel").strip()
# --force-reinstall matters: Databricks pre-installs the packages named in
# this cell into the notebook environment at session startup, so pip sees the
# PUBLISHED lakelogic already present. An unreleased wheel carries the SAME
# version number, so pip reports "already satisfied" and silently keeps the
# released code - the run then tests the wrong build while looking correct.
_version = dbutils.widgets.get("lakelogic_version").strip()
# The PyPI branch needs the same protection the wheel branch already had.
# Databricks PRE-INSTALLS the packages named in this cell at session start,
# so a bare `lakelogic` is "already satisfied" by whatever version the
# environment was built with — pip installs nothing and the run silently
# executes the OLD code. That is exactly how a run on "1.51.0" reproduced a
# bug fixed in 1.51.0: it was really running the pre-installed 1.50.0, and
# the null `lakelogic_version` in its telemetry was the only tell.
# An explicit `==` is unsatisfied by an older pre-install, so pip must act;
# `--upgrade` covers the unpinned case.
if _wheel:
    lakelogic_pkg = f"{_wheel} --force-reinstall"
elif _version:
    lakelogic_pkg = f"lakelogic=={_version}"
else:
    lakelogic_pkg = "lakelogic --upgrade"
print(f"Installing LakeLogic from: {lakelogic_pkg}")

# COMMAND ----------

# MAGIC # Only lakelogic is named: pyyaml/polars/deltalake are its own dependencies.
# MAGIC # pyarrow<25: DBR ships 21.0.0, lakelogic needs >=23.0.1, databricks-connect caps <25.
# MAGIC %pip install $lakelogic_pkg "pyarrow<25" --quiet

# COMMAND ----------

dbutils.widgets.text("volume_path", "/Volumes/catalog/schema/contracts", "Contract Volume Path")
dbutils.widgets.dropdown("fail_on_error", "yes", ["yes", "no"], "Fail on Error")

# COMMAND ----------

import glob
import re
from pathlib import Path

import yaml
from lakelogic import validate_contract

volume_path = dbutils.widgets.get("volume_path")
fail_on_error = dbutils.widgets.get("fail_on_error") == "yes"

print(f"📂 Scanning: {volume_path}")
print(f"   Fail on error: {fail_on_error}")
print()

# COMMAND ----------

# ── Discover all contract YAML files ─────────────────────────────────────────
contract_files = sorted(glob.glob(f"{volume_path}/**/*.yaml", recursive=True))

# Exclude registry and metadata files
contract_files = [f for f in contract_files if not Path(f).name.startswith("_") and Path(f).name != "domain.yaml"]

print(f"📦 Found {len(contract_files)} contract file(s)")

# COMMAND ----------

# ── Validate each contract ───────────────────────────────────────────────────
results = []
errors = []
NAMING_RE = re.compile(r"^[a-z][a-z0-9_.]*\.yaml$")

for filepath in contract_files:
    p = Path(filepath)
    filename = p.name
    rel_path = str(p.relative_to(volume_path)) if filepath.startswith(volume_path) else filepath

    checks = {"file": rel_path, "yaml": "—", "schema": "—", "naming": "—", "errors": []}

    # 1. YAML syntax
    try:
        with open(filepath) as f:
            data = yaml.safe_load(f)
        checks["yaml"] = "✅"
    except yaml.YAMLError as e:
        checks["yaml"] = "❌"
        checks["errors"].append(f"YAML parse error: {e}")
        results.append(checks)
        continue

    # 2. Naming convention
    if NAMING_RE.match(filename):
        checks["naming"] = "✅"
    else:
        checks["naming"] = "⚠️"
        checks["errors"].append(f"Naming: expected lowercase_snake_case.yaml, got '{filename}'")

    # 3. LakeLogic schema validation
    try:
        result = validate_contract(filepath)
        if result.valid:
            checks["schema"] = "✅"
        else:
            checks["schema"] = "❌"
            for err in result.error_only:
                checks["errors"].append(f"[{err.field}] {err.message}")
    except Exception as e:
        checks["schema"] = "❌"
        checks["errors"].append(f"Validation error: {e}")

    results.append(checks)
    if checks["errors"]:
        errors.extend([(rel_path, e) for e in checks["errors"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# ── Summary table ────────────────────────────────────────────────────────────
import pandas as pd

summary_df = pd.DataFrame(
    [
        {
            "Contract": r["file"],
            "YAML": r["yaml"],
            "Schema": r["schema"],
            "Naming": r["naming"],
            "Issues": len(r["errors"]),
        }
        for r in results
    ]
)

passed = sum(1 for r in results if not r["errors"])
failed = len(results) - passed

print(f"\n{'='*60}")
print(f"  ✅ Passed: {passed}    ❌ Failed: {failed}    📦 Total: {len(results)}")
print(f"{'='*60}\n")

display(spark.createDataFrame(summary_df))

# COMMAND ----------

# ── Error details ────────────────────────────────────────────────────────────
if errors:
    print(f"⚠️  {len(errors)} issue(s) found:\n")
    for filepath, err in errors:
        print(f"  ❌ {filepath}")
        print(f"     {err}\n")

    if fail_on_error:
        raise Exception(f"Contract validation failed: {len(errors)} issue(s)")
else:
    print("✅ All contracts passed validation — no issues found")
