"""Static release checks for the RideFlow Databricks demo."""
from __future__ import annotations
import argparse, ast, pathlib, re, sys
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]

def _olc_validation(root: pathlib.Path) -> tuple[list[str], list[str]]:
    """Validate every contract and registry file against the published OLC standard.

    ``yaml.safe_load`` above answers "does this parse". It passes a misspelled key, a
    rule list written where thresholds belong, and an ``on_events`` token no consumer
    matches — all of which reach a demo audience as a pipeline that quietly never
    alerts. This answers "is this the standard".

    Returns ``(errors, notes)``. A check that could NOT run is reported as a note, never
    as a pass: silence and success must not look identical in the log.
    """
    errors: list[str] = []
    notes: list[str] = []

    try:
        from olc.models import load_strict
    except Exception as exc:  # pragma: no cover - import guard
        return errors, [f"OLC contract validation SKIPPED — open-lakehouse-contract not importable ({exc})"]

    contracts = [
        path
        for path in root.rglob("contracts/**/*.yaml")
        if not path.name.startswith("_")
    ]
    for path in contracts:
        try:
            load_strict(yaml.safe_load(path.read_text(encoding="utf-8")) or {})
        except Exception as exc:
            errors.append(f"OLC contract: {path.relative_to(root)}: {str(exc).splitlines()[0]}")
    notes.append(f"OLC: validated {len(contracts)} contracts against OLCContractV1")

    try:
        from olc.models import load_strict_domain, load_strict_system
    except ImportError:
        # The registry documents landed after 0.7.0. Say so plainly and keep the demo
        # green rather than failing on a dependency the repo cannot yet pin — but the
        # line must appear, so nobody reads a passing run as "domains were checked".
        notes.append(
            "OLC registry validation SKIPPED — _domain.yaml / _system.yaml are checked "
            "only once open-lakehouse-contract ships OLCDomainV1/OLCSystemV1"
        )
        return errors, notes

    for pattern, loader, label in (
        ("**/_domain.yaml", load_strict_domain, "OLCDomainV1"),
        ("**/_system.yaml", load_strict_system, "OLCSystemV1"),
    ):
        paths = list(root.glob(pattern))
        for path in paths:
            try:
                loader(yaml.safe_load(path.read_text(encoding="utf-8")) or {})
            except Exception as exc:
                errors.append(f"OLC registry: {path.relative_to(root)}: {str(exc).splitlines()[0]}")
        notes.append(f"OLC: validated {len(paths)} files against {label}")

    return errors, notes


def _powerbi_consistency(root: pathlib.Path) -> list[str]:
    """The Power BI items in reports/ and the gold contracts' `downstream:` blocks must agree.

    Lineage is read from the contracts, so a renamed report or a table dropped from a model
    would leave a consumer the contract still claims. Checks, both ways:
    every model table is a gold contract's table, every report entity is in its model, and every
    semantic_model / report a gold contract names exists under reports/.
    """
    import json

    errors: list[str] = []
    reports = root / "reports"
    if not reports.is_dir():
        return errors
    gold = {}
    for path in root.glob("contracts/*/*/contracts/gold/*.yaml"):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        gold[re.sub(r"_v\d.*$", "", path.stem)] = doc

    def display(item: pathlib.Path) -> str:
        return json.loads((item / ".platform").read_text(encoding="utf-8"))["metadata"]["displayName"]

    model_tables: dict[str, set[str]] = {}
    model_names: dict[str, str] = {}
    for model in reports.glob("*.SemanticModel"):
        model_names[display(model)] = model.name
        tables = {t.stem for t in (model / "definition" / "tables").glob("*.tmdl")}
        model_tables[model.name] = tables
        for table in sorted(tables - gold.keys()):
            errors.append(f"Power BI: {model.name} has table {table}, which is not a gold contract")
    report_names: dict[str, set[str]] = {}
    for rep in reports.glob("*.Report"):
        bound = re.search(r'\.\./([^"]+\.SemanticModel)', (rep / "definition.pbir").read_text(encoding="utf-8"))
        if not bound or bound.group(1) not in model_tables:
            errors.append(f"Power BI: {rep.name} is not bound to a semantic model in reports/")
            continue
        report_names[display(rep)] = rep.name
        for section in json.loads((rep / "report.json").read_text(encoding="utf-8"))["sections"]:
            for visual in section["visualContainers"]:
                for source in json.loads(visual["config"])["singleVisual"]["prototypeQuery"]["From"]:
                    if source["Entity"] not in model_tables[bound.group(1)]:
                        errors.append(f"Power BI: {rep.name} reads {source['Entity']}, which {bound.group(1)} does not hold")
    for name, doc in gold.items():
        for entry in doc.get("downstream") or []:
            if entry.get("platform", "").lower() != "powerbi":
                continue
            if entry.get("type") == "semantic_model" and entry.get("name") not in model_names:
                errors.append(f"Power BI: {name} names semantic model {entry.get('name')}, not in reports/")
            for consumer in entry.get("consumers") or []:
                if consumer.get("type") == "report" and consumer.get("name") not in report_names:
                    errors.append(f"Power BI: {name} names report {consumer.get('name')}, not in reports/")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", action="store_true")
    args = parser.parse_args()
    errors: list[str] = []
    python_files = list(ROOT.rglob("*.py"))
    for path in python_files:
        try: ast.parse(path.read_text(encoding="utf-8"))
        except Exception as exc: errors.append(f"Python syntax: {path.relative_to(ROOT)}: {exc}")
    yaml_files = list(ROOT.rglob("*.yaml")) + list(ROOT.rglob("*.yml"))
    for path in yaml_files:
        try: yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc: errors.append(f"YAML parse: {path.relative_to(ROOT)}: {exc}")
    # The demo must always install the LATEST public lakelogic — it depends on
    # fixes that ship continuously — so notebooks must NOT pin a version.
    pattern = re.compile(r"%pip install\s+[^\n]*\blakelogic==")
    for path in ROOT.joinpath("databricks", "notebooks").rglob("*.py"):
        if pattern.search(path.read_text(encoding="utf-8")):
            errors.append(f"Pinned LakeLogic dependency (demo must use latest, drop the ==): {path.relative_to(ROOT)}")
    if args.release:
        readme = ROOT.joinpath("README.md").read_text(encoding="utf-8")
        if "<PUBLIC_REPOSITORY_URL>" in readme: errors.append("README still contains <PUBLIC_REPOSITORY_URL>")
        if "Pre-release:" in readme: errors.append("README is still marked pre-release")
        if not ROOT.joinpath(".git").exists(): errors.append("Repository has not been initialized with Git")
        for path in ROOT.joinpath("databricks", "resources").rglob("*.yml"):
            if "@company.com" in path.read_text(encoding="utf-8"):
                errors.append(f"Placeholder notification recipient: {path.relative_to(ROOT)}")
    olc_errors, olc_notes = _olc_validation(ROOT)
    errors.extend(olc_errors)
    errors.extend(_powerbi_consistency(ROOT))

    print(f"Checked {len(python_files)} Python files and {len(yaml_files)} YAML files")
    for note in olc_notes:
        print(note)
    if errors:
        print("\n".join(f"ERROR: {error}" for error in errors))
        return 1
    print("Static validation passed")
    return 0

if __name__ == "__main__":
    sys.exit(main())
