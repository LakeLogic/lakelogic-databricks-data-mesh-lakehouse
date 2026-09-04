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
