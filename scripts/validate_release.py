"""Static release checks for the RideFlow Databricks demo."""
from __future__ import annotations
import argparse, ast, pathlib, re, sys
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
REQUIRED_LAKELOGIC = "1.40.0"

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
    pattern = re.compile(r"%pip install\s+lakelogic(?!==" + re.escape(REQUIRED_LAKELOGIC) + r"\b)")
    for path in ROOT.joinpath("databricks", "notebooks").rglob("*.py"):
        if pattern.search(path.read_text(encoding="utf-8")):
            errors.append(f"Unpinned LakeLogic dependency: {path.relative_to(ROOT)}")
    if args.release:
        readme = ROOT.joinpath("README.md").read_text(encoding="utf-8")
        if "<PUBLIC_REPOSITORY_URL>" in readme: errors.append("README still contains <PUBLIC_REPOSITORY_URL>")
        if "Pre-release:" in readme: errors.append("README is still marked pre-release")
        if not ROOT.joinpath(".git").exists(): errors.append("Repository has not been initialized with Git")
        for path in ROOT.joinpath("databricks", "resources").rglob("*.yml"):
            if "@company.com" in path.read_text(encoding="utf-8"):
                errors.append(f"Placeholder notification recipient: {path.relative_to(ROOT)}")
    print(f"Checked {len(python_files)} Python files and {len(yaml_files)} YAML files")
    if errors:
        print("\n".join(f"ERROR: {error}" for error in errors))
        return 1
    print("Static validation passed")
    return 0

if __name__ == "__main__":
    sys.exit(main())
