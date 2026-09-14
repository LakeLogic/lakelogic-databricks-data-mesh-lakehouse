"""Bring every registry entity key onto one naming rule.

THE TWO CONVENTIONS THAT WERE IN PLAY
    bronze  `rider_profiles`                     - bare business name
    silver  `silver_rideflow_rider_profiles`     - layer + system + name
    gold    `gold_<system>_fact_trip_daily_kpis` - layer + system + name

    Nothing could tell a layer from a name by looking at the key, and three
    `depends_on` entries already named bronze as `bronze_<system>_<name>` - the
    form that did NOT exist - so those references resolved to nothing.

THE RULE
    bronze  bronze_<system>_<name>   (table unchanged; the entity now matches it)
    silver  silver_<system>_<name>   (already correct - untouched)
    gold    gold_<name>              (system DROPPED from entity, table and file)

WHY GOLD IS THE EXCEPTION
    A gold table can combine data from several systems inside one domain, so
    naming it after a single system is a claim about its inputs that need not be
    true. Cross-domain gold is built in the `shared` domain instead. The system a
    contract belongs to is recorded in a new `system:` field on the contract
    entry, so nothing is lost - it moves out of the name into a readable field.

GOLD TABLES ARE RENAMED. Deliberate, and chosen explicitly: the physical Delta
tables move too, so the estate matches the contracts. Gold tables under the old
names are orphaned, and SaaS telemetry recorded before the rename stays keyed to
the old names.

    python scripts/rename_entities_to_layer_convention.py            # dry run
    python scripts/rename_entities_to_layer_convention.py --apply
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MESH = ROOT / "domains_rideflow"


def systems():
    out = []
    for f in sorted(MESH.glob("*/*/_system.yaml")):
        m = re.search(r"^system:\s*(\S+)", f.read_text(encoding="utf-8"), re.M)
        if not m:
            raise SystemExit(f"{f} has no top-level `system:` key")
        out.append((f, m.group(1).strip().strip('"').strip("'")))
    return out


def new_entity(layer, entity, system):
    """The rule, in one place."""
    if layer == "bronze":
        if entity.startswith("bronze_" + system + "_"):
            return entity
        name = entity[len("bronze_"):] if entity.startswith("bronze_") else entity
        return "bronze_" + system + "_" + name
    if layer == "gold":
        for prefix in ("gold_" + system + "_", "gold_"):
            if entity.startswith(prefix):
                return "gold_" + entity[len(prefix):]
        return entity
    # Silver keeps its system: a silver table is single-system by construction.
    return entity


def plan():
    """Rename maps are PER SYSTEM, never global.

    `campaigns` is a bronze entity in BOTH google_ads and meta_ads, and they must
    become different keys. A single global map keyed on the old name would have
    given one of them the other's system.
    """
    per_system = {}   # system -> {old: new}
    file_moves = []

    for sys_yaml, system in systems():
        text = sys_yaml.read_text(encoding="utf-8")
        renames = {}
        layer = None
        for line in text.splitlines():
            m_layer = re.match(r"\s*-\s*layer:\s*(\S+)", line)
            if m_layer:
                layer = m_layer.group(1).strip()
                continue
            m_ent = re.match(r"(\s*)entity:\s*(\S+)", line)
            if m_ent and layer:
                old = m_ent.group(2).strip().strip('"').strip("'")
                new = new_entity(layer, old, system)
                if new != old:
                    renames[old] = new
                layer = None
        per_system[system] = renames

        gold_dir = sys_yaml.parent / "contracts" / "gold"
        if gold_dir.is_dir():
            for f in sorted(gold_dir.glob("*.yaml")):
                pre = "gold_" + system + "_"
                if f.name.startswith(pre):
                    file_moves.append((f, f.with_name("gold_" + f.name[len(pre):])))
    return per_system, file_moves


def rewrite_system_yaml(sys_yaml, system, renames, apply):
    text = sys_yaml.read_text(encoding="utf-8")
    out = []
    layer = None
    changed = 0

    for line in text.splitlines(keepends=True):
        m_layer = re.match(r"(\s*)-\s*layer:\s*(\S+)", line)
        if m_layer:
            layer = m_layer.group(2).strip()
            out.append(line)
            continue

        m_ent = re.match(r"(\s*)entity:\s*(\S+)\s*$", line.rstrip("\n"))
        if m_ent and layer:
            indent = m_ent.group(1)
            old = m_ent.group(2).strip().strip('"').strip("'")
            new = renames.get(old, old)
            if new != old:
                changed += 1
            out.append(indent + "entity: " + new + "\n")
            # NO `system:` LINE HERE. An earlier version of this script added one,
            # reasoning that gold drops the system from its NAME so it should be
            # recorded in a field. It was already recorded twice: `_system.yaml`
            # declares `system:` at the top level, and all 66 contracts carry
            # `info.system`. The third copy landed at the top of the contract block,
            # where the contract model has no such key, so every run logged
            #     Unknown key 'system' in 'contract' block - will be ignored
            # 62 times and discarded the value. A fact kept in three places, one of
            # them unreadable, is worse than the two that already worked.
            layer = None
            continue

        # A gold `path:` points at a file that is being renamed.
        if "path:" in line and ("gold_" + system + "_") in line:
            out.append(line.replace("gold_" + system + "_", "gold_"))
            changed += 1
            continue

        out.append(line)

    body = "".join(out)
    # depends_on lists (and every other reference) name entities by their key.
    # Longest-first so a scorecard key is not partly rewritten by the shorter
    # dimension key it happens to start with.
    for old, new in sorted(renames.items(), key=lambda kv: -len(kv[0])):
        body = re.sub(r"(?<![\w])" + re.escape(old) + r"(?![\w])", new, body)

    if body != text:
        if apply:
            sys_yaml.write_text(body, encoding="utf-8", newline="\n")
        return max(changed, 1)
    return 0


def rewrite_gold_contract(path, system, apply):
    text = path.read_text(encoding="utf-8")
    new = text.replace("{gold_layer}_" + system + "_", "{gold_layer}_")
    new = new.replace("{gold_layer}_{system}_", "{gold_layer}_")
    if new != text:
        if apply:
            path.write_text(new, encoding="utf-8", newline="\n")
        return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    per_system, file_moves = plan()

    # Within a system, a rename landing two entities on one key would silently
    # merge two datasets into one contract entry.
    for system, renames in sorted(per_system.items()):
        seen = {}
        for old, new in renames.items():
            if new in seen:
                print("REFUSING: in " + system + ", " + old + " and " + seen[new]
                      + " both become " + new)
                return 2
            seen[new] = old

    total = sum(len(r) for r in per_system.values())
    head = "APPLYING" if args.apply else "DRY RUN"
    print(head + " - " + str(total) + " entity renames across "
          + str(len(per_system)) + " systems, "
          + str(len(file_moves)) + " gold contract files to move\n")
    for system, renames in sorted(per_system.items()):
        if not renames:
            print("  " + system + ": (nothing to change)")
            continue
        print("  " + system + ":")
        for old, new in sorted(renames.items()):
            print("      " + old.ljust(44) + " -> " + new)

    print()
    for src, dst in file_moves:
        print("  MOVE " + str(src.relative_to(ROOT)).replace("\\", "/") + " -> " + dst.name)

    if not args.apply:
        print("\n(nothing written - re-run with --apply)")
        return 0

    for src, dst in file_moves:
        subprocess.run(["git", "mv", str(src), str(dst)], cwd=str(ROOT), check=True)

    for sys_yaml, system in systems():
        n = rewrite_system_yaml(sys_yaml, system, per_system[system], apply=True)
        gold_dir = sys_yaml.parent / "contracts" / "gold"
        g = 0
        if gold_dir.is_dir():
            g = sum(1 for f in sorted(gold_dir.glob("*.yaml"))
                    if rewrite_gold_contract(f, system, apply=True))
        print("  " + str(sys_yaml.relative_to(ROOT)).replace("\\", "/")
              + ": " + str(n) + " registry edits, " + str(g) + " gold contracts retabled")

    # ── Everything else that spells a gold name out loud ──────────────────────
    # Jobs, smoke tests, generators and docs reference gold datasets directly. A
    # rename that stopped at the registry would leave those pointing at tables
    # that no longer exist — green YAML, broken pipeline.
    #
    # CONTRACT FILES ARE INCLUDED, and they are the ones that actually break: a
    # gold contract joins another gold table by NAME —
    #     path: "table:{domain_catalog}.gold_rideflow_dim_driver"
    #     contract: gold_rideflow_dim_rider
    # — and a fact whose dimension has been renamed out from under it resolves
    # every surrogate key to the unknown member instead of failing loudly.
    #
    # Only GOLD names are rewritten globally, and only they safely can be: a gold
    # key carries its system (`gold_dim_driver`) so it is unique across
    # the mesh. Bronze keys are bare (`campaigns` exists in two systems), so a
    # global pass on those would rewrite the wrong system's text.
    gold_renames = {}
    for renames in per_system.values():
        for old, new in renames.items():
            if old.startswith("gold_"):
                gold_renames[old] = new

    skip = {".git", "node_modules", "__pycache__", ".venv"}
    exts = {".py", ".yml", ".yaml", ".sql", ".md", ".json"}
    touched = 0
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in exts:
            continue
        if any(part in skip for part in path.parts):
            continue
        if path.name == "_system.yaml":
            continue  # already handled, with per-system context
        if path.resolve() == Path(__file__).resolve():
            continue  # this file NAMES the old keys to document them
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        body = text
        for old, new in sorted(gold_renames.items(), key=lambda kv: -len(kv[0])):
            body = re.sub(r"(?<![\w])" + re.escape(old) + r"(?![\w])", new, body)
        if body != text:
            path.write_text(body, encoding="utf-8", newline="\n")
            touched += 1
            print("  ref update: " + str(path.relative_to(ROOT)).replace("\\", "/"))
    print("\n  " + str(touched) + " non-registry files updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
