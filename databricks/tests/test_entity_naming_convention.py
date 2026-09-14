"""One naming rule for entity keys, enforced.

    bronze  bronze_<system>_<name>
    silver  silver_<system>_<name>
    gold    gold_<name>              - NO system

WHY GOLD IS THE ODD ONE
    A gold table can combine data from several systems inside one domain, so
    naming it after a single system asserts something about its inputs that need
    not be true. Cross-domain gold is built in the `shared` domain instead. Nothing
    is lost by dropping it from the name: `_system.yaml` declares `system:` at the
    top level and every contract carries `info.system`.

WHAT THIS WOULD HAVE CAUGHT
    Bronze keys used to be bare business names (`rider_profiles`) while silver and
    gold carried the layer. Nothing could tell a layer from a name by looking at a
    key, and three `depends_on` entries named bronze as `bronze_<system>_<name>` -
    a form that did not exist - so those references silently resolved to nothing.

    python -m pytest databricks/tests
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

MESH = Path(__file__).resolve().parents[2] / "domains_rideflow"


def _registries():
    out = []
    for f in sorted(MESH.glob("*/*/_system.yaml")):
        m = re.search(r"^system:\s*(\S+)", f.read_text(encoding="utf-8"), re.M)
        out.append((f, m.group(1).strip().strip('"').strip("'")))
    return out


def _entries(path: Path):
    """Yield (layer, entity, system_field) for every contract entry."""
    layer = entity = system_field = None
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"\s*-\s*layer:\s*(\S+)", line)
        if m:
            if layer and entity:
                yield layer, entity, system_field
            layer, entity, system_field = m.group(1).strip(), None, None
            continue
        m = re.match(r"\s*entity:\s*(\S+)", line)
        if m and layer:
            entity = m.group(1).strip().strip('"').strip("'")
            continue
        m = re.match(r"\s*system:\s*(\S+)", line)
        if m and layer and entity and system_field is None:
            system_field = m.group(1).strip().strip('"').strip("'")
    if layer and entity:
        yield layer, entity, system_field


ALL = [(f, sysname, layer, entity, sysfield)
       for f, sysname in _registries()
       for layer, entity, sysfield in _entries(f)]


def _ids(rows):
    return [f"{r[1]}:{r[3]}" for r in rows]


def test_the_mesh_has_entities_to_check():
    """A parametrised suite over an empty list passes vacuously."""
    assert len(ALL) >= 60, f"only found {len(ALL)} entities — did the parser break?"


@pytest.mark.parametrize("f,sysname,layer,entity,sysfield", ALL, ids=_ids(ALL))
def test_entity_follows_the_layer_convention(f, sysname, layer, entity, sysfield):
    if layer == "gold":
        assert entity.startswith("gold_"), f"{entity} is gold but is not gold_-prefixed"
        assert not entity.startswith(f"gold_{sysname}_"), (
            f"{entity} carries its system; gold drops it (the file already declares it)"
        )
    else:
        expected = f"{layer}_{sysname}_"
        assert entity.startswith(expected), (
            f"{entity} should start with {expected!r} — {layer} keys carry their system"
        )


@pytest.mark.parametrize("f,sysname,layer,entity,sysfield", ALL, ids=_ids(ALL))
def test_no_entry_repeats_the_system_it_already_inherits(f, sysname, layer, entity, sysfield):
    """The system is declared ONCE per file, at the top level.

    A `system:` on each contract entry was added when gold dropped the system from
    its name, on the assumption the fact would otherwise be lost. It was not: the
    registry declares `system:` at the top and all 66 contracts carry `info.system`.
    The third copy landed where the contract model has no such key, so every run
    logged "Unknown key 'system' in 'contract' block" 62 times and threw it away.

    Three homes for one fact, one of them unreadable, is strictly worse than two.
    """
    assert sysfield is None, (
        f"{entity} repeats system={sysfield!r}; the file already declares it at the "
        f"top level, and the contract model ignores it here"
    )


def test_no_gold_entity_name_is_reused_across_systems():
    """Gold keys lost their system, so two domains could now collide on one name.

    They live in different catalogs so it is not fatal, but it makes a name
    ambiguous in every cross-domain surface (lineage, Data Products, telemetry).
    """
    seen = {}
    dupes = []
    for _f, sysname, layer, entity, _sf in ALL:
        if layer != "gold":
            continue
        if entity in seen:
            dupes.append(f"{entity}: {seen[entity]} and {sysname}")
        seen[entity] = sysname
    assert not dupes, "gold names reused across systems: " + "; ".join(dupes)


@pytest.mark.parametrize("f,sysname", _registries(), ids=lambda x: str(x))
def test_every_depends_on_resolves(f, sysname):
    """The bug this convention was fixing: refs that name a key nobody defines."""
    text = f.read_text(encoding="utf-8")
    defined = {e for _l, e, _s in _entries(f)}
    # Cross-system silver refs are legitimate, so collect the whole mesh's keys.
    everywhere = {e for _f, _s, _l, e, _sf in ALL}

    refs = set()
    for m in re.finditer(r"depends_on:\s*\[([^\]]*)\]", text):
        refs |= {x.strip().strip("'\"") for x in m.group(1).split(",") if x.strip()}
    for m in re.finditer(r"depends_on:\s*\n((?:\s*-\s*\S+\n)+)", text):
        refs |= {l.strip().lstrip("-").strip() for l in m.group(1).splitlines() if l.strip()}

    unresolved = sorted(r for r in refs if r not in everywhere)
    assert not unresolved, f"{sysname} depends on undefined entities: {unresolved}"
    assert defined or not refs
