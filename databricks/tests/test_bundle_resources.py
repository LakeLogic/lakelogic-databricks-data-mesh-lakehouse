"""The bundle's jobs follow the LakeLogic Build Centre layout, and every reference resolves.

    resources/<group>/<key>.job.yml   group in admin | data | monitor | privacy | test
    name: "[${bundle.target}] pl_<group>_..."
    notebooks/nb_*.py

`databricks bundle validate` needs a workspace; these are the offline equivalents:
every notebook_path exists and every ${resources.jobs.<key>.id} names a job.

    python -m pytest databricks/tests
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

BUNDLE = Path(__file__).resolve().parents[1]
RESOURCES = BUNDLE / "resources"
GROUPS = {"admin", "data", "monitor", "privacy", "test"}
FILES = sorted(RESOURCES.rglob("*.yml"))


def _jobs():
    out = {}
    for f in FILES:
        for key, job in (yaml.safe_load(f.read_text(encoding="utf-8"))["resources"]["jobs"]).items():
            assert key not in out, f"job key {key} defined twice"
            out[key] = (f, job)
    return out


JOBS = _jobs()


def _walk(node):
    if isinstance(node, dict):
        for k, v in node.items():
            yield k, v
            yield from _walk(v)
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v)


def test_every_job_file_lives_in_a_build_centre_group():
    for f in FILES:
        rel = f.relative_to(RESOURCES)
        assert len(rel.parts) == 2 and rel.parts[0] in GROUPS, f"{rel} is outside the group folders"
        assert f.name.endswith(".job.yml"), f"{rel} should be named <key>.job.yml"


@pytest.mark.parametrize("key", sorted(JOBS))
def test_the_display_name_carries_the_group_prefix(key):
    f, job = JOBS[key]
    group = f.parent.name
    assert job["name"].startswith(f"[${{bundle.target}}] pl_{group}_"), job["name"]
    assert (job.get("tags") or {}).get("group") == group


@pytest.mark.parametrize("key", sorted(JOBS))
def test_every_notebook_path_exists_and_is_nb_prefixed(key):
    f, job = JOBS[key]
    for k, v in _walk(job):
        if k == "notebook_path":
            target = (f.parent / v).resolve()
            assert target.exists(), f"{key}: {v} does not exist"
            assert target.name.startswith("nb_"), f"{key}: {v} is not an nb_* notebook"


def test_every_job_reference_resolves():
    for f in FILES:
        for ref in re.findall(r"\$\{resources\.jobs\.([A-Za-z0-9_]+)\.id\}", f.read_text(encoding="utf-8")):
            assert ref in JOBS, f"{f.name} references unknown job {ref}"


def test_the_expected_cross_system_jobs_exist():
    names = {job["name"].split("] ", 1)[1] for _, job in JOBS.values()}
    for expected in (
        "pl_admin_setup",
        "pl_data_00_mesh_orchestrator",
        "pl_privacy_retention",
        "pl_privacy_erasure",
        "pl_test_synthetic_seed",
    ):
        assert expected in names, f"{expected} is missing"


def test_erasure_runs_dry_by_default():
    _, job = JOBS["erasure"]
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params["dry_run"] == "true"


def test_assert_layers_ran_reads_its_own_domain_run_log():
    """Every system job's closing gate passed `${var.domain}` (always marketplace), so it
    read marketplace's run log for every system. reference/internal runs beside marketplace
    and failed when marketplace gold had not landed yet (2026-09-30)."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "resources" / "data"
    checked = 0
    for f in sorted(root.glob("*.job.yml")):
        if f.name.startswith("_"):
            continue
        for key, job in yaml.safe_load(f.read_text(encoding="utf-8"))["resources"]["jobs"].items():
            gates = [t for t in job["tasks"]
                     if t.get("notebook_task", {}).get("notebook_path", "").endswith("nb_assert_layers_ran.py")]
            assert len(gates) == 1, f"{f.name}: expected one nb_assert_layers_ran task"
            passed = gates[0]["notebook_task"]["base_parameters"]["domain"]
            own = job["tags"]["domain"]
            assert passed == own, f"{key}: gate reads {passed}, job domain is {own}"
            checked += 1
    assert checked == 11


def test_each_system_is_one_job_running_the_layers_as_notebooks():
    """One job per system: bronze/silver/gold are notebook tasks in that job, not
    run_job_task calls to separate per-layer jobs."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "resources" / "data"
    systems = [f for f in sorted(root.glob("*.job.yml")) if not f.name.startswith("_")]
    assert len(systems) == 11
    for f in systems:
        key = f.name[: -len(".job.yml")]
        job = yaml.safe_load(f.read_text(encoding="utf-8"))["resources"]["jobs"][key]
        assert job["name"] == f"[${{bundle.target}}] pl_data_{key}"
        layers = [t["notebook_task"]["base_parameters"]["target_layers"] for t in job["tasks"]
                  if t.get("notebook_task", {}).get("notebook_path", "").endswith("nb_pipeline_driver.py")]
        assert layers == ["bronze", "silver", "gold"], key
        for t in job["tasks"]:
            ref = t.get("run_job_task", {}).get("job_id", "")
            assert "synthetic_seed" in ref or not ref, f"{key}.{t['task_key']} calls {ref}"


def test_shared_is_gold_only_everywhere_it_is_started():
    """shared has only gold contracts. The mesh passed it bronze/silver=true after the
    one-job-per-system collapse, so its closing gate failed on empty layers (2026-09-30)."""
    from pathlib import Path

    import yaml

    data = Path(__file__).resolve().parents[1] / "resources" / "data"
    job = yaml.safe_load((data / "shared_shared.job.yml").read_text(encoding="utf-8"))
    params = {p["name"]: p["default"] for p in next(iter(job["resources"]["jobs"].values()))["parameters"]}
    assert (params["enable_bronze"], params["enable_silver"], params["enable_gold"]) == ("false", "false", "true")

    mesh = yaml.safe_load((data / "_mesh_orchestrator.job.yml").read_text(encoding="utf-8"))
    task = next(t for t in next(iter(mesh["resources"]["jobs"].values()))["tasks"] if t["task_key"] == "shared_shared")
    passed = task["run_job_task"]["job_parameters"]
    assert (passed["enable_bronze"], passed["enable_silver"], passed["enable_gold"]) == ("false", "false", "true")


def test_engine_tables_carry_the_lakelogic_prefix():
    """Run log and SLO checks are engine-owned, so they are `_lakelogic_*` like the evidence
    tables. They were `_pipeline_run_log` / `_slo_checks` (owner flagged 2026-09-30)."""
    from pathlib import Path

    import yaml

    domains = Path(__file__).resolve().parents[2] / "domains_rideflow"
    seen = 0
    for f in domains.rglob("_system.yaml"):
        meta = (yaml.safe_load(f.read_text(encoding="utf-8")) or {}).get("metadata") or {}
        for key in ("run_log_table", "slo_checks_table"):
            # Table identifiers only; a local-run config may point at a path ({log_path}).
            if "{domain_catalog}" in str(meta.get(key, "")):
                assert meta[key].split(".")[-1].startswith("_lakelogic_"), f"{f}: {key}={meta[key]}"
                seen += 1
    assert seen >= 11


def test_every_system_enables_exactly_the_layers_it_has_contracts_for():
    """An enabled layer with no contracts leaves no run-log rows and fails the closing gate.
    Five systems have no gold (and shared no bronze/silver); they only passed while the gate
    read marketplace's log (2026-09-30). Job defaults AND the mesh's parameters must agree."""
    from pathlib import Path

    import yaml

    here = Path(__file__).resolve().parents[1]
    data = here / "resources" / "data"
    domains = here.parent / "domains_rideflow"
    mesh = yaml.safe_load((data / "_mesh_orchestrator.job.yml").read_text(encoding="utf-8"))
    passed = {
        t["task_key"]: t["run_job_task"]["job_parameters"]
        for t in next(iter(mesh["resources"]["jobs"].values()))["tasks"]
        if (t.get("run_job_task") or {}).get("job_parameters")
    }
    checked = 0
    for f in data.glob("*.job.yml"):
        if f.name.startswith("_"):
            continue
        key = f.name[: -len(".job.yml")]
        d, s = key.split("_", 1)
        contracts = domains / d / s / "contracts"
        job = next(iter(yaml.safe_load(f.read_text(encoding="utf-8"))["resources"]["jobs"].values()))
        defaults = {p["name"]: p["default"] for p in job["parameters"]}
        for layer in ("bronze", "silver", "gold"):
            want = "true" if any((contracts / layer).glob("*.yaml")) else "false"
            assert defaults[f"enable_{layer}"] == want, f"{key} default enable_{layer}"
            assert passed[key][f"enable_{layer}"] == want, f"mesh passes {key} enable_{layer}"
        checked += 1
    assert checked == 11


def test_every_system_persists_its_slo_results():
    """Only marketplace and payments declared slo_checks_table, so nine systems' SLO verdicts
    lived only in job output (2026-09-30)."""
    from pathlib import Path

    import yaml

    domains = Path(__file__).resolve().parents[2] / "domains_rideflow"
    systems = [f for f in domains.glob("*/*/_system.yaml")]
    assert len(systems) == 11
    for f in systems:
        meta = (yaml.safe_load(f.read_text(encoding="utf-8")) or {}).get("metadata") or {}
        assert meta.get("slo_checks_table") == "{domain_catalog}._lakelogic_slo_checks", f


def test_mesh_is_scheduled_hourly_and_paused():
    """The mesh has an hourly schedule shipped PAUSED: nothing runs until someone unpauses it."""
    from pathlib import Path

    import yaml

    f = Path(__file__).resolve().parents[1] / "resources" / "data" / "_mesh_orchestrator.job.yml"
    job = next(iter(yaml.safe_load(f.read_text(encoding="utf-8"))["resources"]["jobs"].values()))
    assert job["schedule"] == {"quartz_cron_expression": "0 0 * * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"}


def test_the_seed_runs_every_system_seed_in_fk_order():
    """pl_test_synthetic_seed runs the per-system seed jobs, each after the systems whose
    SILVER its generator reads ids from (_uc_fk_ids in nb_test_data_driver.py)."""
    import re
    from pathlib import Path

    import yaml

    here = Path(__file__).resolve().parents[1]
    job = next(iter(yaml.safe_load((here / "resources" / "test" / "_synthetic_seed.job.yml").read_text(encoding="utf-8"))["resources"]["jobs"].values()))
    tasks = {t["task_key"]: t for t in job["tasks"]}
    for key, t in tasks.items():
        assert t["run_job_task"]["job_id"] == f"${{resources.jobs.synthetic_seed_{key}.id}}"
    nb = (here / "notebooks" / "nb_test_data_driver.py").read_text(encoding="utf-8")
    table_owner = {"marketplace": "marketplace_rideflow", "payments": "payments_stripe"}
    for m in re.finditer(r"def gen_(\w+)\(registry, landing_uri\):(.*?)(?=\ndef |\Z)", nb, re.S):
        key, body = m.group(1), m.group(2)
        if key not in tasks:
            continue
        reads = {table_owner[d] for d in re.findall(r'_uc_fk_ids\("(\w+)"', body)}
        deps = {d["task_key"] for d in tasks[key].get("depends_on", [])}
        assert reads <= deps, f"{key} reads {reads} but waits only for {deps}"


def test_system_seeds_build_bronze_and_silver():
    """The value must be the plain string "true" - an escaped `\\"true\\"` never matched and
    the seed silently skipped bronze+silver."""
    from pathlib import Path

    import yaml

    for f in (Path(__file__).resolve().parents[1] / "resources" / "test").glob("synthetic_seed_*.job.yml"):
        if f.name == "synthetic_seed_shared_shared.job.yml":
            continue
        job = next(iter(yaml.safe_load(f.read_text(encoding="utf-8"))["resources"]["jobs"].values()))
        params = job["tasks"][0]["notebook_task"]["base_parameters"]
        assert params.get("build_bronze_silver") == "true", f.name


def test_the_mesh_runs_the_setup_workflow_and_setup_only_provisions():
    """The mesh called its own copy of the setup notebook, which never passed reset or
    storage_root (a reset deploy did not reset). It now runs pl_admin_setup itself, and
    setup no longer runs marketplace (the mesh does, then smoke-tests it)."""
    from pathlib import Path

    import yaml

    here = Path(__file__).resolve().parents[1] / "resources"
    load = lambda p: next(iter(yaml.safe_load((here / p).read_text(encoding="utf-8"))["resources"]["jobs"].values()))
    setup = load("admin/_setup.job.yml")
    assert [t["task_key"] for t in setup["tasks"]] == ["setup"]
    params = setup["tasks"][0]["notebook_task"]["base_parameters"]
    assert params["reset"] == "${var.reset_catalog}" and "storage_root" in params
    mesh = {t["task_key"]: t for t in load("data/_mesh_orchestrator.job.yml")["tasks"]}
    assert mesh["setup"]["run_job_task"]["job_id"] == "${resources.jobs.setup.id}"
    assert [d["task_key"] for d in mesh["verify_marketplace_outputs"]["depends_on"]] == ["marketplace_rideflow"]
