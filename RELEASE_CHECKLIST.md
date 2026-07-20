# RideFlow Demo Release Checklist

This repository must pass every blocking item below before the public article links to it.

## Blocking release gates

- [x] Publish the repository at its final public GitHub URL and replace every placeholder clone command.
- [x] Install the public `lakelogic` release (unpinned) across every notebook; developed and last verified against 1.40.0.
- [ ] Run `databricks bundle validate -t dev` with a clean authenticated profile.
- [ ] Deploy to a clean Databricks workspace and catalog using only the README.
- [ ] Run `rideflow_demo_bootstrap` successfully.
- [ ] Verify the expected Bronze, Silver, Gold, quarantine, log, lineage, SLO, SCD2, and DAG outputs.
- [ ] Run the documented teardown and confirm that all demo-owned resources are removed.
- [ ] Repeat the golden path on every environment explicitly claimed as supported, including Databricks Free Edition.

## Repository hygiene

- [ ] Add an Apache-2.0 `LICENSE` file approved by the repository owner.
- [ ] Initialize Git, create the public remote, and publish a versioned release tag.
- [ ] Add CI for Python syntax, YAML parsing, secret scanning, contract validation, and Databricks bundle validation.
- [ ] Remove generated caches before the first commit.
- [ ] Confirm that no real credentials, workspace identifiers, customer data, or private endpoints are present.

## Documentation

- [ ] Replace `git clone <this-repo>` with the final public clone URL.
- [ ] Keep a short Marketplace golden path before the full-mesh instructions.
- [ ] Verify every SQL query against the table names produced by the released runtime.
- [ ] Label Marketplace as the one-click quickstart and the remaining domains as the advanced full-mesh path.
- [ ] Record the tested Databricks CLI version, LakeLogic version, workspace type, and verification date.
- [ ] Include expected successful output and screenshots for catalog, quarantine, SCD2, lineage, SLO checks, and DAG execution.

## Deployment safety

- [ ] Ensure Bash and PowerShell deploy wrappers return a non-zero exit code for every failed deployment.
- [ ] Parameterize or remove placeholder notification recipients.
- [ ] Make cleanup commands clearly identify the catalog they will delete and require deliberate confirmation.
- [ ] Confirm that rerunning bootstrap is idempotent.

## Release evidence

Record the final evidence here:

- Release tag:
- Git commit:
- LakeLogic runtime source/version:
- Databricks CLI version:
- Workspace type:
- Catalog used:
- Bundle validation result:
- Bootstrap run ID:
- Verification query result:
- Teardown result:
- Verified by:
- Verified on:
