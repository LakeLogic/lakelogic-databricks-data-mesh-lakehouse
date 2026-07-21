# Provision RideFlow directly in Databricks

Use this path when you want to inspect the catalog and tables without installing the Databricks CLI or deploying an Asset Bundle.

## What this path does

The `provision_all` notebook creates the catalog, domain schemas, Unity Catalog Volumes, landing folders, and empty Bronze, Silver, and Gold tables directly from the contracts.

It does **not** deploy the schedulable jobs that come with the Databricks Asset Bundle. Use the main README Quickstart when you want the complete job-based demo.

## Provision the structures

1. In Databricks, open **Workspace > Repos > Add Repo**.
2. Clone this GitHub repository.
3. Open `databricks/notebooks/_ops/provision_all`.
4. Attach the notebook to serverless compute or another Unity Catalog-enabled cluster.
5. Review the widgets:
   - `catalog` defaults to `rideflow_dev_demo`.
   - Table creation can be disabled when you only want schemas and Volumes.
6. Select **Run All**.

The notebook is idempotent, so it is safe to run again.

## Generate and process data

After provisioning, use the notebooks described in [Run the demo step by step](manual-run.md):

1. Run `test_data_driver.py` to generate synthetic landing data.
2. Run `pipeline_driver.py` to process Bronze, Silver, and Gold.

Alternatively, deploy the Asset Bundle later and run one of its domain orchestrator jobs.

## Which setup path should you choose?

| Goal | Recommended path |
| --- | --- |
| See the catalog, schemas, Volumes, and tables quickly | `provision_all` notebook |
| Run the complete scheduled workflow | Asset Bundle Quickstart in the README |
| Inspect every stage manually | [Manual run guide](manual-run.md) |

## Clean up

The notebook creates Unity Catalog objects at runtime. Remove them when finished:

```bash
databricks catalogs delete rideflow_dev_demo --force -p rideflow_dev
```

If you later deployed the Asset Bundle, also run `databricks bundle destroy` from the `databricks/` directory to remove its jobs and synced workspace files.
