# Use a different Unity Catalog catalog

`rideflow_dev_demo` is the default demo catalog, not a required name. The repository keeps the catalog name as a single configuration value so schemas, Volumes, registry paths, and tables can follow it consistently.

The target catalog must exist and be writable. On workspaces that use Default Storage, create it once in the Databricks UI or reuse an existing catalog such as `workspace`.

## Asset Bundle and jobs

Set `catalog` in:

```text
databricks/databrick_variables.yml
```

You can also override it for a target in:

```text
databricks/databricks.yml
```

The value flows into the deployed job registry paths:

```text
/Volumes/<catalog>/nondelta/_contracts/<domain>/<system>/_system.yaml
```

The drivers derive the catalog from that registry path, so tables, schemas, and operational Volumes are created in the same catalog.

## Manual notebooks

For `00_setup` or `provision_all`, set the `catalog` widget to the catalog you want to use.

For `pipeline_driver.py` and `test_data_driver.py`, set `registry_path` explicitly:

```text
/Volumes/<your-catalog>/nondelta/_contracts/marketplace/rideflow/_system.yaml
```

## Required permissions

To create a new catalog, the identity running setup needs metastore `CREATE CATALOG`.

If you do not have that privilege:

1. Ask an administrator to create an empty catalog using Default Storage.
2. Grant your identity permission to create schemas and Volumes inside it.
3. Set the repository `catalog` value to that existing catalog.
4. Run setup again.

When the catalog already exists, setup adds the required schemas and Volumes instead of requiring permission to create another catalog.

## Verify the target before deployment

Authenticate with an explicit Databricks profile and verify the current identity:

```bash
databricks auth login --host https://<your-workspace>.azuredatabricks.net -p rideflow_dev
databricks -p rideflow_dev current-user me
```

Pass `-p rideflow_dev` to bundle commands or set `DATABRICKS_CONFIG_PROFILE=rideflow_dev`. Otherwise, the CLI may use the default profile and target a different workspace.

## Clean up a renamed catalog

Replace the default name in the deletion command:

```bash
databricks catalogs delete <your-catalog> --force -p rideflow_dev
```

Catalog deletion does not remove bundle-managed jobs or synced workspace files. Run `databricks bundle destroy` separately when the Asset Bundle was deployed.
