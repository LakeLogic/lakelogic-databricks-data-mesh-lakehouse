# Troubleshooting the RideFlow Databricks demo

## Bundle root not found

**Error**

```text
unable to locate bundle root: databricks.yml not found
```

Run every `databricks bundle` command from the repository's `databricks/` directory:

```bash
cd lakelogic-databricks-data-mesh/databricks
ls databricks.yml
```

On Windows PowerShell or Command Prompt, use `dir databricks.yml` for the check.

## Legacy credentials are ignored

**Error**

```text
stored credentials from older CLI versions are no longer used
```

Re-authenticate and complete the browser login:

```bash
databricks auth login --host https://<your-workspace>.azuredatabricks.net -p rideflow_dev
```

Do not interrupt the browser flow. Alternatively, create a personal access token in **Workspace Settings > Developer > Access tokens** and configure it:

```bash
databricks configure --host https://<your-workspace>.azuredatabricks.net --profile rideflow_dev
```

If required as a last resort, preserve the file-based cache with `DATABRICKS_AUTH_STORAGE=plaintext`.

## Deployment targets the wrong workspace

The bundle uses the default Databricks profile unless you specify another profile.

Pass the profile to every command:

```bash
databricks bundle deploy -t dev -p rideflow_dev
databricks bundle run rideflow_demo_bootstrap -t dev -p rideflow_dev
```

You can instead set `DATABRICKS_CONFIG_PROFILE=rideflow_dev` in the shell running the commands.

Verify the current identity before deployment:

```bash
databricks -p rideflow_dev current-user me
```

## Profile conflicts with host

**Error**

```text
--profile conflicts with --host
```

A configured profile already carries its workspace host. Do not pass `--host` and `-p` to the same operational command. To change the host stored for a profile, run `databricks auth login` or `databricks configure` again with the new host.

## Remove an obsolete profile

The Databricks CLI does not provide a `profiles delete` command. Profiles are stored in:

- Windows: `%USERPROFILE%\.databrickscfg`
- macOS and Linux: `~/.databrickscfg`

Remove the relevant `[profile-name]` section and its properties, stopping before the next section header.

## Catalog creation fails

Typical causes are missing `CREATE CATALOG` permission or a workspace using Default Storage where a bare catalog creation cannot allocate storage.

Use one of these approaches:

1. In Databricks, open **Catalog > Create catalog**, create `rideflow_dev_demo` with Default Storage, and rerun setup.
2. Reuse a writable existing catalog such as `workspace` and update the repository configuration as described in [Catalog configuration](catalog-configuration.md).
3. Ask an administrator to create the catalog and grant the required permissions.

## Workspace ID mismatch

**Error**

```text
workspace_id mismatch: provider is configured for workspace X but got Y
```

The local bundle state points to a different workspace, often after changing trial or development workspaces.

When possible, switch back to the original workspace and run `bundle destroy` first. Then remove the stale `databricks/.databricks/` state directory and redeploy to the intended workspace.

The repository wrappers can perform the recovery:

```powershell
./deploy.ps1
```

```bash
./deploy.sh
```

They detect the mismatch, clear the stale local bundle state, and retry deployment.

## Bundle destroy leaves resources behind

`bundle destroy` removes bundle-managed jobs and synced workspace files. It does not remove Unity Catalog objects created by the runtime setup notebook.

Remove both resource groups:

```bash
databricks bundle destroy -t dev -p rideflow_dev
databricks catalogs delete rideflow_dev_demo --force -p rideflow_dev
```

If jobs or files remain because the bundle state was reset or provisioning used the notebook path, remove them explicitly.

Workspace files:

```bash
databricks workspace delete /Workspace/Shared/_data_platform_rideflow_demo --recursive -p rideflow_dev
```

List the jobs carrying the `lakelogic` tag, then delete the returned job IDs:

```bash
databricks jobs list --output json -p rideflow_dev \
  | jq -r '.[] | select(.settings.tags.lakelogic != null) | .job_id'

databricks jobs delete <job-id> -p rideflow_dev
```

## Verify before destructive cleanup

Inspect bundle resources before removal:

```bash
databricks bundle summary -t dev -p rideflow_dev
```

The catalog deletion command uses `--force` and removes the catalog's schemas, Volumes, tables, and data. Confirm the catalog name before running it.
