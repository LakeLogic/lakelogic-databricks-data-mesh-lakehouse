# deploy.ps1 — self-healing bundle deploy (Windows / PowerShell)
#
# Databricks Asset Bundles cache Terraform state per TARGET, not per workspace.
# If you deploy this target to one workspace and later point it at another
# (common with trial workspaces), the cached state is stale and deploy fails with
# "workspace_id mismatch". This wrapper detects that one error, clears the stale
# state for the target, and redeploys — so you never have to do it by hand.
#
# Usage:   .\deploy.ps1                    (target=dev, profile=rideflow_dev)
#          .\deploy.ps1 -Target dev -Profile my_profile
param(
  [string]$Target  = "dev",
  [string]$Profile = "rideflow_dev"
)
Set-Location $PSScriptRoot

$out = databricks bundle deploy -t $Target -p $Profile 2>&1 | Out-String
$code = $LASTEXITCODE
Write-Host $out

if ($code -eq 0) {
  exit 0
}

if ($out -match "workspace_id mismatch") {
  Write-Host "`n>> Workspace changed for target '$Target' — clearing stale Terraform state and redeploying..." -ForegroundColor Yellow
  Remove-Item -Recurse -Force ".databricks\bundle\$Target" -ErrorAction SilentlyContinue
  databricks bundle deploy -t $Target -p $Profile
  exit $LASTEXITCODE
}

Write-Error "Bundle deployment failed. No automatic recovery applies."
exit $code
