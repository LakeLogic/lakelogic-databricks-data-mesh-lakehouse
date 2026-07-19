#!/usr/bin/env bash
# deploy.sh — self-healing bundle deploy (macOS / Linux)
#
# Databricks Asset Bundles cache Terraform state per TARGET, not per workspace.
# If you deploy this target to one workspace and later point it at another
# (common with trial workspaces), the cached state is stale and deploy fails with
# "workspace_id mismatch". This wrapper detects that one error, clears the stale
# state for the target, and redeploys — so you never have to do it by hand.
#
# Usage:   ./deploy.sh                 # target=dev, profile=rideflow_dev
#          ./deploy.sh dev my_profile
set -uo pipefail
cd "$(dirname "$0")"

TARGET="${1:-dev}"
PROFILE="${2:-rideflow_dev}"

out=$(databricks bundle deploy -t "$TARGET" -p "$PROFILE" 2>&1); code=$?
echo "$out"

if [ "$code" -eq 0 ]; then
  exit 0
fi

if echo "$out" | grep -q "workspace_id mismatch"; then
  echo ""
  echo ">> Workspace changed for target '$TARGET' — clearing stale Terraform state and redeploying..."
  rm -rf ".databricks/bundle/$TARGET"
  databricks bundle deploy -t "$TARGET" -p "$PROFILE"
  exit $?
fi

echo ">> Bundle deployment failed. No automatic recovery applies." >&2
exit "$code"
