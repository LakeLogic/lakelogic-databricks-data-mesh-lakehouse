#!/usr/bin/env bash
# upload_lakelogic_wheel.sh — put an UNRELEASED LakeLogic build on Databricks.
#
# The notebooks install `lakelogic` from PyPI, so nothing here can exercise a
# change until it is published — which is the wrong order: a bad release is
# discovered by the demo breaking, after it is public and immutable. This builds
# the wheel from a local checkout and uploads it to the catalog's `_wheels`
# Volume, so the same notebooks can run against it first.
#
# Usage:  ./upload_lakelogic_wheel.sh [profile] [catalog] [lakelogic_repo]
# Default: rideflow_dev  rideflow_dev_demo  ../../lakelogic
#
# Then run a notebook with the `lakelogic_wheel` widget set to the printed path.
# Leave that widget blank and the notebook installs from PyPI exactly as before.
set -euo pipefail
cd "$(dirname "$0")"

PROFILE="${1:-rideflow_dev}"
CATALOG="${2:-rideflow_dev_demo}"
REPO="${3:-$(cd .. && pwd)/../lakelogic}"
VOLUME="/Volumes/${CATALOG}/nondelta/_wheels"

if [ ! -f "${REPO}/pyproject.toml" ]; then
  echo "ERROR: no LakeLogic checkout at ${REPO}" >&2
  echo "       pass it explicitly: $0 ${PROFILE} ${CATALOG} /path/to/lakelogic" >&2
  exit 1
fi

# Use the LakeLogic checkout's OWN interpreter. A bare `python` picks whatever is
# first on PATH, which on this machine is a sibling project's .venv - the same trap
# that made release.bat run its test gate in the wrong environment.
PY="python"
if [ -x "${REPO}/.venv/Scripts/python.exe" ]; then PY="${REPO}/.venv/Scripts/python.exe"   # Windows
elif [ -x "${REPO}/.venv/bin/python" ];   then PY="${REPO}/.venv/bin/python"; fi

echo "==> Building wheel from ${REPO}"
echo "    python: ${PY}"
( cd "${REPO}" && "${PY}" -m build --wheel >/dev/null )
WHEEL_PATH="$(ls -t "${REPO}"/dist/lakelogic-*.whl | head -1)"
WHEEL="$(basename "${WHEEL_PATH}")"
echo "    ${WHEEL}"

echo "==> Ensuring ${VOLUME} exists"
databricks volumes create "${CATALOG}" nondelta _wheels MANAGED -p "${PROFILE}" >/dev/null 2>&1 || true

echo "==> Uploading"
( cd "${REPO}/dist" && databricks fs cp "${WHEEL}" "dbfs:${VOLUME}/${WHEEL}" --overwrite -p "${PROFILE}" )

echo
echo "Uploaded. Run a notebook with:"
echo "    lakelogic_wheel = ${VOLUME}/${WHEEL}"
echo
echo "NOTE: the wheel carries the version in pyproject.toml, so an unreleased build"
echo "      shares its number with the last release. Check the commit, not the version."
