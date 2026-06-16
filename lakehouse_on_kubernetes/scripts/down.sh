#!/usr/bin/env bash
# Tear everything down. Deleting the Kind cluster removes all workloads AND the
# PVCs (data lives inside the kind node container), so this is a clean reset.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

require_tools kind

# Best-effort helm uninstalls first (harmless if the cluster is already gone).
if command -v helm >/dev/null 2>&1 && kind get clusters | grep -qx "$CLUSTER_NAME"; then
  helm --kube-context "kind-${CLUSTER_NAME}" uninstall airflow -n "$NAMESPACE" 2>/dev/null || true
  helm --kube-context "kind-${CLUSTER_NAME}" uninstall spark-operator -n spark-operator 2>/dev/null || true
fi

if kind get clusters | grep -qx "$CLUSTER_NAME"; then
  echo ">> Deleting Kind cluster '$CLUSTER_NAME' (removes all data)"
  kind delete cluster --name "$CLUSTER_NAME"
else
  echo ">> Cluster '$CLUSTER_NAME' not found — nothing to delete."
fi
echo ">> Done."
