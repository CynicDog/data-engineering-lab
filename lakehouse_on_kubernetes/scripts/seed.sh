#!/usr/bin/env bash
# Seed the MinIO landing zone with synthetic Parquet (re-runnable).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

require_tools kubectl

echo ">> (re)running seed Job"
kc delete job seed-source-data -n "$NAMESPACE" --ignore-not-found
kc apply -f "$ROOT/k8s/seed/seed-data-job.yaml"

echo ">> waiting for seed to complete..."
kc wait --for=condition=complete job/seed-source-data -n "$NAMESPACE" --timeout=300s

echo ">> seed logs:"
kc logs job/seed-source-data -n "$NAMESPACE" || true
echo ">> Landing zone populated. Browse it in the MinIO console (localhost:${MINIO_CONSOLE_HOST_PORT:-9031})."
