#!/usr/bin/env bash
# Shared helpers sourced by the other scripts: env loading + tool checks.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load .env if present, else fall back to .env.example defaults.
if [[ -f "$ROOT/.env" ]]; then
  set -a; . "$ROOT/.env"; set +a
elif [[ -f "$ROOT/.env.example" ]]; then
  set -a; . "$ROOT/.env.example"; set +a
fi

CLUSTER_NAME="${CLUSTER_NAME:-lakehouse}"
NAMESPACE="${NAMESPACE:-lakehouse}"
AIRFLOW_IMAGE="${AIRFLOW_IMAGE:-lakehouse/airflow:dev}"
SPARK_IMAGE="${SPARK_IMAGE:-lakehouse/spark:dev}"
LAKE_S3_ACCESS_KEY="${LAKE_S3_ACCESS_KEY:-minioadmin}"
LAKE_S3_SECRET_KEY="${LAKE_S3_SECRET_KEY:-minioadmin}"
SOURCE_PROJECT="${SOURCE_PROJECT:-$ROOT/../databricks_lakehouse}"

# Pinned chart versions — these make the values files' schema valid.
# Chart 1.18.0 is the first official chart with appVersion 3.0.2 (matches our
# Airflow image). 1.16.0 and earlier are Airflow 2 charts and ship an
# airflow.www-based local_settings that crashes on Airflow 3.
AIRFLOW_CHART_VERSION="${AIRFLOW_CHART_VERSION:-1.18.0}"
SPARK_OPERATOR_CHART_VERSION="${SPARK_OPERATOR_CHART_VERSION:-2.1.0}"

need() {
  command -v "$1" >/dev/null 2>&1 && return 0
  echo "ERROR: required tool '$1' not found on PATH." >&2
  case "$1" in
    kind) echo "  install: brew install kind   (or see https://kind.sigs.k8s.io/)" >&2 ;;
    helm) echo "  install: brew install helm   (or see https://helm.sh/docs/intro/install/)" >&2 ;;
    kubectl) echo "  install: brew install kubectl" >&2 ;;
    docker) echo "  install: Docker Desktop, and make sure the daemon is running." >&2 ;;
  esac
  return 1
}

require_tools() {
  local missing=0
  for t in "$@"; do need "$t" || missing=1; done
  [[ $missing -eq 0 ]] || { echo "Install the missing tool(s) above and re-run." >&2; exit 1; }
}

kc() { kubectl --context "kind-${CLUSTER_NAME}" "$@"; }
