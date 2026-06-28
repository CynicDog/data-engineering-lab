#!/usr/bin/env bash
# Bring up the whole stack on a fresh Kind cluster. Idempotent: safe to re-run.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

require_tools docker kind helm kubectl rsync

# 1. Sync the reusable lakehouse package + config + generator from the source project.
echo "==> [1/8] Syncing lakehouse package from $SOURCE_PROJECT"
"$ROOT/scripts/sync-lakehouse.sh" "$SOURCE_PROJECT"

# 2. Create the Kind cluster (with localhost port mappings) if it doesn't exist.
echo "==> [2/8] Ensuring Kind cluster '$CLUSTER_NAME'"
if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --config "$ROOT/helm/kind-config.yaml"
else
  echo "    cluster already exists."
fi

# 3. Build + load the images.
echo "==> [3/8] Building and loading images"
"$ROOT/scripts/build-images.sh"

# 4. Namespace + Secret + ConfigMaps, all as STATIC manifests under k8s/config/:
#    - minio-creds.yaml             (committed)
#    - lakehouse-config.generated.yaml / pg-init.generated.yaml (rendered by sync
#      in step 1 from the single-source files — inspectable, applied declaratively)
echo "==> [4/8] Namespace, secret, configmaps (static manifests)"
kc apply -f "$ROOT/k8s/namespace.yaml"
kc apply -f "$ROOT/k8s/config/"

# 5. RBAC + ResourceQuota + Postgres + MinIO.
echo "==> [5/8] RBAC, ResourceQuota, Postgres, MinIO"
kc apply -f "$ROOT/k8s/rbac/"
kc apply -f "$ROOT/k8s/resource-quota.yaml"
kc apply -f "$ROOT/k8s/postgres/postgres.yaml"
kc apply -f "$ROOT/k8s/minio/minio.yaml"

echo "    waiting for Postgres + MinIO to be ready (up to 5 min each)..."
_wait_deploy() {
  local deploy=$1 elapsed=0 interval=10 limit=300
  until kc rollout status deploy/"$deploy" -n "$NAMESPACE" --timeout="${interval}s" 2>/dev/null; do
    elapsed=$(( elapsed + interval ))
    if [ "$elapsed" -ge "$limit" ]; then
      echo "ERROR: $deploy not ready after ${limit}s — check: kubectl describe pod -n $NAMESPACE -l app=$deploy"
      exit 1
    fi
    echo "    still waiting for $deploy (${elapsed}s elapsed)..."
  done
}
_wait_deploy postgres
_wait_deploy minio

# 6. Create the lake bucket.
echo "==> [6/8] Creating lake bucket"
kc apply -f "$ROOT/k8s/minio/minio-bucket-job.yaml"
_wait_job() {
  local job=$1 elapsed=0 interval=10 limit=300
  until kc wait --for=condition=complete job/"$job" -n "$NAMESPACE" --timeout="${interval}s" 2>/dev/null; do
    elapsed=$(( elapsed + interval ))
    if [ "$elapsed" -ge "$limit" ]; then
      echo "ERROR: job/$job did not complete after ${limit}s"
      echo "       Logs: kubectl logs -n $NAMESPACE job/$job"
      exit 1
    fi
    echo "    still waiting for job/$job (${elapsed}s elapsed)..."
  done
}
_wait_job minio-bucket-init

# 7. Spark Operator.
echo "==> [7/8] Installing Spark Operator (chart $SPARK_OPERATOR_CHART_VERSION)"
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update >/dev/null
helm --kube-context "kind-${CLUSTER_NAME}" upgrade --install spark-operator \
  spark-operator/spark-operator \
  --version "$SPARK_OPERATOR_CHART_VERSION" \
  --namespace spark-operator --create-namespace \
  -f "$ROOT/helm/spark-operator-values.yaml" \
  --wait --timeout 5m

# 8. Airflow.
echo "==> [8/8] Installing Airflow (chart $AIRFLOW_CHART_VERSION)"
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
helm repo update >/dev/null
helm --kube-context "kind-${CLUSTER_NAME}" upgrade --install airflow \
  apache-airflow/airflow \
  --version "$AIRFLOW_CHART_VERSION" \
  --namespace "$NAMESPACE" \
  -f "$ROOT/helm/airflow-values.yaml"

echo "    waiting for Airflow control plane (up to 5 min each)..."
_wait_deploy airflow-scheduler
_wait_deploy airflow-dag-processor
_wait_deploy airflow-api-server

cat <<EOF

================================================================
  Lakehouse on Kubernetes is up.

  Airflow UI : http://localhost:${AIRFLOW_UI_HOST_PORT:-8110}   (admin / admin)
  MinIO      : http://localhost:${MINIO_CONSOLE_HOST_PORT:-9031} ($LAKE_S3_ACCESS_KEY / $LAKE_S3_SECRET_KEY)

  Next:
    make seed       # populate the landing zone with synthetic data
    make trigger    # unpause + run the ingest_bronze DAG
    make status     # watch pods + SparkApplications

  If the UI isn't reachable on localhost (chart service naming varies):
    make ui         # kubectl port-forward fallback
================================================================
EOF
