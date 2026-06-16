#!/usr/bin/env bash
# Build the Airflow + Spark images and load them into the Kind cluster.
# Build context is the PROJECT ROOT so both Dockerfiles can COPY lakehouse/ etc.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

require_tools docker kind

echo ">> Building Airflow image: $AIRFLOW_IMAGE"
docker build -f "$ROOT/images/airflow/Dockerfile" -t "$AIRFLOW_IMAGE" "$ROOT"

echo ">> Building Spark image: $SPARK_IMAGE"
docker build -f "$ROOT/images/spark/Dockerfile" -t "$SPARK_IMAGE" "$ROOT"

echo ">> Loading images into Kind cluster '$CLUSTER_NAME'"
kind load docker-image "$AIRFLOW_IMAGE" --name "$CLUSTER_NAME"
kind load docker-image "$SPARK_IMAGE" --name "$CLUSTER_NAME"

echo ">> Images loaded."
