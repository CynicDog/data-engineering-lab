# Airflow image — orchestration only. No JVM, no PySpark, no Delta:
# Spark now runs in dedicated Operator pods, so the scheduler/worker stay lean.
#
# Build context is the PROJECT ROOT (see scripts/build-images.sh):
#   docker build -f images/airflow/Dockerfile -t lakehouse/airflow:dev .
FROM apache/airflow:3.0.2-python3.12

ARG AIRFLOW_VERSION=3.0.2
ARG PYTHON_VERSION=3.12

# Provider/dependency set this image needs:
#   cncf-kubernetes — SparkKubernetesOperator, submits SparkApplication CRDs.
#   amazon          — S3 remote-logging backend; task logs ship to MinIO (S3 API)
#                     so they outlive the ephemeral KubernetesExecutor worker pods.
#   pyyaml          — DAGs render SparkApplication manifests and parse the table
#                     registry at parse time.
# Constraints are pinned to this Airflow version so the providers stay compatible.
RUN pip install --no-cache-dir \
        "apache-airflow-providers-cncf-kubernetes" \
        "apache-airflow-providers-amazon" \
        "pyyaml>=6.0" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# The lakehouse package + config are needed at DAG-PARSE time: the DAGs call
# load_registry() to generate per-(channel,table) tasks. parents[3] resolution
# expects config at /opt/lakehouse/config. The lakehouse-config ConfigMap mounts
# over this path at runtime (source of truth); the baked copy is a fallback.
ENV PYTHONPATH=/opt/lakehouse/src
COPY --chown=airflow:root lakehouse/src /opt/lakehouse/src
COPY --chown=airflow:root lakehouse/config /opt/lakehouse/config

# DAGs (+ the _spark_app renderer helper). jobs/ are NOT copied here — they run
# in the Spark image, not in Airflow.
COPY --chown=airflow:root dags/ /opt/airflow/dags/
