# Version alignment & known risks

Spark-on-Kubernetes is sensitive to version skew across four boundaries: the
Spark runtime, the Delta jars, the Hadoop/S3A jars, and the orchestration
charts. These are the versions this project locks to and why.

## Locked versions

| component | version | set in | why |
|---|---|---|---|
| Spark (base image) | `3.5.3` | `images/spark/Dockerfile` (`SPARK_BASE`) | must equal the source project's `pyspark 3.5.3`; a Py4J/protocol mismatch otherwise |
| delta-spark / delta-storage | `3.3.0` (Scala 2.12) | `images/spark/Dockerfile` (`DELTA`) | Delta 3.3.0 targets Spark 3.5.x; jars must match the base image's Scala 2.12 build |
| hadoop-aws | `3.3.4` | `images/spark/Dockerfile` (`HADOOP_AWS`) | must equal Spark 3.5's bundled Hadoop; a different version → `NoSuchMethodError` in S3A |
| aws-java-sdk-bundle | `1.12.262` | `images/spark/Dockerfile` (`AWS_SDK`) | the SDK paired with hadoop-aws 3.3.4 |
| Java | 17 | base image tag | source uses OpenJDK 17; Spark 3.5 supports it (the image sets the needed `--add-opens`) |
| Airflow | `3.0.2` | `images/airflow/Dockerfile` | same as source |
| cncf-kubernetes provider | from Airflow 3.0.2 constraints | `images/airflow/Dockerfile` | provider/SDK imports changed in Airflow 3.x; the constraints file pins a compatible version |
| Airflow Helm chart | `1.16.0` | `scripts/_lib.sh` (`AIRFLOW_CHART_VERSION`) | the value schema in `helm/airflow-values.yaml` matches this chart |
| Spark Operator chart | `2.1.0` | `scripts/_lib.sh` (`SPARK_OPERATOR_CHART_VERSION`) | kubeflow chart; uses the `spark.jobNamespaces` (list) value schema and `sparkoperator.k8s.io/v1beta2` CRDs |

The `apiVersion: sparkoperator.k8s.io/v1beta2` in `dags/_spark_app.py` must match
the CRD version the pinned operator chart installs.

## Known risks / things to verify on first run

1. **Spark base image tag.** `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu`
   is assumed to exist on Docker Hub. If the build fails on `FROM`, list the
   available tags and adjust `SPARK_BASE` (e.g. `3.5.3-python3`). Keep Scala 2.12
   + Java 17 to match the baked Delta jars.

2. **Airflow chart UI component name.** Across the Airflow 3 charts the UI moved
   from `webserver` to `api-server`. `helm/airflow-values.yaml` sets a NodePort
   under **both** keys; the chart ignores the one it doesn't recognise. If
   neither lands on `localhost:8110`, use `make ui` (port-forward) — it discovers
   the service by name pattern and always works.

3. **Airflow component ServiceAccount names.** `k8s/rbac/airflow-spark-rbac.yaml`
   binds `airflow-worker` / `airflow-scheduler` / `airflow-triggerer`. These hold
   when the Helm release is named `airflow` (as `up.sh` does). If you rename the
   release, update the RoleBinding subjects, or the worker pods will get a 403
   creating SparkApplications.

4. **Spark Operator SA management.** We create the `spark` SA + driver RBAC
   ourselves and tell the chart **not** to (`spark.serviceAccount.create: false`,
   `spark.rbac.create: false`). If the chart still tries to create a `spark` SA
   and the install fails on a conflict, that flag name changed — check the
   chart's `values.yaml` and disable accordingly.

5. **`pip` on the Spark base image (PEP 668).** If `python3 -m pip install` fails
   with "externally-managed-environment", add `--break-system-packages` to the
   pip command in `images/spark/Dockerfile`. (Not needed on the assumed Ubuntu
   22.04 base.)

## Optional upgrade: deferrable execution

The DAGs use `SparkKubernetesOperator` in standard watch-to-completion mode (the
worker pod lives for the job's duration). The triggerer is already deployed, so
to free the worker slot during long Spark runs, set `deferrable=True` on the
operators in `dags/*.py` — **after** confirming your provider version's
`SparkKubernetesOperator` accepts it. This is the only change needed; the
triggerer handles the async CRD watch.
