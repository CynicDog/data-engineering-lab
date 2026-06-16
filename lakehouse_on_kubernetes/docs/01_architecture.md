# Architecture: from embedded Spark to Spark-on-Kubernetes

## Before (databricks_lakehouse)

```
Airflow (LocalExecutor)
  └─ task: ingest_chan1_customer
       └─ get_spark()  →  SparkSession.master("local[2]")   ← Spark runs IN the
           └─ ingest_table_with_audit(...)                     Airflow process
```

One container. The scheduler process *is* the Spark driver. Simple to run, but
Spark compute cannot scale or fail independently of the orchestrator, and the
JVM competes with Airflow for memory.

## After (this project)

```
┌─────────────────────────────────┐
│ Airflow worker pod              │   KubernetesExecutor spawns one
│   task: ingest_chan1_customer   │   worker pod per task.
│   └─ SparkKubernetesOperator    │
└──────────────┬──────────────────┘
               │ (1) kubectl apply: SparkApplication CRD
               ▼
┌─────────────────────────────────┐
│ Spark Operator                  │   Watches for SparkApplication
│   (controller + webhook)        │   objects and reconciles them.
└──────────────┬──────────────────┘
               │ (2) launches driver from lakehouse/spark:dev
               ▼
┌─────────────────────────────────┐
│ Spark driver pod                │
│   jobs/bronze_job.py            │
│   └─ get_cluster_spark()        │
│        └─ ingest_table_with_audit()
└──────────────┬──────────────────┘
               │ (3) creates executors
               ▼
┌─────────────────────────────────┐
│ Spark executor pods  (x2)       │
└──────────────┬──────────────────┘
               │ (4) read Parquet / write Delta
               ▼
        s3a://lakehouse/...  (MinIO)
```

The Airflow task is now lightweight: render a `SparkApplication` manifest, submit
it, watch it to completion, stream the driver log. All real compute happens in
dedicated, independently-sized pods.

## The submission flow (one task)

1. **DAG parse** — `dags/bronze_dag.py` calls `load_registry()` and creates one
   `SparkKubernetesOperator` per `(channel, table)`. `dags/_spark_app.render_bronze`
   produces the `SparkApplication` YAML (with `{{ ds }}` / `{{ ds_nodash }}`
   macros left for Airflow to template at runtime).
2. **Task run** — the operator (in a KubernetesExecutor worker pod) templates the
   manifest and `kubectl apply`s it via the in-cluster ServiceAccount
   (`airflow-worker`, granted CRD rights by `k8s/rbac/airflow-spark-rbac.yaml`).
3. **Reconcile** — the Spark Operator sees the new `SparkApplication` and launches
   a driver pod from `lakehouse/spark:dev`, running `local:///opt/lakehouse/jobs/bronze_job.py`.
4. **Execute** — the driver (`serviceAccount: spark`) creates 2 executor pods,
   reads Parquet from `s3a://lakehouse/landing/...`, writes Delta to
   `s3a://lakehouse/bronze/...`, and appends an audit row to `control/ingestion_log`.
5. **Complete** — the operator marks the `SparkApplication` `COMPLETED`; the
   Airflow task succeeds and **emits its bronze Asset**, which triggers
   `transform_silver`, which emits silver Assets, which trigger `build_gold`.

## Why the medallion code didn't change

`ingest_table_with_audit`, `transform_table`, and `build_mart` take only a
`spark` handle and `settings`. They never touch the session factory or Spark
config. So the *only* new code is:

- `jobs/_bootstrap.get_cluster_spark()` — a bare `SparkSession.builder` (no
  `.master()`; the Operator injects the k8s master, and Delta/S3A config comes
  from the CRD's `sparkConf`).
- thin argparse wrappers (`jobs/{bronze,silver,gold}_job.py`).

Delta + hadoop-aws jars are **baked into the Spark image**, so there is no
`configure_spark_with_delta_pip` / Maven fetch inside the pods.

## Config & credentials

- **ConfigMap `lakehouse-config`** (source of truth) carries `table_registry.yaml`
  + `dev.yaml`/`prod.yaml`, mounted at `/opt/lakehouse/config` in Airflow pods and
  in every Spark driver/executor (so `parents[3]` resolution finds them).
- **Secret `minio-creds`** carries the S3 credentials, injected as
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (read by S3A's
  `EnvironmentVariableCredentialsProvider`) and `LAKE_*` (read by `load_settings()`).
  Credentials never appear in the rendered manifests or Airflow logs.
