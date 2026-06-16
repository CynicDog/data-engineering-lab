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

## What triggers what — two separate planes

The thing that makes this confusing is that "trigger" means two completely
different mechanisms here, and one Airflow task straddles both. Keep them apart:

- **Plane A — Asset events (Airflow-internal, no Kubernetes).** Airflow Assets are
  just named strings in the scheduler's metadata DB (e.g.
  `s3://lakehouse/bronze/chan1/customer`). They are *not* files and nothing reads
  S3 — when a task with that string in its `outlets` succeeds, Airflow records an
  "asset produced" event. Any DAG whose `schedule=` lists that asset becomes
  eligible to run. This is what advances the **bronze → silver → gold** chain.
  Pure bookkeeping; no pod is created by an asset event itself.

- **Plane B — CRD reconciliation (Kubernetes).** When a *task* actually runs, it
  creates a `SparkApplication` object in the Kubernetes API. The Spark Operator
  watches for those objects and creates the driver/executor **pods**. This is the
  only plane where Kubernetes resources come into existence.

A single Airflow **task** is the hinge between them: a Plane-A event makes its DAG
run → the task executes and drives Plane B (pods) → on success the task emits the
*next* Plane-A event. Mantra: **assets trigger DAGs; tasks create pods.**

### Plane A: the asset chain (DAG → DAG)

```
ingest_bronze  ──emits──▶  s3://…/bronze/<ch>/<tbl>  (one asset per table)
                                      │  schedule=
                                      ▼
transform_silver ─emits─▶  s3://…/silver/<ch>/<tbl>  (one asset per table)
                                      │  schedule=
                                      ▼
build_gold       ─emits─▶  s3://…/gold/<mart>
```

- `ingest_bronze` is the only **clock-driven** DAG (`schedule="@daily"`). The other
  two have `schedule=[…asset list…]`, so they have no clock of their own — they run
  only in response to upstream asset events.
- **OR semantics:** `transform_silver` lists *all* bronze assets, so *any* bronze
  task finishing makes it eligible (Airflow 3 fires on the first satisfying event,
  not after all of them). `build_gold` likewise keys off silver assets. Defined in
  `bronze_dag.BRONZE_ASSETS` / `silver_dag.SILVER_ASSETS` / `gold_dag.GOLD_ASSETS`.
- The asset *strings* are conventions, not storage paths. Nothing enforces that
  `s3://lakehouse/bronze/...` corresponds to where Spark actually wrote — the
  matching is purely string-equality between a task's `outlets` and a DAG's
  `schedule`.

### Plane B: one task → Kubernetes resources

This is the part that materializes pods. Trace a single bronze task:

1. **DAG parse** — `dags/bronze_dag.py` calls `load_registry()` and creates one
   `SparkKubernetesOperator` per `(channel, table)`. `_spark_app.render_bronze`
   builds the `SparkApplication` YAML *as a string*, leaving Jinja macros
   (`{{ (dag_run.logical_date or dag_run.run_after).strftime(...) }}`) for Airflow
   to template at task runtime. Nothing has touched Kubernetes yet.
2. **Task scheduled** — the Plane-A event (or the `@daily` clock for bronze) makes
   the DAG run. KubernetesExecutor creates a **worker pod** for the task
   (`executor: KubernetesExecutor`, `helm/airflow-values.yaml`). The worker pod runs
   as ServiceAccount `airflow-worker`.
3. **Submit the CRD** — inside that worker pod, `SparkKubernetesOperator`
   (`in_cluster=True`, `kubernetes_conn_id=None`) templates the manifest and POSTs
   a `SparkApplication` object to the Kubernetes API. It is allowed to because
   `k8s/rbac/airflow-spark-rbac.yaml` binds `airflow-worker` to a Role with
   `create`/`watch` on `sparkapplications`. **No pod yet — just an API object.**
4. **Reconcile** — the **Spark Operator** (installed by `helm/spark-operator-values.yaml`,
   watching `jobNamespaces: [lakehouse]`) sees the new `SparkApplication` and creates
   the **driver pod** from `lakehouse/spark:dev`, running
   `local:///opt/lakehouse/jobs/bronze_job.py` as ServiceAccount `spark`.
5. **Fan out executors** — the driver (`bronze_job.py` → `get_cluster_spark()`)
   asks the k8s master for executors; it creates **2 executor pods** itself. The
   `spark` SA can do this because `k8s/rbac/spark-rbac.yaml` grants it
   create/delete on `pods`/`services`/`configmaps`. Executors read Parquet from
   `s3a://lakehouse/landing/...`, write Delta to `s3a://lakehouse/bronze/...`.
6. **Watch + complete** — back in the worker pod, the operator (`get_logs=True`)
   watches the `SparkApplication.status` and streams the driver log into the Airflow
   task log. When status flips to `COMPLETED`, the driver/executor pods exit, the
   operator garbage-collects them, and the **Airflow task succeeds**.
7. **Emit the next asset** — task success publishes the task's `outlets` asset
   (Plane A again), and the cycle re-enters at the silver DAG.

So the resource count for **one** bronze table is: 1 worker pod (Airflow) → 1
`SparkApplication` (API object) → 1 driver pod + 2 executor pods (Spark). The
concurrency caps in `airflow-values.yaml` (`MAX_ACTIVE_TASKS_PER_DAG=2`,
`PARALLELISM=4`) exist precisely because that multiplier (×4 pods per task) can
blow past a single 8 GB kind node if every table ran at once.

## Who creates each Kubernetes object

Reading "what runs what" off the manifests:

| Object | Kind | Created by | Runs as (SA) | Lifetime |
|---|---|---|---|---|
| worker pod | Pod | Airflow scheduler (KubernetesExecutor) | `airflow-worker` | one Airflow task |
| `SparkApplication` | CRD object | `SparkKubernetesOperator` in the worker pod | — (API object) | until operator GCs it |
| driver pod | Pod | **Spark Operator** (reconciling the CRD) | `spark` | one Spark job |
| executor pods (×2) | Pod | the **driver** | `spark` (inherited) | one Spark job |
| `lakehouse-config` | ConfigMap | `scripts/up.sh` (static apply) | — | cluster lifetime |
| `minio-creds` | Secret | `scripts/up.sh` (static apply) | — | cluster lifetime |

Two RBAC grants make the chain legal, and they are deliberately split:
`airflow-worker` may **submit** SparkApplications but cannot create pods directly;
`spark` may **create pods** (executors) but knows nothing about the CRD. Neither
service account can do the other's job.

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
