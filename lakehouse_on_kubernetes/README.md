# lakehouse_on_kubernetes

The `databricks_lakehouse` medallion pipeline, refactored to the
**production-faithful** pattern: **Airflow orchestrates, Spark executes as its
own pods.** Airflow submits `SparkApplication` CRDs to the **Spark Operator**;
driver + executor pods run the Bronze→Silver→Gold jobs against a MinIO lake —
all on a local **Kind** cluster, scripted up and down.

This is the contrast to the sibling project, where Spark ran *embedded in-process*
inside Airflow (`LocalExecutor`, `.master("local[2]")`). Here the orchestration
and compute are fully separated, exactly as in a real Databricks / EMR / Spark-on-K8s
deployment. **The `lakehouse` Python package (bronze/silver/gold/config logic) is
reused verbatim** — only the execution boundary changed (see `jobs/` and `dags/`).

## What runs where

```
            Kind cluster
┌─────────────────────────────────────────────────────────────────┐
│  Airflow (KubernetesExecutor)        Spark Operator             │
│   scheduler / api-server / dag-       watches lakehouse ns      │
│   processor / triggerer / workers      │                        │
│        │ submits SparkApplication CRD  ▼                        │
│        └──────────────────────────►  driver pod ─► executor pods│
│                                          │ read/write Delta     │
│  Postgres (airflow meta + ods)       MinIO (s3a://lakehouse/...)│
└─────────────────────────────────────────────────────────────────┘
        localhost:8110 = Airflow UI      localhost:9031 = MinIO console
```

See `docs/01_architecture.md` for the before/after and the CRD submission flow.

## Prerequisites

You need these on your PATH (the scripts check and tell you if one is missing):

| tool | install |
|------|---------|
| docker (running) | Docker Desktop |
| **kind** | `brew install kind` |
| **helm** | `brew install helm` |
| kubectl | `brew install kubectl` |
| rsync | preinstalled on macOS |

> `kind` and `helm` are the two most likely to be missing — install them first.

## Quickstart

```bash
make up        # sync code, build images, create cluster, deploy everything (~5-10 min)
make seed      # populate the MinIO landing zone with synthetic Parquet
make trigger   # unpause + run ingest_bronze (chains to silver -> gold via assets)
make status    # watch pods + SparkApplications
make down      # destroy the cluster and all data
```

Then open:
- **Airflow UI** — http://localhost:8110  (admin / admin)
- **MinIO console** — http://localhost:9031  (minioadmin / minioadmin)

Full walkthrough with what to watch at each step: `docs/02_run_end_to_end.md`.

## Reading the lake from your laptop (Polars / marimo)

The lake lives in MinIO *inside* the cluster, so a local Polars / marimo session
can't reach it over the in-cluster DNS. Expose MinIO's S3 API and point delta-rs
at it:

1. **Forward the S3 API to your host.** kind only maps the MinIO *console* (9031),
   not the S3 API (9000), so forward it yourself and leave it running:

   ```bash
   kubectl --context kind-lakehouse -n lakehouse port-forward svc/minio 9000:9000
   ```

2. **Read the table.** The path is an S3 URI `s3://<bucket>/<prefix>` — the gold
   `policy_summary` mart is `s3://lakehouse/gold/policy_summary`:

   ```python
   import polars as pl

   df = pl.read_delta(
       "s3://lakehouse/gold/policy_summary",
       storage_options={
           "AWS_ENDPOINT_URL": "http://localhost:9000",  # the port-forward
           "AWS_ACCESS_KEY_ID": "minioadmin",
           "AWS_SECRET_ACCESS_KEY": "minioadmin",
           "AWS_REGION": "us-east-1",                     # any value; delta-rs requires one
           "AWS_ALLOW_HTTP": "true",                      # MinIO serves http, not https
       },
   )
   ```

   Use the `s3://` scheme (delta-rs), not the Spark-side `s3a://` — same bucket,
   different client. `AWS_ENDPOINT_URL` is what redirects delta-rs from real AWS to
   MinIO; without it the read goes to s3.amazonaws.com. The same pattern reads any
   layer: `s3://lakehouse/silver/chan1/customer`, `s3://lakehouse/bronze/chan1/policy`, …

## How it maps to the source project

| Concern | databricks_lakehouse | lakehouse_on_kubernetes |
|---|---|---|
| Spark execution | embedded, `local[2]` in the task | `SparkApplication` driver+executor pods |
| Airflow executor | LocalExecutor | KubernetesExecutor |
| Task body | `get_spark()` + call function | `SparkKubernetesOperator` submits a CRD |
| Spark session | `spark_utils.session.get_spark` | `jobs/_bootstrap.get_cluster_spark` |
| Medallion logic | `lakehouse` package | **same package, reused verbatim** |
| Lake storage | docker-compose MinIO | MinIO Deployment (`s3a://` unchanged) |
| Config | `config/*.yaml` files | same files, delivered as a ConfigMap |
| Bronze tasks | one per (channel,table) | one SparkApplication per (channel,table) |
| Silver/Gold | single "do all" task | one SparkApplication per table / per mart |

## Layout

```
lakehouse/        synced-from-source package + config (sync-lakehouse.sh)
jobs/             standalone Spark entrypoints (_bootstrap + bronze/silver/gold)
dags/             SparkKubernetesOperator DAGs + _spark_app.py CRD renderer
images/           airflow + spark Dockerfiles
k8s/              namespace, config (secret + configmaps), rbac, postgres, minio, seed
helm/             kind-config + spark-operator + airflow values
scripts/          sync / build / up / down / seed
docs/             architecture, run guide, version-alignment notes
```

## Notes & caveats

- **Chart value schema** can shift between Helm chart versions. The pinned
  versions live in `scripts/_lib.sh` (`AIRFLOW_CHART_VERSION`,
  `SPARK_OPERATOR_CHART_VERSION`). If the Airflow UI isn't on localhost:8110
  (the UI component was renamed webserver→api-server across Airflow 3 charts),
  use `make ui` (kubectl port-forward) as a guaranteed fallback. See
  `docs/03_version_alignment.md`.
- **Watch mode, not deferrable.** The DAGs use `SparkKubernetesOperator` in its
  standard watch-to-completion mode (robust across provider versions). The
  triggerer is deployed, so switching to `deferrable=True` later is a one-line
  change — see `docs/03_version_alignment.md`.
- **Seed date vs run date.** `make seed` writes a `dt=<today>` partition. The DAGs
  derive their run date from the DAG run's timestamp (`dag_run.run_after` — Airflow
  3 manual runs carry no `logical_date`), so trigger the DAG the same day you seed
  and bronze's `--dt` matches the seeded `dt=` partition.
```
