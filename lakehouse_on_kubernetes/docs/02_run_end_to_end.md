# Running the scenario end to end

## 0. Prerequisites

`docker` (running), `kind`, `helm`, `kubectl`, `rsync`. Install the missing ones
(`brew install kind helm kubectl`). The scripts check and tell you.

## 1. Bring up the stack

```bash
make up
```

This runs `scripts/up.sh`, which:
1. syncs the `lakehouse` package + config from `../databricks_lakehouse`,
2. creates the Kind cluster `lakehouse` (with localhost port mappings),
3. builds + loads the `lakehouse/airflow:dev` and `lakehouse/spark:dev` images,
4. creates the namespace, `minio-creds` Secret, `lakehouse-config` + `pg-init` ConfigMaps,
5. applies RBAC, Postgres, MinIO and waits for them,
6. runs the bucket-init Job,
7. `helm install`s the Spark Operator,
8. `helm install`s Airflow (KubernetesExecutor, external Postgres).

When it finishes:
- Airflow UI → http://localhost:8110 (admin / admin)
- MinIO console → http://localhost:9031 (minioadmin / minioadmin)

If the Airflow UI doesn't load on 8110 (chart service-name differences), run
`make ui` for a port-forward fallback.

## 2. Seed the landing zone

```bash
make seed
```

Runs `generate_source_data.py` (in a Spark-image Job) to write synthetic Parquet
to `s3://lakehouse/landing/{chan1,chan2}/...` for **today**. Verify in the MinIO
console: `lakehouse/landing/chan1/customer/dt=<today>/part.parquet`.

> Trigger the DAG the **same day** you seed — bronze ingests the `dt={{ ds }}`
> partition, and `{{ ds }}` for a manual run is today's date.

## 3. Trigger the pipeline

```bash
make trigger
```

Unpauses and triggers `ingest_bronze`. Watch the Spark pods appear:

```bash
make status                                   # pods + SparkApplications
kubectl get sparkapplications -n lakehouse -w  # live CRD state
kubectl get pods -n lakehouse -w               # driver + executor pods
```

You'll see, per `(channel, table)`:
- a `bronze-<ch>-<table>-<date>-driver` pod, then 2 executor pods,
- the `SparkApplication` go `SUBMITTED → RUNNING → COMPLETED`.

Stream a driver's log:
```bash
make logs                  # newest driver pod
# or a specific one:
kubectl logs -n lakehouse bronze-chan1-customer-<date>-driver
```

## 4. Watch the Asset chain

In the Airflow UI (Assets / DAG graph):
- each successful bronze task emits `s3://lakehouse/bronze/<ch>/<table>` →
  `transform_silver` starts automatically,
- each silver task emits `s3://lakehouse/silver/<ch>/<table>` →
  `build_gold` starts automatically,
- gold runs one SparkApplication per mart (`voc_daily`, `policy_summary`,
  `claims_analysis`).

No flag files, no polling — the Asset graph is the source of truth.

## 5. Verify the output

In the MinIO console, confirm Delta tables exist:
- `lakehouse/bronze/chan1/customer/` (partitioned by `_dt`)
- `lakehouse/silver/chan1/customer/`
- `lakehouse/gold/voc_daily/`, `gold/policy_summary/`, `gold/claims_analysis/`
- `lakehouse/control/ingestion_log/` (the audit Delta table)

To query gold with Spark, run a one-off:
```bash
kubectl run spark-shell -n lakehouse --rm -it --image=lakehouse/spark:dev \
  --overrides='{"spec":{"serviceAccountName":"spark"}}' -- \
  /opt/spark/bin/pyspark
# then in the shell, configure s3a + read s3a://lakehouse/gold/voc_daily
```
(Easier: just browse the parquet/Delta files in the MinIO console.)

## 6. Tear down

```bash
make down
```

Deletes the Kind cluster and all data (PVCs live inside the kind node). A fresh
`make up` rebuilds from scratch.

## Troubleshooting

| symptom | check |
|---|---|
| UI not on :8110 | `make ui` fallback; `kubectl get svc -n lakehouse` |
| SparkApplication stuck `SUBMITTED` | `kubectl describe sparkapplication <name> -n lakehouse`; operator logs in `spark-operator` ns |
| driver `ImagePullBackOff` | image not loaded: re-run `make build` (must `kind load`) |
| bronze finds 0 rows | seed date ≠ run `{{ ds }}`; re-`make seed` today and re-trigger |
| `ModuleNotFoundError: lakehouse` | PYTHONPATH/config mount — confirm `lakehouse-config` ConfigMap is mounted at `/opt/lakehouse/config` |
| S3A `403`/auth | `minio-creds` Secret keys vs MinIO root creds mismatch |
