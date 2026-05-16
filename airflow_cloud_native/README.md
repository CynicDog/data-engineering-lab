# airflow_cloud_native

A hands-on lab for orchestrating cloud-native data pipelines with **Apache Airflow 3.x**, exploring two competing architectures:

| Sub-project | Pattern | Idea in one line |
|---|---|---|
| [`lambda/`](./lambda) | **Lambda architecture** | Two physical layers — a slow, correct batch path and a fast, approximate speed path — reconciled at query time. |
| [`kappa/`](./kappa) | **Kappa architecture** | One physical layer — a single stream pipeline that you *replay from the log* whenever you need a new view. |

Each sub-project is **fully self-contained**: its own `docker-compose.yaml`, its own Airflow stack, its own `pyproject.toml`. You can bring one up without the other.

## Shared shape

Both stacks run the same backing services:

- **Airflow 3.x** (api-server + scheduler + dag-processor + triggerer + celery worker) on Postgres + Redis
- **Redpanda** as the Kafka-compatible broker (with built-in Schema Registry)
- **MinIO** as the S3-compatible object store (Bronze/Silver landing for raw + curated data)
- **Postgres** as both Airflow's metadata DB and the serving warehouse (two databases inside one instance)

Python compute is intentionally lightweight — no Spark cluster — using **Polars**, **PyArrow**, and **delta-rs** (`deltalake`) so the data layer is a single pip install away.

## Why two separate projects?

Running them side-by-side as one mega-stack would obscure where the architecture differs. Keeping them physically separate makes the comparison tactile:

- Open `lambda/dags/` — you'll see a *batch* DAG and a *speed* consumer, plus a *serving merge* job.
- Open `kappa/dags/` — you'll see one stream DAG and a *replay* DAG. No batch/speed distinction.

The compose stacks use different host ports so you can run both at the same time if you want a true side-by-side.

| Service | Lambda port | Kappa port |
|---|---|---|
| Airflow UI | 8080 | 8090 |
| Redpanda broker | 19092 | 29092 |
| Redpanda Console | 8081 | 8091 |
| MinIO console | 9001 | 9011 |
| Postgres | 5432 | 5442 |

## Getting started

```bash
cd lambda    # or: cd kappa
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up airflow-init   # one-time DB migrate + admin user
docker compose up -d
```

Then open the relevant Airflow UI (`admin` / `admin`) and unpause the DAGs.

See each sub-project's `README.md` for the actual pipeline walkthrough.
