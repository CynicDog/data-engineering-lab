# airflow_cloud_native

A hands-on lab for orchestrating cloud-native data pipelines with **Apache Airflow 3.x**, exploring three different architectures:

| Sub-project | Pattern | Idea in one line |
|---|---|---|
| [`lambda/`](./lambda) | **Lambda architecture** | Two physical layers — a slow, correct batch path and a fast, approximate speed path — reconciled at query time. |
| [`kappa/`](./kappa) | **Kappa architecture** | One physical layer — a single stream pipeline that you *replay from the log* whenever you need a new view. |
| [`medallion_lakehouse/`](./medallion_lakehouse) | **Medallion (Bronze/Silver/Gold)** | Three layered, contract-bounded zones on object storage — raw landings, conformed entities, business marts — driven by dbt + DuckDB. |

Each sub-project is **fully self-contained**: its own `docker-compose.yaml`, its own Airflow stack, its own `pyproject.toml`. You can bring one up without the others.

## Shared shape

All three stacks share the same backbone:

- **Airflow 3.x** (api-server + scheduler + dag-processor + triggerer; ± celery worker) on Postgres
- **MinIO** as the S3-compatible object store for Bronze/Silver/Gold (or Delta) data
- **Simple Auth Manager** with `admin` / `admin` seeded via `config/simple_auth_manager_passwords.json`

The streaming stacks (`lambda/`, `kappa/`) add **Redpanda** + Redis; the lakehouse stack adds **dbt-duckdb** for SQL-first transforms.

Python compute is intentionally lightweight — no Spark cluster — using **Polars**, **PyArrow**, **delta-rs** (`deltalake`), and **DuckDB** so the data layer is a single pip install away.

## Why separate projects?

Running them side-by-side as one mega-stack would obscure where each architecture differs. Keeping them physically separate makes the comparison tactile:

- Open `lambda/dags/` — *batch* DAG + *speed* consumer + *serving merge*.
- Open `kappa/dags/` — one stream DAG + a *replay* DAG. No batch/speed distinction.
- Open `medallion_lakehouse/dags/` — *ingest* + *silver* + *gold* + *data quality*, with dbt models driving Silver and Gold (one `dbt build --select tag:<layer>` per DAG).

The compose stacks use different host ports so you can run them side-by-side if you want.

| Service | Lambda | Kappa | Medallion |
|---|---|---|---|
| Airflow UI | 8080 | 8090 | 8100 |
| Redpanda broker | 19092 | 29092 | — |
| Redpanda Console | 8081 | 8091 | — |
| MinIO console | 9001 | 9011 | 9021 |
| Postgres | 5432 | 5442 | 5452 |

## Getting started

```bash
cd lambda    # or: cd kappa, cd medallion_lakehouse
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up airflow-init   # one-time DB migrate
docker compose up -d
```

Then open the relevant Airflow UI (`admin` / `admin`) and unpause the DAGs.

See each sub-project's `README.md` for the actual pipeline walkthrough, testing instructions, and cleanup steps.
