# lambda/ — Lambda architecture on Airflow 3

Two parallel paths into one serving layer:

```
                          ┌──────────────────────────────────┐
                          │  Producers (scripts/seed_kafka)  │
                          └────────────────┬─────────────────┘
                                           │
                                  events topic (Kafka)
                                           │
              ┌─────────────────────────────────────────────────────────┐
              ▼                                                         ▼
   ┌─────────────────────┐                                  ┌─────────────────────┐
   │   BATCH LAYER       │                                  │   SPEED LAYER       │
   │  (Airflow @hourly)  │                                  │  (continuous task)  │
   │                     │                                  │                     │
   │  Kafka → Parquet    │                                  │  Kafka consumer →   │
   │  in MinIO (Bronze)  │                                  │  in-memory rollup → │
   │      ↓              │                                  │  Postgres (speed_*) │
   │  Polars aggregate   │                                  │                     │
   │      ↓              │                                  │                     │
   │  Delta table on     │                                  │                     │
   │  MinIO (Silver)     │                                  │                     │
   │      ↓              │                                  │                     │
   │  Load into Postgres │                                  │                     │
   │  (batch_*)          │                                  │                     │
   └──────────┬──────────┘                                  └───────────┬─────────┘
              │                                                         │
              └───────────────────────────┬─────────────────────────────┘
                                          ▼
                              ┌───────────────────────┐
                              │   SERVING LAYER       │
                              │  Postgres view that   │
                              │  UNIONs batch_* with  │
                              │  speed_* slice newer  │
                              │  than last batch run  │
                              └───────────────────────┘
```

The point: **batch is correct but stale, speed is fresh but approximate**. The serving view stitches them — query path always sees the latest reasonable answer.

## What's in here

| Path | What it is |
|---|---|
| `docker-compose.yaml` | Airflow 3 + Redpanda + MinIO + Postgres + Redis |
| `Dockerfile` | Custom Airflow image that pre-installs project deps |
| `pyproject.toml` | uv-managed local dev environment |
| `dags/batch_layer_dag.py` | Hourly batch: Kafka → Parquet → Delta → Postgres |
| `dags/speed_layer_dag.py` | Long-running consumer task that maintains the speed table |
| `dags/serving_refresh_dag.py` | Rebuilds the unified serving view |
| `src/lambda_pipeline/` | Shared Python code — Kafka client, MinIO/Delta helpers, Postgres loader |
| `scripts/seed_kafka.py` | Emits synthetic clickstream events into the topic |

## Running it

```bash
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env

# 1. start infra without Airflow first to confirm Kafka/MinIO/Postgres are healthy
docker compose up -d postgres redis redpanda minio minio-init

# 2. one-time Airflow init (creates metadata DB + admin user)
docker compose up airflow-init

# 3. bring up the rest
docker compose up -d
```

Then:

- Airflow UI: <http://localhost:8080>  (admin / admin — credentials live in `config/simple_auth_manager_passwords.json`)
- Redpanda Console: <http://localhost:8081>
- MinIO Console: <http://localhost:9001>  (minioadmin / minioadmin)

Seed events into Kafka:

```bash
uv run python scripts/seed_kafka.py --rate 50 --duration 600
```

Then unpause `batch_layer`, `speed_layer`, and `serving_refresh` in the Airflow UI.

## Stopping it

```bash
# Stop containers, keep volumes (Postgres data, MinIO bucket, Delta files survive)
docker compose down

# Stop and wipe volumes — full reset, next start re-runs init from scratch
docker compose down -v

# Also remove the built image (when Dockerfile or requirements.txt changed)
docker compose down -v --rmi local
```

## Testing

Three layers, fastest to slowest feedback:

```bash
# 1. Unit tests on lambda_pipeline/ — pure Python, no Airflow, no services.
uv run pytest tests/test_pipeline_unit.py

# 2. DAG parse — catches bad imports, malformed Asset URIs, schedule typos.
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dags.py

# 3. dag.test() end-to-end run with IO stubbed — exercises task wiring,
#    params, outlets in-process. No scheduler, no celery.
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dag_test_mode.py
```

For ad-hoc iteration on a single DAG without going through the scheduler:

```bash
docker compose exec airflow-scheduler airflow dags test batch_layer
```

## Local Python (no docker)

```bash
uv sync
uv run python -c "from lambda_pipeline.common import settings; print(settings)"
```

The same code runs inside Airflow because `src/` is mounted into the workers and `PYTHONPATH` is set to `/opt/airflow/src`.
