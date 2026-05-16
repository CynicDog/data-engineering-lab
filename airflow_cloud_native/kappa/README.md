# kappa/ — Kappa architecture on Airflow 3

One pipeline. One code path. The log is the source of truth — to backfill or
re-derive, you **replay** from an earlier offset rather than running a separate
batch job.

```
   ┌─────────────────────────┐
   │ Producers (seed_kafka)  │
   └────────────┬────────────┘
                │
        events topic (Kafka, retention = long)
                │
                ▼
   ┌─────────────────────────┐
   │   STREAM DAG (always-on │
   │   tick every 1 minute)  │
   │                         │
   │   Kafka consumer →      │
   │   Polars transform →    │
   │   Delta MERGE on MinIO  │
   │   (Silver, idempotent)  │
   │      ↓                  │
   │   Sync Silver into      │
   │   Postgres serving_*    │
   └─────────────────────────┘

   ┌─────────────────────────┐
   │   REPLAY DAG (manual)   │
   │                         │
   │   Reset consumer group  │
   │   to chosen timestamp,  │
   │   reprocess into a NEW  │
   │   versioned Delta path  │
   │   then swap pointer.    │
   └─────────────────────────┘
```

## How it differs from `lambda/`

| Concern | Lambda | Kappa |
|---|---|---|
| Code paths | Two (batch + speed) | One (stream) |
| Backfill | Re-run batch DAG | Replay log from older offset |
| Reconciliation | Union view at query time | Atomic table swap |
| Failure recovery | Re-run failed batch hour | Reset group offsets, restart |
| Where state lives | Both Silver Delta and Postgres rollups | Just Delta (Postgres mirrors it) |

The key insight: if your log has long enough retention and your transforms are
deterministic, **batch is just stream with an earlier starting offset**.

## What's in here

| Path | What it is |
|---|---|
| `docker-compose.yaml` | Airflow 3 + Redpanda + MinIO + Postgres + Redis (ports shifted by +10 vs lambda) |
| `Dockerfile` | Custom Airflow image with project deps |
| `pyproject.toml` | uv-managed local dev environment |
| `dags/stream_pipeline_dag.py` | Always-on bounded consumer tick that MERGEs into Delta |
| `dags/replay_dag.py` | Manually-triggered replay — rewinds offsets and rebuilds the Silver table |
| `src/kappa_pipeline/` | Shared code (stream/replay/common) |
| `scripts/seed_kafka.py` | Producer with deterministic event ids so replay produces identical Delta versions |

## Running it

```bash
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env

docker compose up -d postgres redis redpanda minio minio-init
docker compose up airflow-init
docker compose up -d
```

- Airflow UI: <http://localhost:8090> (login `admin` / `admin`)
- Redpanda Console: <http://localhost:8091>
- MinIO Console: <http://localhost:9011>

## Stopping it

```bash
# Stop containers, keep volumes (Postgres data, MinIO bucket, Delta files survive)
docker compose down

# Stop and wipe volumes — full reset, next start replays init from scratch
docker compose down -v

# Also remove the built image (when Dockerfile or requirements.txt changed)
docker compose down -v --rmi local
```

Seed events:

```bash
uv run python scripts/seed_kafka.py --rate 50 --duration 600
```

## Testing

Three layers, fastest to slowest feedback:

```bash
# 1. Unit tests on kappa_pipeline/ — pure Python, no Airflow, no services.
uv run pytest tests/test_pipeline_unit.py

# 2. DAG parse — catches bad imports, malformed Asset URIs, schedule typos.
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dags.py

# 3. dag.test() end-to-end run with IO stubbed — exercises task wiring,
#    params, outlets in-process. No scheduler, no celery, no minute wait.
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dag_test_mode.py
```

For ad-hoc iteration on a single DAG without going through the scheduler:

```bash
docker compose exec airflow-scheduler airflow dags test stream_pipeline
```

Try a replay:

1. Note the current Delta version (Airflow logs of `stream_pipeline`).
2. Trigger `replay_pipeline` with config `{"since": "2026-01-01T00:00:00Z"}`.
3. Watch a new versioned Silver path get built, then atomically swapped.
