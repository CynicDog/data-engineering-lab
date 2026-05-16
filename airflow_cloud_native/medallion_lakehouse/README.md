# medallion_lakehouse

A working, **enterprise-shaped medallion lakehouse** that *doesn't use Spark*.

The point of this lab is to teach **why the layered architecture matters**, separately from the engine you happen to run it on. Bronze / Silver / Gold buys you the same things whether the compute is Spark, DuckDB, or hand-rolled Python — what changes is the operational surface.

## Stack

| Concern | Choice | Why |
|---|---|---|
| Storage | **MinIO** (S3-compatible), Parquet files, Hive-style folder layout | Cheap, durable, columnar. Any engine can read it. |
| Compute | **DuckDB** (embedded), one process per task | Vectorized, parallel, no cluster to operate. Scales up not out — fine for ~100GB. |
| Transforms | **dbt** with `dbt-duckdb` | SQL-first, version-controlled, **tested**, **documented**, **lineage-aware**. The enterprise manageability story is dbt. |
| Orchestration | **Airflow 3.x** (LocalExecutor) + plain `BashOperator` for `dbt build` | One DAG per Silver/Gold layer, wired with Airflow 3 Assets — Bronze outlets trigger Silver, Silver outlet triggers Gold. Simpler than per-model task generation; per-model lineage lives in `dbt docs`. |
| Metadata DB | **Postgres** | Airflow only. Lakehouse metadata lives next to the data as Hive-partition layouts; tables are addressed by `s3://` path. |

No Kafka, no Spark, no Iceberg/Delta catalog service. Adding any of those is a follow-on exercise, not the lesson.

## The three layers and what each one buys you

```
       ┌─────────────────────────────────────────────────────────────┐
       │                       Source systems                        │
       │  (operational DBs, web logs, third-party APIs, files)       │
       └────────────────────────────┬────────────────────────────────┘
                                    │
                            scripts/generate_source_data.py
                                    │
                                    ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  BRONZE  —  s3://lakehouse/bronze/{table}/dt=YYYY-MM-DD/        │
   │                                                                 │
   │  • Schema = source schema, unchanged. Types may be loose.       │
   │  • Append-only. Partitioned by ingest date.                     │
   │  • Source-of-truth for replay.                                  │
   │  • Owned by:  bronze_dag.py                                     │
   │                                                                 │
   │  Manageability win:  if a downstream layer is corrupted, you    │
   │  rebuild from here — Bronze never has to be re-fetched.         │
   └─────────────────────────────┬───────────────────────────────────┘
                                 │  dbt source(...)
                                 ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  SILVER  —  s3://lakehouse/silver/{model}.parquet               │
   │                                                                 │
   │  • Cleaned, typed, deduped, conformed across sources.           │
   │  • One model per logical entity (orders, customers, products,   │
   │    web_events). Stable column contracts.                        │
   │  • Tested: not_null, unique, accepted_values, relationships.    │
   │  • Owned by:  silver_dag.py  (one Airflow task per dbt model)   │
   │                                                                 │
   │  Manageability win:  this is the layer analysts join against.   │
   │  Tests gate publication — broken upstream data fails the run,   │
   │  not the dashboard.                                             │
   └─────────────────────────────┬───────────────────────────────────┘
                                 │  dbt ref(...)
                                 ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  GOLD  —  s3://lakehouse/gold/{mart}.parquet                    │
   │                                                                 │
   │  • Business-question-shaped. Pre-aggregated where it pays.      │
   │  • Wide, denormalized, query-friendly.                          │
   │  • Owned by:  gold_dag.py                                       │
   │                                                                 │
   │  Manageability win:  multiple Gold marts can share Silver       │
   │  primitives. A column rename in Bronze touches one Silver       │
   │  model — every Gold mart inherits the fix.                      │
   └─────────────────────────────────────────────────────────────────┘
```

## Files

| Path | What it is |
|---|---|
| `docker-compose.yaml` | Airflow 3 (apiserver/scheduler/dag-processor/triggerer) + Postgres + MinIO |
| `Dockerfile` / `requirements.txt` | Airflow image with dbt-duckdb + Polars/PyArrow + Amazon provider |
| `pyproject.toml` | uv-managed local dev environment |
| `dbt_project/` | dbt project — sources, silver models, gold models, tests |
| `dags/` | Four DAGs: ingest, silver, gold, data quality |
| `src/medallion/` | Helpers for the Bronze ingest task |
| `scripts/generate_source_data.py` | Synthetic e-commerce data generator that simulates external sources |

## Running it

```bash
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env

# 1. Seed the "source systems" (writes raw files to ./source_data)
uv sync
uv run python scripts/generate_source_data.py --days 7

# 2. Bring up the stack
docker compose up airflow-init
docker compose up -d
```

- Airflow UI: <http://localhost:8100>   (admin / admin — credentials in `config/simple_auth_manager_passwords.json`)
- MinIO Console: <http://localhost:9021> (minioadmin / minioadmin)

Ports are shifted by +20 so this stack can run alongside `lambda/` and `kappa/` without colliding.

Then in the Airflow UI, unpause in order: `ingest_bronze` → `silver_models` → `gold_models` → `data_quality`.

## Stopping it

```bash
# Stop containers, keep volumes (Postgres data + MinIO bucket survive)
docker compose down

# Stop and wipe volumes — full reset, next start re-runs init from scratch
docker compose down -v

# Also remove the built image (when Dockerfile or requirements.txt changed)
docker compose down -v --rmi local
```

## Testing

Three layers, fastest to slowest feedback:

```bash
# 1. Unit tests on the Python ingest surface — pure Python, no Airflow, no services.
#    Silver/Gold are dbt SQL and are tested by `dbt test` (see data_quality DAG).
uv run pytest tests/test_pipeline_unit.py

# 2. DAG parse — catches bad imports, malformed Asset URIs, schedule typos.
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dags.py

# 3. dag.test() for ingest_bronze with the inner ingest stubbed — exercises the
#    per-table task fan-out without writing to MinIO.
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dag_test_mode.py
```

For ad-hoc iteration on a single DAG or single dbt model:

```bash
docker compose exec airflow-scheduler airflow dags test ingest_bronze
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt run --select silver_orders"
```

## What to look at first

1. **`dbt_project/models/silver/schema.yml`** — every Silver model has tests. `dbt build --select tag:silver` runs the model *and* its tests; a failed test fails the DAG task. *That* is the enterprise win.
2. **`dags/silver_dag.py` / `dags/gold_dag.py`** — one `BashOperator` per layer running `dbt deps && dbt build --select tag:<layer>`. Triggered by Asset updates from the layer below.
3. **`docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt docs generate"`** — generates the lineage graph from the dbt project. Open `target/index.html`.
4. **Try a backfill**: re-run `ingest_bronze` with a past date — Bronze writes a new partition, the Bronze Asset update fires Silver, Silver's Asset update fires Gold. This is why Bronze is partitioned and append-only.

## Where this would grow next

- **Iceberg/Delta as the table format** — schema evolution, time travel, ACID. PyIceberg or `deltalake` to write, DuckDB extensions to read.
- **OpenLineage emission** — Airflow has a built-in OpenLineage provider; pair with `dbt-ol` for cross-pipeline lineage into Marquez or OpenMetadata.
- **Per-model task fan-out** — if you want one Airflow task per dbt model (instead of one per layer), bring back `astronomer-cosmos` and use `DbtTaskGroup`. Trade-off: tighter Airflow version coupling.
- **Soda Core or Great Expectations** — richer DQ contracts beyond dbt's built-in tests (anomaly detection, freshness SLAs).
- **Trino instead of DuckDB** — when one node isn't enough. The dbt project hardly changes; you swap `dbt-duckdb` for `dbt-trino`.

These are the natural next moves, but the medallion *shape* — the three layers, the test gates, the partition-based reprocessability — is what carries forward no matter which boxes you swap.
