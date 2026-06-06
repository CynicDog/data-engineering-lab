# Databricks Lakehouse Lab

A hands-on simulation of the Azure Databricks data platform — built with open-source
tools (PySpark + Delta Lake + Airflow + Marimo + MinIO) — that addresses the six
production pain points faced by Korean insurance companies running Databricks in a
망분리 (network-isolated) environment.

**Use this lab to**: build the mental model, understand the root causes,
and practice the patterns before applying them to production Databricks.

## The Pain Points Map

```
┌────────────────────────────────────────────────────────────────────────┐
│            6 Pain Points → 6 Patterns                                  │
├──────────────────────────┬─────────────────────┬───────────────────────┤
│ Pain Point               │ Root Cause          │  Pattern in This Lab  │
├──────────────────────────┼─────────────────────┼───────────────────────┤
│ 1. 6–7 min cold start    │ Per-job VM spinup   │ Always-on Spark       │
│ 2. status.txt fragility  │ File-based IPC      │ Delta audit table     │
│ 3. File-based triggers   │ No deliberate design│ Airflow Asset graph   │
│ 4. ESM blocks ops        │ Deploy ≠ run-time   │ API + feature flags   │
│ 5. VOCP/VOCD chaos       │ No config layer     │ Profile Settings      │
│ 6. Notebook + no tests   │ Cultural/tooling gap│  Package + pytest     │
└──────────────────────────┴─────────────────────┴───────────────────────┘
```

Each column links to a deep-dive doc in `docs/`.

## Stack

| Component | Role | Databricks Equivalent |
|-----------|------|-----------------------|
| Airflow 3 | Orchestration + Asset-based triggers | Databricks Workflows |
| PySpark 3.5 + Delta Lake 3.3 | Compute + storage format | Databricks Runtime + Delta |
| MinIO | Object storage (S3-compatible) | Azure Data Lake Storage Gen2 |
| PostgreSQL | ODS source database | On-premise Oracle/MSSQL ODS |
| Marimo | Interactive notebooks | Databricks Notebooks |

**Port assignments** (continuing the repo pattern):

| Service | Port |
|---------|------|
| Airflow UI | [http://localhost:8110](http://localhost:8110) |
| MinIO Console | [http://localhost:9031](http://localhost:9031) |
| PostgreSQL | localhost:5462 |

## Prerequisites

- Docker Desktop (4 GB RAM recommended for Spark inside Airflow containers)
- Java 17 (for running Marimo notebooks locally: `brew install openjdk@17`)
- [uv](https://docs.astral.sh/uv/) for local Python package management

## Quick Start

```bash
# 1. Clone to this directory
cd databricks_lakehouse

# 2. Copy env template
cp .env.example .env

# 3. Start the stack (first build takes ~5 min — downloads JARs)
docker compose up -d --build

# 4. Wait for Airflow to initialize (~60 sec)
docker compose logs -f airflow-apiserver | grep "Listening at"

# 5. Generate source data in MinIO (simulates ADF Parquet output)
uv run scripts/generate_source_data.py --days 3

# 6. Open Airflow UI → unpause ingest_bronze → watch cascade
#    http://localhost:8110  (admin / admin)

# 7. (Optional) Run notebooks locally
uv sync
uv run marimo edit notebooks/01_delta_fundamentals.py
```

## Notebooks (Run Locally — No Docker Needed)

Marimo notebooks run on your local machine and store Delta tables in `/tmp/lakehouse_lab/`.
They demonstrate Delta Lake concepts independently of the pipeline stack.

```bash
uv sync  # Install dependencies including PySpark + Delta + Marimo
```

| Notebook | What You Learn | Addresses |
|----------|---------------|-----------|
| `notebooks/01_delta_fundamentals.py` | Transaction log, time travel, MERGE, Z-ORDER | Foundation for all Delta work |
| `notebooks/02_bronze_antipatterns.py` | Live demo: status.txt overlap bug → Delta audit fix | Pain point 2 |
| `notebooks/03_silver_design.py` | Dedup, PII masking (개인정보보호법), type casting | Pain point 3 (Silver's job) |
| `notebooks/04_gold_marts.py` | VOC daily, policy summary, claims analysis marts | Pain point 3 (Gold's job) |

```bash
uv run marimo edit notebooks/01_delta_fundamentals.py  # opens in browser
```

## DAG Pipeline

Unpausing `ingest_bronze` in the Airflow UI triggers the full chain:

```
ingest_bronze (daily @daily)
    ├── ingest_customer ──┐
    ├── ingest_policy    ─┤ emit Bronze Assets
    ├── ingest_claims    ─┤
    └── ingest_voc       ─┘
              │ (Asset trigger)
              ▼
    transform_silver
              │ (Asset trigger)
              ▼
        build_gold
```

No check files. No flag files. Airflow's Asset graph is the dependency map.
Every ingestion event is recorded in `s3://lakehouse/control/ingestion_log` (Delta table).

## Tests

```bash
# Local unit tests (no Docker, no network, ~60 sec including Spark startup)
uv run pytest tests/test_bronze.py tests/test_silver.py -v

# DAG parse tests (inside the scheduler container)
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dags.py -v
```

## Demo: The status.txt Overlap Bug

To experience the pain point directly:

```bash
# Write BOTH daily and hourly data for the same date
# Watch the daily data get silently overwritten
uv run scripts/generate_source_data.py --demo-overlap
```

Then open `notebooks/02_bronze_antipatterns.py` to see the Delta audit log fix.

## Documentation

| Doc | Addresses |
|-----|-----------|
| [docs/01_pain_points_catalog.md](docs/01_pain_points_catalog.md) | Overview of all 6 pain points |
| [docs/02_compute_strategy.md](docs/02_compute_strategy.md) | Cold start, cluster types, SQL Warehouse |
| [docs/03_ingestion_pipeline.md](docs/03_ingestion_pipeline.md) | ADF→Bronze, status.txt vs Delta audit |
| [docs/04_medallion_design.md](docs/04_medallion_design.md) | Layer contracts, file-based vs Asset triggers |
| [docs/05_cicd_boundary.md](docs/05_cicd_boundary.md) | ESM argument, deploy-time vs run-time control |
| [docs/06_environment_management.md](docs/06_environment_management.md) | VOCP/VOCD problem, profile-based config |
| [docs/07_code_quality.md](docs/07_code_quality.md) | Package structure, pytest, ruff, comments |

## Project Structure

```
databricks_lakehouse/
├── docker-compose.yaml          # Full stack
├── Dockerfile                   # Airflow + Java + PySpark + Delta
├── pyproject.toml               # uv dependencies
├── .env.example                 # Required env vars
│
├── notebooks/                   # Marimo — exploration, run locally
│   ├── 01_delta_fundamentals.py
│   ├── 02_bronze_antipatterns.py
│   ├── 03_silver_design.py
│   └── 04_gold_marts.py
│
├── dags/                        # Airflow DAGs — orchestration
│   ├── bronze_dag.py            # Parquet → Delta + audit log
│   ├── silver_dag.py            # Asset-triggered transforms
│   └── gold_dag.py              # Asset-triggered marts
│
├── src/lakehouse/               # Python package — all business logic
│   ├── config/settings.py       # Profile-based config (solves VOCP/VOCD)
│   ├── bronze/ingest.py         # Parquet → Delta
│   ├── bronze/audit.py          # Ingestion log Delta table (replaces status.txt)
│   ├── silver/transform.py      # Dedup, type cast, PII mask
│   ├── gold/mart.py             # VOC, policy, claims mart builds
│   └── spark_utils/session.py   # SparkSession factory
│
├── tests/                       # pytest
│   ├── conftest.py              # SparkSession fixture
│   ├── test_bronze.py
│   └── test_silver.py
│
├── scripts/
│   ├── generate_source_data.py  # Synthetic Korean insurance data → MinIO
│   └── init_ods.sql             # PostgreSQL ODS schema (customer, policy, claims, voc)
│
├── config/
│   ├── dev.yaml                 # VOCD profile
│   └── prod.yaml                # VOCP profile
│
└── docs/                        # Deep-dive documentation
```

## Relationship to Sibling Projects

| Project | Focus | When to use |
|---------|-------|-------------|
| `medallion_lakehouse/` | dbt + DuckDB medallion pattern | SQL-first transformation with dbt |
| `databricks_lakehouse/` | PySpark + Delta + Airflow | Databricks migration, Python-first pipeline |
| `high_performance_spark/` | Spark internals, DataFrames | Deep PySpark proficiency |
