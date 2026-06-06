# Code Quality — From Notebook Chaos to Testable Python

## The Notebook Problem

Databricks notebooks are excellent for exploration. They're terrible for production pipeline management.

The fundamental issue is not the notebook format itself — it's that notebook-first
culture has no natural forcing function for code quality. There's no CI that runs
`pytest` on merge, no linter that blocks bad code, no package structure that forces
you to think about interfaces.

**What notebook-only pipelines look like after 18 months:**
- 800-line notebooks with 47 cells, no clear structure
- Hardcoded catalog names, environment names, date strings
- `df2 = df` everywhere because reusing variable names is scary
- Comments that say "# this is the important part" without explaining why
- No test coverage: if you change cell 12, you don't know if cell 47 breaks


## The Package Structure

This lab uses the pattern that solves these problems:

```
src/lakehouse/          ← Python package, importable everywhere
├── config/settings.py  ← Pure dataclass, no side effects, trivially testable
├── bronze/
│   ├── ingest.py       ← Functions that take (spark, settings, ...) → return count
│   └── audit.py        ← Delta table operations, same signature
├── silver/transform.py ← Pure DataFrame transformations, no Spark session required
├── gold/mart.py        ← Aggregation functions over Silver tables
└── spark_utils/session.py ← SparkSession factory, one place to configure

dags/                   ← Airflow DAGs: thin orchestration layer
├── bronze_dag.py       ← Imports from lakehouse package, calls functions
├── silver_dag.py
└── gold_dag.py

tests/                  ← Pytest, no Airflow needed for unit tests
├── conftest.py         ← SparkSession fixture (session-scoped — one JVM for all tests)
├── test_bronze.py
└── test_silver.py
```

The DAGs are intentionally thin. All logic lives in the package.


## The Functional vs Side-Effect Principle

The most impactful single change you can make to notebook code: separate
**pure functions** (no side effects, same input always produces same output)
from **side-effectful operations** (writes to Delta, reads from S3).

**Bad (current pattern):**
```python
# Cell in notebook: does everything, impossible to test
df = spark.read.parquet(f"s3://lakehouse/landing/customer/dt={dt}/*.parquet")
df = df.withColumn("_ingest_ts", current_timestamp())
df = df.filter(df.gender.isin(["M", "F"]))
df.write.format("delta").mode("append").save(bronze_path)
status_file.write_text(f"customer|{dt}|{bronze_path}\n")
```

**Good (this lab's pattern):**
```python
# Pure transform function — testable without Spark, without S3
def clean_customer(df: DataFrame) -> DataFrame:
    return (df
            .withColumn("_ingest_ts", current_timestamp())
            .filter(col("gender").isin(["M", "F"])))

# Side-effectful function — thin wrapper, easy to mock in tests
def ingest_table(spark, settings, table, dt) -> int:
    df = spark.read.parquet(settings.landing_path(table, dt))
    clean_df = clean_customer(df)
    clean_df.write.format("delta").mode("append").save(settings.bronze_path(table))
    return clean_df.count()
```

Now `clean_customer` is unit-testable with any DataFrame (no S3, no Delta).
`ingest_table` has one job: call `clean_customer`, write, return count.


## Testing Strategy

### Layer 1: Unit Tests (Local, Fast)

Test pure functions with no external dependencies:

```bash
uv run pytest tests/test_silver.py -v
# Runs in ~30 seconds, no Docker, no network
```

```python
def test_dedup_keeps_latest(spark):
    df = spark.createDataFrame([
        ("C001", datetime(2024,1,1,9,0)),
        ("C001", datetime(2024,1,1,10,0)),  # later
    ], ["customer_id", "_ingest_ts"])
    result = dedup_by_latest(df, pk="customer_id")
    assert result.count() == 1
    assert result.first()["_ingest_ts"].hour == 10
```

### Layer 2: Integration Tests (In Container)

Test that DAGs parse correctly and interact with the real services:

```bash
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dags.py -v
```

### Layer 3: End-to-End (Airflow UI)

Trigger the Bronze DAG, verify the Asset chain fires Silver and Gold,
inspect the audit log and mart tables.


## Linting and Formatting

```bash
# Format and check all Python files
uv run ruff format src/ dags/ tests/ scripts/
uv run ruff check src/ dags/ tests/ scripts/
```

The `pyproject.toml` configures ruff with the most useful rules:
```toml
[tool.ruff.lint]
select = ["E", "F", "I", "N", "W"]
# E: pycodestyle errors
# F: pyflakes (undefined names, unused imports)
# I: isort (import ordering)
# N: pep8 naming conventions
# W: pycodestyle warnings
```

Add `ruff check` to your Azure DevOps pipeline as a pre-merge gate.
This catches the most common quality issues (undefined variables, wrong imports,
naming violations) before code review.


## Moving Databricks Notebooks to .py Files

The goal is not to eliminate all notebooks — Marimo notebooks are excellent for
exploration, and Databricks notebooks are fine for prototyping. The goal is that
**production pipeline code** lives in `.py` files, not notebooks.

Migration path:
1. Extract all business logic from notebooks into `src/lakehouse/` functions
2. The notebook cells become thin wrappers: `from lakehouse.bronze.ingest import ingest_table; ingest_table(...)`
3. Eventually, replace the notebook cells with a proper Airflow DAG or Databricks Job task pointing at the Python wheel
4. The notebook lives on as an exploration tool, but production runs the wheel

In Databricks, install the wheel via:
```python
%pip install /Volumes/VOCD/libraries/lakehouse-0.1.0-py3-none-any.whl
```

Or via cluster libraries in the Databricks UI.


## The Comment Rule

Write comments only for the **WHY**, never the **WHAT**.

```python
# Bad: describes what the code does (the code already says that)
# Deduplicate by customer_id keeping the latest row
w = Window.partitionBy("customer_id").orderBy(col("_ingest_ts").desc())

# Good: explains why this specific approach
# row_number() not rank() — we want exactly 1 row even when ingest_ts ties
w = Window.partitionBy("customer_id").orderBy(col("_ingest_ts").desc())
```

The test for "is this comment worth keeping?": if you remove it, would a
senior engineer be confused? If yes, keep it. If no, delete it.


## Production Proposal

### The Real Problem at Your Company

All production pipeline logic lives in Databricks notebooks. No `pytest`. No
`ruff`. No package structure. Multiple assignment without justification.
`df2 = df` everywhere. Side effects and logic interleaved in every cell.
Hardcoded catalog names, date strings, ADLS paths. No test means every change
is a leap of faith. No linter means every code review is a style argument.
ESM makes every change expensive — and when tests are missing, bugs in ESM-gated
changes are discovered only in production.

### Solution: Python Wheel + AzDevOps CI + Thin Notebooks

The `src/lakehouse/` package in this lab is the production-ready pattern.
The goal is to deploy it as a Python wheel to Databricks cluster libraries,
so notebooks become thin orchestration shells calling well-tested functions.

**Package structure (same as lab):**
```
src/lakehouse/
├── config/settings.py    ← Settings.from_widgets(dbutils)
├── bronze/
│   ├── ingest.py         ← ingest_table(spark, settings, table, dt) → int
│   └── audit.py          ← write_audit(), get_pending_tables()
├── silver/transform.py   ← pure DataFrame transforms, no Spark session required in unit tests
├── gold/mart.py          ← aggregation functions over Silver tables
└── spark_utils/session.py
```

**Thin notebook (all you need in Databricks):**
```python
# Cell 1: install the wheel (deployed by DABs to the cluster library)
# %pip install /Volumes/VOCD/libraries/lakehouse-0.1.0-py3-none-any.whl
# (Or configure as cluster library in databricks.yml — preferred)

# Cell 2: run
from lakehouse.bronze.ingest import ingest_table
from lakehouse.config.settings import Settings

settings = Settings.from_widgets(dbutils)
count = ingest_table(spark, settings, table="customer", dt=dbutils.widgets.get("dt"))
print(f"Ingested {count} rows")
```

No business logic in the notebook. The notebook is a launch pad.

### AzDevOps Pipeline Steps

Replace the current (likely absent) quality gates with this pipeline, added to
your existing DEV/TEST/PROD AzDevOps pipelines:

```yaml
steps:
  - script: uv sync
    displayName: Install dependencies

  - script: uv run ruff format --check src/ dags/ tests/
    displayName: Format check (ruff)

  - script: uv run ruff check src/ dags/ tests/
    displayName: Lint check (ruff)

  - script: uv run pytest tests/ -v --tb=short
    displayName: Unit tests (pytest)

  - script: uv build
    displayName: Build wheel

  - script: databricks bundle deploy --target $(DATABRICKS_TARGET)
    displayName: Deploy bundle
    env:
      DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
      DATABRICKS_TARGET: dev   # or prod for release branch
```

This runs on every PR merge to `development` or `release`. A lint error or
failing test blocks the AzDevOps pipeline — which blocks the ESM cycle.
You find problems before they reach UAT, not after.

### Incremental Migration Plan

Do not rewrite everything at once. The ESM cycle is expensive — a big-bang
rewrite requires one giant ESM ticket and creates maximum risk. Instead:

**Sprint 1**: Extract Silver transforms (highest pain, most logic, most duplication)
into `src/lakehouse/silver/transform.py`. Write unit tests. Deploy the wheel.
Update the Silver notebook to `from lakehouse.silver.transform import *; run_silver(...)`.

**Sprint 2**: Extract Bronze ingest logic. Wire to `ingestion_log` Delta table
(see `03_ingestion_pipeline.md`). Tests cover both happy path and FAILED row handling.

**Sprint 3**: Extract Gold mart queries. Each mart becomes a named function in
`src/lakehouse/gold/mart.py`. Tests verify mart output shape and key column presence.

**Sprint 4**: Add `ruff check` + `pytest` to the AzDevOps pipeline as a quality gate.

After Sprint 4 the migration is complete. The notebook is a thin shell.
All logic is tested. All code is linted. The ESM cycle now catches real problems
earlier — not because the process changed, but because tests run before UAT.

### Functional vs Side-Effect Boundary — Applied to Your Stack

| Function type | Example | Where | Testable without Databricks? |
|---|---|---|---|
| Pure transform | `mask_rrn(df)`, `dedup_by_latest(df, pk)` | `src/lakehouse/silver/transform.py` | Yes — `spark.createDataFrame()` in pytest |
| Side-effectful I/O | `read_parquet(spark, path)`, `write_delta(df, table)` | `src/lakehouse/bronze/ingest.py` | Mock or integration test only |
| Orchestration | calls ingest + writes audit row | Notebook cell or DAG task | E2E test only |

The rule: if a function reads or writes anything external, it is side-effectful.
Everything else should be pure. Pure functions are unit-testable in 30 seconds.
Side-effectful functions are integration-testable in the container. Orchestration
is E2E-testable by running the full DAG or Workflow.

This boundary is not a style preference — it determines whether you can run
`pytest` in the AzDevOps pipeline without connecting to a live Databricks cluster.
