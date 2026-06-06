# Enterprise Table Registry — Metadata-Driven Pipelines

## The Problem with Hardcoded `TABLES` Lists

The original lab hardcodes table names as a Python list:

```python
TABLES = ["customer", "policy", "claims", "voc"]
```

This list is duplicated in three places: `bronze_dag.py`, `silver_dag.py`, and `silver/transform.py`. Adding a single new table requires editing all three files, which means a PR review, a merge, and a deploy — for what is structurally a configuration change.

Silver is worse: each table has its own Python function with hardcoded column names, types, and PII masks. Adding a table means writing a new `transform_X` function and registering it in a dict. The barrier to adding tables is too high, and the similarity across transform functions creates silent drift risk.

At production scale — 4 channels × 15–30 tables each = 60–120 ingestion pipelines — this approach is unmanageable.


## The Fix: Config-Driven Table Registry

Move all table definitions to a single YAML file. Python reads it at startup and generates pipelines dynamically.

**Adding a new table** = one YAML block. Zero code changes. Zero DAG changes.


### `config/table_registry.yaml`

```yaml
channels:
  chan1:
    tables:
      customer:
        pk: customer_id
        dedup_ts_col: _ingest_ts
        schedule_type: daily
        cast_types:
          birth_date: date
          created_at: timestamp
          updated_at: timestamp
        pii:
          rrn_masked: mask_rrn
          phone_masked: mask_phone
        derived_columns: {}

      policy:
        pk: policy_id
        dedup_ts_col: _ingest_ts
        schedule_type: daily
        cast_types:
          start_date: date
          end_date: date
          premium: double
        pii: {}
        derived_columns:
          policy_age_days: "datediff(coalesce(end_date, current_date()), start_date)"

  chan2:
    tables:
      voc:
        pk: voc_id
        dedup_ts_col: _ingest_ts
        schedule_type: daily
        cast_types:
          created_at: timestamp
          resolved_at: timestamp
        pii: {}
        derived_columns:
          resolution_hours: "CASE WHEN resolved_at IS NOT NULL THEN (unix_timestamp(resolved_at) - unix_timestamp(created_at)) / 3600.0 END"

marts:
  - voc_daily
  - policy_summary
  - claims_analysis
```

Three key design choices:

1. **Channels at the top level** — each channel maps to a separate storage namespace (`bronze/chan1/`, `bronze/chan2/`) and a separate Unity Catalog in production. This mirrors the physical on-premise ODS structure.

2. **SQL expressions for derived columns** — evaluated via `F.expr()` in PySpark. Any expression that would work in a Spark SQL `SELECT` clause works here. No new Python functions needed.

3. **PII masking by column and masker name** — the registry says *which* column needs masking and *which* function to apply. The masker implementations live in Python but the per-table application is config-only.


### `src/lakehouse/config/registry.py` — The Dataclasses

```python
@dataclass
class TableSpec:
    name: str
    channel: str
    pk: str
    dedup_ts_col: str
    schedule_type: str
    cast_types: dict[str, str]   # column → Spark type string
    pii: dict[str, str]          # column → masker function name
    derived_columns: dict[str, str]  # new column → SQL expr string

@dataclass
class Registry:
    channels: list[ChannelSpec]
    marts: list[str]

    def all_tables(self) -> list[TableSpec]: ...
    def table_spec(self, channel: str, table: str) -> TableSpec: ...
```

`load_registry()` reads the YAML and returns a `Registry`. All downstream code uses only `Registry` and `TableSpec` — the YAML format is an implementation detail.


### Generic Silver Transform Engine

Instead of per-table Python functions, Silver runs a single generic engine:

```python
def transform_from_spec(df: DataFrame, spec: TableSpec) -> DataFrame:
    # 1. Cast columns to correct types
    for col_name, type_str in spec.cast_types.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(type_str))

    # 2. Dedup — keep latest row per PK by spec.dedup_ts_col
    df = dedup_by_latest(df, pk=spec.pk, ts_col=spec.dedup_ts_col)

    # 3. Apply PII maskers from registry
    for col_name, masker_name in spec.pii.items():
        if col_name in df.columns and masker_name in _PII_MASKERS:
            df = _PII_MASKERS[masker_name](df, col_name=col_name)

    # 4. Compute derived columns via Spark SQL expressions
    for col_name, sql_expr in spec.derived_columns.items():
        df = df.withColumn(col_name, F.expr(sql_expr))

    return df
```

Order matters: cast first (so derived column SQL sees typed data), dedup second (so derived columns are only computed on surviving rows).


### Dynamic DAG Generation

Bronze tasks are generated from the registry at DAG parse time:

```python
registry = load_registry()

BRONZE_ASSETS = {
    (ch.name, t.name): Asset(f"s3://lakehouse/bronze/{ch.name}/{t.name}")
    for ch in registry.channels
    for t in ch.tables
}

@dag(dag_id="ingest_bronze", schedule="@daily", ...)
def ingest_bronze():
    for ch in registry.channels:
        for spec in ch.tables:
            @task(
                task_id=f"ingest_{ch.name}_{spec.name}",
                outlets=[BRONZE_ASSETS[(ch.name, spec.name)]],
            )
            def _ingest(channel=ch.name, table_name=spec.name, ...): ...
            _ingest()
```

With `chan1` (3 tables) + `chan2` (1 table), Airflow shows:
```
ingest_chan1_customer
ingest_chan1_policy
ingest_chan1_claims
ingest_chan2_voc
```

Add a table to `table_registry.yaml` → a new task appears on the next DAG parse. No Python changes.


### Channel-Namespaced Storage Paths

All storage paths include the channel name:

```
s3://lakehouse/landing/chan1/customer/dt=2024-03-15/part.parquet
s3://lakehouse/bronze/chan1/customer/
s3://lakehouse/silver/chan1/customer/
s3://lakehouse/bronze/chan2/voc/
s3://lakehouse/silver/chan2/voc/
```

This namespace isolation mirrors the per-catalog separation in Unity Catalog. It also means you can run bronze ingestion for `chan1` and `chan2` in parallel without path conflicts.


---

## Production Proposal

### The Real Problem at Scale

Your production environment has 4 channel databases (CHAN1, CHAN2, CHAN3, CHAN4), each sourcing 15–30 tables from on-premise ODS systems. Each channel's tables need Bronze ingestion, Silver transforms, and Gold mart aggregations.

Without a registry:
- Every new table requires Python changes across 3 files + a deployment
- Adding a column PII rule requires finding which function handles that table
- Auditing "what tables exist in channel 3 Silver?" requires reading Python source code
- 60–120 near-identical Airflow tasks are maintained by hand

With a registry, the table spec is the authoritative definition. Code is generic. Operators with YAML knowledge can add tables without touching Python.

### Option A — YAML Registry in Repo (Recommended for Most Teams)

Same as the lab. All table definitions live in `config/table_registry.yaml` in the application repository. Every change goes through Git → ESM approval → deploy.

**Strengths:**
- Table additions have the same review process as code changes (good for compliance)
- The registry is version-controlled — audit history is `git log`
- Rollback of a bad table definition = `git revert`

**Operations runbook for adding a table:**
1. Add a YAML block to `config/table_registry.yaml` in a feature branch
2. ESM approval — this qualifies as a config change (not a logic change, faster track)
3. Deploy to DEV → validate first run in Airflow UI
4. Promote to PROD via release branch

No notebook code changes. No DAG code changes. The deploy triggers DAG re-parse and the new task appears automatically.

### Option B — Delta Metadata Table in Unity Catalog

Store the registry as a Delta table in Unity Catalog rather than a YAML file:

```sql
CREATE TABLE control.table_registry (
  channel       STRING NOT NULL,
  table_name    STRING NOT NULL,
  pk            STRING NOT NULL,
  dedup_ts_col  STRING NOT NULL,
  schedule_type STRING NOT NULL,
  cast_types    STRING,  -- JSON: {"birth_date": "date", ...}
  pii_columns   STRING,  -- JSON: {"rrn_masked": "mask_rrn", ...}
  derived_cols  STRING,  -- JSON: {"policy_age_days": "datediff(...)"}
  active        BOOLEAN NOT NULL DEFAULT true,
  updated_at    TIMESTAMP NOT NULL
)
USING DELTA;
```

Operators can activate a new table via SQL `INSERT` without a code deployment. Useful when the ODS team adds tables frequently (weekly velocity).

**Trade-off**: The table definition is no longer code-reviewed. Mistakes (wrong PK, wrong cast type) go live without a PR review. Use this pattern when table additions are frequent and low-risk — not for PII-bearing columns.

### DABs + Parameterized Workflows

In production Databricks, combine the YAML registry with Databricks Asset Bundles (DABs) parameterization:

```yaml
# databricks.yml
resources:
  jobs:
    chan1_pipeline:
      name: "CHAN1 Daily Pipeline [${var.chan1_catalog}]"
      tasks:
        - task_key: bronze_customer
          notebook_task:
            notebook_path: ./notebooks/bronze_ingest
            base_parameters:
              channel: chan1
              table: customer
              catalog: ${var.chan1_catalog}

        - task_key: bronze_policy
          notebook_task:
            notebook_path: ./notebooks/bronze_ingest
            base_parameters:
              channel: chan1
              table: policy
              catalog: ${var.chan1_catalog}

        - task_key: silver_transform
          depends_on:
            - task_key: bronze_customer
            - task_key: bronze_policy
          python_wheel_task:
            package_name: lakehouse
            entry_point: silver_transform
            parameters: ["--channel", "chan1", "--catalog", "${var.chan1_catalog}"]
```

The notebook or wheel task reads the table registry from the deployed YAML. DABs injects the catalog name at deploy time. No branching on environment inside the notebook.

**Generating the DABs job definition from the registry:**

```python
import yaml
from lakehouse.config.registry import load_registry

registry = load_registry()

for ch in registry.channels:
    tasks = [
        {
            "task_key": f"bronze_{spec.name}",
            "notebook_task": {
                "notebook_path": "./notebooks/bronze_ingest",
                "base_parameters": {"channel": ch.name, "table": spec.name},
            }
        }
        for spec in ch.tables
    ]
    # Write tasks[] into databricks.yml template
```

This produces a DABs job definition from the same registry that drives the local Airflow DAG — single source of truth across both environments.

### SLA Tracking per Table

Add a `sla_minutes` field to each table spec:

```yaml
customer:
  pk: customer_id
  sla_minutes: 120  # Silver must complete within 2h of Bronze landing
  ...
```

A monitoring job queries the audit log Delta table:

```sql
SELECT
  source_table,
  dt,
  min(ingested_at) AS bronze_complete,
  max(ingested_at) AS silver_complete,
  datediff(minute, min(ingested_at), max(ingested_at)) AS lag_minutes
FROM control.ingestion_log
WHERE dt = current_date()
  AND status = 'success'
GROUP BY source_table, dt
HAVING lag_minutes > sla_minutes  -- join with registry for threshold
```

Alert fires when a table misses its SLA window. This replaces manual "why is Silver stale?" investigation.

### Schema Drift Detection

When a source ODS team changes a column type or drops a column, Delta's `mergeSchema` catches it at the Bronze write. Add a job that diffs `INFORMATION_SCHEMA.COLUMNS` for each Silver table against the registry `cast_types`:

```sql
SELECT
  r.channel,
  r.table_name,
  r.col_name,
  r.expected_type,
  c.data_type AS actual_type
FROM registry_expected_schema r
LEFT JOIN information_schema.columns c
  ON c.table_name = r.table_name
  AND c.column_name = r.col_name
WHERE r.expected_type != c.data_type
   OR c.column_name IS NULL
```

Column missing from actual Silver → source dropped it or renamed it. Type mismatch → source changed the type. Both are caught before Gold consumes the broken data.

### Summary

| Approach | When to use |
|---|---|
| YAML registry (lab, Option A) | Default. Compliance-friendly, Git-versioned, works at any table count. |
| Delta metadata table (Option B) | ODS-driven teams adding 5+ tables/week; operators should not need PRs. |
| DABs + parameterized Workflows | Any team deploying to Databricks Workflows in production. |

Start with the YAML registry. It costs nothing to graduate to Option B later — the `TableSpec` dataclass is the stable interface and the loader can be swapped to read from Delta instead of YAML without changing any downstream code.
