# Databricks System Tables

Production-grade observability for your Databricks platform, built on Unity Catalog's
`system.*` catalog. Every pipeline run, compute cost, query, lineage event, and audit
action is recorded and queryable as a standard Delta table — no agents, no exporters,
no extra infrastructure.

This lab is a direct companion to [`databricks_lakehouse/`](../databricks_lakehouse/),
which addresses 6 operational pain points. System tables are what you layer on top to
actually *see* whether those patterns are holding in production.


## System Table Catalog

| Category | Tables | Retention | Streaming |
|---|---|---|---|
| **Operations** | `system.lakeflow.jobs`, `job_tasks`, `job_run_timeline`, `job_task_run_timeline` | 365 days | Yes |
| **Compute** | `system.compute.clusters`, `node_types`, `node_timeline`, `instance_events`, `instance_pools` | 365 days (90d for node_timeline) | Yes |
| **Cost** | `system.billing.usage` | 365 days | Yes |
| **Query** | `system.query.history` | 365 days | No |
| **Lineage** | `system.access.table_lineage`, `column_lineage` | 1-year rolling | Yes |
| **Audit** | `system.access.audit` | 365 days | Yes |


## Pain Point → System Table Map

These are the 6 pain points from [`databricks_lakehouse/docs/01_pain_points_catalog.md`](../databricks_lakehouse/docs/01_pain_points_catalog.md)
and the system tables that give you production visibility into each one.

| # | Pain Point | System Table(s) | What You Can Now Answer |
|---|---|---|---|
| 1 | Compute cold start | `system.compute.clusters`, `node_timeline`, `billing.usage` | How long did cold start actually take? Which clusters idle > 30 min after last job? |
| 2 | Fragile pipeline (status.txt) | `system.lakeflow.job_run_timeline`, `job_task_run_timeline` | Which jobs failed silently last night? Which tasks timed out without alerting? |
| 3 | Unclear medallion | `system.access.table_lineage`, `column_lineage` | Does any Gold job bypass Silver and read Bronze directly? |
| 4 | CI/CD ESM mismatch | `system.access.audit` | Who enabled that job at 03:00? Which service principal changed the cluster policy? |
| 5 | VOCP/VOCD catalog chaos | `system.access.table_lineage` | Did any VOCD job accidentally read from VOCP? |
| 6 | Code quality | `system.query.history` | Which notebook produced the 3-hour query? Which job spills > 100 GB to disk? |

→ See [`docs/07_pain_points_addressed.md`](docs/07_pain_points_addressed.md) for the full analysis.


## Structure

```
databricks_system_tables/
├── docs/                          # one doc per table category
│   ├── 01_system_tables_overview.md
│   ├── 02_operations_monitoring.md
│   ├── 03_cost_attribution.md
│   ├── 04_query_performance.md
│   ├── 05_data_lineage_governance.md
│   ├── 06_security_audit.md
│   └── 07_pain_points_addressed.md
├── queries/                       # production SQL, runnable in Databricks SQL editor
│   ├── ops/
│   ├── cost/
│   ├── performance/
│   └── lineage/
├── scripts/
│   └── generate_synthetic_system_data.py   # local Delta tables mirroring system schemas
└── notebooks/                     # Marimo dashboards (run against synthetic data locally)
    ├── 01_ops_dashboard.py
    ├── 02_cost_attribution.py
    ├── 03_query_profiler.py
    └── 04_lineage_explorer.py
```


## Quick Start

### On real Databricks
1. Ensure Unity Catalog is enabled and your metastore is on privilege model v1.0+
2. Grant access: `GRANT USE CATALOG ON CATALOG system TO <principal>`
3. Open any `.sql` file from `queries/` in Databricks SQL editor and run it

### Locally (study mode)
```bash
cd databricks_system_tables
uv run python scripts/generate_synthetic_system_data.py   # seeds local Delta tables
uv run marimo edit notebooks/01_ops_dashboard.py
```


## Prerequisites (local)

- Python 3.12+
- `uv` package manager
- Same pyproject.toml dependencies as `databricks_lakehouse/` (PySpark + Delta + Marimo)

No Docker needed — the synthetic data generator writes Delta tables to a local `./data/`
directory that the notebooks read directly.
