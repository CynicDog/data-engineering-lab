# Data Lineage & Governance

## What Problem This Solves

Your Silver transform broke. A column changed type. You need to know: which Gold
tables read from this Silver table, and which dashboards serve those Gold tables?
Without lineage, you trace this by reading code, asking colleagues, and hoping you
found everything before the business notices.

`system.access.table_lineage` and `system.access.column_lineage` record every
read and write to Unity Catalog tables — automatically, for every job, notebook,
pipeline, and SQL warehouse query.


## Tables

### `system.access.table_lineage`

One row per read or write event on a Unity Catalog table or path.

| Column | Type | Meaning |
|---|---|---|
| `source_table_full_name` | string | `catalog.schema.table` — the table being read |
| `source_type` | string | TABLE, PATH, VIEW, MATERIALIZED_VIEW, STREAMING_TABLE |
| `target_table_full_name` | string | `catalog.schema.table` — the table being written |
| `target_type` | string | Same as source_type |
| `entity_type` | string | NOTEBOOK, JOB, PIPELINE, DASHBOARD_V3, DBSQL_QUERY |
| `entity_id` | string | ID of the notebook, job, pipeline, etc. |
| `event_time` | timestamp | When the lineage event was recorded |
| `event_date` | date | Partition column — always filter on this for performance |
| `created_by` | string | Username or service principal |
| `statement_id` | string | Foreign key to `system.query.history` (warehouses only) |
| `direct_access` | boolean | True if source was directly referenced; false if intermediate dependency |
| `entity_metadata` | struct | `{job_info, notebook_id, dlt_pipeline_info, dashboard_id, ...}` |

**Event types**:
- Read-only: `source_type NOT NULL`, `target_type NULL`
- Write-only: `target_type NOT NULL`, `source_type NULL`
- Read-write: both set

### `system.access.column_lineage`

Same schema as `table_lineage` plus:

| Column | Type | Meaning |
|---|---|---|
| `source_column_name` | string | Column being read |
| `target_column_name` | string | Column being written |

Column lineage is only recorded for read-write events with explicit column references.
Write-only operations (e.g., `INSERT INTO ... VALUES (...)`) do not produce column lineage.

**Retention**: both tables use a rolling 1-year window — old events are removed as new
ones arrive.


## Production Query Patterns

### Downstream impact: what reads this table?

When `VOCP.silver.policy_clean` changes, what breaks?

```sql
SELECT DISTINCT
    target_table_full_name          AS downstream_table,
    target_type,
    entity_type,
    entity_id,
    created_by,
    MAX(event_time)                 AS last_seen
FROM system.access.table_lineage
WHERE source_table_full_name = 'VOCP.silver.policy_clean'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY downstream_table, target_type, entity_type, entity_id, created_by
ORDER BY downstream_table;
```

→ [`../queries/lineage/downstream_impact.sql`](../queries/lineage/downstream_impact.sql)

### Upstream sources: where does this table come from?

```sql
SELECT DISTINCT
    source_table_full_name          AS upstream_table,
    source_type,
    entity_type,
    entity_id,
    created_by,
    MAX(event_time)                 AS last_seen
FROM system.access.table_lineage
WHERE target_table_full_name = '${target_table}'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY upstream_table, source_type, entity_type, entity_id, created_by
ORDER BY upstream_table;
```

→ [`../queries/lineage/upstream_sources.sql`](../queries/lineage/upstream_sources.sql)

### Cross-catalog flow: VOCP ↔ VOCD validation

In a 망분리 environment, VOCD (dev) jobs must never read from VOCP (prod). This query
surfaces any cross-catalog reads:

```sql
SELECT
    source_table_full_name,
    target_table_full_name,
    entity_type,
    entity_id,
    created_by,
    event_time
FROM system.access.table_lineage
WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND (
      (source_table_full_name LIKE 'VOCP.%' AND target_table_full_name LIKE 'VOCD.%')
   OR (source_table_full_name LIKE 'VOCD.%' AND target_table_full_name LIKE 'VOCP.%')
  )
ORDER BY event_time DESC;
```

→ [`../queries/lineage/cross_catalog_flow.sql`](../queries/lineage/cross_catalog_flow.sql)


## Column-Level Impact Analysis

Which jobs read the `premium_amount` column from `VOCP.silver.policy_clean`?
Useful before renaming or changing a column's semantics:

```sql
SELECT DISTINCT
    target_table_full_name,
    target_column_name,
    entity_type,
    entity_id,
    created_by,
    MAX(event_time) AS last_seen
FROM system.access.column_lineage
WHERE source_table_full_name = 'VOCP.silver.policy_clean'
  AND source_column_name = 'premium_amount'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY target_table_full_name, target_column_name, entity_type, entity_id, created_by
ORDER BY target_table_full_name;
```


## Medallion Architecture Validation

The Gold layer should only read from Silver, never from Bronze directly. Use lineage
to verify this invariant is holding in production:

```sql
SELECT DISTINCT
    source_table_full_name          AS bronze_source,
    target_table_full_name          AS gold_target,
    entity_type,
    entity_id,
    created_by,
    MAX(event_time)                 AS last_seen
FROM system.access.table_lineage
WHERE source_table_full_name LIKE '%.bronze.%'
  AND target_table_full_name LIKE '%.gold.%'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY bronze_source, gold_target, entity_type, entity_id, created_by
ORDER BY last_seen DESC;
```

If this query returns rows, someone is bypassing the Silver contract. The `entity_id`
and `entity_type` columns tell you exactly which notebook or job is the offender.


## Connecting to Pain Points 3 and 5

**Pain Point 3 (Medallion clarity)**: the architecture can be documented, but lineage
proves whether it's holding. The medallion validation query above runs in production
and surfaces violations automatically.

**Pain Point 5 (VOCP/VOCD chaos)**: the cross-catalog flow query detects isolation
violations. Schedule it as a daily job; write results to an alert table; surface via
Slack or email. You now have automated catalog isolation enforcement without requiring
Unity Catalog attribute-based access control.

→ See [`07_pain_points_addressed.md`](07_pain_points_addressed.md) for the full analysis.
