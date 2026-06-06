# Query Performance

## What Problem This Solves

A Gold mart query runs for 3 hours every morning. No one knows which notebook
submitted it, which user, or why it takes so long. You can't reproduce it locally
because it ran on a SQL warehouse. The only evidence is a Slack message that says
"dashboard is slow."

`system.query.history` gives you the complete record of every SQL statement run on
SQL warehouses and serverless compute: who ran it, how long each phase took, how much
data it read, how much it spilled to disk. This is the query profiling tool that was
always missing.


## `system.query.history`

| Column | Type | Meaning |
|---|---|---|
| `statement_id` | string | Unique query identifier (foreign key for lineage join) |
| `executed_by` | string | Email or username |
| `statement_text` | string | Full SQL text (may be encrypted with CMK) |
| `statement_type` | string | SELECT, INSERT, ALTER, etc. |
| `execution_status` | string | FINISHED, FAILED, CANCELED |
| `compute` | struct | `{type: WAREHOUSE/SERVERLESS_COMPUTE, warehouse_id, cluster_id}` |
| `client_application` | string | Databricks SQL Editor, Power BI, Tableau, etc. |
| `start_time` | timestamp | When the statement was received (UTC) |
| `end_time` | timestamp | When execution finished (UTC) |
| `total_duration_ms` | bigint | Wall clock time (excludes result fetch) |
| `waiting_for_compute_duration_ms` | bigint | Time waiting for warehouse to start |
| `waiting_at_capacity_duration_ms` | bigint | Time in queue (warehouse at capacity) |
| `compilation_duration_ms` | bigint | Parse, plan, optimize |
| `execution_duration_ms` | bigint | Spark execution time |
| `total_task_duration_ms` | bigint | Sum of all task durations (higher = more parallelism or more work) |
| `read_rows` | bigint | Rows read from storage |
| `produced_rows` | bigint | Rows returned to caller |
| `read_bytes` | bigint | Bytes read from storage |
| `spilled_local_bytes` | bigint | Bytes spilled to local disk (sign of memory pressure) |
| `shuffle_read_bytes` | bigint | Bytes transferred across executors (sign of large joins/sorts) |
| `from_result_cache` | boolean | Served from result cache |
| `read_io_cache_percent` | int | % of data read from IO cache (Delta cache) |
| `read_partitions` | bigint | Partitions read after predicate pushdown |
| `pruned_files` | bigint | Files skipped by partition/Z-order pruning |
| `query_source` | struct | Links back to notebook, job, dashboard, or alert |

**Note**: `system.query.history` does NOT support Structured Streaming.


## Production Query Patterns

### P95 slow queries in the last 7 days

```sql
SELECT
    statement_id,
    executed_by,
    client_application,
    ROUND(total_duration_ms / 1000.0 / 60.0, 2)        AS total_minutes,
    ROUND(execution_duration_ms / 1000.0 / 60.0, 2)    AS exec_minutes,
    ROUND(compilation_duration_ms / 1000.0, 1)         AS compile_seconds,
    ROUND(read_bytes / 1e9, 2)                          AS read_gb,
    ROUND(spilled_local_bytes / 1e9, 2)                 AS spill_gb,
    from_result_cache,
    LEFT(statement_text, 200)                           AS query_preview
FROM system.query.history
WHERE execution_status = 'FINISHED'
  AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND total_duration_ms >= PERCENTILE_CONT(0.95) WITHIN GROUP (
          ORDER BY total_duration_ms
      ) OVER ()
ORDER BY total_duration_ms DESC
LIMIT 50;
```

→ [`../queries/performance/slow_queries_p95.sql`](../queries/performance/slow_queries_p95.sql)

### Spill offenders

Queries that spilled more than 10 GB to disk are burning IO and degrading warehouse
performance for all users sharing the same cluster:

```sql
SELECT
    statement_id,
    executed_by,
    ROUND(spilled_local_bytes / 1e9, 1)                 AS spill_gb,
    ROUND(total_duration_ms / 1000.0 / 60.0, 2)        AS total_minutes,
    ROUND(shuffle_read_bytes / 1e9, 1)                  AS shuffle_gb,
    client_application,
    query_source.job_info.job_id                        AS job_id,
    query_source.notebook_id                            AS notebook_id,
    LEFT(statement_text, 300)                           AS query_preview
FROM system.query.history
WHERE start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND spilled_local_bytes > 10 * 1e9      -- 10 GB threshold
  AND execution_status = 'FINISHED'
ORDER BY spilled_local_bytes DESC;
```

→ [`../queries/performance/spill_offenders.sql`](../queries/performance/spill_offenders.sql)

### Cache hit ratio by warehouse

High IO cache hit ratio means data is served from memory — fast and free. Low ratio
means every query hits ADLS — slower and costs network egress.

```sql
SELECT
    compute.warehouse_id,
    COUNT(*)                                                        AS query_count,
    ROUND(AVG(read_io_cache_percent), 1)                           AS avg_io_cache_pct,
    SUM(CASE WHEN from_result_cache THEN 1 ELSE 0 END) * 100.0
        / COUNT(*)                                                 AS result_cache_hit_pct,
    ROUND(SUM(read_bytes) / 1e12, 3)                               AS total_read_tb,
    ROUND(AVG(total_duration_ms) / 1000.0, 1)                      AS avg_duration_seconds
FROM system.query.history
WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND execution_status = 'FINISHED'
  AND compute.warehouse_id IS NOT NULL
GROUP BY compute.warehouse_id
ORDER BY query_count DESC;
```

→ [`../queries/performance/cache_hit_ratio.sql`](../queries/performance/cache_hit_ratio.sql)


## Diagnosing a Slow Query

When a user reports "the dashboard was slow at 09:00 yesterday":

```sql
-- Step 1: find the query
SELECT statement_id, executed_by, total_duration_ms, start_time, statement_text
FROM system.query.history
WHERE start_time BETWEEN '2025-01-15 08:50:00' AND '2025-01-15 09:15:00'
  AND execution_status = 'FINISHED'
ORDER BY total_duration_ms DESC
LIMIT 10;

-- Step 2: dissect the slow query
SELECT
    compilation_duration_ms,       -- high? -> stats stale, missing Z-order
    waiting_for_compute_duration_ms, -- high? -> warehouse was cold or at capacity
    execution_duration_ms,         -- high? -> data volume, spill, skew
    spilled_local_bytes,           -- > 0? -> too little memory for the join/sort
    shuffle_read_bytes,            -- very high? -> large broadcast or sort
    read_partitions,               -- high relative to pruned_files? -> poor partitioning
    pruned_files,                  -- 0? -> predicate pushdown not working
    read_io_cache_percent          -- low? -> Delta cache cold or table not cached
FROM system.query.history
WHERE statement_id = '${statement_id}';
```

Each metric points to a specific fix:
- High `compilation_duration_ms` → run `ANALYZE TABLE` to refresh statistics
- High `waiting_for_compute_duration_ms` → warehouse was cold; consider auto-start warming
- High `spilled_local_bytes` → increase cluster size or optimize join order
- `pruned_files = 0` → add `ZORDER BY` on the filter columns, or partition by date


## Connecting to Pain Point 6

Notebook-first culture produces notebooks that become production jobs without ever
being profiled. `query.history` is the profiler. Filter by `query_source.notebook_id`
or `query_source.job_info.job_id` to isolate queries from a specific pipeline, sort by
`spilled_local_bytes` or `total_duration_ms`, and you have a ranked list of exactly
which cells to optimize first.

→ See [`07_pain_points_addressed.md`](07_pain_points_addressed.md) for the full analysis.
