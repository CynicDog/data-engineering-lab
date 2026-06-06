# Operations Monitoring

## What Problem This Solves

Your pipelines run at 02:00. You arrive at 09:00. A job failed at 03:17 but there was
no alert — the failure mode was TIMED_OUT (not ERROR), and your alerting only catches
ERROR. By 09:00 the downstream Gold mart is 6 hours stale and no one knows.

`system.lakeflow` is the authoritative record of every job run, every task run, and
every result state. It's what the Databricks UI reads. Querying it directly lets you
build monitoring that goes beyond what the UI exposes.


## Tables

### `system.lakeflow.job_run_timeline`

The primary table for pipeline health monitoring. One row per job run (or per clock-hour
for runs longer than 1 hour).

| Column | Type | Meaning |
|---|---|---|
| `job_id` | string | Unique within workspace |
| `run_id` | string | Run identifier |
| `period_start_time` | timestamp | Run start (UTC) or hour boundary for long runs |
| `period_end_time` | timestamp | Run end (UTC) |
| `result_state` | string | SUCCEEDED, FAILED, SKIPPED, CANCELLED, TIMED_OUT, ERROR, BLOCKED — only in final slice |
| `termination_code` | string | SUCCESS, CANCELLED, DRIVER_ERROR, CLUSTER_ERROR, etc. |
| `trigger_type` | string | CRON, FILE_ARRIVAL, TABLE, ONETIME, etc. |
| `run_type` | string | JOB_RUN, SUBMIT_RUN, WORKFLOW_RUN |
| `compute_ids` | array | Cluster or warehouse IDs used |
| `run_duration_seconds` | long | Total wall-clock duration |
| `queue_duration_seconds` | long | Time spent waiting for compute capacity |
| `setup_duration_seconds` | long | Cluster cold-start time |

**Slicing**: runs longer than 1 hour produce multiple rows aligned to clock-hour
boundaries. `result_state` is only set in the *last* row. Always filter for the
final slice when you need the outcome:

```sql
-- Get only the terminal row for each run
SELECT *
FROM system.lakeflow.job_run_timeline
WHERE result_state IS NOT NULL;
```

### `system.lakeflow.job_task_run_timeline`

Same structure as `job_run_timeline` but at task granularity. Useful for pinpointing
which task within a multi-task job is the bottleneck or failure source.

| Column | Type | Meaning |
|---|---|---|
| `task_key` | string | Task name within the job |
| `job_run_id` | string | Parent job run ID (join to `job_run_timeline`) |
| `execution_duration_seconds` | long | Pure execution time (excludes setup/cleanup) |

### `system.lakeflow.jobs` (SCD2)

Job configuration history. Use to get the job name when joining to run timelines:

```sql
SELECT DISTINCT job_id, name
FROM system.lakeflow.jobs
QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1;
```


## Production Query Patterns

### Failed and timed-out runs in the last 7 days

```sql
SELECT
    j.name                              AS job_name,
    r.run_id,
    r.result_state,
    r.termination_code,
    r.period_start_time,
    r.period_end_time,
    ROUND(r.run_duration_seconds / 60.0, 1) AS duration_minutes
FROM system.lakeflow.job_run_timeline r
JOIN (
    SELECT DISTINCT job_id, name
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
) j USING (job_id)
WHERE r.result_state IN ('FAILED', 'TIMED_OUT', 'ERROR')
  AND r.period_start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY r.period_start_time DESC;
```

→ [`../queries/ops/failed_runs_last_7d.sql`](../queries/ops/failed_runs_last_7d.sql)

### SLA breach detection

Flag jobs that ran longer than their expected SLA.

```sql
-- Define SLA map per job (maintain this in your own table)
WITH sla AS (
    SELECT job_id, sla_minutes
    FROM ops.job_sla_config        -- your table
),
runs AS (
    SELECT
        job_id,
        run_id,
        period_start_time,
        run_duration_seconds,
        result_state
    FROM system.lakeflow.job_run_timeline
    WHERE result_state IS NOT NULL
      AND period_start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT
    j.name,
    r.run_id,
    r.result_state,
    ROUND(r.run_duration_seconds / 60.0, 1) AS actual_minutes,
    s.sla_minutes,
    ROUND(r.run_duration_seconds / 60.0 - s.sla_minutes, 1) AS overage_minutes
FROM runs r
JOIN sla s USING (job_id)
JOIN (
    SELECT DISTINCT job_id, name
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
) j USING (job_id)
WHERE r.run_duration_seconds / 60.0 > s.sla_minutes
ORDER BY overage_minutes DESC;
```

→ [`../queries/ops/job_sla_breach.sql`](../queries/ops/job_sla_breach.sql)

### Task-level failure breakdown

When a job fails, which task failed, and how long did it run before failing?

```sql
SELECT
    t.task_key,
    t.result_state,
    t.termination_code,
    ROUND(t.execution_duration_seconds / 60.0, 1) AS exec_minutes,
    t.period_start_time
FROM system.lakeflow.job_task_run_timeline t
WHERE t.job_run_id = '${job_run_id}'
  AND t.result_state IS NOT NULL
ORDER BY t.period_start_time;
```


## P99 Run Duration Trend

Track whether your pipelines are getting slower over time:

```sql
SELECT
    j.name                              AS job_name,
    DATE_TRUNC('week', r.period_start_time) AS week,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY r.run_duration_seconds) / 60.0, 1) AS p50_min,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY r.run_duration_seconds) / 60.0, 1) AS p95_min,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY r.run_duration_seconds) / 60.0, 1) AS p99_min,
    COUNT(*) AS run_count
FROM system.lakeflow.job_run_timeline r
JOIN (
    SELECT DISTINCT job_id, name
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
) j USING (job_id)
WHERE r.result_state = 'SUCCEEDED'
  AND r.period_start_time >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY j.name, week
ORDER BY j.name, week;
```


## Connecting to Pain Point 2

The status.txt pattern fails silently. `job_run_timeline` fails loudly — every
termination code is recorded. Pair it with a streaming write to an alert table:

```python
(
    spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.lakeflow.job_run_timeline")
        .filter("result_state IN ('FAILED','TIMED_OUT','ERROR')")
        .writeStream
        .trigger(processingTime="5 minutes")
        .toTable("ops.failed_run_alerts")
)
```

An Airflow sensor or Databricks Alert can then read `ops.failed_run_alerts` and
notify on-call. The entire pipeline failure history is auditable, joinable, and
queryable — none of which was true with a status file.

→ See [`07_pain_points_addressed.md`](07_pain_points_addressed.md) for the full mapping.
