# System Tables × Pain Points

This document maps each pain point from
[`databricks_lakehouse/docs/01_pain_points_catalog.md`](../../databricks_lakehouse/docs/01_pain_points_catalog.md)
to the system tables that give you production visibility into whether the fix is
working — and the queries that surface the evidence.


## Pain Point 1 — Compute Cold Start

**The problem**: Job Clusters take 6–7 minutes to provision. You feel it in latency;
you can't prove it in cost; you can't measure how often it happens.

**What system tables give you**:

| Table | What to query |
|---|---|
| `system.lakeflow.job_run_timeline` | `setup_duration_seconds` — the cluster cold-start time per run |
| `system.compute.clusters` | cluster config at the time of the run (join on cluster_id + change_time) |
| `system.billing.usage` | DBU cost during the `setup_duration_seconds` window |
| `system.compute.node_timeline` | CPU/memory during setup — proves the cluster was idle, not working |

**Query**: compute cold-start cost per job, last 30 days:

```sql
SELECT
    j.name                              AS job_name,
    AVG(r.setup_duration_seconds) / 60.0    AS avg_cold_start_minutes,
    AVG(r.run_duration_seconds) / 60.0      AS avg_run_minutes,
    ROUND(
        AVG(r.setup_duration_seconds) * 100.0
        / NULLIF(AVG(r.run_duration_seconds), 0),
        1
    )                                   AS cold_start_pct_of_runtime,
    COUNT(*)                            AS run_count
FROM system.lakeflow.job_run_timeline r
JOIN (
    SELECT DISTINCT job_id, name
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
) j USING (job_id)
WHERE r.result_state = 'SUCCEEDED'
  AND r.period_start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY j.name
ORDER BY cold_start_pct_of_runtime DESC;
```

**The insight this delivers**: "Job X spends 42% of its runtime waiting for the cluster
to start. That's 6 minutes of DBU wasted per run, 180 runs/month = 1,080 wasted
cluster-minutes/month." This is the number that justifies moving to instance pools.


## Pain Point 2 — Fragile Ingestion Pipeline (status.txt)

**The problem**: ADF writes status.txt; Databricks reads it. Daily and hourly jobs
overwrite each other. Failures are silent — the file just disappears.

**What system tables give you**:

| Table | What to query |
|---|---|
| `system.lakeflow.job_run_timeline` | Every run outcome: SUCCEEDED, FAILED, TIMED_OUT — no silent failures |
| `system.lakeflow.job_task_run_timeline` | Which task within a multi-task ingestion job failed and why |
| `system.access.audit` | Did anyone manually trigger a re-run? Was the job paused/unpaused? |

**The replacement pattern**:

```python
# Instead of polling status.txt, stream failed runs to an alert table
spark.readStream \
    .option("skipChangeCommits", "true") \
    .table("system.lakeflow.job_run_timeline") \
    .filter("result_state IN ('FAILED', 'TIMED_OUT', 'ERROR')") \
    .writeStream \
    .trigger(processingTime="5 minutes") \
    .toTable("ops.failed_run_alerts")
```

No file, no race condition, no silent failure. Every termination code is recorded and
queryable.


## Pain Point 3 — Unclear Medallion Architecture

**The problem**: Gold notebooks contain business logic, data quality, AND aggregations.
No one knows what Silver is for. Layers trigger off files instead of assets.

**What system tables give you**:

| Table | What to query |
|---|---|
| `system.access.table_lineage` | Prove Bronze→Silver→Gold data flow; detect Gold→Bronze violations |
| `system.access.column_lineage` | Trace a specific column from source ODS through all layers |

**The enforcement query** (runs as a scheduled job):

```sql
-- This query should return zero rows in a healthy medallion architecture
SELECT DISTINCT
    source_table_full_name      AS bronze_source,
    target_table_full_name      AS gold_target,
    entity_type,
    entity_id,
    created_by
FROM system.access.table_lineage
WHERE source_table_full_name LIKE '%.bronze.%'
  AND target_table_full_name LIKE '%.gold.%'
  AND event_date >= CURRENT_DATE() - INTERVAL 1 DAYS;
```

Write results to `ops.medallion_violations`. If the table is non-empty, alert.
This turns an architectural principle into an automated production check.


## Pain Point 4 — CI/CD ESM Mismatch

**The problem**: Enabling a paused production job requires days of ESM approval.
Operational decisions (on/off) are treated as code deployments.

**What system tables give you**:

| Table | What to query |
|---|---|
| `system.access.audit` | Immutable record of every job enable/disable, with user, timestamp, IP |

**The compliance argument**:

ESM approval exists to prove that production changes are authorized and traceable.
`system.access.audit` provides stronger traceability automatically:

- Every change is recorded (not just the ones routed through ESM)
- 365-day retention
- Queryable — you can answer "what changed on date X?" in seconds
- Includes the user agent — Terraform changes are distinguishable from UI/manual changes

The query to show auditors:

```sql
SELECT event_time, user_identity.email, action_name,
       request_params['job_id'], source_ip_address, user_agent
FROM system.access.audit
WHERE service_name = 'jobs'
  AND action_name IN ('create', 'update', 'delete', 'runNow')
  AND event_date >= CURRENT_DATE() - INTERVAL 90 DAYS
ORDER BY event_time DESC;
```

This is the ESM audit log, automatically generated, with no approval overhead.


## Pain Point 5 — VOCP/VOCD Catalog Chaos

**The problem**: VOCP (prod) and VOCD (dev) are separate Unity Catalog catalogs.
Code that references catalog names must branch on environment. No automated check
that dev jobs don't accidentally read from prod.

**What system tables give you**:

| Table | What to query |
|---|---|
| `system.access.table_lineage` | Cross-catalog read/write events; VOCD→VOCP violations |

**The isolation validation query** (schedule daily):

```sql
-- Should return zero rows
SELECT source_table_full_name, target_table_full_name,
       entity_type, entity_id, created_by, event_time
FROM system.access.table_lineage
WHERE event_date >= CURRENT_DATE() - INTERVAL 1 DAYS
  AND (
      (source_table_full_name LIKE 'VOCP.%' AND target_table_full_name LIKE 'VOCD.%')
   OR (source_table_full_name LIKE 'VOCD.%' AND target_table_full_name LIKE 'VOCP.%')
  );
```

This catches the case where a developer copies a job from prod to dev, forgets to
change the catalog reference, and accidentally starts reading production data in a
dev pipeline.


## Pain Point 6 — Notebook-First Code Quality

**The problem**: Pipelines are written in notebooks, never profiled. Slow queries
run for hours because no one can see how bad they are.

**What system tables give you**:

| Table | What to query |
|---|---|
| `system.query.history` | Full query plan metrics: spill, shuffle, read bytes, duration per phase |

**The profiling query** — find the worst-performing notebook/job queries:

```sql
SELECT
    query_source.notebook_id        AS notebook_id,
    query_source.job_info.job_id    AS job_id,
    executed_by,
    COUNT(*)                        AS query_count,
    ROUND(AVG(total_duration_ms) / 60000.0, 1) AS avg_minutes,
    ROUND(SUM(spilled_local_bytes) / 1e9, 1)   AS total_spill_gb,
    ROUND(SUM(read_bytes) / 1e12, 2)            AS total_read_tb
FROM system.query.history
WHERE start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND execution_status = 'FINISHED'
  AND (query_source.notebook_id IS NOT NULL OR query_source.job_info.job_id IS NOT NULL)
GROUP BY notebook_id, job_id, executed_by
ORDER BY total_spill_gb DESC
LIMIT 20;
```

The result is a ranked list of the notebooks and jobs that need the most attention,
ordered by the worst operational impact metric.


## Summary Table

| Pain Point | Primary System Table | Key Column / Metric | Automated Check? |
|---|---|---|---|
| 1 — Cold start | `system.lakeflow.job_run_timeline` | `setup_duration_seconds` | Threshold alert |
| 2 — Fragile pipeline | `system.lakeflow.job_run_timeline` | `result_state`, `termination_code` | Stream to alert table |
| 3 — Medallion clarity | `system.access.table_lineage` | source/target catalog+schema | Daily scheduled query |
| 4 — ESM mismatch | `system.access.audit` | `action_name`, `user_identity` | Compliance report |
| 5 — Catalog chaos | `system.access.table_lineage` | cross-catalog source/target | Daily isolation check |
| 6 — Code quality | `system.query.history` | `spilled_local_bytes`, `total_duration_ms` | Weekly profiling report |
