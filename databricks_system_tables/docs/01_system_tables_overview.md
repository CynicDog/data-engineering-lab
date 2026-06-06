# System Tables Overview

## What They Are

Unity Catalog's `system` catalog is a read-only, platform-managed set of Delta tables
that Databricks populates automatically. No ETL, no exporters, no agents. Every job
run, every query, every compute event, every data access — all written to Delta tables
with the same ACID guarantees and SQL interface as your own tables.

They are the operational nervous system of your Databricks platform.

```
system
├── access
│   ├── audit            -- every API call and UI action
│   ├── table_lineage    -- table-level read/write events
│   └── column_lineage   -- column-level read/write events
├── billing
│   └── usage            -- DBU consumption per resource per hour
├── compute
│   ├── clusters         -- cluster config history (SCD2)
│   ├── node_types       -- available VM specs
│   ├── node_timeline    -- per-minute CPU/memory/disk/network
│   ├── instance_events  -- VM state transitions
│   └── instance_pools   -- pool config history (SCD2)
├── lakeflow
│   ├── jobs             -- job config history (SCD2)
│   ├── job_tasks        -- task config history (SCD2)
│   ├── job_run_timeline -- job run start/end + result_state
│   ├── job_task_run_timeline -- task run start/end + result_state
│   ├── pipelines        -- DLT pipeline config history
│   └── pipeline_update_timeline -- DLT update start/end + result_state
└── query
    └── history          -- SQL warehouse + serverless query records
```


## Enablement

### Requirements
1. Unity Catalog enabled workspace
2. Metastore privilege model v1.0+ (most workspaces since 2023)
3. Account-level system tables enabled (usually on by default)

### Access Control

Account admins and metastore admins get access automatically. For all other users or
service principals, grant at each level:

```sql
-- Account admin runs this once
GRANT USE CATALOG ON CATALOG system TO `data-platform-team`;
GRANT USE SCHEMA ON SCHEMA system.access TO `data-platform-team`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `data-platform-team`;
GRANT USE SCHEMA ON SCHEMA system.compute TO `data-platform-team`;
GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `data-platform-team`;
GRANT USE SCHEMA ON SCHEMA system.query TO `data-platform-team`;

GRANT SELECT ON TABLE system.access.audit TO `data-platform-team`;
GRANT SELECT ON TABLE system.billing.usage TO `data-platform-team`;
-- ... repeat for each table you want to expose
```

Principle of least privilege: ops teams need `lakeflow` + `compute`; cost owners need
`billing`; data governance needs `access` (lineage + audit); DBAs need `query`.


## Retention

| Table | Retention | Notes |
|---|---|---|
| `system.access.audit` | 365 days | Regional for workspace events, global for account events |
| `system.billing.usage` | 365 days | Global; corrections create RETRACTION + RESTATEMENT rows |
| `system.compute.clusters` | 365 days | SCD2; latest record per cluster always kept regardless |
| `system.compute.node_timeline` | 90 days | Only for runs > 10 minutes |
| `system.compute.instance_events` | 365 days | State transitions only |
| `system.compute.instance_pools` | 365 days | SCD2 |
| `system.lakeflow.jobs` | 365 days | SCD2; latest record per job always kept |
| `system.lakeflow.job_run_timeline` | 365 days | Runs > 1 hour sliced into hourly rows |
| `system.lakeflow.job_task_run_timeline` | 365 days | Same slicing as job_run_timeline |
| `system.query.history` | 365 days | No streaming support |
| `system.access.table_lineage` | 1-year rolling | Old events removed as new ones arrive |
| `system.access.column_lineage` | 1-year rolling | Same as table_lineage |


## SCD2 Tables

`clusters`, `jobs`, `job_tasks`, `instance_pools`, and `pipelines` are slow-changing
dimension tables. Each configuration change creates a new row — the old row stays.

To get the current state of a job:

```sql
SELECT *
FROM system.lakeflow.jobs
QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1;
```

To get the state of a job at a point in time:

```sql
SELECT *
FROM system.lakeflow.jobs
WHERE job_id = '${job_id}'
  AND change_time <= '2025-01-15 03:00:00'
QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1;
```


## Streaming System Tables

Most tables support Structured Streaming. Key constraint: always set `skipChangeCommits`
to `true` because system tables use Delta's change data feed internally.

```python
spark.readStream \
    .option("skipChangeCommits", "true") \
    .table("system.lakeflow.job_run_timeline") \
    .writeStream \
    .trigger(processingTime="5 minutes") \
    .table("ops.job_run_alerts")
```

The 7-day VACUUM window on system tables is the hard constraint: if your stream lags
more than 7 days, it will fail to restart. Monitor stream lag aggressively.


## Customer-Managed Keys (CMK)

If your workspace uses customer-managed keys (relevant for 망분리 environments with
strict data residency), `system.query.history` will have `statement_text` and
`error_message` encrypted by default. You must configure the decryption key in the
`system` catalog before these fields become readable. Allow 24 hours after configuration
for decryption to propagate.

→ See [`02_operations_monitoring.md`](02_operations_monitoring.md) for job run queries.
→ See [`03_cost_attribution.md`](03_cost_attribution.md) for billing queries.
→ See [`04_query_performance.md`](04_query_performance.md) for query history queries.
→ See [`05_data_lineage_governance.md`](05_data_lineage_governance.md) for lineage queries.
→ See [`06_security_audit.md`](06_security_audit.md) for audit log queries.
