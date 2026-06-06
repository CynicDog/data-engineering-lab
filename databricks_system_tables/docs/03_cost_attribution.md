# Cost Attribution

## What Problem This Solves

Your Databricks invoice arrives. It says you spent ₩48M last month. Your manager asks:
which team? which job? which cluster type? You have no answer because all compute was
provisioned under one workspace with no tagging strategy.

`system.billing.usage` records every DBU consumed — by job, by cluster, by warehouse,
by pipeline, by user — with 365-day history. Combined with cluster and job metadata,
you can build a complete cost attribution system entirely in SQL.


## `system.billing.usage`

The single source of truth for all billable consumption across your entire account.
Global scope — one table, all workspaces, all regions.

| Column | Type | Meaning |
|---|---|---|
| `record_id` | string | Unique usage record (idempotency key for ETL) |
| `account_id` | string | Account |
| `workspace_id` | string | Workspace |
| `sku_name` | string | e.g., `STANDARD_ALL_PURPOSE_COMPUTE`, `PREMIUM_JOBS_COMPUTE`, `ENTERPRISE_SQL_PRO_COMPUTE` |
| `usage_date` | date | Calendar date (use for aggregation — avoids timestamp timezone issues) |
| `usage_start_time` | timestamp | UTC |
| `usage_end_time` | timestamp | UTC |
| `usage_quantity` | decimal | DBU consumed (or GB for storage) |
| `usage_unit` | string | `DBU`, `STORAGE_GB_HOUR`, `GPU_TIME`, `TOKEN`, etc. |
| `custom_tags` | map | Team, project, environment tags — **this is your cost allocation key** |
| `billing_origin_product` | string | `JOBS`, `ALL_PURPOSE`, `SQL`, `DLT`, `MODEL_SERVING`, etc. |
| `usage_metadata` | struct | Contains `cluster_id`, `job_id`, `warehouse_id`, `job_run_id`, `node_type`, `run_name` |
| `record_type` | string | `ORIGINAL`, `RETRACTION`, `RESTATEMENT` — corrections add all three |
| `identity_metadata` | struct | `run_as`, `owned_by`, `created_by` |

### Correction handling

Billing records can be corrected. When they are, Databricks inserts:
1. A `RETRACTION` row with negative `usage_quantity`
2. A `RESTATEMENT` row with the corrected value

Always aggregate using `SUM(usage_quantity)` — this correctly nets out retractions:

```sql
-- Correct
SELECT SUM(usage_quantity) FROM system.billing.usage WHERE ...;

-- Wrong — double-counts corrected records
SELECT COUNT(*), SUM(usage_quantity) FROM system.billing.usage WHERE record_type = 'ORIGINAL';
```


## Tagging Strategy (prerequisite for attribution)

System tables can only attribute cost to resources that were tagged at creation time.
Without tags, you get workspace-level aggregates at best.

Recommended tag schema:
- `team` — owning team (e.g., `actuarial`, `claims`, `platform`)
- `project` — initiative or product (e.g., `vocp-silver`, `loss-ratio-mart`)
- `environment` — `prod`, `dev`, `test`
- `managed_by` — `terraform`, `dagster`, `airflow`, `manual`

Apply tags in cluster policies or job cluster configs. In Terraform:

```hcl
resource "databricks_cluster" "silver_transform" {
  custom_tags = {
    team        = "actuarial"
    project     = "vocp-silver"
    environment = "prod"
    managed_by  = "terraform"
  }
}
```


## Production Query Patterns

### DBU spend by team (last 30 days)

```sql
SELECT
    custom_tags['team']             AS team,
    billing_origin_product          AS product,
    sku_name,
    SUM(usage_quantity)             AS total_dbu,
    COUNT(DISTINCT usage_metadata.cluster_id) AS cluster_count
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND usage_unit = 'DBU'
GROUP BY team, product, sku_name
ORDER BY total_dbu DESC;
```

→ [`../queries/cost/top_consumers_by_team.sql`](../queries/cost/top_consumers_by_team.sql)

### DBU spend by job (last 30 days)

```sql
SELECT
    usage_metadata.job_id,
    j.name                          AS job_name,
    SUM(usage_quantity)             AS total_dbu,
    COUNT(DISTINCT usage_metadata.job_run_id) AS run_count,
    ROUND(SUM(usage_quantity) / COUNT(DISTINCT usage_metadata.job_run_id), 2) AS avg_dbu_per_run
FROM system.billing.usage b
LEFT JOIN (
    SELECT DISTINCT job_id, name
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
) j ON b.usage_metadata.job_id = j.job_id
WHERE b.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND b.usage_unit = 'DBU'
  AND b.billing_origin_product = 'JOBS'
  AND b.usage_metadata.job_id IS NOT NULL
GROUP BY usage_metadata.job_id, job_name
ORDER BY total_dbu DESC
LIMIT 20;
```

→ [`../queries/cost/dbu_by_job.sql`](../queries/cost/dbu_by_job.sql)

### Daily DBU trend with 7-day moving average

```sql
WITH daily AS (
    SELECT
        usage_date,
        SUM(usage_quantity) AS daily_dbu
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE() - INTERVAL 90 DAYS
      AND usage_unit = 'DBU'
    GROUP BY usage_date
)
SELECT
    usage_date,
    daily_dbu,
    ROUND(AVG(daily_dbu) OVER (
        ORDER BY usage_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_7d
FROM daily
ORDER BY usage_date;
```

→ [`../queries/cost/daily_spend_trend.sql`](../queries/cost/daily_spend_trend.sql)


## Cluster Cost vs Utilization

Pair `billing.usage` with `compute.node_timeline` to find expensive-but-idle clusters:

```sql
WITH cluster_cost AS (
    SELECT
        usage_metadata.cluster_id                   AS cluster_id,
        SUM(usage_quantity)                         AS total_dbu
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
      AND billing_origin_product = 'ALL_PURPOSE'
    GROUP BY cluster_id
),
cluster_util AS (
    SELECT
        cluster_id,
        AVG(cpu_user_percent + cpu_system_percent)  AS avg_cpu_pct,
        AVG(mem_used_percent)                       AS avg_mem_pct
    FROM system.compute.node_timeline
    WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
      AND driver = false
    GROUP BY cluster_id
)
SELECT
    c.cluster_id,
    cl.cluster_name,
    ROUND(cost.total_dbu, 1)    AS total_dbu_7d,
    ROUND(util.avg_cpu_pct, 1)  AS avg_cpu_pct,
    ROUND(util.avg_mem_pct, 1)  AS avg_mem_pct
FROM cluster_cost cost
JOIN cluster_util util USING (cluster_id)
JOIN (
    SELECT DISTINCT cluster_id, cluster_name
    FROM system.compute.clusters
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
) cl USING (cluster_id)
WHERE util.avg_cpu_pct < 20                  -- low CPU utilization
ORDER BY cost.total_dbu_7d DESC;
```

→ See [`../queries/ops/cluster_idle_waste.sql`](../queries/ops/cluster_idle_waste.sql)


## Connecting to Pain Point 1

You know cold start costs 6–7 minutes of VM time. `billing.usage` + `compute.clusters`
lets you quantify what that costs in DBU: join `setup_duration_seconds` from
`job_run_timeline` to billing records for the same cluster/run, and you can compute
the exact DBU and cost attributable to cluster startup per job per month.

→ See [`07_pain_points_addressed.md`](07_pain_points_addressed.md) for the full analysis.
