-- Purpose: find expensive all-purpose clusters with low CPU utilization (idle waste)
-- Tables:  system.billing.usage, system.compute.node_timeline, system.compute.clusters
-- Output:  cluster_name, total DBU cost, avg CPU%, avg mem% for the last 7 days

WITH cluster_cost AS (
    SELECT
        usage_metadata.cluster_id                       AS cluster_id,
        SUM(usage_quantity)                             AS total_dbu,
        SUM(usage_quantity) * COUNT(DISTINCT usage_date) AS dbu_day_product
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
      AND billing_origin_product = 'ALL_PURPOSE'
      AND usage_unit = 'DBU'
    GROUP BY cluster_id
),
cluster_util AS (
    SELECT
        cluster_id,
        AVG(cpu_user_percent + cpu_system_percent)      AS avg_cpu_pct,
        AVG(mem_used_percent)                           AS avg_mem_pct,
        AVG(cpu_wait_percent)                           AS avg_io_wait_pct,
        COUNT(*)                                        AS sample_count
    FROM system.compute.node_timeline
    WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
      AND driver = false
    GROUP BY cluster_id
),
cluster_meta AS (
    SELECT DISTINCT cluster_id, cluster_name, owned_by,
                    worker_count, min_autoscale_workers, max_autoscale_workers,
                    dbr_version, auto_termination_minutes
    FROM system.compute.clusters
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
)
SELECT
    m.cluster_name,
    m.owned_by,
    m.worker_count,
    m.auto_termination_minutes,
    ROUND(c.total_dbu, 1)               AS total_dbu_7d,
    ROUND(u.avg_cpu_pct, 1)             AS avg_cpu_pct,
    ROUND(u.avg_mem_pct, 1)             AS avg_mem_pct,
    ROUND(u.avg_io_wait_pct, 1)         AS avg_io_wait_pct,
    u.sample_count                      AS telemetry_samples
FROM cluster_cost c
JOIN cluster_util u USING (cluster_id)
JOIN cluster_meta m USING (cluster_id)
WHERE u.avg_cpu_pct < 20               -- flag clusters under 20% CPU utilization
ORDER BY c.total_dbu_7d DESC;
