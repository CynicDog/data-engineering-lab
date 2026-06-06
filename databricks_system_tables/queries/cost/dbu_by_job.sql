-- Purpose: rank jobs by DBU consumption with avg cost per run
-- Tables:  system.billing.usage, system.lakeflow.jobs
-- Output:  job_name, total_dbu, run_count, avg_dbu_per_run for the last 30 days

SELECT
    b.usage_metadata.job_id                         AS job_id,
    j.name                                          AS job_name,
    j.run_as_user_name                              AS run_as,
    SUM(b.usage_quantity)                           AS total_dbu,
    COUNT(DISTINCT b.usage_metadata.job_run_id)     AS run_count,
    ROUND(
        SUM(b.usage_quantity)
        / NULLIF(COUNT(DISTINCT b.usage_metadata.job_run_id), 0),
        2
    )                                               AS avg_dbu_per_run,
    b.sku_name,
    MIN(b.usage_date)                               AS first_seen,
    MAX(b.usage_date)                               AS last_seen
FROM system.billing.usage b
LEFT JOIN (
    SELECT DISTINCT job_id, name, run_as_user_name
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
) j ON b.usage_metadata.job_id = j.job_id
WHERE b.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND b.usage_unit = 'DBU'
  AND b.billing_origin_product = 'JOBS'
  AND b.usage_metadata.job_id IS NOT NULL
GROUP BY job_id, job_name, run_as, b.sku_name
ORDER BY total_dbu DESC
LIMIT 50;
