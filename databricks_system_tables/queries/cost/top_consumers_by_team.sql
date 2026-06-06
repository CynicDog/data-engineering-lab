-- Purpose: DBU consumption ranked by team tag, with per-product breakdown
-- Tables:  system.billing.usage
-- Output:  team, product, sku, total_dbu, cluster_count for the last 30 days
--
-- Prerequisite: clusters and jobs must have custom_tags['team'] set
-- See docs/03_cost_attribution.md for tagging strategy

SELECT
    COALESCE(custom_tags['team'], 'untagged')           AS team,
    COALESCE(custom_tags['project'], 'untagged')        AS project,
    COALESCE(custom_tags['environment'], 'untagged')    AS environment,
    billing_origin_product                              AS product,
    sku_name,
    SUM(usage_quantity)                                 AS total_dbu,
    COUNT(DISTINCT usage_metadata.cluster_id)           AS cluster_count,
    COUNT(DISTINCT usage_metadata.job_id)               AS job_count,
    COUNT(DISTINCT usage_metadata.warehouse_id)         AS warehouse_count,
    MIN(usage_date)                                     AS first_usage_date,
    MAX(usage_date)                                     AS last_usage_date
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND usage_unit = 'DBU'
GROUP BY team, project, environment, product, sku_name
ORDER BY total_dbu DESC;
