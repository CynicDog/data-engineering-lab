-- Purpose: find all tables that are read to produce a given target table
-- Tables:  system.access.table_lineage
-- Output:  upstream table, entity type, who writes to target, last seen
-- Usage:   set ${target_table} to e.g. 'VOCP.gold.loss_ratio_daily'

SELECT DISTINCT
    source_table_full_name                      AS upstream_table,
    source_type,
    entity_type,
    entity_id,
    entity_metadata.job_info.job_id             AS job_id,
    entity_metadata.notebook_id                 AS notebook_id,
    entity_metadata.dlt_pipeline_info.dlt_pipeline_id AS pipeline_id,
    created_by,
    direct_access,
    MAX(event_time)                             AS last_seen,
    COUNT(*)                                    AS event_count
FROM system.access.table_lineage
WHERE target_table_full_name = '${target_table}'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND source_table_full_name IS NOT NULL
GROUP BY
    upstream_table, source_type, entity_type, entity_id,
    job_id, notebook_id, pipeline_id, created_by, direct_access
ORDER BY direct_access DESC, last_seen DESC;
