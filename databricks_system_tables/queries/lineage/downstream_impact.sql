-- Purpose: find all tables and entities that read from a given source table
-- Tables:  system.access.table_lineage
-- Output:  downstream table name, entity type, entity id, last seen
-- Usage:   set ${source_table} to e.g. 'VOCP.silver.policy_clean'

SELECT DISTINCT
    target_table_full_name                      AS downstream_table,
    target_type,
    entity_type,
    entity_id,
    entity_metadata.job_info.job_id             AS job_id,
    entity_metadata.notebook_id                 AS notebook_id,
    entity_metadata.dlt_pipeline_info.dlt_pipeline_id AS pipeline_id,
    entity_metadata.dashboard_id                AS dashboard_id,
    created_by,
    MAX(event_time)                             AS last_seen,
    COUNT(*)                                    AS event_count
FROM system.access.table_lineage
WHERE source_table_full_name = '${source_table}'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND target_table_full_name IS NOT NULL
GROUP BY
    downstream_table, target_type, entity_type, entity_id,
    job_id, notebook_id, pipeline_id, dashboard_id, created_by
ORDER BY last_seen DESC;
