-- Purpose: detect unauthorized cross-catalog reads between VOCP (prod) and VOCD (dev)
-- Tables:  system.access.table_lineage
-- Output:  violating source/target pairs, entity, creator, event time
--
-- This query should return zero rows in a healthy environment.
-- Schedule as a daily job; write results to ops.catalog_isolation_violations.
-- Adjust catalog name prefixes to match your Unity Catalog naming convention.

SELECT
    source_table_full_name,
    source_type,
    target_table_full_name,
    target_type,
    entity_type,
    entity_id,
    entity_metadata.job_info.job_id             AS job_id,
    entity_metadata.notebook_id                 AS notebook_id,
    created_by,
    event_time,
    CASE
        WHEN source_table_full_name LIKE 'VOCP.%'
         AND target_table_full_name LIKE 'VOCD.%'
        THEN 'prod_to_dev'
        WHEN source_table_full_name LIKE 'VOCD.%'
         AND target_table_full_name LIKE 'VOCP.%'
        THEN 'dev_to_prod'
    END                                         AS violation_direction
FROM system.access.table_lineage
WHERE event_date >= CURRENT_DATE() - INTERVAL 1 DAYS
  AND (
      (source_table_full_name LIKE 'VOCP.%' AND target_table_full_name LIKE 'VOCD.%')
   OR (source_table_full_name LIKE 'VOCD.%' AND target_table_full_name LIKE 'VOCP.%')
  )
ORDER BY event_time DESC;
