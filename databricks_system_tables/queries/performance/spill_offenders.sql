-- Purpose: find queries that spilled significant data to local disk (memory pressure)
-- Tables:  system.query.history
-- Output:  who, what, how much spill, how much shuffle, which job/notebook caused it

SELECT
    statement_id,
    executed_by,
    ROUND(spilled_local_bytes / 1e9, 1)                 AS spill_gb,
    ROUND(shuffle_read_bytes / 1e9, 1)                  AS shuffle_gb,
    ROUND(total_duration_ms / 60000.0, 2)               AS total_minutes,
    ROUND(execution_duration_ms / 60000.0, 2)           AS exec_minutes,
    ROUND(read_bytes / 1e9, 2)                          AS read_gb,
    ROUND(produced_rows / 1e6, 2)                       AS produced_millions_rows,
    client_application,
    compute.warehouse_id,
    compute.cluster_id,
    query_source.job_info.job_id                        AS job_id,
    query_source.job_info.job_run_id                    AS job_run_id,
    query_source.notebook_id                            AS notebook_id,
    statement_type,
    start_time,
    LEFT(statement_text, 400)                           AS query_preview
FROM system.query.history
WHERE start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND spilled_local_bytes > 1e10          -- 10 GB threshold
  AND execution_status = 'FINISHED'
ORDER BY spilled_local_bytes DESC
LIMIT 50;
