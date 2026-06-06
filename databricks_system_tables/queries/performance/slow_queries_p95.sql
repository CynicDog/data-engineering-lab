-- Purpose: surface the slowest queries (above P95 duration) in the last 7 days
-- Tables:  system.query.history
-- Output:  statement details, duration breakdown, read stats, spill, query source

WITH p95 AS (
    SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) AS threshold
    FROM system.query.history
    WHERE execution_status = 'FINISHED'
      AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
)
SELECT
    h.statement_id,
    h.executed_by,
    h.client_application,
    ROUND(h.total_duration_ms / 60000.0, 2)                 AS total_minutes,
    ROUND(h.waiting_for_compute_duration_ms / 1000.0, 1)    AS compute_wait_seconds,
    ROUND(h.waiting_at_capacity_duration_ms / 1000.0, 1)    AS capacity_wait_seconds,
    ROUND(h.compilation_duration_ms / 1000.0, 1)            AS compile_seconds,
    ROUND(h.execution_duration_ms / 60000.0, 2)             AS exec_minutes,
    ROUND(h.read_bytes / 1e9, 2)                            AS read_gb,
    ROUND(h.spilled_local_bytes / 1e9, 2)                   AS spill_gb,
    ROUND(h.shuffle_read_bytes / 1e9, 2)                    AS shuffle_gb,
    h.read_partitions,
    h.pruned_files,
    h.read_io_cache_percent,
    h.from_result_cache,
    h.statement_type,
    h.query_source.job_info.job_id                          AS job_id,
    h.query_source.notebook_id                              AS notebook_id,
    h.query_source.dashboard_id                             AS dashboard_id,
    h.start_time,
    LEFT(h.statement_text, 300)                             AS query_preview
FROM system.query.history h
CROSS JOIN p95
WHERE h.execution_status = 'FINISHED'
  AND h.start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND h.total_duration_ms >= p95.threshold
ORDER BY h.total_duration_ms DESC
LIMIT 100;
