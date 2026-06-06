-- Purpose: measure Delta IO cache and result cache effectiveness per warehouse
-- Tables:  system.query.history
-- Output:  warehouse_id, query_count, cache hit rates, avg duration, total data read

SELECT
    COALESCE(compute.warehouse_id, compute.cluster_id)  AS compute_id,
    CASE
        WHEN compute.warehouse_id IS NOT NULL THEN 'WAREHOUSE'
        ELSE 'SERVERLESS'
    END                                                 AS compute_type,
    COUNT(*)                                            AS query_count,
    ROUND(AVG(read_io_cache_percent), 1)                AS avg_io_cache_pct,
    ROUND(
        SUM(CASE WHEN from_result_cache THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    )                                                   AS result_cache_hit_pct,
    ROUND(SUM(read_bytes) / 1e12, 3)                    AS total_read_tb,
    ROUND(AVG(total_duration_ms) / 1000.0, 1)           AS avg_duration_seconds,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
        ORDER BY total_duration_ms) / 1000.0, 1)        AS p95_duration_seconds,
    SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    MIN(start_time)                                     AS window_start,
    MAX(end_time)                                       AS window_end
FROM system.query.history
WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND execution_status IN ('FINISHED', 'FAILED')
GROUP BY compute_id, compute_type
ORDER BY query_count DESC;
