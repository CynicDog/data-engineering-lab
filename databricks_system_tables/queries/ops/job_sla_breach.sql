-- Purpose: identify job runs that exceeded their expected SLA duration
-- Tables:  system.lakeflow.job_run_timeline, system.lakeflow.jobs, ops.job_sla_config (yours)
-- Output:  job_name, run_id, actual vs SLA duration, overage
--
-- Prerequisite: create ops.job_sla_config with columns (job_id STRING, sla_minutes INT)
-- Example:  INSERT INTO ops.job_sla_config VALUES ('123456789', 30);

WITH sla AS (
    SELECT job_id, sla_minutes
    FROM ops.job_sla_config
),
runs AS (
    SELECT
        job_id,
        run_id,
        period_start_time,
        run_duration_seconds,
        setup_duration_seconds,
        queue_duration_seconds,
        result_state
    FROM system.lakeflow.job_run_timeline
    WHERE result_state IS NOT NULL
      AND period_start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
),
job_names AS (
    SELECT DISTINCT job_id, name
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
)
SELECT
    jn.name                                                             AS job_name,
    r.run_id,
    r.result_state,
    ROUND(r.run_duration_seconds / 60.0, 1)                            AS actual_minutes,
    s.sla_minutes,
    ROUND(r.run_duration_seconds / 60.0 - s.sla_minutes, 1)           AS overage_minutes,
    ROUND(r.setup_duration_seconds / 60.0, 1)                          AS setup_minutes,
    ROUND(r.queue_duration_seconds / 60.0, 1)                          AS queue_minutes,
    r.period_start_time
FROM runs r
JOIN sla s USING (job_id)
JOIN job_names jn USING (job_id)
WHERE r.run_duration_seconds / 60.0 > s.sla_minutes
ORDER BY overage_minutes DESC;
