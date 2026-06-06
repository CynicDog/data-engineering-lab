-- Purpose: surface all failed, timed-out, and errored job runs in the last 7 days
-- Tables:  system.lakeflow.job_run_timeline, system.lakeflow.jobs
-- Output:  job_name, run_id, result_state, termination_code, duration, start_time

SELECT
    j.name                                          AS job_name,
    r.run_id,
    r.result_state,
    r.termination_code,
    r.period_start_time,
    r.period_end_time,
    ROUND(r.run_duration_seconds / 60.0, 1)         AS duration_minutes,
    ROUND(r.setup_duration_seconds / 60.0, 1)       AS setup_minutes,
    r.trigger_type,
    r.run_type,
    ARRAY_JOIN(r.compute_ids, ', ')                 AS cluster_ids
FROM system.lakeflow.job_run_timeline r
JOIN (
    SELECT DISTINCT job_id, name
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
) j USING (job_id)
WHERE r.result_state IN ('FAILED', 'TIMED_OUT', 'ERROR', 'BLOCKED')
  AND r.period_start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY r.period_start_time DESC;
