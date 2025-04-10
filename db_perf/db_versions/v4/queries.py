COST_ATTRIBUTION_QUERY = """
WITH pipeline_run_stats AS (
  SELECT
    e.pipeline_name,
    e.environment,
    e.pipeline_type,
    e.user_operator,
    e.run_id,
    MIN(e.timestamp) AS start_time,
    MAX(e.timestamp) AS end_time,
    MAX(am.cost_per_hour) AS cost_per_hour,
    MAX(sm.cpu_utilization) AS cpu_usage,
    MAX(sm.memory_used / 1024.0 / 1024 / 1024) AS mem_used_gb,
    ROW_NUMBER() OVER (
      PARTITION BY e.pipeline_name, e.environment, e.pipeline_type, e.user_operator
      ORDER BY MAX(e.timestamp) DESC
    ) AS last_run_rank
  FROM events e
  LEFT JOIN system_metrics sm ON e.event_id = sm.event_id
  LEFT JOIN aws_metadata am ON e.event_id = am.event_id
  WHERE e.pipeline_name IS NOT NULL AND e.pipeline_name != ''
  GROUP BY e.pipeline_name, e.environment, e.pipeline_type, e.user_operator, e.run_id
),

pipeline_summary AS (
  SELECT
    pipeline_name,
    environment,
    pipeline_type,
    user_operator,
    COUNT(*) AS run_count,
    MAX(end_time) AS last_activity_timestamp,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 3600 * cost_per_hour) AS avg_cost_per_run,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS avg_run_time_minutes,
    AVG(cpu_usage) AS avg_cpu_usage,
    AVG(mem_used_gb) AS avg_ram_used_gb,
    MAX(start_time) FILTER (WHERE last_run_rank = 1) AS last_run_start_date
  FROM pipeline_run_stats
  GROUP BY pipeline_name, environment, pipeline_type, user_operator
)

SELECT
  COALESCE(NULLIF(ps.pipeline_name, ''), 'pipeline_name_not_available') AS "Pipeline Name",
  CASE
    WHEN ps.last_activity_timestamp < NOW() - INTERVAL '20 seconds' THEN 'Completed'
    ELSE 'Running'
  END AS "Status",
  e.department AS "Analysis type",
  COALESCE(
    CONCAT_WS(', ',
      CASE WHEN ps.environment IS NOT NULL THEN 'env:' || ps.environment END,
      CASE WHEN ps.pipeline_type IS NOT NULL THEN 'type:' || ps.pipeline_type END,
      CASE WHEN ps.user_operator IS NOT NULL THEN 'operator:' || ps.user_operator END
    ),
    ''
  ) AS "Tags",
  ps.run_count AS "# of Runs",
  TO_CHAR(ps.last_run_start_date, 'DD Mon YYYY HH24:MI') AS "Last Run Date",
  ps.avg_run_time_minutes AS "AVG Runtime",
  ps.avg_ram_used_gb AS "Avg Max RAM",
  ps.avg_cpu_usage AS "Avg Max CPU %",
  ps.avg_cost_per_run AS "AVG Costs"
FROM pipeline_summary ps
JOIN events e ON ps.pipeline_name = e.pipeline_name
GROUP BY ps.pipeline_name, ps.environment, ps.pipeline_type, ps.user_operator, ps.run_count, 
         ps.last_activity_timestamp, ps.avg_run_time_minutes, ps.avg_ram_used_gb, 
         ps.avg_cpu_usage, ps.avg_cost_per_run, ps.last_run_start_date, e.department
ORDER BY ps.last_activity_timestamp DESC, ps.run_count;
"""


# Average pipeline Duration 6 months
AVG_PIPELINE_DURATION_6MONTHS = """
WITH months AS (
  SELECT generate_series(
    DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 months',
    DATE_TRUNC('month', CURRENT_DATE),
    INTERVAL '1 month'
  ) AS month_timestamp
),

run_durations AS (
  SELECT
    e.run_id,
    e.pipeline_name,
    DATE_TRUNC('month', e.timestamp) AS month_timestamp,
    EXTRACT(EPOCH FROM (MAX(e.timestamp) - MIN(e.timestamp))) / 3600 AS run_duration_hours
  FROM events e
  WHERE e.pipeline_name IS NOT NULL
    AND e.timestamp >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 months'
  GROUP BY e.run_id, e.pipeline_name, DATE_TRUNC('month', e.timestamp)
),

time_series_runtime AS (
  SELECT
    month_timestamp,
    AVG(run_duration_hours) AS average_runtime_hours,
    COUNT(DISTINCT pipeline_name) AS unique_pipelines
  FROM run_durations
  GROUP BY month_timestamp
)

SELECT
  m.month_timestamp::timestamp AS time,
  COALESCE(t.average_runtime_hours, 0)::float AS average_pipeline_runtime_hours
FROM months m
LEFT JOIN time_series_runtime t ON m.month_timestamp = t.month_timestamp
ORDER BY time;

"""


# status pipeline runs this month.
STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY = """
WITH current_month_runs AS (
  SELECT DISTINCT run_id
  FROM events
  WHERE timestamp >= DATE_TRUNC('month', CURRENT_DATE)
    AND timestamp < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
),

run_statuses AS (
  SELECT
    e.run_id,
    CASE
      WHEN e.process_status = 'failed' THEN 'Failed'
      WHEN MAX(e.timestamp) < NOW() - INTERVAL '30 seconds' THEN 'Completed'
      ELSE 'Running'
    END AS status
  FROM events e
  JOIN current_month_runs cmr ON e.run_id = cmr.run_id
  GROUP BY e.run_id, e.process_status
)

SELECT
  COUNT(*) FILTER (WHERE status = 'Completed') AS "Completed",
  COUNT(*) FILTER (WHERE status = 'Failed') AS "Failed",
  COUNT(*) FILTER (WHERE status = 'Running') AS "Running"
FROM run_statuses;

"""
