COST_ATTRIBUTION_QUERY = """

WITH pipeline_run_stats AS (
  SELECT
    p.name AS pipeline_name,
    pr.environment,
    pr.pipeline_type,
    pr.user_operator,
    pr.run_id,
    MIN(rm.timestamp) AS start_time,
    MAX(rm.timestamp) AS end_time,
    MAX(rm.ec2_cost_per_hour) AS cost_per_hour,
    MAX(rm.cpu_usage) AS cpu_usage,
    MAX(rm.mem_used_gb) AS mem_used_gb,
    ROW_NUMBER() OVER (
      PARTITION BY p.name, pr.environment, pr.pipeline_type, pr.user_operator
      ORDER BY MAX(rm.timestamp) DESC
    ) AS last_run_rank
  FROM pipeline_run_status pr  -- Changed to use the view
  JOIN pipelines p ON pr.pipeline_id = p.id
  LEFT JOIN run_metrics rm ON pr.id = rm.run_id
  WHERE p.name IS NOT NULL AND p.name != ''
  GROUP BY p.name, pr.environment, pr.pipeline_type, pr.user_operator, pr.run_id
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
  p.analysis_type AS "Analysis type",
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
JOIN pipelines p ON ps.pipeline_name = p.name
ORDER BY ps.last_activity_timestamp DESC, ps.run_count;
"""


# Average pipeline Duration 6 months
AVG_PIPELINE_DURATION_6MONTHS = """
WITH months AS (
  SELECT generate_series(
    DATE_TRUNC('month', NOW()) - INTERVAL '6 months',
    DATE_TRUNC('month', NOW()),
    INTERVAL '1 month'
  ) AS month_timestamp
),

run_durations AS (
  SELECT
    pr.run_id,
    p.name AS pipeline_name,
    DATE_TRUNC('month', pr.start_time) AS month_timestamp,
    EXTRACT(EPOCH FROM (MAX(rm.timestamp) - MIN(rm.timestamp))) / 3600 AS run_duration_hours
  FROM pipeline_runs pr  -- Keeps using base table (no status needed)
  JOIN pipelines p ON pr.pipeline_id = p.id
  JOIN run_metrics rm ON pr.id = rm.run_id
  WHERE p.name IS NOT NULL
  GROUP BY pr.run_id, p.name, DATE_TRUNC('month', pr.start_time)
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
  SELECT 
    pr.id, 
    pr.run_id,
    CASE
      WHEN pr.raw_attributes @> '{"status": "failed"}'::jsonb THEN 'Failed'
      WHEN pr.is_active THEN 'Running'
      ELSE 'Completed'
    END AS computed_status
  FROM pipeline_runs pr
  WHERE pr.start_time >= DATE_TRUNC('month', CURRENT_DATE)
    AND pr.start_time < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
)

SELECT
  COUNT(*) FILTER (WHERE computed_status = 'Completed') AS "Completed",
  COUNT(*) FILTER (WHERE computed_status = 'Failed') AS "Failed",
  COUNT(*) FILTER (WHERE computed_status = 'Running') AS "Running"
FROM current_month_runs;

"""

# makes use of the view
STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY_V1 = """
WITH current_month_runs AS (
  SELECT 
    pr.id, 
    pr.run_id,  -- Explicitly use pr.run_id
    prs.status,
    pr.end_time,
    pr.is_active
  FROM pipeline_runs pr
  JOIN pipeline_run_status prs ON pr.id = prs.id
  WHERE pr.start_time >= DATE_TRUNC('month', CURRENT_DATE)
    AND pr.start_time < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
),

failed_runs AS (
  SELECT DISTINCT pr.run_id  -- Explicitly specify pr.run_id
  FROM pipeline_runs pr
  JOIN run_metrics rm ON pr.id = rm.run_id
  WHERE rm.timestamp >= DATE_TRUNC('month', CURRENT_DATE)
    AND pr.raw_attributes @> '{"status": "failed"}'::jsonb
)

SELECT
  COUNT(*) FILTER (WHERE NOT is_active AND status = 'Completed') AS "Completed",
  COUNT(*) FILTER (WHERE EXISTS (
    SELECT 1 FROM failed_runs fr WHERE fr.run_id = current_month_runs.run_id  -- Use full qualifier
  )) AS "Failed",
  COUNT(*) FILTER (WHERE is_active) AS "Running"
FROM current_month_runs;

"""
