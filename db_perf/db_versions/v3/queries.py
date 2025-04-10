COST_ATTRIBUTION_QUERY = """
WITH run_stats AS (
  SELECT
    pipeline_name,
    tags,
    run_id,
    MIN(event_timestamp) AS start_time,
    MAX(event_timestamp) AS end_time,
    MAX(ec2_cost_per_hour) AS cost_per_hour,
    FIRST_VALUE(MIN(event_timestamp)) OVER (
      PARTITION BY pipeline_name, tags 
      ORDER BY MAX(event_timestamp) DESC
    ) AS last_run_start
  FROM batch_jobs_logs
  WHERE pipeline_name IS NOT NULL
  GROUP BY pipeline_name, tags, run_id
)
SELECT 
  pipeline_name AS "Pipeline Name",
  CASE WHEN MAX(end_time) < NOW() - INTERVAL '20 seconds' THEN 'Completed' ELSE 'Running' END AS "Status",
  CASE 
    WHEN pipeline_name ILIKE '%atac%' THEN 'ATAC-seq'
    WHEN pipeline_name ILIKE '%chip%' THEN 'ChIP-seq' 
    ELSE 'RNA-seq'
  END AS "Analysis type",
  COALESCE(
    (SELECT STRING_AGG(value, ', ' ORDER BY key) 
     FROM jsonb_each_text(tags) 
     WHERE jsonb_typeof(tags) = 'object'),
    ''
  ) AS "Tags",
  COUNT(*) AS "# of Runs",
  TO_CHAR(MAX(last_run_start), 'DD Mon YYYY HH24:MI') AS "Last Run Date",
  AVG(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS "AVG Runtime",
  AVG(mem_used)/1073741824 AS "Avg Max RAM",
  AVG(cpu_usage) AS "Avg Max CPU %",
  AVG(EXTRACT(EPOCH FROM (end_time - start_time))/3600 * cost_per_hour) AS "AVG Costs"
FROM run_stats
GROUP BY pipeline_name, tags
ORDER BY MAX(end_time) DESC;
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
    DATE_TRUNC('month', first_event) AS month_timestamp,
    pipeline_name,
    EXTRACT(EPOCH FROM (last_event - first_event)) / 3600 AS duration_hours
  FROM (
    -- This subquery leverages idx_core_metrics (pipeline_name, event_timestamp DESC, run_id)
    SELECT
      pipeline_name,
      run_id,
      MIN(event_timestamp) AS first_event,
      MAX(event_timestamp) AS last_event
    FROM batch_jobs_logs
    WHERE 
      pipeline_name IS NOT NULL
      AND event_timestamp >= DATE_TRUNC('month', NOW()) - INTERVAL '6 months'
    GROUP BY pipeline_name, run_id  -- Grouping order matches index
  ) runs
  /* Index Usage:
     1. idx_core_metrics: 
        - Filters on pipeline_name IS NOT NULL
        - Range scan on event_timestamp 
        - Provides run_id for grouping
     2. idx_pipeline_types (text_pattern_ops):
        - Helps with pipeline_name grouping
  */
),

monthly_stats AS (
  SELECT
    month_timestamp,
    AVG(duration_hours) AS avg_runtime,
    COUNT(DISTINCT pipeline_name) AS unique_pipelines
  FROM run_durations
  GROUP BY month_timestamp
  /* No index needed - works in memory */
)

SELECT
  m.month_timestamp::timestamp AS time,
  COALESCE(s.avg_runtime, 0)::float AS average_pipeline_runtime_hours
FROM months m
LEFT JOIN monthly_stats s ON m.month_timestamp = s.month_timestamp
ORDER BY time;
"""

# status pipeline runs this month.
STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY = """
WITH run_status AS (
  SELECT 
    run_id,
    MAX(event_timestamp) AS last_ts,
    BOOL_OR(tags @> '{"status":"failed"}') AS is_failed
  FROM batch_jobs_logs
  WHERE event_timestamp >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY run_id
)
SELECT
  COUNT(*) FILTER (WHERE NOT is_failed AND last_ts < NOW() - INTERVAL '30 seconds') AS "Completed",
  COUNT(*) FILTER (WHERE is_failed) AS "Failed",
  COUNT(*) FILTER (WHERE last_ts >= NOW() - INTERVAL '30 seconds') AS "Running"
FROM run_status;
"""
