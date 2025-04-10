COST_ATTRIBUTION_QUERY = """

WITH run_metrics AS (
  SELECT
    pipeline_name,
    tags,
    run_id,
    MIN(event_timestamp) AS start_time,
    MAX(event_timestamp) AS end_time,
    MAX(ec2_cost_per_hour) AS cost_per_hour,
    MAX(cpu_usage) AS cpu_usage,
    MAX(mem_used) AS mem_used,
    ROW_NUMBER() OVER (
      PARTITION BY pipeline_name, tags
      ORDER BY MAX(event_timestamp) DESC
    ) AS last_run_rank
  FROM batch_jobs_logs
  WHERE
    pipeline_name IS NOT NULL
    AND pipeline_name != ''
  GROUP BY pipeline_name, tags, run_id
),

pipeline_summary AS (
  SELECT
    pipeline_name,
    tags,
    COUNT(*) AS run_count,
    MAX(end_time) AS last_activity_timestamp,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 3600 * cost_per_hour) AS avg_cost_per_run,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS avg_run_time_minutes,
    AVG(cpu_usage) FILTER (WHERE cpu_usage IS NOT NULL) AS avg_cpu_usage,
    AVG(mem_used) FILTER (WHERE mem_used IS NOT NULL) / 1073741824 AS avg_ram_used_gb,
    MAX(start_time) FILTER (WHERE last_run_rank = 1) AS last_run_start_date
  FROM run_metrics
  GROUP BY pipeline_name, tags
)

SELECT
  COALESCE(NULLIF(ps.pipeline_name, ''), 'pipeline_name_not_available') AS "Pipeline Name",
  CASE
    WHEN ps.last_activity_timestamp < NOW() - INTERVAL '20 seconds' THEN 'Completed'
    ELSE 'Running'
  END AS "Status",
  CASE
    WHEN ps.pipeline_name ILIKE '%atac%' THEN 'ATAC-seq'
    WHEN ps.pipeline_name ILIKE '%chip%' THEN 'ChIP-seq'
    ELSE 'RNA-seq'
  END AS "Analysis type",
  COALESCE(
    (SELECT STRING_AGG(value, ', ')
     FROM jsonb_each_text(ps.tags)
     WHERE jsonb_typeof(ps.tags) = 'object'),
    ''
  ) AS "Tags",
  ps.run_count AS "# of Runs",
  TO_CHAR(ps.last_run_start_date, 'DD Mon YYYY HH24:MI') AS "Last Run Date",
  ps.avg_run_time_minutes AS "AVG Runtime",
  ps.avg_ram_used_gb AS "Avg Max RAM",
  ps.avg_cpu_usage AS "Avg Max CPU %",
  ps.avg_cost_per_run AS "AVG Costs"
FROM pipeline_summary ps
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

job_metrics AS (
  SELECT 
    run_id,
    pipeline_name,
    event_timestamp,
    EXTRACT(EPOCH FROM (MAX(event_timestamp) OVER (PARTITION BY run_id) - 
             MIN(event_timestamp) OVER (PARTITION BY run_id))) / 3600 AS run_duration_hours
  FROM batch_jobs_logs
  WHERE pipeline_name IS NOT NULL
),

time_series_runtime AS (
  SELECT 
    DATE_TRUNC('month', event_timestamp) AS month_timestamp,
    AVG(run_duration_hours) AS average_runtime_hours,
    COUNT(DISTINCT pipeline_name) AS unique_pipelines
  FROM (
    SELECT 
      event_timestamp,
      run_id,
      pipeline_name,
      EXTRACT(EPOCH FROM (MAX(event_timestamp) OVER (PARTITION BY run_id) - 
               MIN(event_timestamp) OVER (PARTITION BY run_id))) / 3600 AS run_duration_hours
    FROM batch_jobs_logs
    WHERE pipeline_name IS NOT NULL
  ) AS run_durations
  GROUP BY DATE_TRUNC('month', event_timestamp)
)

SELECT 
  m.month_timestamp::timestamp AS time,
  COALESCE(t.average_runtime_hours, 0)::float AS average_pipeline_runtime_hours
FROM months m
LEFT JOIN time_series_runtime t
ON m.month_timestamp = t.month_timestamp
ORDER BY time;
"""

# status pipeline runs this month.
STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY = """
WITH run_pool AS (
  SELECT DISTINCT run_id
  FROM batch_jobs_logs
  WHERE event_timestamp >= DATE_TRUNC('month', CURRENT_DATE)
    AND event_timestamp < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
),

job_metrics AS (
  SELECT 
    b.run_id,
    b.tags,
    MAX(b.event_timestamp) AS ts
  FROM batch_jobs_logs b
  JOIN run_pool r ON b.run_id = r.run_id
  GROUP BY b.run_id, b.tags
),

job_states AS (
  SELECT
    run_id,
    CASE 
      WHEN tags::text ILIKE '%failed%' THEN 'Failed'
      WHEN ts < NOW() - INTERVAL '30 seconds' THEN 'Completed'
      ELSE 'Running'
    END AS status
  FROM job_metrics
)

SELECT 
  COUNT(*) FILTER (WHERE status = 'Completed') AS "Completed",
  COUNT(*) FILTER (WHERE status = 'Failed') AS "Failed",
  COUNT(*) FILTER (WHERE status = 'Running') AS "Running"
FROM job_states;
"""
