COST_ATTRIBUTION_QUERY = """

WITH job_metrics AS (
  SELECT 
    run_id,
    pipeline_name,
    tags,
    event_timestamp as ts,
    ec2_cost_per_hour as cost_per_hour,
    cpu_usage,
    mem_used,
    COALESCE(processed_dataset, 0) AS processed_dataset
  FROM batch_jobs_logs b
  WHERE pipeline_name IS NOT NULL and pipeline_name != ''
),

last_run_start AS (
  SELECT DISTINCT ON (pipeline_name, tags)
    pipeline_name,
    tags,
    run_id,
    MIN(ts) AS last_run_start_date
  FROM job_metrics
  GROUP BY pipeline_name, tags, run_id
  ORDER BY pipeline_name, tags, MAX(ts) DESC
),

pipeline_summary AS (
  SELECT
    pipeline_name,
    tags,
    COUNT(*) AS run_count,
    MAX(end_time) AS last_activity_timestamp,
    AVG(run_duration_hours * cost_per_hour) AS avg_cost_per_run,
    AVG(run_duration_hours * 60) AS avg_run_time_minutes,
    AVG(cpu_usage) FILTER (WHERE cpu_usage IS NOT NULL) AS avg_cpu_usage,
    AVG(mem_used) FILTER (WHERE mem_used IS NOT NULL) / 1073741824 AS avg_ram_used_gb
  FROM (
    SELECT
      pipeline_name,
      tags,
      run_id,
      (EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) / 3600) AS run_duration_hours,
      MAX(cost_per_hour) AS cost_per_hour,
      MAX(ts) AS end_time,
      MAX(cpu_usage) AS cpu_usage,
      MAX(mem_used) AS mem_used
    FROM job_metrics
    GROUP BY pipeline_name, tags, run_id
  ) job_aggregations
  GROUP BY pipeline_name, tags
),

tag_aggregation AS (
  SELECT 
    pipeline_name, 
    tags, 
    STRING_AGG(value, ', ') AS tags_str
  FROM (
    SELECT 
      ps.pipeline_name, 
      ps.tags, 
      jt.value
    FROM pipeline_summary ps
    CROSS JOIN LATERAL jsonb_each_text(ps.tags) AS jt(key, value)
    WHERE jsonb_typeof(ps.tags) = 'object'
  ) tag_expansion
  GROUP BY pipeline_name, tags
)

SELECT
  COALESCE(NULLIF(pipeline_summary.pipeline_name, ''), 'pipeline_name_not_available') AS "Pipeline Name",
  CASE 
    WHEN pipeline_summary.last_activity_timestamp < NOW() - INTERVAL '20 seconds' THEN 'Completed'
    ELSE 'Running'
  END AS "Status",
  CASE 
    WHEN pipeline_summary.pipeline_name ILIKE '%atac%' THEN 'ATAC-seq'
    WHEN pipeline_summary.pipeline_name ILIKE '%chip%' THEN 'ChIP-seq'
    ELSE 'RNA-seq'
  END AS "Analysis type",
  COALESCE(tag_aggregation.tags_str, '') AS "Tags",
  pipeline_summary.run_count AS "Number of Runs",
  last_run_start.last_run_start_date AS "Last Run Date",
  pipeline_summary.avg_run_time_minutes AS "AVG Runtime (Minutes)",
  pipeline_summary.avg_ram_used_gb AS "Avg Max RAM",
  pipeline_summary.avg_cpu_usage AS "Avg Max CPU %",
  pipeline_summary.avg_cost_per_run AS "AVG Costs"
FROM pipeline_summary
LEFT JOIN tag_aggregation 
  ON pipeline_summary.pipeline_name = tag_aggregation.pipeline_name 
  AND pipeline_summary.tags = tag_aggregation.tags
LEFT JOIN last_run_start
  ON pipeline_summary.pipeline_name = last_run_start.pipeline_name
  AND pipeline_summary.tags = last_run_start.tags
ORDER BY pipeline_summary.last_activity_timestamp DESC, pipeline_summary.run_count;
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
