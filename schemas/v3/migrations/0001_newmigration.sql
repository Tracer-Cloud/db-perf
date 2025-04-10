create table batch_jobs_logs
(
    id                    serial
        primary key,
    data                  jsonb not null,
    job_id                text,
    creation_date         timestamp default now(),
    run_name              text,
    run_id                text,
    pipeline_name         text,
    nextflow_session_uuid text,
    job_ids               text[],
    tags                  jsonb,
    event_timestamp       timestamp,
    ec2_cost_per_hour     double precision,
    cpu_usage             double precision,
    mem_used              double precision,
    processed_dataset     integer
);

-- alter table batch_jobs_logs
--     owner to tracer_user;


-- Core indexes that will actually be used
-- CREATE INDEX idx_batch_jobs_logs_composite ON batch_jobs_logs (
--     pipeline_name, 
--     event_timestamp DESC
-- ) INCLUDE (run_id, tags, ec2_cost_per_hour, cpu_usage, mem_used);

-- CREATE INDEX idx_batch_jobs_tags_search ON batch_jobs_logs USING GIN (
--     tags jsonb_path_ops
-- ) WHERE (pipeline_name IS NOT NULL);

-- CREATE INDEX idx_batch_jobs_active_runs ON batch_jobs_logs (
--     run_id, 
--     event_timestamp DESC
-- ) WHERE (event_timestamp > NOW() - INTERVAL '1 day');
CREATE INDEX idx_core_metrics ON batch_jobs_logs (
    pipeline_name,
    event_timestamp DESC,
    run_id
) INCLUDE (tags, ec2_cost_per_hour, cpu_usage, mem_used);

CREATE INDEX idx_tags_gin ON batch_jobs_logs USING GIN (tags jsonb_path_ops);

CREATE INDEX idx_run_id_ordered ON batch_jobs_logs (run_id, event_timestamp DESC);

CREATE INDEX idx_pipeline_filter ON batch_jobs_logs (pipeline_name) 
WHERE (pipeline_name IS NOT NULL);

CREATE INDEX idx_failed_runs ON batch_jobs_logs (run_id) 
WHERE (tags @> '{"status":"failed"}');

CREATE INDEX idx_pipeline_types ON batch_jobs_logs (pipeline_name text_pattern_ops);