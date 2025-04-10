-- Base event table (common fields)
CREATE TABLE events (
    event_id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    message TEXT NOT NULL,
    event_type TEXT NOT NULL,
    process_type TEXT NOT NULL,
    process_status TEXT NOT NULL,
    pipeline_name TEXT,
    run_name TEXT,
    run_id TEXT,
    environment TEXT,  -- Extracted from PipelineTags
    pipeline_type TEXT,
    user_operator TEXT,
    department TEXT DEFAULT 'Research',
    team TEXT DEFAULT 'Oncology'
);

-- Process-related events
CREATE TABLE process_events (
    event_id BIGINT PRIMARY KEY REFERENCES events(event_id),
    tool_name TEXT NOT NULL,
    tool_pid TEXT NOT NULL,
    parent_pid TEXT,
    binary_path TEXT,
    cmd TEXT,
    start_time TIMESTAMPTZ,
    cpu_utilization FLOAT,
    memory_usage BIGINT,
    memory_virtual BIGINT,
    run_time BIGINT,
    disk_read_last BIGINT,
    disk_write_last BIGINT,
    disk_read_total BIGINT,
    disk_write_total BIGINT,
    status TEXT,
    container_id TEXT,
    job_id TEXT,
    working_dir TEXT
);

-- System metrics
CREATE TABLE system_metrics (
    event_id BIGINT PRIMARY KEY REFERENCES events(event_id),
    metric_name TEXT,
    memory_total BIGINT,
    memory_used BIGINT,
    memory_available BIGINT,
    memory_utilization FLOAT,
    swap_total BIGINT,
    swap_used BIGINT,
    cpu_utilization FLOAT
);

-- Disk IO metrics (related to system metrics)
CREATE TABLE disk_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    event_id BIGINT REFERENCES system_metrics(event_id),
    device TEXT,
    total_space BIGINT,
    used_space BIGINT,
    available_space BIGINT,
    utilization FLOAT
);

-- Nextflow-specific events
CREATE TABLE nextflow_events (
    event_id BIGINT PRIMARY KEY REFERENCES events(event_id),
    session_uuid TEXT,
    job_ids TEXT[]
);

-- AWS metadata (shared across event types)
CREATE TABLE aws_metadata (
    event_id BIGINT REFERENCES events(event_id),
    region TEXT,
    availability_zone TEXT,
    instance_id TEXT,
    account_id TEXT,
    ami_id TEXT,
    instance_type TEXT,
    local_hostname TEXT,
    hostname TEXT,
    public_hostname TEXT,
    cost_per_hour FLOAT
);

-- Input files (for process events)
CREATE TABLE process_input_files (
    file_id BIGSERIAL PRIMARY KEY,
    event_id BIGINT REFERENCES process_events(event_id),
    file_name TEXT NOT NULL,
    file_size BIGINT,
    file_path TEXT,
    directory TEXT,
    updated_at TIMESTAMPTZ
);

-- Error logs (for syslog events)
CREATE TABLE error_logs (
    event_id BIGINT PRIMARY KEY REFERENCES events(event_id),
    error_name TEXT,
    error_id TEXT,
    error_line TEXT,
    line_number BIGINT
);

-- Error log context
CREATE TABLE error_log_context (
    log_id BIGSERIAL PRIMARY KEY,
    event_id BIGINT REFERENCES error_logs(event_id),
    log_message TEXT
);



CREATE INDEX idx_events_timestamp ON events(timestamp);
CREATE INDEX idx_events_run_id ON events(run_id);
CREATE INDEX idx_events_pipeline_name ON events(pipeline_name);
CREATE INDEX idx_events_process_status ON events(process_status);

CREATE INDEX IF NOT EXISTS idx_events_timestamp_run_id ON events(timestamp, run_id);
CREATE INDEX IF NOT EXISTS idx_events_pipeline_name ON events(pipeline_name) WHERE pipeline_name IS NOT NULL;