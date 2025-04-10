-- Updated schema with JSON preservation
CREATE TABLE pipelines (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    analysis_type TEXT GENERATED ALWAYS AS (
        CASE 
            WHEN name ILIKE '%atac%' THEN 'ATAC-seq'
            WHEN name ILIKE '%chip%' THEN 'ChIP-seq'
            ELSE 'RNA-seq'
        END
    ) STORED
);

CREATE TABLE pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipelines(id),
    run_id TEXT NOT NULL,
    run_name TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    is_active BOOLEAN GENERATED ALWAYS AS (
        end_time IS NULL
    ) STORED,
    nextflow_session_uuid TEXT,
    -- Flattened tags (optimized for querying)
    environment TEXT,
    pipeline_type TEXT,
    user_operator TEXT,
    department TEXT,
    team TEXT,
    -- Raw JSON preservation
    raw_attributes JSONB NOT NULL,
    data JSONB,
    UNIQUE(pipeline_id, run_id)
);

-- Metrics table remains optimized
CREATE TABLE run_metrics (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES pipeline_runs(id),
    timestamp TIMESTAMP NOT NULL,
    ec2_cost_per_hour DOUBLE PRECISION,
    cpu_usage DOUBLE PRECISION,
    mem_used_gb DOUBLE PRECISION,
    processed_datasets INTEGER
);

-- Create a view to handle the time-based status calculation
CREATE OR REPLACE VIEW pipeline_run_status AS
SELECT 
    pr.*,
    CASE
        WHEN pr.end_time IS NULL THEN 'Running'
        WHEN pr.end_time > (NOW() - INTERVAL '20 seconds') THEN 'Running'
        ELSE 'Completed'
    END AS status
FROM pipeline_runs pr;

-- Indexes
CREATE INDEX idx_run_metrics_timestamp ON run_metrics(run_id, timestamp);
CREATE INDEX idx_pipeline_runs_active ON pipeline_runs(is_active);
CREATE INDEX idx_pipeline_runs_metrics ON pipeline_runs(pipeline_id, environment, pipeline_type, user_operator);