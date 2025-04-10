from pathlib import Path
from typing import Dict, List
from psycopg2.extras import Json

from db_perf.db_versions.base import BaseClient

from db_perf.db_versions.v2.queries import (
    AVG_PIPELINE_DURATION_6MONTHS,
    COST_ATTRIBUTION_QUERY,
    STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY_V1,
    STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY,
)
from db_perf.models.events import Event
from db_perf.models.query import Query

QUERIES = [
    Query(name="cost_attribution_query", query=COST_ATTRIBUTION_QUERY),
    Query(name="avg_pipeline_duration_6months", query=AVG_PIPELINE_DURATION_6MONTHS),
    Query(
        name="status_pipeline_runs_this_month_query",
        query=STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY,
    ),
    # Query(
    #     name="status_pipeline_runs_this_month_query_v0_slow",
    #     query=STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY_V1,
    # ),
]


class DbClientV2(BaseClient):

    def name(self) -> str:
        return "schema_flat_tags_and_hot_entries"

    def _get_correct_schema_path(self) -> Path:
        return self.schema_basedir / "v2/migrations"

    def _ensure_pipeline(self, pipeline_name: str | None) -> int:
        if not pipeline_name:
            pipeline_name = "unnamed_pipeline"

        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO pipelines (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
            (pipeline_name,),
        )
        pipeline_id = cursor.fetchone()[0]
        self.conn.commit()
        return pipeline_id

    def insert_event(self, event: Event):
        pipeline_id = self._ensure_pipeline(event.pipeline_name)
        cursor = self.conn.cursor()

        # For streaming data, we need to:
        # 1. Set start_time to MIN(event timestamp)
        # 2. Set end_time to MAX(event timestamp)
        cursor.execute(
            """
            INSERT INTO pipeline_runs (
                pipeline_id, run_id, run_name, 
                start_time, end_time,
                raw_attributes, data
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pipeline_id, run_id) DO UPDATE SET
                start_time = LEAST(pipeline_runs.start_time, EXCLUDED.start_time),
                end_time = GREATEST(pipeline_runs.end_time, EXCLUDED.end_time),
                raw_attributes = EXCLUDED.raw_attributes,
                data = EXCLUDED.data
            RETURNING id
        """,
            (
                pipeline_id,
                event.run_id,
                event.run_name,
                event.timestamp,  # start_time candidate
                event.timestamp,  # end_time candidate
                Json(
                    event.attributes.model_dump(mode="json") if event.attributes else {}
                ),
                Json(event.model_dump(mode="json")),
            ),
        )

        run_id = cursor.fetchone()[0]

        # Insert metrics (unchanged)
        # Insert metrics if available
        if event.attributes:
            metric = None
            if event.attributes.system_metric:
                metric = event.attributes.system_metric
            elif hasattr(event.attributes, "process") and event.attributes.process:
                metric = event.attributes.process

            if metric:
                cursor.execute(
                    """
                        INSERT INTO run_metrics (
                            run_id, timestamp, ec2_cost_per_hour,
                            cpu_usage, mem_used_gb, processed_datasets
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        event.timestamp,
                        getattr(
                            event.attributes.system_properties,
                            "ec2_cost_per_hour",
                            None,
                        ),
                        getattr(metric, "system_cpu_utilization", None),
                        getattr(metric, "system_memory_used", 0)
                        / 1073741824,  # Convert to GB
                        getattr(
                            getattr(event.attributes, "process_dataset_stats", {}),
                            "total",
                            None,
                        ),
                    ),
                )

        self.conn.commit()
        cursor.close()

    def batch_inserts(self, events: List[Event]):

        print(f"batch inserts running for {self.name()}..")
        cursor = self.conn.cursor()

        # 1. Ensure all pipelines exist
        pipeline_names = {e.pipeline_name for e in events if e.pipeline_name}
        pipeline_map = {name: self._ensure_pipeline(name) for name in pipeline_names}

        # 2. Prepare batch data with proper timestamp handling
        run_records = []
        metric_records = []

        for event in events:
            pipeline_id = pipeline_map.get(
                event.pipeline_name, self._ensure_pipeline(event.pipeline_name)
            )
            tags = event.tags.dict() if event.tags else {}

            tags = getattr(event, "tags", None)

            # Each event contributes to both start_time (min) and end_time (max)
            run_records.append(
                (
                    pipeline_id,
                    event.run_id,
                    event.run_name,
                    event.timestamp,  # start_time candidate
                    event.timestamp,  # end_time candidate
                    getattr(tags, "environment", None),
                    getattr(tags, "pipeline_type", None),
                    getattr(tags, "user_operator", None),
                    getattr(tags, "department", None),
                    getattr(tags, "team", None),
                    Json(
                        event.attributes.model_dump(mode="json")
                        if event.attributes
                        else {}
                    ),
                    Json(event.model_dump(mode="json")),
                )
            )

            # Prepare metrics if available
            if event.attributes:
                metric = None
                if event.attributes.system_metric:
                    metric = event.attributes.system_metric
                elif hasattr(event.attributes, "process") and event.attributes.process:
                    metric = event.attributes.process

                if metric:
                    metric_records.append(
                        (
                            event.run_id,
                            pipeline_id,  # Needed for efficient join
                            event.timestamp,
                            getattr(
                                event.attributes.system_properties,
                                "ec2_cost_per_hour",
                                None,
                            ),
                            getattr(metric, "system_cpu_utilization", None),
                            getattr(metric, "system_memory_used", 0) / 1073741824,
                            getattr(
                                getattr(event.attributes, "process_dataset_stats", {}),
                                "total",
                                None,
                            ),
                        )
                    )

        # 3. Batch upsert runs with correct timestamp handling
        cursor.executemany(
            """
            INSERT INTO pipeline_runs (
                pipeline_id, run_id, run_name, 
                start_time, end_time,
                environment, pipeline_type, user_operator,
                department, team, raw_attributes, data
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pipeline_id, run_id) DO UPDATE SET
                start_time = LEAST(pipeline_runs.start_time, EXCLUDED.start_time),
                end_time = GREATEST(pipeline_runs.end_time, EXCLUDED.end_time),
                environment = COALESCE(EXCLUDED.environment, pipeline_runs.environment),
                pipeline_type = COALESCE(EXCLUDED.pipeline_type, pipeline_runs.pipeline_type),
                user_operator = COALESCE(EXCLUDED.user_operator, pipeline_runs.user_operator),
                department = COALESCE(EXCLUDED.department, pipeline_runs.department),
                team = COALESCE(EXCLUDED.team, pipeline_runs.team),
                raw_attributes = EXCLUDED.raw_attributes,
                data = EXCLUDED.data
        """,
            run_records,
        )

        # 4. Batch insert metrics with optimized run_id resolution
        if metric_records:
            cursor.executemany(
                """
                INSERT INTO run_metrics (
                    run_id, timestamp, ec2_cost_per_hour,
                    cpu_usage, mem_used_gb, processed_datasets
                ) VALUES (
                    (SELECT id FROM pipeline_runs 
                    WHERE run_id = %s AND pipeline_id = %s),
                    %s, %s, %s, %s, %s
                )
            """,
                metric_records,
            )

        self.conn.commit()
        cursor.close()

    def benchmark_queries(self) -> Dict[str, float]:
        results = {}
        for query in QUERIES:
            label = f"query_{query.name}"
            print(f"Running query benchmark on {label}")

            explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query.query}"

            cur = self.conn.cursor()
            cur.execute(explain_query)
            result = cur.fetchone()
            if not result:
                return {}

            print(" result ..", result)

            result = result[0]  # EXPLAIN result as JSON
            cur.close()
            execution_time_ms = result[0]["Execution Time"]

            results[label] = execution_time_ms
        self.conn.close()
        return results
