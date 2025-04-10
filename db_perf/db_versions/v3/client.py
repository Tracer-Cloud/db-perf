import json
from pathlib import Path
from typing import Dict, List

from psycopg2.extras import Json

from db_perf.db_versions.base import BaseClient
from db_perf.db_versions.v1.queries import (
    AVG_PIPELINE_DURATION_6MONTHS,
    COST_ATTRIBUTION_QUERY,
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
]


class DbClientV3(BaseClient):
    def name(self) -> str:
        return "default_json_with_other_indexes"

    def _get_correct_schema_path(self) -> Path:
        return self.schema_basedir / "v3/migrations"

    def insert_event(self, event: Event):
        cursor = self.conn.cursor()
        data_json = Json(event.model_dump(mode="json"), dumps=json.dumps)

        attributes = event.attributes
        system_metric = (
            attributes.system_metric
            if attributes and attributes.system_metric
            else None
        )
        system_props = (
            attributes.system_properties
            if attributes and attributes.system_properties
            else None
        )
        process_dataset = getattr(attributes, "process_dataset_stats", None)

        cursor.execute(
            """
            INSERT INTO batch_jobs_logs (
                data, job_id, run_name, run_id, pipeline_name, nextflow_session_uuid, job_ids,
                tags, event_timestamp, ec2_cost_per_hour, cpu_usage, mem_used, processed_dataset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data_json,
                "default",  # fallback job_id
                event.run_name,
                event.run_id,
                event.pipeline_name,
                getattr(attributes, "session_uuid", None),
                getattr(attributes, "jobs_ids", []),
                Json(event.tags.model_dump(mode="json")) if event.tags else None,
                event.timestamp,
                getattr(system_props, "ec2_cost_per_hour", None),
                getattr(system_metric, "system_cpu_utilization", None),
                getattr(system_metric, "system_memory_used", None),
                getattr(process_dataset, "total", None),
            ),
        )
        self.conn.commit()
        cursor.close()

    def batch_inserts(self, events: List[Event]):
        print("calling batch inserts....")
        cursor = self.conn.cursor()

        records = []
        for event in events:
            attributes = event.attributes
            system_metric = (
                attributes.system_metric
                if attributes and attributes.system_metric
                else None
            )
            system_props = (
                attributes.system_properties
                if attributes and attributes.system_properties
                else None
            )
            process_dataset = getattr(attributes, "process_dataset_stats", None)

            record = (
                Json(event.model_dump(mode="json"), dumps=json.dumps),
                "default",
                event.run_name,
                event.run_id,
                event.pipeline_name,
                getattr(attributes, "session_uuid", None),
                getattr(attributes, "jobs_ids", []),
                Json(event.tags.model_dump(mode="json")) if event.tags else None,
                event.timestamp,
                getattr(system_props, "ec2_cost_per_hour", None),
                getattr(system_metric, "system_cpu_utilization", None),
                getattr(system_metric, "system_memory_used", None),
                getattr(process_dataset, "total", None),
            )
            records.append(record)

        cursor.executemany(
            """
                INSERT INTO batch_jobs_logs (
                    data, job_id, run_name, run_id, pipeline_name, nextflow_session_uuid, job_ids,
                    tags, event_timestamp, ec2_cost_per_hour, cpu_usage, mem_used, processed_dataset
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
            records,
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
