import json
from pathlib import Path
from typing import Dict, List

from psycopg2.extras import Json

from db_perf.db_versions.base import BaseClient
from db_perf.models.events import Event

QUERIES = []


class DbClient(BaseClient):
    def _get_correct_schema_path(self) -> Path:
        return self.schema_basedir / "v1/migrations"

    def insert_event(self, conn, event: Event):
        cursor = self.conn.cursor()
        data_json = Json(event.__dict__, dumps=json.dumps)

        # Transpose
        attributes = event.attributes or {}
        system_metric = attributes.get("system_metric", {})

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
                attributes.get("session_uuid"),
                attributes.get("jobs_ids", []),
                Json(event.tags.__dict__) if event.tags else None,
                event.timestamp,
                attributes.get("system_properties", {}).get("ec2_cost_per_hour"),
                system_metric.get("system_cpu_utilization"),
                system_metric.get("system_memory_used"),
                attributes.get("process_dataset_stats", {}).get("total"),
            ),
        )
        conn.commit()
        cursor.close()

    def batch_inserts(self, events: List[Event]):
        cursor = self.conn.cursor()

        @staticmethod
        def get_value_or_zero(d, *keys) -> int:

            for key in keys:
                if d is None:
                    return 0
                d = d.get(key, 0)
            return d

        records = []
        for event in events:
            attributes = event.attributes or {}
            system_metric = attributes.get("system_metric", {})

            record = (
                Json(event.__dict__, dumps=json.dumps),
                "default",  # fallback job_id
                event.run_name,
                event.run_id,
                event.pipeline_name,
                attributes.get("session_uuid"),
                attributes.get("jobs_ids", []),
                Json(event.tags.__dict__) if event.tags else None,
                event.timestamp,
                get_value_or_zero(attributes, "system_properties", "ec2_cost_per_hour"),
                system_metric.get("system_cpu_utilization"),
                system_metric.get("system_memory_used"),
                get_value_or_zero(attributes, "process_dataset_stats", "total"),
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
        for idx, query in enumerate(QUERIES):
            label = f"query_{idx}"
            print(f"Running query benchmark on {label}")

            explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"

            cur = self.conn.cursor()
            cur.execute(explain_query)
            result = cur.fetchone()
            if not result:
                return {}

            print(" result ..", result)

            result = result[0]  # EXPLAIN result as JSON
            cur.close()
            self.conn.close()
            execution_time_ms = result[0]["Execution Time"]

            results[label] = execution_time_ms
        return results
