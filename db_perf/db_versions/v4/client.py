from pathlib import Path
from psycopg2.extras import execute_batch
from typing import List, Dict


from db_perf.db_versions.base import BaseClient
from db_perf.db_versions.v4.queries import (
    AVG_PIPELINE_DURATION_6MONTHS,
    COST_ATTRIBUTION_QUERY,
    STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY,
)

from db_perf.models.events import (
    Event,
    ProcessProperties,
    SystemMetric,
    NextflowLog,
)
from db_perf.models.query import Query


QUERIES = [
    Query(name="cost_attribution_query", query=COST_ATTRIBUTION_QUERY),
    Query(name="avg_pipeline_duration_6months", query=AVG_PIPELINE_DURATION_6MONTHS),
    Query(
        name="status_pipeline_runs_this_month_query",
        query=STATUS_PIPELINE_RUNS_THIS_MONTH_QUERY,
    ),
]


class DBClientV4(BaseClient):

    def name(self) -> str:
        return "schema_fully_independent_tables"

    def _get_correct_schema_path(self) -> Path:
        return self.schema_basedir / "v4/migrations"

    def insert_event(self, event: Event):
        cursor = self.conn.cursor()

        try:
            # Insert base event
            cursor.execute(
                """
                INSERT INTO events (
                    timestamp, message, event_type, process_type, process_status,
                    pipeline_name, run_name, run_id, environment, pipeline_type, user_operator,
                    department, team
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING event_id
                """,
                (
                    event.timestamp,
                    event.message,
                    event.event_type,
                    event.process_type,
                    event.process_status,
                    event.pipeline_name,
                    event.run_name,
                    event.run_id,
                    event.tags.environment if event.tags else None,
                    event.tags.pipeline_type if event.tags else None,
                    event.tags.user_operator if event.tags else None,
                    event.tags.department if event.tags else "Research",
                    event.tags.team if event.tags else "Oncology",
                ),
            )
            event_id = cursor.fetchone()[0]

            # Handle attributes
            if event.attributes:
                if event.attributes.process:
                    self._insert_process_event(
                        cursor, event_id, event.attributes.process
                    )
                if event.attributes.system_metric:
                    self._insert_system_metric(
                        cursor, event_id, event.attributes.system_metric
                    )
                if event.attributes.nextflow_log:
                    self._insert_nextflow_event(
                        cursor, event_id, event.attributes.nextflow_log
                    )
                # Add other attribute types as needed

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def _insert_process_event(self, cursor, event_id: int, process: ProcessProperties):
        cursor.execute(
            """
            INSERT INTO process_events (
                event_id, tool_name, tool_pid, parent_pid, binary_path, cmd,
                start_time, cpu_utilization, memory_usage, memory_virtual, run_time,
                disk_read_last, disk_write_last, disk_read_total, disk_write_total,
                status, container_id, job_id, working_dir
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                process.tool_name,
                process.tool_pid,
                process.tool_parent_pid,
                process.tool_binary_path,
                process.tool_cmd,
                process.start_timestamp,
                process.process_cpu_utilization,
                process.process_memory_usage,
                process.process_memory_virtual,
                process.process_run_time,
                process.process_disk_usage_read_last_interval,
                process.process_disk_usage_write_last_interval,
                process.process_disk_usage_read_total,
                process.process_disk_usage_write_total,
                process.process_status,
                process.container_id,
                process.job_id,
                process.working_directory,
            ),
        )

        # Insert input files if they exist
        if process.input_files:
            for file in process.input_files:
                cursor.execute(
                    """
                    INSERT INTO process_input_files (
                        event_id, file_name, file_size, file_path, directory, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event_id,
                        file.file_name,
                        file.file_size,
                        file.file_path,
                        file.file_directory,
                        file.file_updated_at_timestamp,
                    ),
                )

    def _insert_system_metric(self, cursor, event_id: int, metric: SystemMetric):
        cursor.execute(
            """
            INSERT INTO system_metrics (
                event_id, metric_name, memory_total, memory_used, memory_available,
                memory_utilization, swap_total, swap_used, cpu_utilization
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING event_id
            """,
            (
                event_id,
                metric.events_name,
                metric.system_memory_total,
                metric.system_memory_used,
                metric.system_memory_available,
                metric.system_memory_utilization,
                metric.system_memory_swap_total,
                metric.system_memory_swap_used,
                metric.system_cpu_utilization,
            ),
        )

        # Insert disk metrics
        for device, stats in metric.system_disk_io.items():
            cursor.execute(
                """
                INSERT INTO disk_metrics (
                    event_id, device, total_space, used_space, available_space, utilization
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    event_id,
                    device,
                    stats.disk_total_space,
                    stats.disk_used_space,
                    stats.disk_available_space,
                    stats.disk_utilization,
                ),
            )

    def _insert_nextflow_event(self, cursor, event_id: int, nf_log: NextflowLog):
        cursor.execute(
            """
            INSERT INTO nextflow_events (
                event_id, session_uuid, job_ids
            ) VALUES (%s, %s, %s)
            """,
            (event_id, nf_log.session_uuid, nf_log.jobs_ids),
        )

    def batch_inserts(self, events: List[Event]):
        print("Batch inserting events with polymorphic schema...")
        cursor = self.conn.cursor()

        try:
            # Process in batches of 1000
            for i in range(0, len(events), 1000):
                batch = events[i : i + 1000]

                # Insert base events first
                base_records = []
                for event in batch:
                    base_records.append(
                        (
                            event.timestamp,
                            event.message,
                            event.event_type,
                            event.process_type,
                            event.process_status,
                            event.pipeline_name,
                            event.run_name,
                            event.run_id,
                            event.tags.environment if event.tags else None,
                            event.tags.pipeline_type if event.tags else None,
                            event.tags.user_operator if event.tags else None,
                            event.tags.department if event.tags else "Research",
                            event.tags.team if event.tags else "Oncology",
                        )
                    )

                # Use execute_batch for better performance
                execute_batch(
                    cursor,
                    """
                    INSERT INTO events (
                        timestamp, message, event_type, process_type, process_status,
                        pipeline_name, run_name, run_id, environment, pipeline_type, user_operator,
                        department, team
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING event_id
                    """,
                    base_records,
                )

                # Get the inserted IDs
                event_ids = [id[0] for id in cursor.fetchall()]

                # Process attributes for each event
                for event_id, event in zip(event_ids, batch):
                    if event.attributes:
                        if event.attributes.process:
                            self._insert_process_event(
                                cursor, event_id, event.attributes.process
                            )
                        if event.attributes.system_metric:
                            self._insert_system_metric(
                                cursor, event_id, event.attributes.system_metric
                            )
                        # Handle other attribute types as needed

                self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
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
