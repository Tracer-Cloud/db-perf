from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List

import psycopg2
from psycopg2.extensions import connection

from db_perf.factories.event import EventFactory
from db_perf.migrator import DatabaseMigrator
from db_perf.models.events import Event


class BaseClient(ABC):

    def __init__(self, database_url: str) -> None:

        self.database_url = database_url
        self.schema_basedir = Path(__file__).resolve().parent.parent.parent / "schemas"
        print("getting schema_basedir", self.schema_basedir)

        self.migrator = self._create_migrator()
        self.conn = self.connect_to_db()

    def connect_to_db(self) -> connection:
        try:
            conn = psycopg2.connect(self.database_url)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise e

    def execute_query(self, query: str):
        try:
            conn = self.connect_to_db()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()  # If it's a query that modifies the database (e.g., INSERT, UPDATE)
            cur.close()
        except Exception as e:
            print(f"Error executing query: {e}")
            # raise e

    def _create_migrator(self):
        migrations_folder = self._get_correct_schema_path()
        migrator = DatabaseMigrator(self.database_url, migrations_folder)
        return migrator

    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def _get_correct_schema_path(self) -> Path: ...

    @abstractmethod
    def batch_inserts(self, events: List[Event]): ...

    @staticmethod
    def generate_insert_payload(num_of_events: int) -> List[Event]:
        return [EventFactory() for _ in range(num_of_events)]

    @abstractmethod
    def benchmark_queries(self) -> Dict[str, float]:
        """Returns the average execution time for each query
        :returns: Dict [string, float] => { query_1: avg_time, ... }
        """
        ...
