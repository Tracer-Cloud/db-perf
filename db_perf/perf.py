from typing import Dict

import pandas as pd

from db_perf.db_versions.base import BaseClient


class PerfClient:

    def __init__(self, clients: list[BaseClient]):
        self.clients = clients
        self.results: Dict[str, Dict[str, float]] = (
            {}
        )  # dict of number of records : benchmark data

    def run_insert_and_benchmark_client_queries(self, num_records: int):

        for client in self.clients:
            label = client.name()
            print(f"Running insert benchmark on {label}")

            payload = client.generate_insert_payload(
                num_records
            )  # generates List[Event]
            print("Running migrations ...")
            client.migrator.run_migrations()
            client.batch_inserts(payload)

            print("benchmarking Queries for ...")
            self.results[label] = client.benchmark_queries()

            print("Cleaning up after bench mark...")
            client.migrator.rollback_migrations()

    def to_dataframe(self):
        return pd.DataFrame.from_dict(self.results, orient="index")
