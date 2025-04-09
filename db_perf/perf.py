import time
from typing import Dict

import pandas as pd

from db_perf.db_versions.base import BaseClient


class PerfClient:

    def __init__(self, clients: list[BaseClient]):
        self.clients = clients

    def run_insert_benchmark(self, num_records: int):
        for client in self.clients:
            label = client._get_correct_schema_path().name
            print(f"Running insert benchmark on {label}")

            payload = client.generate_insert_payload(
                num_records
            )  # generates List[Event]
            #start = time.perf_counter()
            client.batch_inserts(payload)
            #elapsed = time.perf_counter() - start

            #self.results.setdefault(label, {})["insert_time_sec"] = elapsed

    def run_query_benchmark(self) -> Dict[str, Dict[str, float]]:
        results = {}
        for client in self.clients:
            label = client._get_correct_schema_path().name
            print(f"Running query benchmark for Client: {label}")
            result = client.benchmark_queries()
            results[label] = result
        return results

    def to_dataframe(self, results: dict):
        return pd.DataFrame.from_dict(results, orient="index")

