from typing import List, Dict
import pandas as pd
import matplotlib.pyplot as plt
from db_perf.db_versions.base import BaseClient


class PerfClient:
    def __init__(self, clients: List[BaseClient], number_of_records: List[int]):
        self.clients = clients
        self.number_of_records = number_of_records
        self.results: Dict[str, List[Dict[str, float]]] = {}
        # client_name → list of benchmark results over time

    def run_insert_and_benchmark_client_queries(self, num_records: int):

        for client in self.clients:
            benchmark_result = client.run_benchmark(num_records)
            for client_name, queries in benchmark_result.items():
                if client_name not in self.results:
                    self.results[client_name] = []
                self.results[client_name].append(
                    {"records": num_records, **queries}  # query_name: time_ms
                )

    def to_dataframe(self):
        # Transform to long format
        rows = []
        for client_name, benchmarks in self.results.items():
            for entry in benchmarks:
                num_records = entry["records"]
                for query_name, time_ms in entry.items():
                    if query_name == "records":
                        continue
                    rows.append(
                        {
                            "records": num_records,
                            "client": client_name,
                            "query": query_name,
                            "time_ms": time_ms,
                        }
                    )
        return pd.DataFrame(rows)

    def plot(self):
        df = self.to_dataframe()

        plt.figure(figsize=(12, 8))
        for (client, query), group in df.groupby(["client", "query"]):
            group_sorted = group.sort_values("records")
            plt.plot(
                group_sorted["records"],
                group_sorted["time_ms"],
                marker="o",
                label=f"{client} - {query}",
            )

        plt.title("Query Performance vs. Number of Records")

        plt.title("Query Performance vs. Number of Records")
        plt.xlabel("Number of Records")
        plt.ylabel("Time (ms, log scale)")
        plt.yscale("log")
        plt.grid(True, which="both", linestyle="--", linewidth=0.5)

        plt.legend()
        plt.tight_layout()
        plt.savefig("db_query_performance_plot.png")
        plt.close()

    def run(self):

        total_records = 0
        for count in self.number_of_records:
            total_records += count
            print(f"Inserting and Benchmarking {total_records} records...")
            self.run_insert_and_benchmark_client_queries(total_records)

        self.plot()
