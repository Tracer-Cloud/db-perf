from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd

from db_perf.db_versions.base import BaseClient


class PerfClient:

    def __init__(self, clients: list[BaseClient], number_of_records: list[int]):
        self.clients = clients
        self.number_of_records = number_of_records
        self.results: Dict[int, Dict[str, Dict[str, float]]] = (
            {}
        )  # number of records:  dict of number of records : benchmark data

    def run_insert_and_benchmark_client_queries(self, num_records: int):

        for client in self.clients:
            self.results[num_records] = client.run_benchmark(num_records)

    def to_dataframe(self):
        # Transform to long format
        records = []
        for num_records, clients in self.results.items():
            for client_name, queries in clients.items():
                for query_name, time in queries.items():
                    records.append(
                        {
                            "records": num_records,
                            "client": client_name,
                            "query": query_name,
                            "time_ms": time,
                        }
                    )

        return pd.DataFrame(records)

    def plot(self):
        df = self.to_dataframe()

        # Plot
        plt.figure(figsize=(10, 6))
        for (client, query), group in df.groupby(["client", "query"]):
            group_sorted = group.sort_values("records")
            plt.plot(
                group_sorted["records"],
                group_sorted["time_ms"],
                marker="o",
                label=f"{client} - {query}",
            )

        plt.title("Query Performance vs. Number of Records")
        plt.xlabel("Number of Records")
        plt.ylabel("Time (ms)")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig("db_query_performance_plot.png")
        plt.close()

    def run(self):
        total_entires = 0

        for num_of_records in self.number_of_records:
            total_entires += num_of_records
            print(f"inserting {total_entires}...")
            print(f"benchmark database at {total_entires}...")
            self.run_insert_and_benchmark_client_queries(total_entires)

        self.plot()
