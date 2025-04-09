import os

from factory import Factory, Faker, LazyFunction, SubFactory

from db_perf.db_versions.v1 import DbClient as DbClientV1
from db_perf.factories.event import (
    AwsInstanceMetaDataFactory,
    DiskStatisticFactory,
    EventFactory,
    NextflowLogFactory,
    SyslogPropertiesFactory,
    SystemMetricFactory,
    SystemPropertiesFactory,
)
from db_perf.perf import PerfClient

NUMBER_OF_RECORDS = [100]
# NUMBER_OF_RECORDS = [100, 1_000, 10_000, 1_000_000, 2_000_000, 10_000_000]


def main():

    database_url = os.getenv(
        "DATABASE_URL", "postgres://postgres:postgres@localhost:5432/tracer_db"
    )

    client_list = [
        DbClientV1(database_url),
    ]
    perf = PerfClient(client_list)

    total_entires = 0

    for num_of_records in NUMBER_OF_RECORDS:
        total_entires += num_of_records
        print(f"inserting {total_entires}...")
        print(f"benchmark database at {total_entires}...")
        perf.run_insert_and_benchmark_client_queries(total_entires)

    print(perf.results)
