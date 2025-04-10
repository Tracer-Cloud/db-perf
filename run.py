import os

from db_perf.db_versions.v1 import DbClient as DbClientV1
from db_perf.db_versions.v2 import DbClientV2
from db_perf.db_versions.v3 import DbClientV3
from db_perf.db_versions.v4 import DBClientV4

from db_perf.perf import PerfClient

NUMBER_OF_RECORDS = [100, 1000, 10000]
# NUMBER_OF_RECORDS = [100, 1_000, 10_000, 1_000_000, 2_000_000, 10_000_000]


def main():

    database_url = os.getenv(
        "DATABASE_URL", "postgres://postgres:postgres@localhost:5432/tracer_db"
    )

    client_list = [
        DbClientV1(database_url=database_url),
        DbClientV2(database_url=database_url),
        DbClientV3(database_url=database_url),
        DBClientV4(database_url=database_url),
    ]
    perf = PerfClient(clients=client_list, number_of_records=NUMBER_OF_RECORDS)

    perf.run()
