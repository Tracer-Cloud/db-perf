import random
import uuid
from datetime import datetime

from factory import Factory, Faker, LazyFunction, SubFactory

from db_perf.models.events import (
    AwsInstanceMetaData,
    DiskStatistic,
    Event,
    InputFile,
    NextflowLog,
    PipelineTags,
    ProcessProperties,
    SyslogProperties,
    SystemMetric,
    SystemProperties,
)

fake = __import__("faker").Faker()


def float_field(min_val, max_val, precision=2):
    return LazyFunction(lambda: round(random.uniform(min_val, max_val), precision))


class InputFileFactory(Factory):
    class Meta:
        model = InputFile

    file_name = Faker("file_name")
    file_size = Faker("random_int", min=1024, max=10_000_000)
    file_path = Faker("file_path")
    file_directory = Faker("file_path")
    file_updated_at_timestamp = Faker("iso8601")


class ProcessPropertiesFactory(Factory):
    class Meta:
        model = ProcessProperties

    tool_name = LazyFunction(lambda: fake.word())
    tool_pid = LazyFunction(lambda: str(fake.random_number(digits=5)).zfill(5))
    tool_parent_pid = LazyFunction(lambda: str(fake.random_number(digits=5)).zfill(5))
    tool_binary_path = Faker("file_path")
    tool_cmd = LazyFunction(lambda: fake.sentence())
    start_timestamp = Faker("iso8601")
    process_cpu_utilization = Faker(
        "pyfloat", left_digits=2, right_digits=2, positive=True, max_value=100
    )
    process_memory_usage = Faker("random_int", min=1024, max=1048576)
    process_memory_virtual = Faker("random_int", min=2048, max=2097152)
    process_run_time = Faker("random_int", min=1, max=10000)
    process_disk_usage_read_last_interval = Faker("random_int", min=0, max=10000)
    process_disk_usage_write_last_interval = Faker("random_int", min=0, max=10000)
    process_disk_usage_read_total = Faker("random_int", min=0, max=100000)
    process_disk_usage_write_total = Faker("random_int", min=0, max=100000)
    process_status = Faker(
        "random_element", elements=["running", "completed", "failed"]
    )
    input_files = LazyFunction(lambda: [InputFileFactory() for _ in range(2)])
    container_id = Faker("uuid4")
    job_id = Faker("uuid4")
    working_directory = Faker("file_path")


class DiskStatisticFactory(Factory):
    class Meta:
        model = DiskStatistic

    disk_total_space = Faker("random_int", min=100_000, max=1_000_000)
    disk_used_space = Faker("random_int", min=50_000, max=900_000)
    disk_available_space = Faker("random_int", min=10_000, max=500_000)
    disk_utilization = Faker(
        "pyfloat", left_digits=2, right_digits=2, min_value=0.0, max_value=1.0
    )


class SystemMetricFactory(Factory):
    class Meta:
        model = SystemMetric

    events_name = Faker("word")
    system_memory_total = Faker("random_int", min=4096, max=65536)
    system_memory_used = Faker("random_int", min=1024, max=65536)
    system_memory_available = Faker("random_int", min=1024, max=65536)
    system_memory_utilization = Faker(
        "pyfloat", left_digits=2, right_digits=2, positive=True, max_value=100
    )
    system_memory_swap_total = Faker("random_int", min=1024, max=8192)
    system_memory_swap_used = Faker("random_int", min=0, max=8192)
    system_cpu_utilization = Faker(
        "pyfloat", left_digits=2, right_digits=2, positive=True, max_value=100
    )

    @LazyFunction
    def system_disk_io():
        return {f"/dev/sd{chr(i)}": DiskStatisticFactory() for i in range(97, 100)}


class SyslogPropertiesFactory(Factory):
    class Meta:
        model = SyslogProperties

    system_metrics = LazyFunction(SystemMetricFactory)
    error_display_name = Faker("word")
    error_id = Faker("uuid4")
    error_line = LazyFunction(lambda: fake.sentence())
    file_line_number = Faker("random_int", min=1, max=1000)
    file_previous_logs = LazyFunction(lambda: [fake.sentence() for _ in range(3)])


class PipelineTagsFactory(Factory):
    class Meta:
        model = PipelineTags

    environment = Faker("random_element", elements=["dev", "staging", "prod"])
    pipeline_type = Faker("random_element", elements=["ETL", "DataSync", "Analytics"])
    user_operator = Faker("first_name")
    department = "Research"
    team = "Oncology"
    others = Faker(
        "random_elements",
        elements=["AI", "ML", "NLP", "Imaging", "Genomics"],
        unique=False,
        length=2,
    )


class AwsInstanceMetaDataFactory(Factory):
    class Meta:
        model = AwsInstanceMetaData

    instance_id = Faker("uuid4")
    instance_type = Faker(
        "random_element", elements=["t2.micro", "m5.large", "c5.2xlarge"]
    )
    availability_zone = Faker("random_element", elements=["us-east-1a", "us-west-2b"])
    region = Faker("random_element", elements=["us-east-1", "us-west-2"])


class SystemPropertiesFactory(Factory):
    class Meta:
        model = SystemProperties

    os = Faker("linux_platform_token")
    os_version = Faker("numerify", text="##.##.##")
    kernel_version = Faker("linux_platform_token")
    arch = Faker("random_element", elements=["x86_64", "arm64"])
    num_cpus = Faker("random_int", min=1, max=64)
    hostname = Faker("hostname")
    total_memory = Faker("random_int", min=4096, max=131072)
    total_swap = Faker("random_int", min=0, max=32768)
    uptime = Faker("random_int", min=100, max=1000000)
    aws_metadata = SubFactory(AwsInstanceMetaDataFactory)
    is_aws_instance = Faker("boolean")
    system_disk_io = LazyFunction(
        lambda: {f"/dev/sd{chr(i)}": DiskStatisticFactory() for i in range(97, 99)}
    )
    ec2_cost_per_hour = LazyFunction(lambda: random.choice([0.24, 1.13]))


class NextflowLogFactory(Factory):
    class Meta:
        model = NextflowLog

    session_uuid = Faker("uuid4")
    jobs_ids = LazyFunction(lambda: [str(fake.uuid4()) for _ in range(3)])


class EventFactory(Factory):
    class Meta:
        model = Event

    timestamp = LazyFunction(datetime.now)
    message = LazyFunction(lambda: fake.sentence())
    event_type = Faker("random_element", elements=["process", "system", "log"])
    process_type = Faker("random_element", elements=["ingest", "transform", "export"])
    process_status = Faker(
        "random_element", elements=["running", "failed", "completed"]
    )
    pipeline_name = Faker("word")
    run_name = Faker("word")
    run_id = LazyFunction(lambda: str(uuid.uuid4()))
    tags = SubFactory(PipelineTagsFactory)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        kwargs["attributes"] = {
            "process": ProcessPropertiesFactory(),
            "system_metric": SystemMetricFactory(),
            "syslog": SyslogPropertiesFactory(),
            "system_properties": SystemPropertiesFactory(),
            "nextflow_log": NextflowLogFactory(),
        }
        return super()._create(model_class, *args, **kwargs)
