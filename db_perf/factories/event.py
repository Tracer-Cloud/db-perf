import random
import uuid
from datetime import datetime
from typing import Optional, Union

from factory import Factory, Faker, LazyFunction, SubFactory
from pydantic import BaseModel

from db_perf.models.events import (
    Event,
    PipelineTags,
    ProcessAttributes,
    Syslog,
    SystemMetrics,
)

# --- Attribute Factories ---


class ProcessAttributesFactory(Factory):
    class Meta:
        model = ProcessAttributes

    pid = Faker("random_int", min=100, max=5000)
    name = Faker("word")
    cpu_usage = Faker(
        "pyfloat", left_digits=1, right_digits=2, positive=True, max_value=100
    )


class SystemMetricsFactory(Factory):
    class Meta:
        model = SystemMetrics

    memory_used = Faker("random_int", min=1024, max=16384)
    cpu_load = Faker(
        "pyfloat", left_digits=1, right_digits=2, positive=True, max_value=1.0
    )
    disk_io = Faker(
        "pyfloat", left_digits=2, right_digits=2, positive=True, max_value=1000
    )


class SyslogFactory(Factory):
    class Meta:
        model = Syslog

    facility = Faker("word")
    severity = Faker("word")
    message = Faker("sentence")


# --- PipelineTags Factory ---


class PipelineTagsFactory(Factory):
    class Meta:
        model = PipelineTags

    env = Faker("random_element", elements=["dev", "staging", "prod"])
    owner = Faker("first_name")


# --- Main Event Factory ---


class EventFactory(Factory):
    class Meta:
        model = Event

    timestamp = LazyFunction(datetime.utcnow)
    message = Faker("sentence")
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
        # Randomly assign one of the attributes types
        attr_type = random.choice(
            [ProcessAttributesFactory, SystemMetricsFactory, SyslogFactory]
        )
        kwargs["attributes"] = attr_type()
        return super()._create(model_class, *args, **kwargs)
