from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel

# --- Event Attribute Types (mimicking your enum-like variants) ---


class ProcessAttributes(BaseModel):
    pid: int
    name: str
    cpu_usage: float


class SystemMetrics(BaseModel):
    memory_used: int
    cpu_load: float
    disk_io: float


class Syslog(BaseModel):
    facility: str
    severity: str
    message: str


EventAttributes = Union[ProcessAttributes, SystemMetrics, Syslog]


# --- PipelineTags placeholder (adapt to match your Rust struct) ---


class PipelineTags(BaseModel):
    env: str
    owner: str


# --- Main Event Model ---


class Event(BaseModel):
    timestamp: datetime
    message: str
    event_type: str
    process_type: str
    process_status: str
    pipeline_name: Optional[str]
    run_name: Optional[str]
    run_id: Optional[str]
    attributes: Optional[EventAttributes]
    tags: Optional[PipelineTags]
