from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel

# --- Event Attribute Types (mimicking your enum-like variants) ---


class InputFile(BaseModel):
    file_name: str
    file_size: int
    file_path: str
    file_directory: str
    file_updated_at_timestamp: str


class ProcessProperties(BaseModel):
    tool_name: str
    tool_pid: str
    tool_parent_pid: str
    tool_binary_path: str
    tool_cmd: str
    start_timestamp: str
    process_cpu_utilization: float
    process_memory_usage: int
    process_memory_virtual: int
    process_run_time: int
    process_disk_usage_read_last_interval: int
    process_disk_usage_write_last_interval: int
    process_disk_usage_read_total: int
    process_disk_usage_write_total: int
    process_status: str
    input_files: Optional[List[InputFile]] = None
    container_id: Optional[str] = None
    job_id: Optional[str] = None
    working_directory: Optional[str] = None


class CompletedProcess(BaseModel):
    tool_name: str
    tool_pid: str
    duration_sec: int


class DataSetsProcessed(BaseModel):
    datasets: str
    total: int


class DiskStatistic(BaseModel):
    disk_total_space: int
    disk_used_space: int
    disk_available_space: int
    disk_utilization: float


class SystemMetric(BaseModel):
    events_name: str
    system_memory_total: int
    system_memory_used: int
    system_memory_available: int
    system_memory_utilization: float
    system_memory_swap_total: int
    system_memory_swap_used: int
    system_cpu_utilization: float
    system_disk_io: Dict[str, DiskStatistic]


class SyslogProperties(BaseModel):
    system_metrics: SystemMetric
    error_display_name: str
    error_id: str
    error_line: str
    file_line_number: int
    file_previous_logs: List[str]


class AwsInstanceMetaData(BaseModel):
    instance_id: Optional[str]
    instance_type: Optional[str]
    availability_zone: Optional[str]
    region: Optional[str]


class SystemProperties(BaseModel):
    os: Optional[str]
    os_version: Optional[str]
    kernel_version: Optional[str]
    arch: Optional[str]
    num_cpus: int
    hostname: Optional[str]
    total_memory: int
    total_swap: int
    uptime: int
    aws_metadata: Optional[AwsInstanceMetaData]
    is_aws_instance: bool
    system_disk_io: Dict[str, DiskStatistic]
    ec2_cost_per_hour: Optional[float]


class NextflowLog(BaseModel):
    session_uuid: Optional[str]
    jobs_ids: Optional[List[str]]


class EventAttributes(BaseModel):
    process: Optional[ProcessProperties]
    system_metric: Optional[SystemMetric]
    syslog: Optional[SyslogProperties]
    system_properties: Optional[SystemProperties]
    nextflow_log: Optional[NextflowLog]


# --- PipelineTags placeholder (adapt to match your Rust struct) ---


class PipelineTags(BaseModel):
    environment: str
    pipeline_type: str
    user_operator: str
    department: str = "Research"
    team: str = "Oncology"
    others: List[str] = []


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
