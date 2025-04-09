from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class InputFile:
    file_name: str
    file_size: int
    file_path: str
    file_directory: str
    file_updated_at_timestamp: str


@dataclass
class ProcessProperties:
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


@dataclass
class CompletedProcess:
    tool_name: str
    tool_pid: str
    duration_sec: int


@dataclass
class DataSetsProcessed:
    datasets: str
    total: int


@dataclass
class DiskStatistic:
    disk_total_space: int
    disk_used_space: int
    disk_available_space: int
    disk_utilization: float


@dataclass
class SystemMetric:
    events_name: str
    system_memory_total: int
    system_memory_used: int
    system_memory_available: int
    system_memory_utilization: float
    system_memory_swap_total: int
    system_memory_swap_used: int
    system_cpu_utilization: float
    system_disk_io: Dict[str, DiskStatistic]


@dataclass
class SystemProperties:
    os: Optional[str]
    os_version: Optional[str]
    kernel_version: Optional[str]
    arch: Optional[str]
    num_cpus: int
    hostname: Optional[str]
    total_memory: int
    total_swap: int
    uptime: int
    aws_metadata: Optional[dict]
    is_aws_instance: bool
    system_disk_io: Dict[str, DiskStatistic]
    ec2_cost_per_hour: Optional[float]


@dataclass
class NextflowLog:
    session_uuid: Optional[str]
    jobs_ids: Optional[List[str]]


@dataclass
class SyslogProperties:
    system_metrics: SystemMetric
    error_display_name: str
    error_id: str
    error_line: str
    file_line_number: int
    file_previous_logs: List[str]


@dataclass
class PipelineTags:
    environment: str
    pipeline_type: str
    user_operator: str
    department: str = "Research"
    team: str = "Oncology"
    others: List[str] = field(default_factory=list)


@dataclass
class Event:
    timestamp: datetime
    message: str
    event_type: str
    process_type: str
    process_status: str
    pipeline_name: Optional[str]
    run_name: Optional[str]
    run_id: Optional[str]
    attributes: Optional[dict]  # Serialized to JSON
    tags: Optional[PipelineTags]
