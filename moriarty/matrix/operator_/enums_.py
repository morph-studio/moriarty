import enum


class MetricType(str, enum.Enum):
    pending_jobs = "pending_jobs"
    pending_jobs_per_instance = "pending_jobs_per_instance"
