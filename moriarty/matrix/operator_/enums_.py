import enum


class EndpointMetrics(str, enum.Enum):
    pendding_jobs = "pendding_jobs"
    pendding_jobs_per_instance = "pendding_jobs_per_instance"
