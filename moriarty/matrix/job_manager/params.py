from pydantic import BaseModel


class InferenceJob(BaseModel):
    job_id: str


class InferenceResult(BaseModel):
    job_id: str
    result: dict
