from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel

if TYPE_CHECKING:
    from moriarty.matrix.job_manager.params import InferenceJob


class InvokeParams(BaseModel):
    inference_id: Optional[str] = None
    endpoint_name: str
    invoke_params: dict
    priority: Optional[int] = None
    """
    1-100, Higher number means higher priority
    """

    created_at: datetime = datetime.now()
    expires_at: Optional[datetime] = None

    def model_dump_metadata(self) -> dict:
        return self.model_dump(
            exclude={"invoke_params"},
        )

    @classmethod
    def from_inference_job(cls, job: "InferenceJob") -> InvokeParams:
        return cls(
            inference_id=job.inference_id,
            endpoint_name=job.endpoint_name,
            invoke_params=job.payload,
            **job.metadata,
        )


class InvokeResponse(BaseModel):
    inference_id: str
