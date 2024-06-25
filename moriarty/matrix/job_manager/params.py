from __future__ import annotations

import enum
from typing import Optional

from pydantic import BaseModel

from moriarty.sidecar.params import InferenceProxyStatus, MatrixCallback


class InferenceJob(BaseModel):
    inference_id: str
    payload: dict
    """
    Request payload
    """
    metadata: dict = dict()


class InferenceResultStatus(enum.Enum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class InferenceResult(BaseModel):
    inference_id: str
    status: InferenceResultStatus
    message: Optional[str] = None
    payload: Optional[str] = None
    """
    Response payload
    """

    @classmethod
    def from_proxy_callback(cls, callback: MatrixCallback) -> InferenceResult:
        return InferenceResult(
            inference_id=callback.inference_id,
            status=(
                InferenceResultStatus.COMPLETED
                if callback.status == InferenceProxyStatus.FINISHED
                else InferenceResultStatus.FAILED
            ),
            message=callback.msg,
            payload=callback.payload,
        )
