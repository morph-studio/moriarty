from __future__ import annotations

import enum
import traceback
from typing import Optional

import httpx
from pydantic import BaseModel


class InferenceProxyStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    TIMEOUT = "TIMEOUT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class MatrixCallback(BaseModel):
    inference_id: str
    status: InferenceProxyStatus
    msg: Optional[str] = None
    payload: Optional[str] = None

    @classmethod
    def from_exception(cls, inference_id: str, e: Exception) -> MatrixCallback:
        trb = "".join(traceback.format_tb(e.__traceback__))
        if isinstance(e, httpx.TimeoutException):
            return MatrixCallback(
                inference_id=inference_id,
                status=InferenceProxyStatus.TIMEOUT,
                msg=f"Inference {inference_id} timeout",
                payload=trb,
            )
        else:
            return MatrixCallback(
                inference_id=inference_id,
                status=InferenceProxyStatus.INTERNAL_ERROR,
                msg=f"Inference {inference_id} failed: {e}",
                payload=trb,
            )

    @classmethod
    def from_response(cls, inference_id: str, response: httpx.Response) -> MatrixCallback:
        return MatrixCallback(
            inference_id=inference_id,
            status=InferenceProxyStatus.FINISHED,
            msg=f"Inference {inference_id} finished",
            payload=response.text,
        )
