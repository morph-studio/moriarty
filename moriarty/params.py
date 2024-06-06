from __future__ import annotations

import httpx
from pydantic import BaseModel


class MatrixCallback(BaseModel):
    pass

    @classmethod
    def from_exception(cls, e: Exception) -> MatrixCallback:
        return MatrixCallback()

    @classmethod
    def from_response(cls, response: httpx.Response) -> MatrixCallback:
        return MatrixCallback()
