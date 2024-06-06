from __future__ import annotations

import httpx
from pydantic import BaseModel


class MetrixCallback(BaseModel):
    pass

    @classmethod
    def from_exception(cls, e: Exception) -> MetrixCallback:
        return MetrixCallback()

    @classmethod
    def from_response(cls, response: httpx.Response) -> MetrixCallback:
        return MetrixCallback()
