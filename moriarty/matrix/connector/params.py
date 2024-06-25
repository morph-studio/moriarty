from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class InvokeParams(BaseModel):
    endpoint_name: str
    invoke_params: dict
    priority: Optional[int] = None
    created_at: datetime
    expires_at: Optional[datetime] = None
