from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class InvokeParams(BaseModel):
    endpoint_name: str
    invoke_params: dict
    priority: Optional[int] = None
    """
    1-100, Higher number means higher priority
    """

    created_at: datetime
    expires_at: Optional[datetime] = None
