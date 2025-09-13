from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Finding(BaseModel):
    code: str
    message: str
    path: str
    severity: Severity = Severity.ERROR

    def to_dict(self: Finding) -> dict[str, Any]:
        # Convenience for callers; uses Pydantic v2 model_dump under the hood
        return self.model_dump()
