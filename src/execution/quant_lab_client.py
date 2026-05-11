from __future__ import annotations

# Compatibility wrapper. New quant-lab API access lives in src.quant_lab_client.
import os
from typing import Any, Optional

from src.quant_lab_client.client import (
    QuantLabClient as _QuantLabClient,
    QuantLabResponse,
    append_jsonl,
    sanitize_quant_lab_obj,
    summarize_response,
)


class QuantLabClient(_QuantLabClient):
    def __init__(
        self,
        base_url: str,
        *,
        timeout_sec: Optional[float] = None,
        token_env: Optional[str] = None,
        session: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        if timeout_sec is not None and "timeout_seconds" not in kwargs:
            kwargs["timeout_seconds"] = timeout_sec
        if session is not None and "http_client" not in kwargs:
            kwargs["http_client"] = session
        if token_env is not None and "api_token" not in kwargs:
            kwargs["api_token"] = os.getenv(str(token_env or ""), "").strip() or None
        super().__init__(base_url=base_url, **kwargs)

__all__ = [
    "QuantLabClient",
    "QuantLabResponse",
    "append_jsonl",
    "sanitize_quant_lab_obj",
    "summarize_response",
]
