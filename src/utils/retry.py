from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar


T = TypeVar("T")


@dataclass
class RetryConfig:
    """RetryConfig类"""
    max_attempts: int = 5
    base_delay_sec: float = 0.25
    max_delay_sec: float = 5.0
    jitter_frac: float = 0.25


def _sleep_with_jitter(delay: float, jitter_frac: float) -> None:
    delay = max(0.0, float(delay))
    jf = max(0.0, float(jitter_frac))
    jitter = delay * jf * random.random()
    time.sleep(delay + jitter)


def retry(
    fn: Callable[[], T],
    *,
    should_retry: Callable[[Exception], bool],
    cfg: Optional[RetryConfig] = None,
) -> T:
    """Generic retry with exponential backoff + jitter."""
    c = cfg or RetryConfig()
    attempt = 0
    last_exc: Optional[Exception] = None

    while attempt < int(c.max_attempts):
        try:
            return fn()
        except Exception as e:  # 修复：从 BaseException 改为 Exception，避免捕获 KeyboardInterrupt/SystemExit
            last_exc = e
            attempt += 1
            if attempt >= int(c.max_attempts) or not should_retry(e):
                raise
            delay = min(float(c.max_delay_sec), float(c.base_delay_sec) * (2 ** (attempt - 1)))
            _sleep_with_jitter(delay, c.jitter_frac)

    assert last_exc is not None
    raise last_exc
