from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol


class TradingClock(Protocol):
    def now(self) -> datetime: ...


@dataclass
class SystemClock:
    tz: timezone = timezone.utc

    def now(self) -> datetime:
        return datetime.now(self.tz)


@dataclass
class FixedClock:
    t: datetime

    def now(self) -> datetime:
        return self.t
