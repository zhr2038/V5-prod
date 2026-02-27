from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol


class TradingClock(Protocol):
    """交易时钟协议"""
    def now(self) -> datetime: ...


@dataclass
class SystemClock:
    """系统时钟 - 返回当前时间"""
    tz: timezone = timezone.utc

    def now(self) -> datetime:
        """获取当前时间"""
        return datetime.now(self.tz)


@dataclass
class FixedClock:
    """固定时钟 - 用于测试，返回固定时间"""
    t: datetime

    def now(self) -> datetime:
        """返回固定时间"""
        return self.t
