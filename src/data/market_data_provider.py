from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List

from src.core.models import MarketSeries


class MarketDataProvider(ABC):
    @abstractmethod
    def fetch_ohlcv(
        self,
        symbols: List[str],
        timeframe: str,
        limit: int,
        end_ts_ms: int | None = None,
    ) -> Dict[str, MarketSeries]:
        raise NotImplementedError
