from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List

from src.core.models import MarketSeries


class MarketDataProvider(ABC):
    @abstractmethod
    def fetch_ohlcv(self, symbols: List[str], timeframe: str, limit: int) -> Dict[str, MarketSeries]:
        raise NotImplementedError
