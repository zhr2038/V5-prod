#!/usr/bin/env python3
"""
Simple price fetcher for event-driven trading.
Fetches real-time prices from OKX API.
"""
from __future__ import print_function

import json
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class PriceFetcher:
    """Fetches real-time prices from OKX."""
    
    def __init__(self, exchange=None):
        self.exchange = exchange
        self._prices = {}
    
    def fetch_all_prices(self) -> Dict[str, float]:
        """Fetch all USDT pair prices from OKX."""
        try:
            import requests
            
            # OKX API endpoint for tickers
            url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
            response = requests.get(url, timeout=10)
            data = response.json()

            if data.get('code') != '0':
                logger.error(f"OKX API error: {data}")
                return {}

            prices = {}
            for ticker in data.get('data', []):
                inst_id = ticker.get('instId', '')
                if inst_id.endswith('-USDT'):
                    # Convert BTC-USDT to BTC/USDT format
                    symbol = inst_id.replace('-', '/')
                    try:
                        last_px = float(ticker.get('last', 0))
                        if last_px > 0:
                            prices[symbol] = last_px
                    except (ValueError, TypeError):
                        continue
            
            self._prices = prices
            logger.info(f"Fetched {len(prices)} prices from OKX")
            return prices

        except Exception as e:
            logger.error(f"Failed to fetch prices: {e}")
            return {}
    
    def get_price(self, symbol: str) -> Optional[float]:
        """Get price for a specific symbol."""
        return self._prices.get(symbol)


def fetch_prices() -> Dict[str, float]:
    """Convenience function to fetch all prices."""
    fetcher = PriceFetcher()
    return fetcher.fetch_all_prices()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    prices = fetch_prices()
    print(f"Fetched {len(prices)} prices")
    for sym, px in sorted(prices.items())[:5]:
        print(f"  {sym}: {px}")
