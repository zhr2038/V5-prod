from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from configs.loader import load_blacklist


OKX_PUBLIC = "https://www.okx.com"


@dataclass
class UniverseItem:
    symbol: str  # e.g. BTC/USDT
    inst_id: str  # e.g. BTC-USDT
    quote_volume_usdt_24h: float


class OKXUniverseProvider:
    """Dynamic spot universe for USDT pairs using OKX public REST.

    - Fetch instruments (SPOT)
    - Fetch tickers (SPOT)
    - Filter by quote=USDT, liquidity threshold, stablecoin exclusion, blacklist
    - Cache to JSON with TTL

    Note: This is public-data only. It does NOT require API keys.
    """

    def __init__(
        self,
        base_url: str = OKX_PUBLIC,
        cache_path: str = "reports/universe_cache.json",
        cache_ttl_sec: int = 3600,
        min_24h_quote_volume_usdt: float = 5_000_000.0,
        blacklist_path: str = "configs/blacklist.json",
        exclude_stablecoins: bool = True,
        timeout_sec: int = 10,
    ):
        self.base_url = base_url.rstrip("/")
        self.cache_path = Path(cache_path)
        self.cache_ttl_sec = int(cache_ttl_sec)
        self.min_24h_quote_volume_usdt = float(min_24h_quote_volume_usdt)
        self.blacklist_path = blacklist_path
        self.exclude_stablecoins = bool(exclude_stablecoins)
        self.timeout_sec = int(timeout_sec)

    def get_universe(self, now_ts: Optional[float] = None) -> List[str]:
        now_ts = float(now_ts or time.time())
        cached = self._load_cache(now_ts)
        if cached is not None:
            return cached

        inst = self._fetch_instruments()
        tks = self._fetch_tickers()
        items = self._build(inst, tks)
        syms = [it.symbol for it in items]
        self._save_cache(now_ts, syms)
        return syms

    def _load_cache(self, now_ts: float) -> Optional[List[str]]:
        try:
            if not self.cache_path.exists():
                return None
            obj = json.loads(self.cache_path.read_text(encoding="utf-8"))
            ts = float(obj.get("ts") or 0.0)
            ttl = float(self.cache_ttl_sec)
            if ttl > 0 and (now_ts - ts) < ttl:
                syms = obj.get("symbols")
                if isinstance(syms, list) and syms:
                    return [str(s) for s in syms]
        except Exception:
            return None
        return None

    def _save_cache(self, now_ts: float, symbols: List[str]) -> None:
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(
                json.dumps({"ts": now_ts, "ttl_sec": self.cache_ttl_sec, "symbols": symbols}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _fetch_instruments(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v5/public/instruments"
        r = requests.get(url, params={"instType": "SPOT"}, timeout=self.timeout_sec)
        r.raise_for_status()
        obj = r.json()
        return list(obj.get("data") or [])

    def _fetch_tickers(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v5/market/tickers"
        r = requests.get(url, params={"instType": "SPOT"}, timeout=self.timeout_sec)
        r.raise_for_status()
        obj = r.json()
        return list(obj.get("data") or [])

    @staticmethod
    def _is_stablecoin(asset: str) -> bool:
        a = (asset or "").upper()
        return a in {"USDT", "USDC", "DAI", "TUSD", "USDG", "FDUSD", "BUSD", "USDP"}

    def _build(self, instruments: List[Dict[str, Any]], tickers: List[Dict[str, Any]]) -> List[UniverseItem]:
        # instId -> (base, quote)
        pairs: Dict[str, Tuple[str, str]] = {}
        for it in instruments or []:
            inst_id = str(it.get("instId") or "")
            if not inst_id:
                continue
            base = str(it.get("baseCcy") or "").upper()
            quote = str(it.get("quoteCcy") or "").upper()
            if quote != "USDT":
                continue
            if self.exclude_stablecoins and self._is_stablecoin(base):
                continue
            pairs[inst_id] = (base, quote)

        bl = load_blacklist(self.blacklist_path)
        bl_syms = set(str(s).upper() for s in (bl.get("symbols") or []))

        out: List[UniverseItem] = []
        for t in tickers or []:
            inst_id = str(t.get("instId") or "")
            if inst_id not in pairs:
                continue
            base, quote = pairs[inst_id]
            sym = f"{base}/{quote}"
            if sym.upper() in bl_syms or inst_id.upper().replace("-", "/") in bl_syms:
                continue

            # OKX tickers: volCcyQuote = quote volume (in quote currency)
            qv = t.get("volCcyQuote")
            try:
                qv_f = float(qv or 0.0)
            except Exception:
                qv_f = 0.0
            if qv_f < self.min_24h_quote_volume_usdt:
                continue
            out.append(UniverseItem(symbol=sym, inst_id=inst_id, quote_volume_usdt_24h=qv_f))

        out.sort(key=lambda x: float(x.quote_volume_usdt_24h), reverse=True)
        return out
