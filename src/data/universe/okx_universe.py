from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from configs.loader import load_blacklist

# Optional dynamic auto-blacklist (best-effort)
try:
    from src.utils.auto_blacklist import add_symbol as auto_blacklist_add
except Exception:  # pragma: no cover
    auto_blacklist_add = None


OKX_PUBLIC = "https://www.okx.com"


@dataclass
class UniverseItem:
    """UniverseItem类"""
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
        top_n: int = 30,
        min_24h_quote_volume_usdt: float = 5_000_000.0,
        blacklist_path: str = "configs/blacklist.json",
        exclude_stablecoins: bool = True,
        timeout_sec: int = 10,
        max_spread_bps: Optional[float] = None,
        exclude_symbols: Optional[List[str]] = None,
        *,
        refine_with_single_ticker: bool = False,
        refine_single_ticker_max_candidates: int = 200,
        refine_single_ticker_sleep_sec: float = 0.02,
    ):
        self.base_url = base_url.rstrip("/")
        self.cache_path = Path(cache_path)
        self.cache_ttl_sec = int(cache_ttl_sec)
        self.top_n = int(top_n)
        self.min_24h_quote_volume_usdt = float(min_24h_quote_volume_usdt)
        self.blacklist_path = blacklist_path
        self.exclude_stablecoins = bool(exclude_stablecoins)
        self.timeout_sec = int(timeout_sec)
        self.max_spread_bps = (float(max_spread_bps) if max_spread_bps is not None else None)
        self.exclude_symbols = [str(s) for s in (exclude_symbols or []) if str(s).strip()]
        self.refine_with_single_ticker = bool(refine_with_single_ticker)
        self.refine_single_ticker_max_candidates = int(refine_single_ticker_max_candidates)
        self.refine_single_ticker_sleep_sec = float(refine_single_ticker_sleep_sec)

    def get_universe(self, now_ts: Optional[float] = None) -> List[str]:
        """Get universe"""
        now_ts = float(now_ts or time.time())
        cached = self._load_cache(now_ts)
        if cached is not None:
            if int(self.top_n) > 0:
                return list(cached)[: int(self.top_n)]
            return cached

        inst = self._fetch_instruments()
        tks = self._fetch_tickers()
        items = self._build(inst, tks)

        # Optional refinement: per-instrument ticker gives a more reliable 24h quote volume on some mirrors.
        if self.refine_with_single_ticker and items:
            items = self._refine_by_single_ticker(items)

        syms = self._apply_symbol_filters([it.symbol for it in items])
        self._save_cache(now_ts, syms)
        return syms

    def _load_cache(self, now_ts: float) -> Optional[List[str]]:
        try:
            if not self.cache_path.exists():
                return None
            obj = json.loads(self.cache_path.read_text(encoding="utf-8"))
            ts = float(obj.get("ts") or 0.0)
            ttl = float(self.cache_ttl_sec)
            if obj.get("config_signature") != self._cache_signature():
                return None
            if ttl > 0 and (now_ts - ts) < ttl:
                syms = obj.get("symbols")
                if isinstance(syms, list) and syms:
                    return self._apply_symbol_filters([str(s) for s in syms])
        except Exception:
            return None
        return None

    def _save_cache(self, now_ts: float, symbols: List[str]) -> None:
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(
                json.dumps(
                    {
                        "ts": now_ts,
                        "ttl_sec": self.cache_ttl_sec,
                        "config_signature": self._cache_signature(),
                        "symbols": list(symbols or []),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _cache_signature(self) -> Dict[str, Any]:
        return {
            "base_url": self.base_url,
            "min_24h_quote_volume_usdt": float(self.min_24h_quote_volume_usdt),
            "exclude_stablecoins": bool(self.exclude_stablecoins),
            "max_spread_bps": self.max_spread_bps,
            "refine_with_single_ticker": bool(self.refine_with_single_ticker),
            "refine_single_ticker_max_candidates": int(self.refine_single_ticker_max_candidates),
            "exclude_symbols": sorted({str(s).upper() for s in self.exclude_symbols}),
        }

    def _blocked_symbols(self) -> set[str]:
        blocked = set()
        try:
            bl = load_blacklist(self.blacklist_path)
            for raw in (bl.get("symbols") or []):
                s = str(raw).strip().upper()
                if not s:
                    continue
                blocked.add(s)
                blocked.add(s.replace("/", "-"))
                blocked.add(s.replace("-", "/"))
        except Exception:
            pass

        for raw in self.exclude_symbols:
            s = str(raw).strip().upper()
            if not s:
                continue
            blocked.add(s)
            blocked.add(s.replace("/", "-"))
            blocked.add(s.replace("-", "/"))
        return blocked

    def _apply_symbol_filters(self, symbols: List[str]) -> List[str]:
        blocked = self._blocked_symbols()
        out: List[str] = []
        seen = set()
        for raw in symbols or []:
            symbol = str(raw).strip()
            if not symbol:
                continue
            upper = symbol.upper()
            if upper in blocked or upper.replace("/", "-") in blocked:
                continue
            if self.exclude_stablecoins:
                base = upper.split("/", 1)[0]
                if self._is_stablecoin(base):
                    continue
            if upper in seen:
                continue
            seen.add(upper)
            out.append(symbol)
        return out

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

    def _fetch_ticker(self, inst_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/api/v5/market/ticker"
        r = requests.get(url, params={"instId": str(inst_id)}, timeout=self.timeout_sec)
        r.raise_for_status()
        obj = r.json()
        rows = obj.get("data") if isinstance(obj, dict) else None
        if isinstance(rows, list) and rows:
            if isinstance(rows[0], dict):
                return rows[0]
        return None

    @staticmethod
    def _is_stablecoin(asset: str) -> bool:
        a = (asset or "").upper()
        return a in {"USDT", "USDC", "DAI", "TUSD", "USDG", "FDUSD", "BUSD", "USDP"}

    def _quote_vol_usdt_from_ticker(self, t: Dict[str, Any]) -> float:
        qv_f = 0.0
        for k in ("volCcyQuote", "volCcy24h"):
            try:
                qv_f = float(t.get(k) or 0.0)
            except Exception:
                qv_f = 0.0
            if qv_f > 0:
                return float(qv_f)

        # Fallback: estimate quote volume from base volume * last.
        try:
            vol_base = float(t.get("vol24h") or 0.0)
        except Exception:
            vol_base = 0.0
        try:
            last_px = float(t.get("last") or 0.0)
        except Exception:
            last_px = 0.0
        if vol_base > 0 and last_px > 0:
            return float(vol_base * last_px)
        return 0.0

    def _refine_by_single_ticker(self, items: List[UniverseItem]) -> List[UniverseItem]:
        # Take a subset of candidates (already sorted by batch volume) and re-rank using per-inst ticker.
        import time as _time

        m = int(self.refine_single_ticker_max_candidates or 0)
        cand = list(items)[:m] if m > 0 else list(items)
        refined: List[UniverseItem] = []
        for it in cand:
            t = self._fetch_ticker(it.inst_id)
            if not isinstance(t, dict):
                continue
            qv = self._quote_vol_usdt_from_ticker(t)
            refined.append(UniverseItem(symbol=it.symbol, inst_id=it.inst_id, quote_volume_usdt_24h=float(qv)))
            if float(self.refine_single_ticker_sleep_sec) > 0:
                _time.sleep(float(self.refine_single_ticker_sleep_sec))

        refined.sort(key=lambda x: float(x.quote_volume_usdt_24h), reverse=True)
        refined = [x for x in refined if float(x.quote_volume_usdt_24h) >= float(self.min_24h_quote_volume_usdt)]
        if int(self.top_n) > 0:
            refined = refined[: int(self.top_n)]
        return refined

    def _build(self, instruments: List[Dict[str, Any]], tickers: List[Dict[str, Any]]) -> List[UniverseItem]:
        # instId -> (base, quote, minSz)
        pairs: Dict[str, Tuple[str, str, float]] = {}
        for it in instruments or []:
            inst_id = str(it.get("instId") or "")
            if not inst_id:
                continue
            state = str(it.get("state") or "").strip().lower()
            if state and state != "live":
                continue
            base = str(it.get("baseCcy") or "").upper()
            quote = str(it.get("quoteCcy") or "").upper()
            if quote != "USDT":
                continue
            if self.exclude_stablecoins and self._is_stablecoin(base):
                continue
            try:
                min_sz = float(it.get("minSz") or 0.0)
            except Exception:
                min_sz = 0.0
            pairs[inst_id] = (base, quote, float(min_sz))

        bl_syms = self._blocked_symbols()

        out: List[UniverseItem] = []
        for t in tickers or []:
            inst_id = str(t.get("instId") or "")
            if inst_id not in pairs:
                continue
            base, quote, min_sz = pairs[inst_id]
            sym = f"{base}/{quote}"
            if sym.upper() in bl_syms or inst_id.upper() in bl_syms or inst_id.upper().replace("-", "/") in bl_syms:
                continue

            qv_f = self._quote_vol_usdt_from_ticker(t)
            if qv_f < self.min_24h_quote_volume_usdt:
                continue

            # Optional tradability filter: spread
            if self.max_spread_bps is not None:
                try:
                    bid = float(t.get("bidPx") or 0.0)
                    ask = float(t.get("askPx") or 0.0)
                    if bid > 0 and ask > 0:
                        mid = (bid + ask) / 2.0
                        spread_bps = (ask - bid) / mid * 10_000.0 if mid > 0 else 0.0
                        if spread_bps > float(self.max_spread_bps):
                            # Add to dynamic blacklist to avoid repeated selection.
                            try:
                                if auto_blacklist_add is not None:
                                    auto_blacklist_add(sym, reason=f"spread_too_wide>{self.max_spread_bps}bps", ttl_sec=7 * 24 * 3600, meta={"spread_bps": spread_bps})
                            except Exception:
                                pass
                            continue
                except Exception:
                    pass

            out.append(UniverseItem(symbol=sym, inst_id=inst_id, quote_volume_usdt_24h=float(qv_f)))

        out.sort(key=lambda x: float(x.quote_volume_usdt_24h), reverse=True)
        if int(self.top_n) > 0:
            out = out[: int(self.top_n)]
        return out
