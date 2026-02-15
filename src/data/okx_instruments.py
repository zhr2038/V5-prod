from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import requests


@dataclass
class SpotSpec:
    inst_id: str
    base_ccy: str
    quote_ccy: str
    min_sz: float
    lot_sz: float


def round_down_to_lot(sz: float, lot_sz: float) -> float:
    """Round down size to OKX lot step."""
    sz_f = float(sz or 0.0)
    step = float(lot_sz or 0.0)
    if step <= 0:
        return sz_f
    return math.floor(sz_f / step) * step


class OKXSpotInstrumentsCache:
    def __init__(
        self,
        *,
        base_url: str = "https://www.okx.com",
        cache_path: str = "reports/okx_spot_instruments.json",
        ttl_sec: int = 6 * 3600,
        timeout_sec: float = 10.0,
    ):
        self.base_url = str(base_url).rstrip("/")
        self.cache_path = Path(cache_path)
        self.ttl_sec = int(ttl_sec)
        self.timeout_sec = float(timeout_sec)

    def _load_cache(self) -> Optional[Dict[str, Any]]:
        try:
            if not self.cache_path.exists():
                return None
            obj = json.loads(self.cache_path.read_text(encoding="utf-8"))
            if not isinstance(obj, dict):
                return None
            ts = float(obj.get("ts") or 0.0)
            if self.ttl_sec > 0 and (time.time() - ts) < float(self.ttl_sec):
                return obj
        except Exception:
            return None
        return None

    def _save_cache(self, obj: Dict[str, Any]) -> None:
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def _fetch(self) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v5/public/instruments"
        r = requests.get(url, params={"instType": "SPOT"}, timeout=self.timeout_sec)
        r.raise_for_status()
        obj = r.json()
        data = obj.get("data") if isinstance(obj, dict) else None
        return {"ts": time.time(), "data": data or []}

    def get_spec(self, inst_id: str) -> Optional[SpotSpec]:
        inst_id_u = str(inst_id or "").upper()
        if not inst_id_u:
            return None

        obj = self._load_cache()
        if obj is None:
            obj = self._fetch()
            self._save_cache(obj)

        rows = obj.get("data") or []
        if not isinstance(rows, list):
            return None

        for r in rows:
            if not isinstance(r, dict):
                continue
            iid = str(r.get("instId") or "").upper()
            if iid != inst_id_u:
                continue
            base = str(r.get("baseCcy") or "").upper()
            quote = str(r.get("quoteCcy") or "").upper()
            try:
                min_sz = float(r.get("minSz") or 0.0)
            except Exception:
                min_sz = 0.0
            try:
                lot_sz = float(r.get("lotSz") or 0.0)
            except Exception:
                lot_sz = 0.0

            return SpotSpec(inst_id=inst_id_u, base_ccy=base, quote_ccy=quote, min_sz=min_sz, lot_sz=lot_sz)

        return None
