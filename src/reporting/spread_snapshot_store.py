from __future__ import annotations

import bisect
import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class SpreadSnapshot:
    """SpreadSnapshot类"""
    ts_ms: int
    symbol: str
    bid: float
    ask: float
    mid: float
    spread_bps: Optional[float]


def _utc_yyyymmdd_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def _ymd_prev(ymd: str) -> str:
    dt = datetime.strptime(ymd, "%Y%m%d").replace(tzinfo=timezone.utc)
    return (dt - timedelta(days=1)).strftime("%Y%m%d")


class SpreadSnapshotStore:
    """Lightweight reader for reports/spread_snapshots/YYYYMMDD.jsonl.

    Each line event schema:
      { window_end_ts: epoch seconds, symbols: [{symbol,bid,ask,mid,spread_bps,...}, ...] }

    We cache per-day, per-symbol sorted snapshots for fast lookup.
    """

    def __init__(self, base_dir: str = "reports/spread_snapshots"):
        self.base_dir = Path(base_dir)
        self._cache: Dict[Tuple[str, str], List[SpreadSnapshot]] = {}

    def _load_day_symbol(self, ymd: str, symbol: str) -> List[SpreadSnapshot]:
        key = (str(ymd), str(symbol))
        if key in self._cache:
            return self._cache[key]

        path = self.base_dir / f"{ymd}.jsonl"
        out: List[SpreadSnapshot] = []
        if not path.exists():
            self._cache[key] = out
            return out

        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    evt = json.loads(line)
                except Exception:
                    continue
                we = evt.get("window_end_ts")
                if we is None:
                    continue
                ts_ms = int(we) * 1000
                rows = evt.get("symbols") or []
                if not isinstance(rows, list):
                    continue
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    if str(r.get("symbol")) != str(symbol):
                        continue
                    try:
                        bid = float(r.get("bid"))
                        ask = float(r.get("ask"))
                        mid = float(r.get("mid"))
                    except Exception:
                        continue
                    sb = r.get("spread_bps")
                    try:
                        spread_bps = float(sb) if sb is not None else None
                    except Exception:
                        spread_bps = None
                    out.append(SpreadSnapshot(ts_ms=ts_ms, symbol=symbol, bid=bid, ask=ask, mid=mid, spread_bps=spread_bps))
        except Exception:
            out = []

        out.sort(key=lambda x: int(x.ts_ms))
        self._cache[key] = out
        return out

    def get_latest_before(self, *, symbol: str, ts_ms: int) -> Optional[SpreadSnapshot]:
        """Return the latest snapshot with snapshot.ts_ms <= ts_ms.

        We check the UTC day of ts_ms and the previous day (fills near midnight).
        """
        ymd = _utc_yyyymmdd_from_ts_ms(int(ts_ms))
        days = [ymd, _ymd_prev(ymd)]

        best: Optional[SpreadSnapshot] = None
        for d in days:
            xs = self._load_day_symbol(d, symbol)
            if not xs:
                continue
            idx = bisect.bisect_right([x.ts_ms for x in xs], int(ts_ms)) - 1
            if idx >= 0:
                cand = xs[idx]
                if best is None or cand.ts_ms > best.ts_ms:
                    best = cand
        return best
