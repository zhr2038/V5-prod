from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _utc_today_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def load_latest_cost_stats(stats_dir: str, max_age_days: int = 7) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Load newest daily_cost_stats_YYYYMMDD.json within max_age_days (UTC).

    Returns (stats_dict, stats_path) or (None, None).
    """
    d = Path(stats_dir)
    if not d.exists():
        return None, None
    files = sorted(d.glob("daily_cost_stats_*.json"))
    if not files:
        return None, None

    # newest by filename (YYYYMMDD)
    files.sort(key=lambda p: p.name)
    latest = files[-1]

    # age check by day tag
    try:
        tag = latest.stem.split("_")[-1]
        dt = datetime.strptime(tag, "%Y%m%d").replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - dt).days
        if age_days > int(max_age_days):
            return None, None
    except Exception:
        # if parse fails, allow load
        pass

    try:
        return json.loads(latest.read_text(encoding="utf-8")), str(latest)
    except Exception:
        return None, None


def _bucket_key(symbol: str, regime: str, router_action: str, notional_bucket: str) -> str:
    return f"{symbol}|{regime}|{router_action}|{notional_bucket}"


def _notional_bucket(x: float) -> str:
    x = float(x)
    if x < 25:
        return "lt25"
    if x < 50:
        return "25_50"
    if x < 100:
        return "50_100"
    if x < 250:
        return "100_250"
    return "ge250"


@dataclass
class FixedCostModel:
    """FixedCostModel类"""
    fee_bps: float = 6.0
    slippage_bps: float = 5.0

    def resolve(self, symbol: str, regime: str, router_action: str, notional_usdt: float):
        """Resolve"""
        return float(self.fee_bps), float(self.slippage_bps), {
            "mode": "default",
            "fallback_level": "DEFAULT",
            "bucket_key_used": "fixed",
        }


@dataclass
class CalibratedCostModel:
    """CalibratedCostModel类"""
    stats: Dict[str, Any]
    fee_quantile: str = "p75"
    slippage_quantile: str = "p90"
    min_fills_global: int = 30
    min_fills_bucket: int = 10
    default_fee_bps: float = 6.0
    default_slippage_bps: float = 5.0

    def _global_fills(self) -> int:
        cov = (self.stats.get("coverage") or {})
        try:
            return int(cov.get("fills") or 0)
        except Exception:
            return 0

    def _get_bucket(self, key: str) -> Optional[Dict[str, Any]]:
        b = (self.stats.get("buckets") or {}).get(key)
        if not b:
            return None
        try:
            if int(b.get("count") or 0) <= 0:
                return None
        except Exception:
            return None
        return b

    def _pick_bps(self, bucket: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        fee = None
        slp = None
        try:
            fee = (bucket.get("fee_bps") or {}).get(self.fee_quantile)
        except Exception:
            fee = None
        try:
            slp = (bucket.get("slippage_bps") or {}).get(self.slippage_quantile)
        except Exception:
            slp = None
        try:
            fee = None if fee is None else float(fee)
        except Exception:
            fee = None
        try:
            slp = None if slp is None else float(slp)
        except Exception:
            slp = None
        return fee, slp

    def resolve(self, symbol: str, regime: str, router_action: str, notional_usdt: float):
        """Resolve"""
        # global guard
        if self._global_fills() < int(self.min_fills_global):
            return float(self.default_fee_bps), float(self.default_slippage_bps), {
                "mode": "default",
                "reason": "min_fills_global",
                "fills_global": self._global_fills(),
                "fallback_level": "DEFAULT",
                "bucket_key_used": "DEFAULT",
                "source_day": self.stats.get("day"),
            }

        nb = _notional_bucket(float(notional_usdt))
        regime = str(regime or "ALL")
        router_action = str(router_action or "ALL")

        # fallback ladder
        levels = [
            (symbol, regime, router_action, nb, "L0_exact"),
            ("ALL", regime, router_action, nb, "L1_no_symbol"),
            ("ALL", "ALL", router_action, nb, "L2_no_symbol_regime"),
            ("ALL", "ALL", "ALL", "ALL", "L4_global"),
        ]

        for i, (sym2, reg2, act2, nb2, lvl) in enumerate(levels):
            key = _bucket_key(sym2, reg2, act2, nb2)
            b = self._get_bucket(key)
            if not b:
                continue
            try:
                if int(b.get("count") or 0) < int(self.min_fills_bucket) and i < (len(levels) - 1):
                    continue
            except Exception:
                pass

            fee, slp = self._pick_bps(b)
            if fee is None or slp is None:
                continue

            return fee, slp, {
                "mode": "calibrated",
                "fallback_level": lvl,
                "bucket_key_used": key,
                "fills_bucket": b.get("count"),
                "fee_quantile": self.fee_quantile,
                "slippage_quantile": self.slippage_quantile,
                "source_day": self.stats.get("day"),
            }

        # final fallback
        return float(self.default_fee_bps), float(self.default_slippage_bps), {
            "mode": "default",
            "reason": "no_bucket",
            "source_day": self.stats.get("day"),
            "fallback_level": "DEFAULT",
            "bucket_key_used": "DEFAULT",
        }
