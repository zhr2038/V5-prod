
import csv
import json
import logging
import sqlite3
import time
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Iterable, Set

from src.execution.fill_store import derive_fill_store_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
logger = logging.getLogger(__name__)
RELEASE_START_NOT_OBSERVABLE = "not_observable"


def negative_expectancy_adjusted_block_audit(
    stat: Dict[str, Any] | None,
    *,
    net_floor_bps: float,
    fast_fail_floor_bps: float,
) -> Dict[str, Any]:
    stat = stat or {}

    def _float_value(key: str, default: float = 0.0) -> float:
        try:
            return float(stat.get(key) if stat.get(key) is not None else default)
        except Exception:
            return float(default)

    def _int_value(key: str) -> int:
        try:
            return int(float(stat.get(key) or 0))
        except Exception:
            return 0

    raw_expectancy_bps = _float_value("net_expectancy_bps", _float_value("expectancy_bps", 0.0))
    adjusted_expectancy_bps = _float_value("adjusted_entry_expectancy_bps", raw_expectancy_bps)
    fast_fail_expectancy_bps = _float_value(
        "fast_fail_net_expectancy_bps",
        _float_value("fast_fail_expectancy_bps", 0.0),
    )
    raw_net_breach = raw_expectancy_bps <= float(net_floor_bps)
    adjusted_net_breach = adjusted_expectancy_bps <= float(net_floor_bps)
    fast_fail_breach = fast_fail_expectancy_bps <= float(fast_fail_floor_bps)
    would_unblock_if_adjusted = bool(raw_net_breach and not adjusted_net_breach)
    attribution_conflict = bool(
        would_unblock_if_adjusted
        and (
            _int_value("exit_bad_cycles") > 0
            or _int_value("min_hold_violation_cycles") > 0
            or _int_value("gave_back_profit_cycles") > 0
            or _int_value("trailing_too_early_cycles") > 0
        )
    )
    return {
        "entry_bad_cycles": _int_value("entry_bad_cycles"),
        "exit_bad_cycles": _int_value("exit_bad_cycles"),
        "min_hold_violation_cycles": _int_value("min_hold_violation_cycles"),
        "gave_back_profit_cycles": _int_value("gave_back_profit_cycles"),
        "trailing_too_early_cycles": _int_value("trailing_too_early_cycles"),
        "adjusted_entry_expectancy_bps": float(adjusted_expectancy_bps),
        "raw_would_block": bool(raw_net_breach or fast_fail_breach),
        "adjusted_would_block": bool(adjusted_net_breach or fast_fail_breach),
        "would_unblock_if_adjusted": would_unblock_if_adjusted,
        "block_attribution_conflict": attribution_conflict,
    }


def negative_expectancy_config_fingerprint(config: Any) -> str:
    execution_cfg = getattr(config, "execution", None)
    payload = {
        "symbols": [str(sym) for sym in (getattr(config, "symbols", None) or []) if str(sym).strip()],
        "universe": {
            "enabled": bool(getattr(getattr(config, "universe", None), "enabled", False)),
            "use_universe_symbols": bool(
                getattr(getattr(config, "universe", None), "use_universe_symbols", False)
            ),
        },
        "alpha": {
            "alpha158_overlay": {
                "enabled": bool(
                    getattr(
                        getattr(getattr(config, "alpha", None), "alpha158_overlay", None),
                        "enabled",
                        False,
                    )
                ),
            },
        },
        "execution": {
            "prefer_net_from_fills": bool(getattr(execution_cfg, "prefer_net_from_fills", True)),
            "negative_expectancy_cooldown_enabled": bool(
                getattr(execution_cfg, "negative_expectancy_cooldown_enabled", False)
            ),
            "negative_expectancy_lookback_hours": int(
                getattr(execution_cfg, "negative_expectancy_lookback_hours", 24) or 24
            ),
            "negative_expectancy_min_closed_cycles": int(
                getattr(execution_cfg, "negative_expectancy_min_closed_cycles", 4) or 4
            ),
            "negative_expectancy_threshold_bps": getattr(execution_cfg, "negative_expectancy_threshold_bps", None),
            "negative_expectancy_threshold_usdt": float(
                getattr(execution_cfg, "negative_expectancy_threshold_usdt", 0.0) or 0.0
            ),
            "negative_expectancy_cooldown_hours": int(
                getattr(execution_cfg, "negative_expectancy_cooldown_hours", 24) or 24
            ),
            "negative_expectancy_score_penalty_enabled": bool(
                getattr(execution_cfg, "negative_expectancy_score_penalty_enabled", False)
            ),
            "negative_expectancy_score_penalty_min_closed_cycles": int(
                getattr(execution_cfg, "negative_expectancy_score_penalty_min_closed_cycles", 2) or 2
            ),
            "negative_expectancy_score_penalty_floor_bps": float(
                getattr(execution_cfg, "negative_expectancy_score_penalty_floor_bps", 5.0) or 0.0
            ),
            "negative_expectancy_score_penalty_per_bps": float(
                getattr(execution_cfg, "negative_expectancy_score_penalty_per_bps", 0.015) or 0.0
            ),
            "negative_expectancy_score_penalty_max": float(
                getattr(execution_cfg, "negative_expectancy_score_penalty_max", 0.60) or 0.0
            ),
            "negative_expectancy_open_block_enabled": bool(
                getattr(execution_cfg, "negative_expectancy_open_block_enabled", False)
            ),
            "negative_expectancy_open_block_min_closed_cycles": int(
                getattr(execution_cfg, "negative_expectancy_open_block_min_closed_cycles", 2) or 2
            ),
            "negative_expectancy_open_block_floor_bps": float(
                getattr(execution_cfg, "negative_expectancy_open_block_floor_bps", 5.0) or 0.0
            ),
            "negative_expectancy_fast_fail_max_hold_minutes": int(
                getattr(execution_cfg, "negative_expectancy_fast_fail_max_hold_minutes", 120) or 120
            ),
            "negative_expectancy_fast_fail_open_block_enabled": bool(
                getattr(execution_cfg, "negative_expectancy_fast_fail_open_block_enabled", False)
            ),
            "negative_expectancy_fast_fail_open_block_min_closed_cycles": int(
                getattr(execution_cfg, "negative_expectancy_fast_fail_open_block_min_closed_cycles", 2) or 2
            ),
            "negative_expectancy_fast_fail_open_block_floor_bps": float(
                getattr(execution_cfg, "negative_expectancy_fast_fail_open_block_floor_bps", 0.0) or 0.0
            ),
            "protect_negative_expectancy_short_cycle_guard_enabled": bool(
                getattr(execution_cfg, "protect_negative_expectancy_short_cycle_guard_enabled", False)
            ),
            "protect_negative_expectancy_short_cycle_min_cycles": int(
                getattr(execution_cfg, "protect_negative_expectancy_short_cycle_min_cycles", 2) or 2
            ),
            "protect_negative_expectancy_short_cycle_floor_bps": float(
                getattr(execution_cfg, "protect_negative_expectancy_short_cycle_floor_bps", -80.0) or 0.0
            ),
            "protect_negative_expectancy_short_cycle_apply_to_normal_entry": bool(
                getattr(execution_cfg, "protect_negative_expectancy_short_cycle_apply_to_normal_entry", True)
            ),
            "protect_negative_expectancy_short_cycle_apply_to_probe": bool(
                getattr(execution_cfg, "protect_negative_expectancy_short_cycle_apply_to_probe", False)
            ),
            "protect_alt_short_cycle_guard_enabled": bool(
                getattr(execution_cfg, "protect_alt_short_cycle_guard_enabled", False)
            ),
            "protect_alt_short_cycle_symbols": [
                str(sym)
                for sym in (getattr(execution_cfg, "protect_alt_short_cycle_symbols", []) or [])
                if str(sym).strip()
            ],
            "protect_alt_short_cycle_min_cycles": int(
                getattr(execution_cfg, "protect_alt_short_cycle_min_cycles", 2) or 2
            ),
            "protect_alt_short_cycle_net_floor_bps": float(
                getattr(execution_cfg, "protect_alt_short_cycle_net_floor_bps", -20.0) or 0.0
            ),
            "protect_alt_short_cycle_fast_fail_floor_bps": float(
                getattr(execution_cfg, "protect_alt_short_cycle_fast_fail_floor_bps", -30.0) or 0.0
            ),
            "protect_alt_short_cycle_apply_to_normal_entry": bool(
                getattr(execution_cfg, "protect_alt_short_cycle_apply_to_normal_entry", True)
            ),
            "protect_alt_short_cycle_apply_to_probe": bool(
                getattr(execution_cfg, "protect_alt_short_cycle_apply_to_probe", False)
            ),
        },
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


@dataclass
class NegativeExpectancyConfig:
    enabled: bool = False
    lookback_hours: int = 24
    min_closed_cycles: int = 4
    expectancy_threshold_bps: Optional[float] = None
    expectancy_threshold_usdt: float = 0.0
    cooldown_hours: int = 24
    state_path: str = "reports/negative_expectancy_cooldown.json"
    orders_db_path: str = "reports/orders.sqlite"
    fills_db_path: str = "reports/fills.sqlite"
    prefer_net_from_fills: bool = True
    fast_fail_max_hold_minutes: int = 120
    whitelist_symbols: list[str] = field(default_factory=list)
    open_position_symbols: list[str] = field(default_factory=list)
    managed_symbols: list[str] = field(default_factory=list)
    config_fingerprint: str = ""


class NegativeExpectancyCooldown:
    """基于最近成交闭环的负期望标的冷却器（FIFO 近似）。"""

    def __init__(self, cfg: NegativeExpectancyConfig):
        self.cfg = cfg
        self.cfg.state_path = str(self._resolve_path(self.cfg.state_path))
        self.cfg.orders_db_path = str(self._resolve_path(self.cfg.orders_db_path))
        raw_fills_path = str(getattr(self.cfg, "fills_db_path", "") or "").strip()
        if not raw_fills_path or raw_fills_path == "reports/fills.sqlite":
            self.cfg.fills_db_path = str(derive_fill_store_path(self.cfg.orders_db_path).resolve())
        else:
            self.cfg.fills_db_path = str(self._resolve_path(raw_fills_path))
        self._last_refresh_ms = 0
        self._cache: Dict[str, Any] = self._load_state()
        self._scope_symbols: Optional[Set[str]] = None
        self._scope_metadata: Dict[str, Any] = {}

    @staticmethod
    def _resolve_path(path: str | Path) -> Path:
        resolved = Path(path)
        if not resolved.is_absolute():
            resolved = (PROJECT_ROOT / resolved).resolve()
        return resolved

    @staticmethod
    def _norm_symbol(inst_id: str) -> str:
        s = str(inst_id or "")
        return s.replace("-", "/") if "-" in s else s

    def _load_state(self) -> Dict[str, Any]:
        p = Path(self.cfg.state_path)
        if not p.exists():
            return {"symbols": {}, "updated_ts_ms": 0}
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                if "symbols" not in obj:
                    obj["symbols"] = {}
                return obj
        except Exception:
            pass
        return {"symbols": {}, "updated_ts_ms": 0}

    @staticmethod
    def _coerce_release_start_ts(raw: Any) -> Optional[int]:
        if raw is None or raw == "" or str(raw).strip() == RELEASE_START_NOT_OBSERVABLE:
            return None
        try:
            value = int(float(raw))
        except Exception:
            return None
        return value if value > 0 else None

    @staticmethod
    def _release_warning(code: str, detail: str = "") -> str:
        return f"{code}: {detail}" if detail else code

    def _save_state(self) -> None:
        p = Path(self.cfg.state_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(self._cache, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)

    def _attribution_path(self) -> Path:
        return Path(self.cfg.state_path).with_name("negative_expectancy_attribution.json")

    @staticmethod
    def _attribution_counts_from_stat(stat: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "entry_bad_cycles": int(stat.get("entry_bad_cycles") or 0),
            "exit_bad_cycles": int(stat.get("exit_bad_cycles") or 0),
            "min_hold_violation_cycles": int(stat.get("min_hold_violation_cycles") or 0),
            "gave_back_profit_cycles": int(stat.get("gave_back_profit_cycles") or 0),
            "trailing_too_early_cycles": int(stat.get("trailing_too_early_cycles") or 0),
            "unknown_attribution_cycles": int(stat.get("unknown_attribution_cycles") or 0),
            "adjusted_entry_cycles": int(stat.get("adjusted_entry_cycles") or 0),
            "adjusted_entry_expectancy_bps": float(stat.get("adjusted_entry_expectancy_bps") or 0.0),
        }

    def _save_attribution_report(self, *, now_ms: int, stats_cache: Dict[str, Dict[str, Any]]) -> None:
        symbols: Dict[str, Any] = {}
        for symbol, stat in sorted(stats_cache.items()):
            if not isinstance(stat, dict):
                continue
            cycle_attributions = stat.get("cycle_attributions")
            symbols[symbol] = {
                **self._attribution_counts_from_stat(stat),
                "closed_cycles": int(stat.get("closed_cycles") or 0),
                "net_expectancy_bps": float(stat.get("net_expectancy_bps") or 0.0),
                "cycle_attributions": list(cycle_attributions) if isinstance(cycle_attributions, list) else [],
            }
        payload = {
            "schema_version": "v5.negative_expectancy_attribution.v1",
            "diagnostic_only": True,
            "live_order_effect": "none",
            "updated_ts_ms": int(now_ms),
            "symbols": symbols,
        }
        path = self._attribution_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def set_scope(
        self,
        *,
        whitelist_symbols: Optional[Iterable[str]] = None,
        open_position_symbols: Optional[Iterable[str]] = None,
        managed_symbols: Optional[Iterable[str]] = None,
        config_fingerprint: Optional[str] = None,
    ) -> None:
        if whitelist_symbols is not None:
            self.cfg.whitelist_symbols = self._normalize_symbols(whitelist_symbols)
        if open_position_symbols is not None:
            self.cfg.open_position_symbols = self._normalize_symbols(open_position_symbols)
        if managed_symbols is not None:
            self.cfg.managed_symbols = self._normalize_symbols(managed_symbols)
        if config_fingerprint is not None:
            self.cfg.config_fingerprint = str(config_fingerprint or "").strip()
        self._refresh_scope_metadata()

    @staticmethod
    def _normalize_symbols(symbols: Iterable[str] | None) -> list[str]:
        seen = set()
        out: list[str] = []
        for sym in symbols or []:
            norm = NegativeExpectancyCooldown._norm_symbol(str(sym or "").strip())
            if not norm or norm in seen:
                continue
            seen.add(norm)
            out.append(norm)
        return out

    def _refresh_scope_metadata(self) -> None:
        whitelist_symbols = self._normalize_symbols(getattr(self.cfg, "whitelist_symbols", []))
        open_position_symbols = self._normalize_symbols(getattr(self.cfg, "open_position_symbols", []))
        managed_symbols = self._normalize_symbols(getattr(self.cfg, "managed_symbols", []))
        scope = sorted({*whitelist_symbols, *open_position_symbols, *managed_symbols})
        self._scope_symbols = set(scope) if scope else None
        self._scope_metadata = {
            "whitelist_symbols": whitelist_symbols,
            "open_position_symbols": open_position_symbols,
            "managed_symbols": managed_symbols,
            "scope_symbols": scope,
            "config_fingerprint": str(getattr(self.cfg, "config_fingerprint", "") or "").strip(),
        }

    @staticmethod
    def _fee_to_usdt_cost(*, fee: Any, fee_ccy: Any, inst_id: str, fill_px: Any) -> float:
        try:
            fee_val = float(fee or 0.0)
        except Exception:
            return 0.0
        fee_ccy_norm = str(fee_ccy or "").strip().upper()
        if not fee_ccy_norm or abs(fee_val) <= 1e-12:
            return 0.0

        base_ccy = str(inst_id).split("-")[0].upper() if "-" in str(inst_id) else ""
        quote_ccy = str(inst_id).split("-")[1].upper() if "-" in str(inst_id) else ""
        if fee_ccy_norm == quote_ccy:
            signed_fee_usdt = fee_val
        elif fee_ccy_norm == base_ccy:
            try:
                signed_fee_usdt = fee_val * float(fill_px or 0.0)
            except Exception:
                return 0.0
        else:
            return 0.0

        return float(0.0 - signed_fee_usdt)

    @staticmethod
    def _orders_fee_to_usdt_best_effort(*, fee: Any) -> float:
        try:
            fee_val = float(fee or 0.0)
        except Exception:
            return 0.0
        return float(0.0 - fee_val)

    @staticmethod
    def _empty_expectancy_accumulator() -> Dict[str, Any]:
        return {
            "gross_pnl_sum_usdt": 0.0,
            "net_pnl_sum_usdt": 0.0,
            "closed_cycles": 0.0,
            "closed_notional_usdt": 0.0,
            "last_close_ts_ms": 0.0,
            "fast_fail_gross_pnl_sum_usdt": 0.0,
            "fast_fail_net_pnl_sum_usdt": 0.0,
            "fast_fail_closed_cycles": 0.0,
            "fast_fail_closed_notional_usdt": 0.0,
            "fast_fail_hold_minutes_sum": 0.0,
            "closed_cycles_included_by_close_ts": 0.0,
            "closed_cycles_with_entry_before_window": 0.0,
            "missing_entry_leg_count": 0.0,
            "premature_soft_exit_count": 0.0,
            "premature_soft_exit_net_bps_sum": 0.0,
            "premature_soft_exit_net_pnl_sum_usdt": 0.0,
            "premature_soft_exit_closed_notional_usdt": 0.0,
            "excluded_from_fast_fail_count": 0.0,
            "entry_bad_cycles": 0.0,
            "exit_bad_cycles": 0.0,
            "min_hold_violation_cycles": 0.0,
            "gave_back_profit_cycles": 0.0,
            "trailing_too_early_cycles": 0.0,
            "unknown_attribution_cycles": 0.0,
            "adjusted_entry_cycles": 0.0,
            "adjusted_entry_net_pnl_sum_usdt": 0.0,
            "adjusted_entry_closed_notional_usdt": 0.0,
            "cycle_attributions": [],
        }

    @staticmethod
    def _truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _falsey_or_missing(value: Any) -> bool:
        if value is None or str(value).strip() == "":
            return True
        if isinstance(value, bool):
            return not value
        return str(value).strip().lower() in {"0", "false", "no", "off"}

    @staticmethod
    def _json_obj(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        try:
            parsed = json.loads(str(value or "{}"))
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}

    @classmethod
    def _order_meta_from_req_json(cls, value: Any) -> Dict[str, Any]:
        req = cls._json_obj(value)
        meta = req.get("_v5_order_meta")
        return dict(meta) if isinstance(meta, dict) else {}

    @staticmethod
    def _missing_value(value: Any) -> bool:
        if value is None:
            return True
        text = str(value).strip()
        return text == "" or text.lower() in {"none", "null", "not_observable"}

    @classmethod
    def _expanded_exit_metadata(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row or {})

        def merge_missing(mapping: Dict[str, Any]) -> None:
            for key, value in mapping.items():
                if cls._missing_value(out.get(key)):
                    out[key] = value

        for key in ("raw_meta", "raw_json", "req_json"):
            obj = cls._json_obj(out.get(key))
            if not obj:
                continue
            meta = obj.get("_v5_order_meta")
            if isinstance(meta, dict):
                merge_missing(dict(meta))
            merge_missing(obj)
        return out

    @classmethod
    def _diagnostic_text(cls, row: Dict[str, Any]) -> str:
        parts: list[str] = []
        for key in (
            "diagnosis",
            "issue_code",
            "issue",
            "router_reason",
            "reason",
            "min_hold_block_reason",
            "blocked_exit_reason",
            "raw_json",
            "raw_meta",
        ):
            value = row.get(key)
            if value is not None:
                parts.append(str(value))
        return " ".join(parts).lower()

    @classmethod
    def _is_premature_swing_soft_exit(cls, row: Dict[str, Any]) -> bool:
        row = cls._expanded_exit_metadata(row)
        if not cls._truthy(row.get("swing_hold_position")):
            return False
        reason = str(row.get("exit_reason") or row.get("reason") or row.get("source_reason") or "").strip().lower()
        soft_reasons = {"atr_trailing", "zero_target_close", "rank_exit", "regime_exit"}
        if reason not in soft_reasons and not any(reason.startswith(f"{prefix}_") for prefix in ("rank_exit", "regime_exit")):
            return False
        hold_hours = cls._coerce_float(row.get("hold_hours") or row.get("held_hours"))
        if hold_hours is None:
            hold_minutes = cls._coerce_float(row.get("hold_minutes") or row.get("held_minutes"))
            if hold_minutes is not None:
                hold_hours = float(hold_minutes) / 60.0
        min_hold_hours = cls._coerce_float(
            row.get("swing_min_hold_hours")
            or row.get("min_hold_hours")
            or row.get("required_hold_hours")
        )
        if hold_hours is None or min_hold_hours is None or not float(hold_hours) < float(min_hold_hours):
            return False
        priority = str(row.get("exit_priority") or "").strip().lower()
        if priority and priority != "soft":
            return False
        if not priority and reason not in soft_reasons and not reason.startswith(("rank_exit", "regime_exit")):
            return False
        if not cls._falsey_or_missing(row.get("exit_blocked_by_min_hold")):
            return False
        diagnostic = cls._diagnostic_text(row)
        markers = (
            "soft_exit_violated_swing_min_hold",
            "swing_soft_exit_before_min_hold_filled",
            "post_fix_soft_exit_before_min_hold",
        )
        if any(marker in diagnostic for marker in markers):
            return True
        return cls._truthy(row.get("exited_before_min_hold")) and (
            priority == "soft"
            or reason in soft_reasons
            or reason.startswith(("rank_exit", "regime_exit"))
        )

    @classmethod
    def _record_premature_soft_exit(
        cls,
        st: Dict[str, float],
        *,
        net_pnl: float,
        notional: float,
        net_bps: Optional[float],
        excluded_from_fast_fail: bool,
    ) -> None:
        observed_net_bps = float(net_bps) if net_bps is not None else (
            float(net_pnl) / float(notional) * 10000.0 if float(notional) > 1e-12 else 0.0
        )
        st["premature_soft_exit_count"] += 1.0
        st["premature_soft_exit_net_bps_sum"] += float(observed_net_bps)
        st["premature_soft_exit_net_pnl_sum_usdt"] += float(net_pnl)
        st["premature_soft_exit_closed_notional_usdt"] += float(notional)
        if excluded_from_fast_fail:
            st["excluded_from_fast_fail_count"] += 1.0

    @classmethod
    def _cycle_attribution(cls, row: Dict[str, Any], *, net_bps: Optional[float]) -> list[str]:
        ctx = cls._expanded_exit_metadata(row)
        reason = str(ctx.get("exit_reason") or ctx.get("reason") or ctx.get("source_reason") or "").strip().lower()
        attrs: list[str] = []
        if cls._is_premature_swing_soft_exit(ctx):
            attrs.extend(["exit_bad", "min_hold_violation"])
            if reason == "atr_trailing":
                attrs.append("trailing_too_early")
        max_unrealized = cls._coerce_float(
            ctx.get("max_unrealized_bps")
            or ctx.get("highest_net_bps")
            or ctx.get("highest_unrealized_net_bps")
        )
        if max_unrealized is not None and net_bps is not None:
            if float(max_unrealized) >= 30.0 and float(max_unrealized) - float(net_bps) >= 50.0:
                attrs.extend(["exit_bad", "gave_back_profit"])
        if net_bps is not None and float(net_bps) < 0.0 and "exit_bad" not in attrs:
            attrs.append("entry_bad")
        if not attrs:
            attrs.append("unknown")
        return list(dict.fromkeys(attrs))

    @classmethod
    def _record_cycle_attribution(
        cls,
        st: Dict[str, Any],
        *,
        row: Dict[str, Any],
        net_pnl: float,
        notional: float,
        net_bps: Optional[float],
    ) -> list[str]:
        attrs = cls._cycle_attribution(row, net_bps=net_bps)
        for attr, key in (
            ("entry_bad", "entry_bad_cycles"),
            ("exit_bad", "exit_bad_cycles"),
            ("min_hold_violation", "min_hold_violation_cycles"),
            ("gave_back_profit", "gave_back_profit_cycles"),
            ("trailing_too_early", "trailing_too_early_cycles"),
            ("unknown", "unknown_attribution_cycles"),
        ):
            if attr in attrs:
                st[key] = float(st.get(key) or 0.0) + 1.0

        exit_attributed = bool(
            {"exit_bad", "min_hold_violation", "gave_back_profit", "trailing_too_early"}.intersection(attrs)
        )
        if not exit_attributed and net_bps is not None:
            st["adjusted_entry_cycles"] = float(st.get("adjusted_entry_cycles") or 0.0) + 1.0
            st["adjusted_entry_net_pnl_sum_usdt"] = float(st.get("adjusted_entry_net_pnl_sum_usdt") or 0.0) + float(net_pnl)
            st["adjusted_entry_closed_notional_usdt"] = (
                float(st.get("adjusted_entry_closed_notional_usdt") or 0.0) + float(notional)
            )

        cycles = st.setdefault("cycle_attributions", [])
        if isinstance(cycles, list) and len(cycles) < 50:
            expanded = cls._expanded_exit_metadata(row)
            cycles.append(
                {
                    "entry_ts": str(
                        expanded.get("entry_ts")
                        or expanded.get("open_time_utc")
                        or expanded.get("open_ts")
                        or ""
                    ),
                    "exit_ts": str(
                        expanded.get("exit_ts")
                        or expanded.get("close_time_utc")
                        or expanded.get("ts")
                        or expanded.get("timestamp")
                        or ""
                    ),
                    "exit_reason": str(expanded.get("exit_reason") or expanded.get("reason") or ""),
                    "exit_priority": str(expanded.get("exit_priority") or ""),
                    "net_bps": float(net_bps) if net_bps is not None else None,
                    "attribution": attrs,
                }
            )
        return attrs

    @classmethod
    def _record_fast_fail_or_premature_soft_exit(
        cls,
        st: Dict[str, Any],
        *,
        row: Dict[str, Any],
        gross_pnl: float,
        net_pnl: float,
        notional: float,
        hold_ms: float,
        fast_fail_hold_ms: int,
        net_bps: Optional[float] = None,
    ) -> None:
        ctx = cls._expanded_exit_metadata(row)
        ctx.setdefault("hold_minutes", float(hold_ms) / 60000.0)
        ctx.setdefault("hold_hours", float(hold_ms) / 3600000.0)
        if net_bps is not None:
            ctx.setdefault("net_bps", float(net_bps))

        cls._record_cycle_attribution(
            st,
            row=ctx,
            net_pnl=float(net_pnl),
            notional=float(notional),
            net_bps=net_bps,
        )

        fast_fail_candidate = bool(fast_fail_hold_ms > 0 and float(hold_ms) <= float(fast_fail_hold_ms))
        premature_soft_exit = cls._is_premature_swing_soft_exit(ctx)
        if premature_soft_exit:
            cls._record_premature_soft_exit(
                st,
                net_pnl=float(net_pnl),
                notional=float(notional),
                net_bps=net_bps,
                excluded_from_fast_fail=fast_fail_candidate,
            )
            if fast_fail_candidate:
                return

        if fast_fail_candidate:
            st["fast_fail_closed_cycles"] += 1.0
            st["fast_fail_gross_pnl_sum_usdt"] += float(gross_pnl)
            st["fast_fail_net_pnl_sum_usdt"] += float(net_pnl)
            st["fast_fail_closed_notional_usdt"] += float(notional)
            st["fast_fail_hold_minutes_sum"] += float(hold_ms / 60000.0)

    def _load_order_meta_by_id(self) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        p = Path(self.cfg.orders_db_path)
        if not p.exists():
            return {}, {}
        by_clid: Dict[str, Dict[str, Any]] = {}
        by_oid: Dict[str, Dict[str, Any]] = {}
        conn = None
        try:
            conn = sqlite3.connect(str(p))
            conn.row_factory = sqlite3.Row
            col_rows = conn.execute("PRAGMA table_info(orders)").fetchall()
            cols = {str(r["name"]) for r in col_rows}
            if "req_json" not in cols:
                return {}, {}
            id_cols = [col for col in ("cl_ord_id", "ord_id") if col in cols]
            if not id_cols:
                return {}, {}
            rows = conn.execute(
                f"SELECT {', '.join(id_cols)}, req_json FROM orders WHERE req_json IS NOT NULL"
            ).fetchall()
            for row in rows:
                meta = self._order_meta_from_req_json(row["req_json"])
                if not meta:
                    continue
                if "cl_ord_id" in row.keys():
                    clid = str(row["cl_ord_id"] or "").strip()
                    if clid:
                        by_clid[clid] = dict(meta)
                if "ord_id" in row.keys():
                    oid = str(row["ord_id"] or "").strip()
                    if oid:
                        by_oid[oid] = dict(meta)
        except Exception:
            return by_clid, by_oid
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return by_clid, by_oid

    @staticmethod
    def _build_expectancy_row(
        *,
        gross_pnl_sum_usdt: float,
        net_pnl_sum_usdt: float,
        closed_cycles: float,
        closed_notional_usdt: float,
        last_close_ts_ms: float,
        fast_fail_gross_pnl_sum_usdt: float,
        fast_fail_net_pnl_sum_usdt: float,
        fast_fail_closed_cycles: float,
        fast_fail_closed_notional_usdt: float,
        fast_fail_hold_minutes_sum: float,
        source: str,
        degraded_fee_model: bool = False,
        degraded_reason: str = "",
        closed_cycles_included_by_close_ts: float = 0.0,
        closed_cycles_with_entry_before_window: float = 0.0,
        missing_entry_leg_count: float = 0.0,
        premature_soft_exit_count: float = 0.0,
        premature_soft_exit_net_bps_sum: float = 0.0,
        premature_soft_exit_net_pnl_sum_usdt: float = 0.0,
        premature_soft_exit_closed_notional_usdt: float = 0.0,
        excluded_from_fast_fail_count: float = 0.0,
        entry_bad_cycles: float = 0.0,
        exit_bad_cycles: float = 0.0,
        min_hold_violation_cycles: float = 0.0,
        gave_back_profit_cycles: float = 0.0,
        trailing_too_early_cycles: float = 0.0,
        unknown_attribution_cycles: float = 0.0,
        adjusted_entry_cycles: float = 0.0,
        adjusted_entry_net_pnl_sum_usdt: float = 0.0,
        adjusted_entry_closed_notional_usdt: float = 0.0,
        cycle_attributions: Optional[list[Dict[str, Any]]] = None,
        lookback_filter_mode: str = "close_ts",
    ) -> Dict[str, Any]:
        n = int(closed_cycles or 0)
        ff_n = int(fast_fail_closed_cycles or 0)
        missing_n = int(missing_entry_leg_count or 0)
        last_close_ms = int(float(last_close_ts_ms or 0.0))
        degraded_reasons = [str(degraded_reason or "").strip()] if str(degraded_reason or "").strip() else []
        if missing_n > 0:
            degraded_reasons.append("missing_entry_leg_for_close_cycle")
        degraded_reason_out = "; ".join(dict.fromkeys(degraded_reasons))
        gross_expectancy_usdt = float(gross_pnl_sum_usdt) / n if n > 0 else 0.0
        net_expectancy_usdt = float(net_pnl_sum_usdt) / n if n > 0 else 0.0
        gross_expectancy_bps = (
            float(gross_pnl_sum_usdt) / float(closed_notional_usdt) * 10000.0
            if float(closed_notional_usdt) > 1e-12
            else 0.0
        )
        net_expectancy_bps = (
            float(net_pnl_sum_usdt) / float(closed_notional_usdt) * 10000.0
            if float(closed_notional_usdt) > 1e-12
            else 0.0
        )
        fast_fail_gross_expectancy_usdt = float(fast_fail_gross_pnl_sum_usdt) / ff_n if ff_n > 0 else 0.0
        fast_fail_net_expectancy_usdt = float(fast_fail_net_pnl_sum_usdt) / ff_n if ff_n > 0 else 0.0
        fast_fail_gross_expectancy_bps = (
            float(fast_fail_gross_pnl_sum_usdt) / float(fast_fail_closed_notional_usdt) * 10000.0
            if float(fast_fail_closed_notional_usdt) > 1e-12
            else 0.0
        )
        fast_fail_net_expectancy_bps = (
            float(fast_fail_net_pnl_sum_usdt) / float(fast_fail_closed_notional_usdt) * 10000.0
            if float(fast_fail_closed_notional_usdt) > 1e-12
            else 0.0
        )
        adjusted_entry_expectancy_bps = (
            float(adjusted_entry_net_pnl_sum_usdt) / float(adjusted_entry_closed_notional_usdt) * 10000.0
            if float(adjusted_entry_closed_notional_usdt) > 1e-12
            else 0.0
        )
        ff_hold_minutes_avg = float(fast_fail_hold_minutes_sum) / ff_n if ff_n > 0 else 0.0
        return {
            "source": source,
            "degraded_fee_model": bool(degraded_fee_model),
            "degraded": bool(degraded_fee_model or degraded_reason_out),
            "degraded_reason": degraded_reason_out,
            "lookback_filter_mode": str(lookback_filter_mode or "close_ts"),
            "closed_cycles_included_by_close_ts": int(closed_cycles_included_by_close_ts or n),
            "closed_cycles_with_entry_before_window": int(closed_cycles_with_entry_before_window or 0),
            "missing_entry_leg_count": missing_n,
            "closed_cycles": n,
            "closed_notional_usdt": float(closed_notional_usdt),
            "last_close_ts_ms": last_close_ms if last_close_ms > 0 else None,
            "last_close_ts": NegativeExpectancyCooldown._ms_to_iso(last_close_ms) if last_close_ms > 0 else None,
            "gross_pnl_sum_usdt": float(gross_pnl_sum_usdt),
            "net_pnl_sum_usdt": float(net_pnl_sum_usdt),
            "gross_expectancy_usdt": float(gross_expectancy_usdt),
            "net_expectancy_usdt": float(net_expectancy_usdt),
            "gross_expectancy_bps": float(gross_expectancy_bps),
            "net_expectancy_bps": float(net_expectancy_bps),
            # legacy compatibility
            "pnl_sum_usdt": float(gross_pnl_sum_usdt),
            "expectancy_usdt": float(gross_expectancy_usdt),
            "expectancy_bps": float(gross_expectancy_bps),
            "fast_fail_closed_cycles": ff_n,
            "fast_fail_closed_notional_usdt": float(fast_fail_closed_notional_usdt),
            "fast_fail_gross_pnl_sum_usdt": float(fast_fail_gross_pnl_sum_usdt),
            "fast_fail_net_pnl_sum_usdt": float(fast_fail_net_pnl_sum_usdt),
            "fast_fail_gross_expectancy_usdt": float(fast_fail_gross_expectancy_usdt),
            "fast_fail_net_expectancy_usdt": float(fast_fail_net_expectancy_usdt),
            "fast_fail_gross_expectancy_bps": float(fast_fail_gross_expectancy_bps),
            "fast_fail_net_expectancy_bps": float(fast_fail_net_expectancy_bps),
            "adjusted_fast_fail_net_expectancy_bps": float(fast_fail_net_expectancy_bps),
            "premature_soft_exit_count": int(premature_soft_exit_count or 0),
            "premature_soft_exit_net_bps_sum": float(premature_soft_exit_net_bps_sum or 0.0),
            "premature_soft_exit_net_pnl_sum_usdt": float(premature_soft_exit_net_pnl_sum_usdt or 0.0),
            "premature_soft_exit_closed_notional_usdt": float(premature_soft_exit_closed_notional_usdt or 0.0),
            "excluded_from_fast_fail_count": int(excluded_from_fast_fail_count or 0),
            "entry_bad_cycles": int(entry_bad_cycles or 0),
            "exit_bad_cycles": int(exit_bad_cycles or 0),
            "min_hold_violation_cycles": int(min_hold_violation_cycles or 0),
            "gave_back_profit_cycles": int(gave_back_profit_cycles or 0),
            "trailing_too_early_cycles": int(trailing_too_early_cycles or 0),
            "unknown_attribution_cycles": int(unknown_attribution_cycles or 0),
            "adjusted_entry_cycles": int(adjusted_entry_cycles or 0),
            "adjusted_entry_net_pnl_sum_usdt": float(adjusted_entry_net_pnl_sum_usdt or 0.0),
            "adjusted_entry_closed_notional_usdt": float(adjusted_entry_closed_notional_usdt or 0.0),
            "adjusted_entry_expectancy_bps": float(adjusted_entry_expectancy_bps),
            "cycle_attributions": list(cycle_attributions or []),
            # legacy compatibility
            "fast_fail_pnl_sum_usdt": float(fast_fail_gross_pnl_sum_usdt),
            "fast_fail_expectancy_usdt": float(fast_fail_gross_expectancy_usdt),
            "fast_fail_expectancy_bps": float(fast_fail_gross_expectancy_bps),
            "fast_fail_avg_hold_minutes": float(ff_hold_minutes_avg),
        }

    @staticmethod
    def _ms_to_iso(ts_ms: int | float) -> str:
        try:
            value = int(float(ts_ms))
        except Exception:
            return ""
        if value <= 0:
            return ""
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _closed_cycle_count(row: Optional[Dict[str, Any]]) -> int:
        try:
            return int(float((row or {}).get("closed_cycles") or 0))
        except Exception:
            return 0

    @staticmethod
    def _int_stat(row: Optional[Dict[str, Any]], key: str) -> int:
        try:
            return int(float((row or {}).get(key) or 0))
        except Exception:
            return 0

    def _overlay_premature_soft_exit_diagnostics(
        self,
        existing: Dict[str, Any],
        diagnostic: Dict[str, Any],
    ) -> Dict[str, Any]:
        if self._int_stat(diagnostic, "premature_soft_exit_count") <= self._int_stat(existing, "premature_soft_exit_count"):
            return existing
        existing_cycles = self._closed_cycle_count(existing)
        diagnostic_cycles = self._closed_cycle_count(diagnostic)
        existing_fast_fail = self._int_stat(existing, "fast_fail_closed_cycles")
        diagnostic_fast_fail = self._int_stat(diagnostic, "fast_fail_closed_cycles")
        diagnostic_excluded = self._int_stat(diagnostic, "excluded_from_fast_fail_count")
        can_overlay_fast_fail = bool(
            existing_cycles == diagnostic_cycles
            or existing_fast_fail <= diagnostic_fast_fail + diagnostic_excluded
        )
        out = dict(existing)
        for key in (
            "premature_soft_exit_count",
            "premature_soft_exit_net_bps_sum",
            "premature_soft_exit_net_pnl_sum_usdt",
            "premature_soft_exit_closed_notional_usdt",
            "excluded_from_fast_fail_count",
            "entry_bad_cycles",
            "exit_bad_cycles",
            "min_hold_violation_cycles",
            "gave_back_profit_cycles",
            "trailing_too_early_cycles",
            "unknown_attribution_cycles",
            "adjusted_entry_cycles",
            "adjusted_entry_net_pnl_sum_usdt",
            "adjusted_entry_closed_notional_usdt",
            "adjusted_entry_expectancy_bps",
            "cycle_attributions",
        ):
            out[key] = diagnostic.get(key, out.get(key))
        out["premature_soft_exit_diagnostic_source"] = str(diagnostic.get("source") or "")
        if can_overlay_fast_fail:
            for key in (
                "fast_fail_closed_cycles",
                "fast_fail_closed_notional_usdt",
                "fast_fail_gross_pnl_sum_usdt",
                "fast_fail_net_pnl_sum_usdt",
                "fast_fail_gross_expectancy_usdt",
                "fast_fail_net_expectancy_usdt",
                "fast_fail_gross_expectancy_bps",
                "fast_fail_net_expectancy_bps",
                "adjusted_fast_fail_net_expectancy_bps",
                "fast_fail_pnl_sum_usdt",
                "fast_fail_expectancy_usdt",
                "fast_fail_expectancy_bps",
                "fast_fail_avg_hold_minutes",
            ):
                if key in diagnostic:
                    out[key] = diagnostic.get(key)
        return out

    def _merge_missing_symbol_stats(self, *sources: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for source in sources:
            for sym, stat in (source or {}).items():
                if not isinstance(stat, dict):
                    continue
                existing = merged.get(sym)
                if existing is None:
                    merged[sym] = dict(stat)
                    continue
                if self._closed_cycle_count(existing) <= 0 and self._closed_cycle_count(stat) > 0:
                    merged[sym] = dict(stat)
                    continue
                merged[sym] = self._overlay_premature_soft_exit_diagnostics(dict(existing), dict(stat))
        return merged

    def _empty_expectancy_row(self, *, source: str = "no_closed_cycles") -> Dict[str, Any]:
        return self._build_expectancy_row(
            gross_pnl_sum_usdt=0.0,
            net_pnl_sum_usdt=0.0,
            closed_cycles=0.0,
            closed_notional_usdt=0.0,
            last_close_ts_ms=0.0,
            fast_fail_gross_pnl_sum_usdt=0.0,
            fast_fail_net_pnl_sum_usdt=0.0,
            fast_fail_closed_cycles=0.0,
            fast_fail_closed_notional_usdt=0.0,
            fast_fail_hold_minutes_sum=0.0,
            source=source,
            lookback_filter_mode="close_ts",
        )

    def _ensure_scope_symbol_stats(self, stats: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        if not self._scope_symbols:
            return stats
        out = {str(sym): dict(st) for sym, st in (stats or {}).items() if isinstance(st, dict)}
        for sym in sorted(self._scope_symbols):
            out.setdefault(sym, self._empty_expectancy_row())
        return out

    def _scan_expectancy_from_fills(
        self,
        *,
        since_ms: int,
        allowed_symbols: Optional[Set[str]],
    ) -> Dict[str, Dict[str, Any]]:
        p = Path(self.cfg.fills_db_path)
        if not p.exists():
            return {}

        conn = None
        try:
            conn = sqlite3.connect(str(p))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT inst_id, trade_id, ord_id, cl_ord_id, side, fill_px, fill_sz, fee, fee_ccy, ts_ms, raw_json
                FROM fills
                WHERE fill_px IS NOT NULL
                  AND fill_sz IS NOT NULL
                ORDER BY inst_id ASC, ts_ms ASC, trade_id ASC
                """,
            ).fetchall()
        except Exception:
            return {}
        finally:
            try:
                conn.close()
            except Exception:
                pass

        inv_lots: Dict[str, list[Dict[str, float]]] = {}
        by_symbol: Dict[str, Dict[str, float]] = {}
        fast_fail_hold_ms = max(0, int(self.cfg.fast_fail_max_hold_minutes)) * 60 * 1000
        order_meta_by_clid, order_meta_by_oid = self._load_order_meta_by_id()

        for r in rows:
            inst_id = str(r["inst_id"] or "")
            sym = self._norm_symbol(inst_id)
            if allowed_symbols is not None and sym not in allowed_symbols:
                continue
            side = str(r["side"] or "").lower()
            try:
                qty = float(r["fill_sz"] or 0.0)
                px = float(r["fill_px"] or 0.0)
                event_ts = int(r["ts_ms"] or 0)
            except Exception:
                continue
            if qty <= 0 or px <= 0 or event_ts <= 0 or side not in {"buy", "sell"}:
                continue

            event_meta: Dict[str, Any] = {}
            clid = str(r["cl_ord_id"] or "").strip()
            oid = str(r["ord_id"] or "").strip()
            if clid:
                event_meta.update(order_meta_by_clid.get(clid, {}))
            if oid:
                for key, value in order_meta_by_oid.get(oid, {}).items():
                    event_meta.setdefault(key, value)
            event_meta.setdefault("raw_json", r["raw_json"])
            event_meta.setdefault("cl_ord_id", clid)
            event_meta.setdefault("ord_id", oid)

            fee_cost_usdt = self._fee_to_usdt_cost(
                fee=r["fee"],
                fee_ccy=r["fee_ccy"],
                inst_id=inst_id,
                fill_px=r["fill_px"],
            )

            inv_lots.setdefault(sym, [])

            if side == "buy":
                inv_lots[sym].append(
                    {
                        "qty": qty,
                        "px": px,
                        "ts": float(event_ts),
                        "fee_cost_usdt_remaining": float(fee_cost_usdt),
                        "meta": dict(event_meta),
                    }
                )
                continue

            remaining = qty
            sell_fee_remaining = float(fee_cost_usdt)
            matched_any = False
            while remaining > 1e-12 and inv_lots[sym]:
                lot = inv_lots[sym][0]
                lot_qty = float(lot.get("qty") or 0.0)
                if lot_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                    continue
                close_qty = min(lot_qty, remaining)
                buy_px = float(lot.get("px") or px)
                buy_notional = buy_px * close_qty
                sell_notional = px * close_qty
                buy_fee_remaining = float(lot.get("fee_cost_usdt_remaining") or 0.0)
                buy_fee_alloc = buy_fee_remaining * (close_qty / lot_qty) if lot_qty > 0 else 0.0
                sell_fee_alloc = sell_fee_remaining * (close_qty / remaining) if remaining > 0 else 0.0

                gross_pnl = sell_notional - buy_notional
                net_pnl = gross_pnl - buy_fee_alloc - sell_fee_alloc
                hold_ms = max(0.0, float(event_ts) - float(lot.get("ts") or event_ts))
                if event_ts >= int(since_ms):
                    matched_any = True
                    st = by_symbol.setdefault(sym, self._empty_expectancy_accumulator())
                    st["closed_cycles"] += 1.0
                    st["closed_cycles_included_by_close_ts"] += 1.0
                    if float(lot.get("ts") or 0.0) < float(since_ms):
                        st["closed_cycles_with_entry_before_window"] += 1.0
                    st["gross_pnl_sum_usdt"] += float(gross_pnl)
                    st["net_pnl_sum_usdt"] += float(net_pnl)
                    st["closed_notional_usdt"] += float(buy_notional)
                    st["last_close_ts_ms"] = max(float(st.get("last_close_ts_ms") or 0.0), float(event_ts))
                    ctx = dict(lot.get("meta") or {})
                    ctx.update(event_meta)
                    ctx.setdefault("exit_reason", ctx.get("reason") or ctx.get("source_reason"))
                    ctx.setdefault("hold_minutes", float(hold_ms / 60000.0))
                    ctx.setdefault("hold_hours", float(hold_ms / 3600000.0))
                    net_bps = float(net_pnl) / float(buy_notional) * 10000.0 if float(buy_notional) > 1e-12 else None
                    self._record_fast_fail_or_premature_soft_exit(
                        st,
                        row=ctx,
                        gross_pnl=float(gross_pnl),
                        net_pnl=float(net_pnl),
                        notional=float(buy_notional),
                        hold_ms=float(hold_ms),
                        fast_fail_hold_ms=int(fast_fail_hold_ms),
                        net_bps=net_bps,
                    )

                remaining = max(0.0, remaining - close_qty)
                sell_fee_remaining = float(sell_fee_remaining - sell_fee_alloc)
                left_qty = max(0.0, lot_qty - close_qty)
                left_buy_fee = float(buy_fee_remaining - buy_fee_alloc)
                if left_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                else:
                    lot["qty"] = left_qty
                    lot["fee_cost_usdt_remaining"] = left_buy_fee
            if event_ts >= int(since_ms) and remaining > 1e-12 and not inv_lots[sym]:
                st = by_symbol.setdefault(sym, self._empty_expectancy_accumulator())
                st["missing_entry_leg_count"] += 1.0
                if not matched_any:
                    logger.warning(
                        "NegativeExpectancy missing_entry_leg_for_close_cycle: source=fills symbol=%s close_ts_ms=%s qty=%s",
                        sym,
                        event_ts,
                        qty,
                    )

        out: Dict[str, Dict[str, Any]] = {}
        for sym, st in by_symbol.items():
            out[sym] = self._build_expectancy_row(
                gross_pnl_sum_usdt=float(st.get("gross_pnl_sum_usdt") or 0.0),
                net_pnl_sum_usdt=float(st.get("net_pnl_sum_usdt") or 0.0),
                closed_cycles=float(st.get("closed_cycles") or 0.0),
                closed_notional_usdt=float(st.get("closed_notional_usdt") or 0.0),
                last_close_ts_ms=float(st.get("last_close_ts_ms") or 0.0),
                fast_fail_gross_pnl_sum_usdt=float(st.get("fast_fail_gross_pnl_sum_usdt") or 0.0),
                fast_fail_net_pnl_sum_usdt=float(st.get("fast_fail_net_pnl_sum_usdt") or 0.0),
                fast_fail_closed_cycles=float(st.get("fast_fail_closed_cycles") or 0.0),
                fast_fail_closed_notional_usdt=float(st.get("fast_fail_closed_notional_usdt") or 0.0),
                fast_fail_hold_minutes_sum=float(st.get("fast_fail_hold_minutes_sum") or 0.0),
                source="fills",
                closed_cycles_included_by_close_ts=float(st.get("closed_cycles_included_by_close_ts") or 0.0),
                closed_cycles_with_entry_before_window=float(st.get("closed_cycles_with_entry_before_window") or 0.0),
                missing_entry_leg_count=float(st.get("missing_entry_leg_count") or 0.0),
                premature_soft_exit_count=float(st.get("premature_soft_exit_count") or 0.0),
                premature_soft_exit_net_bps_sum=float(st.get("premature_soft_exit_net_bps_sum") or 0.0),
                premature_soft_exit_net_pnl_sum_usdt=float(st.get("premature_soft_exit_net_pnl_sum_usdt") or 0.0),
                premature_soft_exit_closed_notional_usdt=float(st.get("premature_soft_exit_closed_notional_usdt") or 0.0),
                excluded_from_fast_fail_count=float(st.get("excluded_from_fast_fail_count") or 0.0),
                entry_bad_cycles=float(st.get("entry_bad_cycles") or 0.0),
                exit_bad_cycles=float(st.get("exit_bad_cycles") or 0.0),
                min_hold_violation_cycles=float(st.get("min_hold_violation_cycles") or 0.0),
                gave_back_profit_cycles=float(st.get("gave_back_profit_cycles") or 0.0),
                trailing_too_early_cycles=float(st.get("trailing_too_early_cycles") or 0.0),
                unknown_attribution_cycles=float(st.get("unknown_attribution_cycles") or 0.0),
                adjusted_entry_cycles=float(st.get("adjusted_entry_cycles") or 0.0),
                adjusted_entry_net_pnl_sum_usdt=float(st.get("adjusted_entry_net_pnl_sum_usdt") or 0.0),
                adjusted_entry_closed_notional_usdt=float(st.get("adjusted_entry_closed_notional_usdt") or 0.0),
                cycle_attributions=st.get("cycle_attributions") if isinstance(st.get("cycle_attributions"), list) else [],
                lookback_filter_mode="close_ts",
            )
        return out

    def _scan_expectancy_from_orders(
        self,
        *,
        since_ms: int,
        allowed_symbols: Optional[Set[str]],
    ) -> Dict[str, Dict[str, Any]]:
        """Fallback path from orders.sqlite.

        This is a degraded approximation because orders.sqlite does not preserve fee_ccy.
        We assume orders.fee is already quote/USDT-denominated signed fee and convert it
        into a cost-oriented USDT number best-effort.
        """
        p = Path(self.cfg.orders_db_path)
        if not p.exists():
            return {}

        by_symbol: Dict[str, Dict[str, float]] = {}
        conn = None
        try:
            conn = sqlite3.connect(str(p))
            conn.row_factory = sqlite3.Row
            col_rows = conn.execute("PRAGMA table_info(orders)").fetchall()
            cols = {str(r["name"]) for r in col_rows}
            extra_cols = [col for col in ("cl_ord_id", "ord_id", "intent", "req_json") if col in cols]
            extra_select = (", " + ", ".join(extra_cols)) if extra_cols else ""
            if "updated_ts" in cols and "created_ts" in cols:
                sql = """
                    SELECT inst_id, side, state, acc_fill_sz, avg_px, fee{extra_select},
                           COALESCE(NULLIF(updated_ts, 0), created_ts) AS event_ts
                    FROM orders
                    WHERE state='FILLED'
                      AND acc_fill_sz IS NOT NULL AND avg_px IS NOT NULL
                    ORDER BY inst_id, COALESCE(NULLIF(updated_ts, 0), created_ts) ASC
                """.format(extra_select=extra_select)
            elif "created_ts" in cols:
                sql = """
                    SELECT inst_id, side, state, acc_fill_sz, avg_px, fee{extra_select}, created_ts AS event_ts
                    FROM orders
                    WHERE state='FILLED'
                      AND acc_fill_sz IS NOT NULL AND avg_px IS NOT NULL
                    ORDER BY inst_id, created_ts ASC
                """.format(extra_select=extra_select)
            elif "updated_ts" in cols:
                sql = """
                    SELECT inst_id, side, state, acc_fill_sz, avg_px, fee{extra_select}, updated_ts AS event_ts
                    FROM orders
                    WHERE state='FILLED'
                      AND acc_fill_sz IS NOT NULL AND avg_px IS NOT NULL
                    ORDER BY inst_id, updated_ts ASC
                """.format(extra_select=extra_select)
            else:
                return {}
            rows = conn.execute(sql).fetchall()
        except Exception:
            return {}
        finally:
            try:
                conn.close()
            except Exception:
                pass

        inv_lots: Dict[str, list[Dict[str, float]]] = {}
        fast_fail_hold_ms = max(0, int(self.cfg.fast_fail_max_hold_minutes)) * 60 * 1000

        for r in rows:
            inst_id = str(r["inst_id"] or "")
            sym = self._norm_symbol(inst_id)
            if allowed_symbols is not None and sym not in allowed_symbols:
                continue
            side = str(r["side"] or "").lower()
            try:
                qty = float(r["acc_fill_sz"] or 0.0)
                px = float(r["avg_px"] or 0.0)
                event_ts = int(r["event_ts"] or 0)
            except Exception:
                continue
            if qty <= 0 or px <= 0 or event_ts <= 0 or side not in {"buy", "sell"}:
                continue

            row_keys = set(r.keys())
            event_meta = self._order_meta_from_req_json(r["req_json"]) if "req_json" in row_keys else {}
            if "intent" in row_keys:
                event_meta.setdefault("intent", r["intent"])
            if "cl_ord_id" in row_keys:
                event_meta.setdefault("cl_ord_id", r["cl_ord_id"])
            if "ord_id" in row_keys:
                event_meta.setdefault("ord_id", r["ord_id"])

            fee_cost_usdt = self._orders_fee_to_usdt_best_effort(fee=r["fee"])

            inv_lots.setdefault(sym, [])

            if side == "buy":
                inv_lots[sym].append(
                    {
                        "qty": qty,
                        "px": px,
                        "ts": float(event_ts),
                        "fee_cost_usdt_remaining": float(fee_cost_usdt),
                        "meta": dict(event_meta),
                    }
                )
                continue

            remaining = qty
            sell_fee_remaining = float(fee_cost_usdt)
            matched_any = False
            while remaining > 1e-12 and inv_lots[sym]:
                lot = inv_lots[sym][0]
                lot_qty = float(lot.get("qty") or 0.0)
                if lot_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                    continue
                close_qty = min(lot_qty, remaining)
                buy_px = float(lot.get("px") or px)
                buy_notional = buy_px * close_qty
                sell_notional = px * close_qty
                buy_fee_remaining = float(lot.get("fee_cost_usdt_remaining") or 0.0)
                buy_fee_alloc = buy_fee_remaining * (close_qty / lot_qty) if lot_qty > 0 else 0.0
                sell_fee_alloc = sell_fee_remaining * (close_qty / remaining) if remaining > 0 else 0.0

                gross_pnl = sell_notional - buy_notional
                net_pnl = gross_pnl - buy_fee_alloc - sell_fee_alloc
                hold_ms = max(0.0, float(event_ts) - float(lot.get("ts") or event_ts))
                if event_ts >= int(since_ms):
                    matched_any = True
                    st = by_symbol.setdefault(sym, self._empty_expectancy_accumulator())
                    st["closed_cycles"] += 1.0
                    st["closed_cycles_included_by_close_ts"] += 1.0
                    if float(lot.get("ts") or 0.0) < float(since_ms):
                        st["closed_cycles_with_entry_before_window"] += 1.0
                    st["gross_pnl_sum_usdt"] += float(gross_pnl)
                    st["net_pnl_sum_usdt"] += float(net_pnl)
                    st["closed_notional_usdt"] += float(buy_notional)
                    st["last_close_ts_ms"] = max(float(st.get("last_close_ts_ms") or 0.0), float(event_ts))
                    ctx = dict(lot.get("meta") or {})
                    ctx.update(event_meta)
                    ctx.setdefault("exit_reason", ctx.get("reason") or ctx.get("source_reason"))
                    ctx.setdefault("hold_minutes", float(hold_ms / 60000.0))
                    ctx.setdefault("hold_hours", float(hold_ms / 3600000.0))
                    net_bps = float(net_pnl) / float(buy_notional) * 10000.0 if float(buy_notional) > 1e-12 else None
                    self._record_fast_fail_or_premature_soft_exit(
                        st,
                        row=ctx,
                        gross_pnl=float(gross_pnl),
                        net_pnl=float(net_pnl),
                        notional=float(buy_notional),
                        hold_ms=float(hold_ms),
                        fast_fail_hold_ms=int(fast_fail_hold_ms),
                        net_bps=net_bps,
                    )

                remaining = max(0.0, remaining - close_qty)
                sell_fee_remaining = float(sell_fee_remaining - sell_fee_alloc)
                left_qty = max(0.0, lot_qty - close_qty)
                left_buy_fee = float(buy_fee_remaining - buy_fee_alloc)
                if left_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                else:
                    lot["qty"] = left_qty
                    lot["fee_cost_usdt_remaining"] = left_buy_fee
            if event_ts >= int(since_ms) and remaining > 1e-12 and not inv_lots[sym]:
                st = by_symbol.setdefault(sym, self._empty_expectancy_accumulator())
                st["missing_entry_leg_count"] += 1.0
                if not matched_any:
                    logger.warning(
                        "NegativeExpectancy missing_entry_leg_for_close_cycle: source=orders symbol=%s close_ts_ms=%s qty=%s",
                        sym,
                        event_ts,
                        qty,
                    )

        out: Dict[str, Dict[str, Any]] = {}
        for sym, st in by_symbol.items():
            out[sym] = self._build_expectancy_row(
                gross_pnl_sum_usdt=float(st.get("gross_pnl_sum_usdt") or 0.0),
                net_pnl_sum_usdt=float(st.get("net_pnl_sum_usdt") or 0.0),
                closed_cycles=float(st.get("closed_cycles") or 0.0),
                closed_notional_usdt=float(st.get("closed_notional_usdt") or 0.0),
                last_close_ts_ms=float(st.get("last_close_ts_ms") or 0.0),
                fast_fail_gross_pnl_sum_usdt=float(st.get("fast_fail_gross_pnl_sum_usdt") or 0.0),
                fast_fail_net_pnl_sum_usdt=float(st.get("fast_fail_net_pnl_sum_usdt") or 0.0),
                fast_fail_closed_cycles=float(st.get("fast_fail_closed_cycles") or 0.0),
                fast_fail_closed_notional_usdt=float(st.get("fast_fail_closed_notional_usdt") or 0.0),
                fast_fail_hold_minutes_sum=float(st.get("fast_fail_hold_minutes_sum") or 0.0),
                source="orders",
                degraded_fee_model=True,
                degraded_reason="orders.sqlite fallback assumes orders.fee is quote/USDT-denominated signed fee",
                closed_cycles_included_by_close_ts=float(st.get("closed_cycles_included_by_close_ts") or 0.0),
                closed_cycles_with_entry_before_window=float(st.get("closed_cycles_with_entry_before_window") or 0.0),
                missing_entry_leg_count=float(st.get("missing_entry_leg_count") or 0.0),
                premature_soft_exit_count=float(st.get("premature_soft_exit_count") or 0.0),
                premature_soft_exit_net_bps_sum=float(st.get("premature_soft_exit_net_bps_sum") or 0.0),
                premature_soft_exit_net_pnl_sum_usdt=float(st.get("premature_soft_exit_net_pnl_sum_usdt") or 0.0),
                premature_soft_exit_closed_notional_usdt=float(st.get("premature_soft_exit_closed_notional_usdt") or 0.0),
                excluded_from_fast_fail_count=float(st.get("excluded_from_fast_fail_count") or 0.0),
                entry_bad_cycles=float(st.get("entry_bad_cycles") or 0.0),
                exit_bad_cycles=float(st.get("exit_bad_cycles") or 0.0),
                min_hold_violation_cycles=float(st.get("min_hold_violation_cycles") or 0.0),
                gave_back_profit_cycles=float(st.get("gave_back_profit_cycles") or 0.0),
                trailing_too_early_cycles=float(st.get("trailing_too_early_cycles") or 0.0),
                unknown_attribution_cycles=float(st.get("unknown_attribution_cycles") or 0.0),
                adjusted_entry_cycles=float(st.get("adjusted_entry_cycles") or 0.0),
                adjusted_entry_net_pnl_sum_usdt=float(st.get("adjusted_entry_net_pnl_sum_usdt") or 0.0),
                adjusted_entry_closed_notional_usdt=float(st.get("adjusted_entry_closed_notional_usdt") or 0.0),
                cycle_attributions=st.get("cycle_attributions") if isinstance(st.get("cycle_attributions"), list) else [],
                lookback_filter_mode="close_ts",
            )
        return out

    def _recent_trade_csv_path_candidates(self) -> list[Path]:
        orders_parent = Path(self.cfg.orders_db_path).parent
        report_roots = []
        for candidate in (
            orders_parent,
            orders_parent.parent,
            PROJECT_ROOT / "reports",
        ):
            try:
                resolved = candidate.resolve()
            except Exception:
                resolved = candidate
            if resolved not in report_roots:
                report_roots.append(resolved)

        paths: list[Path] = []
        seen: set[Path] = set()
        for root in report_roots:
            for pattern in ("runs/prod/*/trades.csv", "recent_runs/*/trades.csv"):
                for path in sorted(root.glob(pattern)):
                    try:
                        resolved = path.resolve()
                    except Exception:
                        resolved = path
                    if resolved in seen:
                        continue
                    seen.add(resolved)
                    paths.append(resolved)
        return paths

    def _scan_expectancy_from_recent_trade_csvs(
        self,
        *,
        since_ms: int,
        allowed_symbols: Optional[Set[str]],
    ) -> Dict[str, Dict[str, Any]]:
        paths = self._recent_trade_csv_path_candidates()
        if not paths:
            return {}

        events: list[Dict[str, Any]] = []
        for path in paths:
            try:
                with path.open("r", encoding="utf-8", newline="") as fh:
                    for row in csv.DictReader(fh):
                        sym = self._norm_symbol(str(row.get("symbol") or row.get("inst_id") or "").strip())
                        if not sym or (allowed_symbols is not None and sym not in allowed_symbols):
                            continue
                        event_ts = self._coerce_iso_ms(row.get("ts") or row.get("timestamp") or row.get("ts_utc"))
                        qty = self._coerce_float(row.get("qty") or row.get("amount") or row.get("sz"))
                        px = self._coerce_float(row.get("price") or row.get("px") or row.get("avg_px"))
                        if event_ts is None or qty is None or px is None:
                            continue
                        side = str(row.get("side") or "").strip().lower()
                        intent = str(row.get("intent") or "").strip().upper()
                        if not side:
                            if intent == "OPEN_LONG":
                                side = "buy"
                            elif intent == "CLOSE_LONG":
                                side = "sell"
                        if side not in {"buy", "sell"}:
                            continue
                        fee_raw = self._coerce_float(
                            row.get("fee_usdt")
                            or row.get("fee")
                            or row.get("commission_usdt")
                            or row.get("commission")
                        )
                        fee_cost_usdt = abs(float(fee_raw or 0.0))
                        events.append(
                            {
                                "symbol": sym,
                                "side": side,
                                "qty": float(qty),
                                "px": float(px),
                                "ts": int(event_ts),
                                "fee_cost_usdt": float(fee_cost_usdt),
                                "meta": dict(row),
                            }
                        )
            except Exception:
                continue

        if not events:
            return {}
        events.sort(key=lambda item: (str(item["symbol"]), int(item["ts"]), 0 if item["side"] == "buy" else 1))

        inv_lots: Dict[str, list[Dict[str, float]]] = {}
        by_symbol: Dict[str, Dict[str, float]] = {}
        fast_fail_hold_ms = max(0, int(self.cfg.fast_fail_max_hold_minutes)) * 60 * 1000

        for event in events:
            sym = str(event["symbol"])
            side = str(event["side"])
            qty = float(event["qty"])
            px = float(event["px"])
            event_ts = int(event["ts"])
            fee_cost_usdt = float(event.get("fee_cost_usdt") or 0.0)
            if qty <= 0 or px <= 0 or event_ts <= 0:
                continue

            inv_lots.setdefault(sym, [])
            if side == "buy":
                inv_lots[sym].append(
                    {
                        "qty": qty,
                        "px": px,
                        "ts": float(event_ts),
                        "fee_cost_usdt_remaining": fee_cost_usdt,
                        "meta": dict(event.get("meta") or {}),
                    }
                )
                continue

            remaining = qty
            sell_fee_remaining = fee_cost_usdt
            matched_any = False
            while remaining > 1e-12 and inv_lots[sym]:
                lot = inv_lots[sym][0]
                lot_qty = float(lot.get("qty") or 0.0)
                if lot_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                    continue
                close_qty = min(lot_qty, remaining)
                buy_px = float(lot.get("px") or px)
                buy_notional = buy_px * close_qty
                sell_notional = px * close_qty
                buy_fee_remaining = float(lot.get("fee_cost_usdt_remaining") or 0.0)
                buy_fee_alloc = buy_fee_remaining * (close_qty / lot_qty) if lot_qty > 0 else 0.0
                sell_fee_alloc = sell_fee_remaining * (close_qty / remaining) if remaining > 0 else 0.0

                gross_pnl = sell_notional - buy_notional
                net_pnl = gross_pnl - buy_fee_alloc - sell_fee_alloc
                hold_ms = max(0.0, float(event_ts) - float(lot.get("ts") or event_ts))
                if event_ts >= int(since_ms):
                    matched_any = True
                    st = by_symbol.setdefault(sym, self._empty_expectancy_accumulator())
                    st["closed_cycles"] += 1.0
                    st["closed_cycles_included_by_close_ts"] += 1.0
                    if float(lot.get("ts") or 0.0) < float(since_ms):
                        st["closed_cycles_with_entry_before_window"] += 1.0
                    st["gross_pnl_sum_usdt"] += float(gross_pnl)
                    st["net_pnl_sum_usdt"] += float(net_pnl)
                    st["closed_notional_usdt"] += float(buy_notional)
                    st["last_close_ts_ms"] = max(float(st.get("last_close_ts_ms") or 0.0), float(event_ts))
                    ctx = dict(lot.get("meta") or {})
                    ctx.update(dict(event.get("meta") or {}))
                    ctx.setdefault("exit_reason", ctx.get("reason") or ctx.get("source_reason"))
                    ctx.setdefault("hold_minutes", float(hold_ms / 60000.0))
                    ctx.setdefault("hold_hours", float(hold_ms / 3600000.0))
                    net_bps = float(net_pnl) / float(buy_notional) * 10000.0 if float(buy_notional) > 1e-12 else None
                    self._record_fast_fail_or_premature_soft_exit(
                        st,
                        row=ctx,
                        gross_pnl=float(gross_pnl),
                        net_pnl=float(net_pnl),
                        notional=float(buy_notional),
                        hold_ms=float(hold_ms),
                        fast_fail_hold_ms=int(fast_fail_hold_ms),
                        net_bps=net_bps,
                    )

                remaining = max(0.0, remaining - close_qty)
                sell_fee_remaining = float(sell_fee_remaining - sell_fee_alloc)
                left_qty = max(0.0, lot_qty - close_qty)
                left_buy_fee = float(buy_fee_remaining - buy_fee_alloc)
                if left_qty <= 1e-12:
                    inv_lots[sym].pop(0)
                else:
                    lot["qty"] = left_qty
                    lot["fee_cost_usdt_remaining"] = left_buy_fee
            if event_ts >= int(since_ms) and remaining > 1e-12 and not inv_lots[sym]:
                st = by_symbol.setdefault(sym, self._empty_expectancy_accumulator())
                st["missing_entry_leg_count"] += 1.0
                if not matched_any:
                    logger.warning(
                        "NegativeExpectancy missing_entry_leg_for_close_cycle: source=recent_trades symbol=%s close_ts_ms=%s qty=%s",
                        sym,
                        event_ts,
                        qty,
                    )

        out: Dict[str, Dict[str, Any]] = {}
        for sym, st in by_symbol.items():
            out[sym] = self._build_expectancy_row(
                gross_pnl_sum_usdt=float(st.get("gross_pnl_sum_usdt") or 0.0),
                net_pnl_sum_usdt=float(st.get("net_pnl_sum_usdt") or 0.0),
                closed_cycles=float(st.get("closed_cycles") or 0.0),
                closed_notional_usdt=float(st.get("closed_notional_usdt") or 0.0),
                last_close_ts_ms=float(st.get("last_close_ts_ms") or 0.0),
                fast_fail_gross_pnl_sum_usdt=float(st.get("fast_fail_gross_pnl_sum_usdt") or 0.0),
                fast_fail_net_pnl_sum_usdt=float(st.get("fast_fail_net_pnl_sum_usdt") or 0.0),
                fast_fail_closed_cycles=float(st.get("fast_fail_closed_cycles") or 0.0),
                fast_fail_closed_notional_usdt=float(st.get("fast_fail_closed_notional_usdt") or 0.0),
                fast_fail_hold_minutes_sum=float(st.get("fast_fail_hold_minutes_sum") or 0.0),
                source="recent_trades_csv",
                degraded_fee_model=True,
                degraded_reason="recent run trades.csv fallback uses fee_usdt best-effort",
                closed_cycles_included_by_close_ts=float(st.get("closed_cycles_included_by_close_ts") or 0.0),
                closed_cycles_with_entry_before_window=float(st.get("closed_cycles_with_entry_before_window") or 0.0),
                missing_entry_leg_count=float(st.get("missing_entry_leg_count") or 0.0),
                premature_soft_exit_count=float(st.get("premature_soft_exit_count") or 0.0),
                premature_soft_exit_net_bps_sum=float(st.get("premature_soft_exit_net_bps_sum") or 0.0),
                premature_soft_exit_net_pnl_sum_usdt=float(st.get("premature_soft_exit_net_pnl_sum_usdt") or 0.0),
                premature_soft_exit_closed_notional_usdt=float(st.get("premature_soft_exit_closed_notional_usdt") or 0.0),
                excluded_from_fast_fail_count=float(st.get("excluded_from_fast_fail_count") or 0.0),
                entry_bad_cycles=float(st.get("entry_bad_cycles") or 0.0),
                exit_bad_cycles=float(st.get("exit_bad_cycles") or 0.0),
                min_hold_violation_cycles=float(st.get("min_hold_violation_cycles") or 0.0),
                gave_back_profit_cycles=float(st.get("gave_back_profit_cycles") or 0.0),
                trailing_too_early_cycles=float(st.get("trailing_too_early_cycles") or 0.0),
                unknown_attribution_cycles=float(st.get("unknown_attribution_cycles") or 0.0),
                adjusted_entry_cycles=float(st.get("adjusted_entry_cycles") or 0.0),
                adjusted_entry_net_pnl_sum_usdt=float(st.get("adjusted_entry_net_pnl_sum_usdt") or 0.0),
                adjusted_entry_closed_notional_usdt=float(st.get("adjusted_entry_closed_notional_usdt") or 0.0),
                cycle_attributions=st.get("cycle_attributions") if isinstance(st.get("cycle_attributions"), list) else [],
                lookback_filter_mode="close_ts",
            )
        return out

    def _scan_expectancy_from_roundtrip_summary_csvs(
        self,
        *,
        since_ms: int,
        allowed_symbols: Optional[Set[str]],
    ) -> Dict[str, Dict[str, Any]]:
        paths = [p for p in self._roundtrip_summary_path_candidates() if p.exists()]
        if not paths:
            return {}

        by_symbol: Dict[str, Dict[str, float]] = {}
        fast_fail_hold_ms = max(0, int(self.cfg.fast_fail_max_hold_minutes)) * 60 * 1000
        seen_rows: set[tuple[str, int, str, str]] = set()

        for path in paths:
            try:
                with path.open("r", encoding="utf-8", newline="") as fh:
                    for row in csv.DictReader(fh):
                        sym = self._norm_symbol(str(row.get("symbol") or "").strip())
                        if not sym or (allowed_symbols is not None and sym not in allowed_symbols):
                            continue
                        close_ms = self._coerce_iso_ms(row.get("close_time_utc") or row.get("exit_ts"))
                        if close_ms is None or int(close_ms) < int(since_ms):
                            continue
                        entry_ms = self._coerce_iso_ms(row.get("open_time_utc") or row.get("entry_ts"))
                        dedupe_key = (
                            sym,
                            int(close_ms),
                            str(row.get("open_time_utc") or row.get("entry_ts") or ""),
                            str(row.get("exit_reason") or ""),
                        )
                        if dedupe_key in seen_rows:
                            continue
                        seen_rows.add(dedupe_key)

                        qty = self._coerce_float(row.get("qty"))
                        entry_px = self._coerce_float(row.get("entry_px"))
                        notional = (abs(float(qty)) * float(entry_px)) if qty and entry_px else None
                        net_pnl = self._coerce_float(row.get("net_pnl_usdt"))
                        net_bps = self._coerce_float(row.get("net_bps"))
                        if (notional is None or notional <= 0.0) and net_pnl is not None and net_bps not in (None, 0.0):
                            notional = abs(float(net_pnl) / float(net_bps) * 10000.0)
                        if notional is None or notional <= 0.0:
                            continue
                        if net_pnl is None and net_bps is not None:
                            net_pnl = float(notional) * float(net_bps) / 10000.0
                        if net_pnl is None:
                            continue
                        gross_pnl = self._coerce_float(row.get("gross_pnl_usdt"))
                        gross_bps = self._coerce_float(row.get("gross_bps"))
                        if gross_pnl is None and gross_bps is not None:
                            gross_pnl = float(notional) * float(gross_bps) / 10000.0
                        if gross_pnl is None:
                            gross_pnl = float(net_pnl)

                        hold_ms = 0.0
                        hold_minutes = self._coerce_float(row.get("hold_minutes"))
                        if hold_minutes is not None:
                            hold_ms = max(0.0, float(hold_minutes) * 60000.0)
                        elif entry_ms is not None:
                            hold_ms = max(0.0, float(close_ms) - float(entry_ms))

                        st = by_symbol.setdefault(sym, self._empty_expectancy_accumulator())
                        st["closed_cycles"] += 1.0
                        st["closed_cycles_included_by_close_ts"] += 1.0
                        if entry_ms is not None and float(entry_ms) < float(since_ms):
                            st["closed_cycles_with_entry_before_window"] += 1.0
                        st["gross_pnl_sum_usdt"] += float(gross_pnl)
                        st["net_pnl_sum_usdt"] += float(net_pnl)
                        st["closed_notional_usdt"] += float(notional)
                        st["last_close_ts_ms"] = max(float(st.get("last_close_ts_ms") or 0.0), float(close_ms))
                        ctx = dict(row)
                        ctx.setdefault("hold_minutes", float(hold_ms / 60000.0))
                        ctx.setdefault("hold_hours", float(hold_ms / 3600000.0))
                        self._record_fast_fail_or_premature_soft_exit(
                            st,
                            row=ctx,
                            gross_pnl=float(gross_pnl),
                            net_pnl=float(net_pnl),
                            notional=float(notional),
                            hold_ms=float(hold_ms),
                            fast_fail_hold_ms=int(fast_fail_hold_ms),
                            net_bps=net_bps,
                        )
            except Exception:
                continue

        out: Dict[str, Dict[str, Any]] = {}
        for sym, st in by_symbol.items():
            out[sym] = self._build_expectancy_row(
                gross_pnl_sum_usdt=float(st.get("gross_pnl_sum_usdt") or 0.0),
                net_pnl_sum_usdt=float(st.get("net_pnl_sum_usdt") or 0.0),
                closed_cycles=float(st.get("closed_cycles") or 0.0),
                closed_notional_usdt=float(st.get("closed_notional_usdt") or 0.0),
                last_close_ts_ms=float(st.get("last_close_ts_ms") or 0.0),
                fast_fail_gross_pnl_sum_usdt=float(st.get("fast_fail_gross_pnl_sum_usdt") or 0.0),
                fast_fail_net_pnl_sum_usdt=float(st.get("fast_fail_net_pnl_sum_usdt") or 0.0),
                fast_fail_closed_cycles=float(st.get("fast_fail_closed_cycles") or 0.0),
                fast_fail_closed_notional_usdt=float(st.get("fast_fail_closed_notional_usdt") or 0.0),
                fast_fail_hold_minutes_sum=float(st.get("fast_fail_hold_minutes_sum") or 0.0),
                source="trades_roundtrips_csv",
                degraded_fee_model=True,
                degraded_reason="trades_roundtrips.csv fallback uses exported net/gross fields",
                closed_cycles_included_by_close_ts=float(st.get("closed_cycles_included_by_close_ts") or 0.0),
                closed_cycles_with_entry_before_window=float(st.get("closed_cycles_with_entry_before_window") or 0.0),
                missing_entry_leg_count=float(st.get("missing_entry_leg_count") or 0.0),
                premature_soft_exit_count=float(st.get("premature_soft_exit_count") or 0.0),
                premature_soft_exit_net_bps_sum=float(st.get("premature_soft_exit_net_bps_sum") or 0.0),
                premature_soft_exit_net_pnl_sum_usdt=float(st.get("premature_soft_exit_net_pnl_sum_usdt") or 0.0),
                premature_soft_exit_closed_notional_usdt=float(st.get("premature_soft_exit_closed_notional_usdt") or 0.0),
                excluded_from_fast_fail_count=float(st.get("excluded_from_fast_fail_count") or 0.0),
                entry_bad_cycles=float(st.get("entry_bad_cycles") or 0.0),
                exit_bad_cycles=float(st.get("exit_bad_cycles") or 0.0),
                min_hold_violation_cycles=float(st.get("min_hold_violation_cycles") or 0.0),
                gave_back_profit_cycles=float(st.get("gave_back_profit_cycles") or 0.0),
                trailing_too_early_cycles=float(st.get("trailing_too_early_cycles") or 0.0),
                unknown_attribution_cycles=float(st.get("unknown_attribution_cycles") or 0.0),
                adjusted_entry_cycles=float(st.get("adjusted_entry_cycles") or 0.0),
                adjusted_entry_net_pnl_sum_usdt=float(st.get("adjusted_entry_net_pnl_sum_usdt") or 0.0),
                adjusted_entry_closed_notional_usdt=float(st.get("adjusted_entry_closed_notional_usdt") or 0.0),
                cycle_attributions=st.get("cycle_attributions") if isinstance(st.get("cycle_attributions"), list) else [],
                lookback_filter_mode="close_ts",
            )
        return out

    def _scan_expectancy(
        self,
        *,
        since_ms: int,
        allowed_symbols: Optional[Set[str]],
    ) -> Dict[str, Dict[str, Any]]:
        fills_stats: Dict[str, Dict[str, Any]] = {}
        orders_stats: Dict[str, Dict[str, Any]] = {}
        recent_trade_stats: Dict[str, Dict[str, Any]] = {}
        if bool(getattr(self.cfg, "prefer_net_from_fills", True)):
            fills_stats = self._scan_expectancy_from_fills(
                since_ms=since_ms,
                allowed_symbols=allowed_symbols,
            )
            orders_stats = self._scan_expectancy_from_orders(
                since_ms=since_ms,
                allowed_symbols=allowed_symbols,
            )
            recent_trade_stats = self._scan_expectancy_from_recent_trade_csvs(
                since_ms=since_ms,
                allowed_symbols=allowed_symbols,
            )
            roundtrip_stats = self._scan_expectancy_from_roundtrip_summary_csvs(
                since_ms=since_ms,
                allowed_symbols=allowed_symbols,
            )
            return self._merge_missing_symbol_stats(fills_stats, orders_stats, recent_trade_stats, roundtrip_stats)
        orders_stats = self._scan_expectancy_from_orders(
            since_ms=since_ms,
            allowed_symbols=allowed_symbols,
        )
        fills_stats = self._scan_expectancy_from_fills(
            since_ms=since_ms,
            allowed_symbols=allowed_symbols,
        )
        recent_trade_stats = self._scan_expectancy_from_recent_trade_csvs(
            since_ms=since_ms,
            allowed_symbols=allowed_symbols,
        )
        roundtrip_stats = self._scan_expectancy_from_roundtrip_summary_csvs(
            since_ms=since_ms,
            allowed_symbols=allowed_symbols,
        )
        return self._merge_missing_symbol_stats(orders_stats, fills_stats, recent_trade_stats, roundtrip_stats)

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        try:
            if value is None or str(value).strip() == "":
                return None
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _coerce_iso_ms(value: Any) -> Optional[int]:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.astimezone(timezone.utc).timestamp() * 1000)
        except Exception:
            return None

    def _roundtrip_summary_path_candidates(self) -> list[Path]:
        orders_parent = Path(self.cfg.orders_db_path).parent
        paths: list[Path] = []
        seen: set[Path] = set()
        for root in (orders_parent, orders_parent.parent, PROJECT_ROOT / "reports"):
            path = root / "summaries" / "trades_roundtrips.csv"
            try:
                resolved = path.resolve()
            except Exception:
                resolved = path
            if resolved in seen:
                continue
            seen.add(resolved)
            paths.append(resolved)
        return paths

    def _roundtrip_summary_by_symbol(self, *, since_ms: int) -> Dict[str, Dict[str, float]]:
        paths = [p for p in self._roundtrip_summary_path_candidates() if p.exists()]
        if not paths:
            return {}
        by_symbol: Dict[str, Dict[str, float]] = {}
        seen_rows: set[tuple[str, int, str]] = set()
        for path in paths:
            try:
                with path.open("r", encoding="utf-8", newline="") as fh:
                    for row in csv.DictReader(fh):
                        sym = self._norm_symbol(str(row.get("symbol") or "").strip())
                        if not sym:
                            continue
                        close_ms = self._coerce_iso_ms(row.get("close_time_utc") or row.get("exit_ts"))
                        if close_ms is None or int(close_ms) < int(since_ms):
                            continue
                        dedupe_key = (
                            sym,
                            int(close_ms),
                            str(row.get("open_time_utc") or row.get("entry_ts") or ""),
                        )
                        if dedupe_key in seen_rows:
                            continue
                        seen_rows.add(dedupe_key)
                        net_pnl = self._coerce_float(row.get("net_pnl_usdt"))
                        if net_pnl is None:
                            continue
                        qty = self._coerce_float(row.get("qty"))
                        entry_px = self._coerce_float(row.get("entry_px"))
                        notional = (float(qty) * float(entry_px)) if qty and entry_px else None
                        if notional is None or notional <= 0:
                            net_bps = self._coerce_float(row.get("net_bps"))
                            if net_bps is not None and abs(float(net_bps)) > 1e-12:
                                notional = abs(float(net_pnl) / float(net_bps) * 10000.0)
                        if notional is None or notional <= 0:
                            continue
                        st = by_symbol.setdefault(
                            sym,
                            {
                                "roundtrip_summary_net_pnl_usdt": 0.0,
                                "roundtrip_summary_closed_notional_usdt": 0.0,
                                "roundtrip_summary_closed_cycles": 0.0,
                            },
                        )
                        st["roundtrip_summary_net_pnl_usdt"] += float(net_pnl)
                        st["roundtrip_summary_closed_notional_usdt"] += float(notional)
                        st["roundtrip_summary_closed_cycles"] += 1.0
            except Exception:
                continue
        for st in by_symbol.values():
            notional = float(st.get("roundtrip_summary_closed_notional_usdt") or 0.0)
            st["roundtrip_summary_net_bps"] = (
                float(st.get("roundtrip_summary_net_pnl_usdt") or 0.0) / notional * 10000.0
                if notional > 1e-12
                else 0.0
            )
        return by_symbol

    def refresh(self, force: bool = False) -> Dict[str, Any]:
        if not self.cfg.enabled:
            return self._cache

        self._refresh_scope_metadata()

        now_ms = int(time.time() * 1000)
        # 至多每15分钟刷新一次，避免频繁扫库
        if (not force) and (now_ms - int(self._last_refresh_ms) < 15 * 60 * 1000):
            return self._cache

        cached_symbols = self._cache.get("symbols") or {}
        symbols = {
            self._norm_symbol(sym): dict(st)
            for sym, st in cached_symbols.items()
            if isinstance(st, dict)
            and (self._scope_symbols is None or self._norm_symbol(sym) in self._scope_symbols)
        }

        cached_fp = str(self._cache.get("config_fingerprint") or "").strip()
        current_fp = str(self._scope_metadata.get("config_fingerprint") or "").strip()
        legacy_state_present = bool((self._cache.get("stats") or {}) or cached_symbols)
        raw_release_start_ts = self._cache.get("release_start_ts")
        release_start_ts = self._coerce_release_start_ts(raw_release_start_ts)
        release_start_ts_out: Any = release_start_ts if release_start_ts is not None else RELEASE_START_NOT_OBSERVABLE
        release_start_ts_status = "ok" if release_start_ts is not None else "not_observable"
        warnings: list[str] = []
        if current_fp and cached_fp != current_fp:
            if cached_fp or legacy_state_present:
                logger.warning(
                    "NegativeExpectancy scope fingerprint changed: previous=%s current=%s; resetting scoped state",
                    cached_fp or "missing",
                    current_fp,
                )
                symbols = {}
            release_start_ts = now_ms
            release_start_ts_out = now_ms
            release_start_ts_status = "ok"
        elif current_fp:
            if release_start_ts is None:
                warnings.append(
                    self._release_warning(
                        "negative_expectancy_release_start_ts_recovered",
                        "cached release_start_ts is missing, zero, or not_observable; initialized at current refresh",
                    )
                )
                symbols = {}
                release_start_ts = now_ms
                release_start_ts_out = now_ms
                release_start_ts_status = "recovered"
            else:
                release_start_ts_out = release_start_ts
                release_start_ts_status = str(self._cache.get("release_start_ts_status") or "ok")
        else:
            if release_start_ts is not None:
                release_start_ts_out = release_start_ts
                release_start_ts_status = "legacy"
                warnings.append(
                    self._release_warning(
                        "negative_expectancy_release_start_ts_legacy_scope",
                        "config_fingerprint is missing; release scope cannot be tied to a config fingerprint",
                    )
                )
            else:
                warnings.append(
                    self._release_warning(
                        "negative_expectancy_release_start_ts_not_observable",
                        "config_fingerprint is missing",
                    )
                )

        should_log_not_observable = str(raw_release_start_ts or "").strip() != RELEASE_START_NOT_OBSERVABLE
        for warning in warnings:
            if "negative_expectancy_release_start_ts_not_observable" not in warning or should_log_not_observable:
                logger.warning("NegativeExpectancy %s", warning)

        lookback_ms = int(self.cfg.lookback_hours) * 3600 * 1000
        since_ms = int(now_ms - max(0, lookback_ms))
        if release_start_ts is not None and release_start_ts > 0:
            since_ms = max(since_ms, release_start_ts)

        stats = self._scan_expectancy(
            since_ms=since_ms,
            allowed_symbols=self._scope_symbols,
        )
        stats = self._ensure_scope_symbol_stats(stats)
        roundtrip_summary = self._roundtrip_summary_by_symbol(since_ms=since_ms)
        roundtrip_sanity: Dict[str, Dict[str, Any]] = {}
        for sym, st in stats.items():
            missing_entry_count = int((st or {}).get("missing_entry_leg_count") or 0)
            if missing_entry_count > 0:
                warnings.append(
                    self._release_warning(
                        "missing_entry_leg_for_close_cycle",
                        f"{sym} count={missing_entry_count}",
                    )
                )
            summary = roundtrip_summary.get(sym)
            if not summary:
                continue
            neg_net_bps = float((st or {}).get("net_expectancy_bps") or 0.0)
            summary_net_bps = float(summary.get("roundtrip_summary_net_bps") or 0.0)
            mismatch_bps = float(neg_net_bps - summary_net_bps)
            sanity = {
                "negative_expectancy_net_bps": float(neg_net_bps),
                "roundtrip_summary_net_bps": float(summary_net_bps),
                "mismatch_bps": float(mismatch_bps),
                "roundtrip_summary_net_pnl_usdt": float(summary.get("roundtrip_summary_net_pnl_usdt") or 0.0),
                "roundtrip_summary_closed_cycles": int(summary.get("roundtrip_summary_closed_cycles") or 0),
            }
            roundtrip_sanity[sym] = sanity
            st.update(sanity)
            if abs(mismatch_bps) > 50.0:
                warnings.append(
                    self._release_warning(
                        "negative_expectancy_roundtrip_mismatch",
                        f"{sym} mismatch_bps={mismatch_bps:.2f} negative_expectancy_net_bps={neg_net_bps:.2f} roundtrip_summary_net_bps={summary_net_bps:.2f}",
                    )
                )

        min_cycles = int(self.cfg.min_closed_cycles)
        exp_th_usdt = float(self.cfg.expectancy_threshold_usdt)
        exp_th_bps = (
            float(self.cfg.expectancy_threshold_bps)
            if self.cfg.expectancy_threshold_bps is not None
            else None
        )
        cd_ms = int(self.cfg.cooldown_hours) * 3600 * 1000

        # 清理过期
        for sym in list(symbols.keys()):
            until_ms = int((symbols.get(sym) or {}).get("cooldown_until_ms") or 0)
            if until_ms > 0 and now_ms >= until_ms:
                symbols.pop(sym, None)

        stats_cache: Dict[str, Dict[str, Any]] = {}
        for sym, st in stats.items():
            n = int(st.get("closed_cycles") or 0)
            gross_exp_usdt = float(st.get("gross_expectancy_usdt", st.get("expectancy_usdt") or 0.0))
            net_exp_usdt = float(st.get("net_expectancy_usdt", gross_exp_usdt))
            net_exp_bps = float(st.get("net_expectancy_bps", st.get("expectancy_bps") or 0.0))
            stat_row = dict(st)
            stat_row["updated_ts_ms"] = now_ms
            stats_cache[sym] = stat_row

            trigger_negative = False
            metric_used = "net_expectancy_bps" if exp_th_bps is not None else "net_expectancy_usdt"
            trigger_value = net_exp_bps if exp_th_bps is not None else net_exp_usdt
            threshold_value = float(exp_th_bps) if exp_th_bps is not None else float(exp_th_usdt)
            if n >= min_cycles and trigger_value < threshold_value:
                trigger_negative = True
            if n >= min_cycles and trigger_negative:
                symbols[sym] = {
                    "cooldown_until_ms": now_ms + cd_ms,
                    "metric_used": metric_used,
                    "threshold_value": threshold_value,
                    "gross_expectancy_usdt": gross_exp_usdt,
                    "net_expectancy_usdt": net_exp_usdt,
                    "gross_expectancy_bps": float(st.get("gross_expectancy_bps", st.get("expectancy_bps") or 0.0)),
                    "net_expectancy_bps": net_exp_bps,
                    # legacy compatibility
                    "expectancy_usdt": float(st.get("expectancy_usdt") or gross_exp_usdt),
                    "expectancy_bps": float(st.get("expectancy_bps") or st.get("gross_expectancy_bps") or 0.0),
                    "closed_cycles": n,
                    "last_close_ts": st.get("last_close_ts"),
                    "last_close_ts_ms": st.get("last_close_ts_ms"),
                    "gross_pnl_sum_usdt": float(st.get("gross_pnl_sum_usdt", st.get("pnl_sum_usdt") or 0.0)),
                    "net_pnl_sum_usdt": float(st.get("net_pnl_sum_usdt", st.get("pnl_sum_usdt") or 0.0)),
                    "pnl_sum_usdt": float(st.get("pnl_sum_usdt") or st.get("gross_pnl_sum_usdt") or 0.0),
                    "closed_notional_usdt": float(st.get("closed_notional_usdt") or 0.0),
                    "premature_soft_exit_count": int(st.get("premature_soft_exit_count") or 0),
                    "premature_soft_exit_net_bps_sum": float(st.get("premature_soft_exit_net_bps_sum") or 0.0),
                    "premature_soft_exit_net_pnl_sum_usdt": float(st.get("premature_soft_exit_net_pnl_sum_usdt") or 0.0),
                    "premature_soft_exit_closed_notional_usdt": float(st.get("premature_soft_exit_closed_notional_usdt") or 0.0),
                    "excluded_from_fast_fail_count": int(st.get("excluded_from_fast_fail_count") or 0),
                    "adjusted_fast_fail_net_expectancy_bps": float(
                        st.get("adjusted_fast_fail_net_expectancy_bps", st.get("fast_fail_net_expectancy_bps") or 0.0)
                    ),
                    "entry_bad_cycles": int(st.get("entry_bad_cycles") or 0),
                    "exit_bad_cycles": int(st.get("exit_bad_cycles") or 0),
                    "min_hold_violation_cycles": int(st.get("min_hold_violation_cycles") or 0),
                    "gave_back_profit_cycles": int(st.get("gave_back_profit_cycles") or 0),
                    "trailing_too_early_cycles": int(st.get("trailing_too_early_cycles") or 0),
                    "unknown_attribution_cycles": int(st.get("unknown_attribution_cycles") or 0),
                    "adjusted_entry_cycles": int(st.get("adjusted_entry_cycles") or 0),
                    "adjusted_entry_net_pnl_sum_usdt": float(st.get("adjusted_entry_net_pnl_sum_usdt") or 0.0),
                    "adjusted_entry_closed_notional_usdt": float(st.get("adjusted_entry_closed_notional_usdt") or 0.0),
                    "adjusted_entry_expectancy_bps": float(st.get("adjusted_entry_expectancy_bps") or 0.0),
                    "cycle_attributions": list(st.get("cycle_attributions") or []) if isinstance(st.get("cycle_attributions"), list) else [],
                    "source": str(st.get("source") or "orders"),
                    "degraded_fee_model": bool(st.get("degraded_fee_model", False)),
                    "degraded_reason": str(st.get("degraded_reason") or ""),
                    "updated_ts_ms": now_ms,
                }

        first_sanity = next(iter(roundtrip_sanity.values()), None)

        self._cache = {
            "updated_ts_ms": now_ms,
            "lookback_hours": int(self.cfg.lookback_hours),
            "min_closed_cycles": min_cycles,
            "expectancy_threshold_bps": exp_th_bps,
            "expectancy_threshold_usdt": exp_th_usdt,
            "cooldown_hours": int(self.cfg.cooldown_hours),
            "prefer_net_from_fills": bool(getattr(self.cfg, "prefer_net_from_fills", True)),
            "fills_db_path": str(self.cfg.fills_db_path),
            "orders_db_path": str(self.cfg.orders_db_path),
            "config_fingerprint": current_fp,
            "release_start_ts": release_start_ts_out,
            "release_start_ts_status": release_start_ts_status,
            "warnings": warnings,
            "lookback_filter_mode": "close_ts",
            "roundtrip_sanity": roundtrip_sanity,
            "negative_expectancy_net_bps": (
                first_sanity.get("negative_expectancy_net_bps") if isinstance(first_sanity, dict) else None
            ),
            "roundtrip_summary_net_bps": (
                first_sanity.get("roundtrip_summary_net_bps") if isinstance(first_sanity, dict) else None
            ),
            "mismatch_bps": first_sanity.get("mismatch_bps") if isinstance(first_sanity, dict) else None,
            "whitelist_symbols": list(self._scope_metadata.get("whitelist_symbols") or []),
            "managed_symbols": list(self._scope_metadata.get("managed_symbols") or []),
            "scope_symbols": list(self._scope_metadata.get("scope_symbols") or []),
            "symbols": symbols,
            "stats": stats_cache,
        }
        self._last_refresh_ms = now_ms
        self._save_state()
        self._save_attribution_report(now_ms=now_ms, stats_cache=stats_cache)
        return self._cache

    def is_blocked(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.cfg.enabled:
            return None
        now_ms = int(time.time() * 1000)
        st = (self._cache.get("symbols") or {}).get(self._norm_symbol(symbol))
        if not isinstance(st, dict):
            return None
        until_ms = int(st.get("cooldown_until_ms") or 0)
        if until_ms <= now_ms:
            return None
        out = dict(st)
        out["remain_seconds"] = float(max(0, until_ms - now_ms) / 1000.0)
        return out

    def get_symbol_stats(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.cfg.enabled:
            return None
        sym = self._norm_symbol(symbol)
        stats = (self._cache.get("stats") or {}).get(sym)
        if not isinstance(stats, dict):
            return None
        out = dict(stats)
        blocked = self.is_blocked(sym)
        out["cooldown_active"] = blocked is not None
        if blocked:
            out["cooldown_until_ms"] = int(blocked.get("cooldown_until_ms") or 0)
            out["remain_seconds"] = float(blocked.get("remain_seconds") or 0.0)
        return out

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        if not self.cfg.enabled:
            return {}
        stats = self._cache.get("stats") or {}
        return {str(sym): dict(st) for sym, st in stats.items() if isinstance(st, dict)}
