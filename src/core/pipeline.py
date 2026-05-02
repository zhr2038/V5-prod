from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any, Mapping

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 定义报告目录
REPORTS_DIR = Path(__file__).parent.parent.parent / 'reports'


def _effective_deadband(base: float, cfg: AppConfig, audit: Optional[DecisionAudit]) -> float:
    """F3.1: widen deadband when daily budget exceeded (monitor-driven, controlled).

    Budget is computed in main() from persisted daily state; pipeline consumes audit.budget.
    """
    db = float(base)
    try:
        if not cfg.budget.action_enabled:
            return db
        b = (audit.budget or {}) if audit else {}
        if not b or not bool(b.get("exceeded")):
            return db
        mult = float(cfg.budget.deadband_multiplier_exceeded)
        cap = float(cfg.budget.deadband_cap)
        return float(min(db * mult, cap))
    except Exception:
        return db


def _parse_iso_utc(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO-like timestamp to aware UTC datetime (best effort)."""
    try:
        s = str(ts or "").strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _coerce_state_epoch(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    dt = _parse_iso_utc(str(value or "").strip())
    return dt.timestamp() if dt is not None else None


def _risk_state_epoch(payload: Any, *, primary_keys: tuple[str, ...]) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    for key in primary_keys:
        epoch = _coerce_state_epoch(payload.get(key))
        if epoch is not None:
            return epoch
    history = payload.get("history")
    if isinstance(history, list):
        latest_history = max(
            (item for item in history if isinstance(item, dict)),
            key=lambda item: float(_coerce_state_epoch(item.get("ts")) or float("-inf")),
            default=None,
        )
        if isinstance(latest_history, dict):
            epoch = _coerce_state_epoch(latest_history.get("ts"))
            if epoch is not None:
                return epoch
    return None


def _holding_minutes(entry_ts: Optional[str], now_utc: datetime) -> Optional[float]:
    ent = _parse_iso_utc(entry_ts)
    if ent is None:
        return None
    return max(0.0, (now_utc - ent).total_seconds() / 60.0)


def _coalesce(value: Any, default: Any) -> Any:
    return default if value is None else value


def _normalize_market_series(series: MarketSeries) -> MarketSeries:
    points = []
    for idx, values in enumerate(
        zip(
            series.ts or [],
            series.open or [],
            series.high or [],
            series.low or [],
            series.close or [],
            series.volume or [],
        )
    ):
        ts_value, open_px, high_px, low_px, close_px, volume = values
        try:
            ts_ms = int(ts_value)
        except Exception:
            continue
        if abs(ts_ms) < 10_000_000_000:
            ts_ms *= 1000
        points.append((ts_ms, idx, open_px, high_px, low_px, close_px, volume))

    if not points:
        return MarketSeries(symbol=series.symbol, timeframe=series.timeframe, ts=[], open=[], high=[], low=[], close=[], volume=[])

    points.sort(key=lambda item: (item[0], item[1]))
    deduped = []
    for point in points:
        if deduped and deduped[-1][0] == point[0]:
            deduped[-1] = point
        else:
            deduped.append(point)

    return MarketSeries(
        symbol=series.symbol,
        timeframe=series.timeframe,
        ts=[int(item[0]) for item in deduped],
        open=[item[2] for item in deduped],
        high=[item[3] for item in deduped],
        low=[item[4] for item in deduped],
        close=[float(item[5]) for item in deduped],
        volume=[item[6] for item in deduped],
    )


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _config_float(config: Any, name: str) -> Optional[float]:
    for section_name in ("execution", "budget"):
        section = getattr(config, section_name, None)
        if section is not None and hasattr(section, name):
            parsed = _float_or_none(getattr(section, name, None))
            if parsed is not None:
                return parsed
    if hasattr(config, name):
        return _float_or_none(getattr(config, name, None))
    return None


def _exchange_min_notional_usdt(
    symbol: str,
    qty: float,
    value_usdt: float,
    exchange_rules: Any,
) -> Optional[float]:
    if exchange_rules is None:
        return None

    for name in ("exchange_min_notional_usdt", "min_notional_usdt", "min_notional", "min_notional_ex"):
        raw = exchange_rules.get(name) if isinstance(exchange_rules, dict) else getattr(exchange_rules, name, None)
        parsed = _float_or_none(raw)
        if parsed is not None and parsed > 0:
            return parsed

    min_sz_raw = exchange_rules.get("min_sz") if isinstance(exchange_rules, dict) else getattr(exchange_rules, "min_sz", None)
    min_sz = _float_or_none(min_sz_raw)
    qty_abs = abs(float(qty or 0.0))
    value_abs = abs(float(value_usdt or 0.0))
    if min_sz is not None and min_sz > 0 and qty_abs > 0 and value_abs > 0:
        return float(min_sz) * (value_abs / qty_abs)
    return None


def dust_position_threshold_usdt(
    symbol: str,
    qty: float,
    value_usdt: float,
    config: Any,
    exchange_rules: Any = None,
) -> float:
    dust_value_threshold = max(0.0, float(_config_float(config, "dust_value_threshold") or 0.0))
    dust_usdt_ignore = _config_float(config, "dust_usdt_ignore")
    reconcile_dust = _config_float(config, "reconcile_dust_usdt_ignore")
    dust_ignore = max(
        0.0,
        float(dust_usdt_ignore or 0.0),
        float(reconcile_dust or 0.0),
    )
    min_trade_value = max(0.0, float(_config_float(config, "min_trade_value_usdt") or 0.0))
    exchange_min_notional = _exchange_min_notional_usdt(symbol, qty, value_usdt, exchange_rules)
    exchange_component = (
        0.1 * float(exchange_min_notional)
        if exchange_min_notional is not None and float(exchange_min_notional) > 0
        else 1.0
    )
    return max(
        float(dust_value_threshold),
        float(dust_ignore),
        0.1 * float(min_trade_value),
        float(exchange_component),
    )


def is_dust_position(
    symbol: str,
    qty: float,
    value_usdt: float,
    config: Any,
    exchange_rules: Any = None,
) -> bool:
    qty_abs = abs(float(qty or 0.0))
    if qty_abs <= 0:
        return False
    threshold = dust_position_threshold_usdt(symbol, qty, value_usdt, config, exchange_rules)
    return abs(float(value_usdt or 0.0)) < float(threshold)


from configs.schema import AppConfig
from src.alpha.alpha_engine import AlphaEngine, AlphaSnapshot
from src.core.models import MarketSeries, Order
from src.execution.position_store import Position
from src.execution.probe_metadata import PROBE_POSITION_TYPES, probe_tags_from_order_meta, probe_type_from_meta
from src.execution.same_symbol_reentry_guard import evaluate_same_symbol_reentry_guard
from src.utils.time import utc_now_iso
from src.execution.fill_store import (
    derive_fill_store_path,
    derive_position_store_path,
    derive_runtime_auto_risk_eval_path,
    derive_runtime_auto_risk_guard_path,
    derive_runtime_named_artifact_path,
    derive_runtime_named_json_path,
    derive_runtime_runs_dir,
)
from src.execution.position_builder import PositionBuilder  # Phase 2: 分批建仓
from src.execution.multi_level_stop_loss import MultiLevelStopLoss, StopLossConfig  # Phase 2: 动态止损
from src.portfolio.portfolio_engine import PortfolioEngine, PortfolioSnapshot

# RegimeEngine选择：Ensemble（推荐）或传统MA
try:
    from src.regime.ensemble_regime_engine import EnsembleRegimeEngine
    ENSEMBLE_AVAILABLE = True
except ImportError:
    ENSEMBLE_AVAILABLE = False
from src.regime.regime_engine import RegimeEngine, RegimeResult

from src.risk.exit_policy import ExitPolicy, ExitConfig
from src.risk.risk_engine import RiskEngine
from src.risk.fixed_stop_loss import FixedStopLossManager, FixedStopLossConfig
from src.risk.profit_taking import PeakDrawdownLevel, ProfitTakingManager  # 程序化利润管理
from src.risk.auto_risk_guard import AutoRiskGuard, extract_risk_level, get_auto_risk_guard  # 自动风险档位
from src.risk.negative_expectancy_cooldown import (
    NegativeExpectancyCooldown,
    NegativeExpectancyConfig,
    negative_expectancy_config_fingerprint,
)
from src.core.models import PositionState
from src.reporting.decision_audit import DecisionAudit


def _load_borrow_prevention_rules(path: str) -> Dict[str, Any]:
    try:
        p = Path(path)
        if not p.exists():
            return {}
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _is_high_risk_symbol(sym: str, *, rules: Dict[str, Any]) -> bool:
    s = str(sym)
    hr = rules.get("high_risk_symbols") or []
    if isinstance(hr, list) and s in [str(x) for x in hr]:
        return True
    return False


def _min_price_usdt(*, rules: Dict[str, Any]) -> Optional[float]:
    try:
        th = rules.get("rules") or []
        for r in th:
            if isinstance(r, dict) and "thresholds" in r:
                t = r.get("thresholds") or {}
                if isinstance(t, dict) and "min_price_usdt" in t:
                    return float(t.get("min_price_usdt"))
    except Exception:
        pass
    return None


@dataclass
class PipelineOutput:
    """PipelineOutput类"""
    alpha: AlphaSnapshot
    regime: RegimeResult
    portfolio: PortfolioSnapshot
    orders: List[Order]


class V5Pipeline:
    """Shared Alpha->Regime->Portfolio->Risk->Exit pipeline.

    Adds Commit-B semantics:
    - mark-to-market at cycle start (highest_px/mark/pnl/update_ts)
    - equity = cash + Σ(qty*mark)
    - portfolio drawdown scaling via RiskEngine

    Designed so live(dry-run) and backtest can share the same semantics.
    """

    def __init__(self, cfg: AppConfig, clock=None, data_provider=None):
        self.cfg = cfg
        from src.core.clock import SystemClock

        self.clock = clock or SystemClock()
        self._data_provider = data_provider  # 数据提供者（用于ML数据收集器从API获取历史K线）
        self._live_symbol_whitelist = (
            {
                str(sym).strip()
                for sym in (getattr(cfg, "symbols", None) or [])
                if str(sym).strip()
            }
            if str(getattr(getattr(cfg, "execution", None), "mode", "dry_run") or "dry_run").lower() == "live"
            else set()
        )
        runtime_order_store_path = Path(
            str(getattr(cfg.execution, "order_store_path", "reports/orders.sqlite"))
        )
        if not runtime_order_store_path.is_absolute():
            runtime_order_store_path = (REPORTS_DIR.parent / runtime_order_store_path).resolve()
        self._runtime_order_store_path = runtime_order_store_path.resolve()
        self.alpha_engine = AlphaEngine(cfg.alpha)
        
        # RegimeEngine选择：Ensemble（HMM+情绪）或传统MA
        if ENSEMBLE_AVAILABLE and getattr(cfg.regime, 'use_ensemble', False):
            print("[Pipeline] 使用EnsembleRegimeEngine (HMM+资金费率+RSS)")
            self.regime_engine = EnsembleRegimeEngine(cfg.regime)
        else:
            print("[Pipeline] 使用传统RegimeEngine (MA+ATR)")
            self.regime_engine = RegimeEngine(cfg.regime, 
                                              use_hmm=getattr(cfg.regime, 'use_hmm', False))
        
        self.portfolio_engine = PortfolioEngine(alpha_cfg=cfg.alpha, risk_cfg=cfg.risk)
        self.risk_engine = RiskEngine(cfg.risk)
        self.exit_policy = ExitPolicy(ExitConfig(), clock=self.clock)
        
        # Phase 2: 初始化分批建仓和动态止损管理器
        self.position_builder = PositionBuilder(
            stages=[0.3, 0.3, 0.4],
            price_drop_threshold=0.02,
            trend_confirmation_bars=2,
            state_path=str(
                derive_runtime_named_json_path(
                    runtime_order_store_path,
                    "position_builder_state",
                ).resolve()
            ),
        )
        self.stop_loss_manager = MultiLevelStopLoss(
            config=StopLossConfig(
                tight_pct=0.03,
                normal_pct=0.05,
                loose_pct=0.08
            ),
            state_path=str(
                derive_runtime_named_json_path(
                    runtime_order_store_path,
                    "stop_loss_state",
                ).resolve()
            ),
        )
        
        # 固定比例止损（买入后立即生效的硬性止损）
        self.fixed_stop_loss = FixedStopLossManager(
            config=FixedStopLossConfig(
                enabled=True,
                base_stop_pct=0.05  # 5%硬性止损
            ),
            state_path=str(
                derive_runtime_named_json_path(
                    runtime_order_store_path,
                    "fixed_stop_loss_state",
                ).resolve()
            ),
        )
        
        # 程序化利润管理
        peak_drawdown_cfg = getattr(cfg.execution, "peak_drawdown_exit", None)
        peak_drawdown_levels = []
        if peak_drawdown_cfg is not None and bool(getattr(peak_drawdown_cfg, "enabled", False)):
            peak_drawdown_levels = [
                PeakDrawdownLevel(
                    profit_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier1_profit_pct", None), 0.08)),
                    retrace_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier1_retrace_pct", None), 0.025)),
                    sell_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier1_sell_pct", None), 0.33)),
                ),
                PeakDrawdownLevel(
                    profit_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier2_profit_pct", None), 0.15)),
                    retrace_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier2_retrace_pct", None), 0.04)),
                    sell_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier2_sell_pct", None), 0.50)),
                ),
                PeakDrawdownLevel(
                    profit_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier3_profit_pct", None), 0.25)),
                    retrace_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier3_retrace_pct", None), 0.06)),
                    sell_pct=float(_coalesce(getattr(peak_drawdown_cfg, "tier3_sell_pct", None), 1.0)),
                ),
            ]
        self.profit_taking = ProfitTakingManager(
            rank_exit_strict_mode=bool(getattr(cfg.execution, "rank_exit_strict_mode", False)),
            rank_exit_buffer_positions=int(getattr(cfg.execution, "rank_exit_buffer_positions", 0) or 0),
            take_profit_sell_all_pct=float(getattr(cfg.execution, "take_profit_sell_all_pct", 0.0) or 0.0),
            peak_drawdown_levels=peak_drawdown_levels,
            state_path=str(
                derive_runtime_named_json_path(
                    runtime_order_store_path,
                    "profit_taking_state",
                ).resolve()
            ),
        )
        
        # 自动风险档位守卫
        order_store_path = str(runtime_order_store_path)
        self.auto_risk_guard = get_auto_risk_guard(
            derive_runtime_auto_risk_guard_path(order_store_path)
        )
        self.market_impulse_probe_state_path = derive_runtime_named_json_path(
            runtime_order_store_path,
            "market_impulse_probe_state",
        ).resolve()
        self.same_symbol_reentry_memory_path = derive_runtime_named_json_path(
            runtime_order_store_path,
            "same_symbol_reentry_exit_memory",
        ).resolve()

        # 负期望标的自动冷却（根因级抑制高成本来回交易）
        neg_feedback_enabled = any(
            [
                bool(getattr(cfg.execution, 'negative_expectancy_cooldown_enabled', False)),
                bool(getattr(cfg.execution, 'negative_expectancy_score_penalty_enabled', False)),
                bool(getattr(cfg.execution, 'negative_expectancy_open_block_enabled', False)),
                bool(getattr(cfg.execution, 'negative_expectancy_fast_fail_open_block_enabled', False)),
            ]
        )
        raw_negexp_state_path = str(
            getattr(cfg.execution, "negative_expectancy_state_path", "reports/negative_expectancy_cooldown.json")
            or ""
        ).strip()
        raw_negexp_fills_path = str(
            getattr(cfg.execution, "fills_db_path", "reports/fills.sqlite")
            or ""
        ).strip()
        if not raw_negexp_state_path or raw_negexp_state_path == "reports/negative_expectancy_cooldown.json":
            negexp_state_path = derive_runtime_named_json_path(
                runtime_order_store_path,
                "negative_expectancy_cooldown",
            ).resolve()
        else:
            negexp_state_path = Path(raw_negexp_state_path)
            if not negexp_state_path.is_absolute():
                negexp_state_path = (REPORTS_DIR.parent / negexp_state_path).resolve()

        if not raw_negexp_fills_path or raw_negexp_fills_path == "reports/fills.sqlite":
            negexp_fills_path = derive_fill_store_path(runtime_order_store_path).resolve()
        else:
            negexp_fills_path = Path(raw_negexp_fills_path)
            if not negexp_fills_path.is_absolute():
                negexp_fills_path = (REPORTS_DIR.parent / negexp_fills_path).resolve()

        self.negative_expectancy_cooldown = NegativeExpectancyCooldown(
            NegativeExpectancyConfig(
                enabled=neg_feedback_enabled,
                lookback_hours=int(getattr(cfg.execution, 'negative_expectancy_lookback_hours', 24) or 24),
                min_closed_cycles=int(getattr(cfg.execution, 'negative_expectancy_min_closed_cycles', 4) or 4),
                expectancy_threshold_bps=(
                    float(getattr(cfg.execution, 'negative_expectancy_threshold_bps'))
                    if getattr(cfg.execution, 'negative_expectancy_threshold_bps', None) is not None
                    else None
                ),
                expectancy_threshold_usdt=float(getattr(cfg.execution, 'negative_expectancy_threshold_usdt', 0.0) or 0.0),
                cooldown_hours=int(getattr(cfg.execution, 'negative_expectancy_cooldown_hours', 24) or 24),
                state_path=str(negexp_state_path),
                orders_db_path=str(runtime_order_store_path),
                fills_db_path=str(negexp_fills_path),
                prefer_net_from_fills=bool(getattr(cfg.execution, 'prefer_net_from_fills', True)),
                fast_fail_max_hold_minutes=int(
                    getattr(cfg.execution, 'negative_expectancy_fast_fail_max_hold_minutes', 120) or 120
                ),
            )
        )
        
        # Phase 3: 初始化ML数据收集器（传入data_provider以便从API获取历史K线）
        from src.execution.ml_data_collector import MLDataCollector
        self.data_collector = MLDataCollector(
            db_path=str(
                derive_runtime_named_artifact_path(
                    runtime_order_store_path,
                    "ml_training_data",
                    ".db",
                ).resolve()
            ),
            data_provider=self._data_provider,
        )

    def _record_live_whitelist_drop(
        self,
        *,
        audit: Optional[DecisionAudit],
        stage: str,
        dropped_symbols: List[str],
    ) -> None:
        unique = sorted({str(sym) for sym in (dropped_symbols or []) if str(sym).strip()})
        if not unique or audit is None:
            return
        for _ in unique:
            audit.reject("live_symbol_not_whitelisted")
        audit.add_note(
            f"live whitelist enforced at {stage}: dropped_non_whitelist_symbols={unique}"
        )

    def _filter_live_alpha_snapshot(
        self,
        alpha: AlphaSnapshot,
        *,
        audit: Optional[DecisionAudit],
    ) -> AlphaSnapshot:
        if not self._live_symbol_whitelist:
            return alpha

        dropped = sorted(
            {
                str(sym)
                for sym in (
                    list((alpha.scores or {}).keys())
                    + list((alpha.raw_factors or {}).keys())
                    + list((alpha.z_factors or {}).keys())
                )
                if str(sym) not in self._live_symbol_whitelist
            }
        )
        self._record_live_whitelist_drop(audit=audit, stage="alpha_snapshot", dropped_symbols=dropped)

        def _filter_optional_scores(payload):
            if payload is None:
                return None
            return {
                sym: score
                for sym, score in (payload or {}).items()
                if sym in self._live_symbol_whitelist
            }

        return AlphaSnapshot(
            raw_factors={
                sym: factors
                for sym, factors in (alpha.raw_factors or {}).items()
                if sym in self._live_symbol_whitelist
            },
            z_factors={
                sym: factors
                for sym, factors in (alpha.z_factors or {}).items()
                if sym in self._live_symbol_whitelist
            },
            scores={
                sym: score
                for sym, score in (alpha.scores or {}).items()
                if sym in self._live_symbol_whitelist
            },
            raw_scores=_filter_optional_scores(alpha.raw_scores),
            telemetry_scores=_filter_optional_scores(alpha.telemetry_scores),
            base_scores=_filter_optional_scores(alpha.base_scores),
            base_raw_scores=_filter_optional_scores(alpha.base_raw_scores),
            ml_attribution_scores=_filter_optional_scores(alpha.ml_attribution_scores),
            ml_overlay_scores=_filter_optional_scores(alpha.ml_overlay_scores),
            ml_overlay_raw_scores=_filter_optional_scores(alpha.ml_overlay_raw_scores),
            ml_runtime=alpha.ml_runtime,
        )

    def _load_current_auto_risk_level(self) -> Optional[str]:
        try:
            eval_path = derive_runtime_auto_risk_eval_path(self._runtime_order_store_path).resolve()
            eval_obj = {}
            eval_level = ""
            eval_epoch = None
            if eval_path.exists():
                eval_obj = json.loads(eval_path.read_text(encoding="utf-8"))
                eval_level = extract_risk_level(eval_obj)
                eval_epoch = _risk_state_epoch(eval_obj, primary_keys=("ts",))

            guard_path = derive_runtime_auto_risk_guard_path(self._runtime_order_store_path).resolve()
            guard_obj = {}
            guard_file_level = ""
            guard_epoch = None
            if guard_path.exists():
                guard_obj = json.loads(guard_path.read_text(encoding="utf-8"))
                guard_file_level = extract_risk_level(guard_obj)
                guard_epoch = _risk_state_epoch(guard_obj, primary_keys=("last_update",))

            guard_level = str(getattr(self.auto_risk_guard, "current_level", "") or "").strip().upper()
            if eval_level and (not guard_file_level or guard_epoch is None or (eval_epoch is not None and eval_epoch >= guard_epoch)):
                return eval_level
            if guard_file_level:
                return guard_file_level
            return guard_level or None
        except Exception:
            return None

    def _resolve_strategy_signal_lookup(self, audit: Optional[DecisionAudit]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        strategy_entries = []
        if audit is not None and getattr(audit, "strategy_signals", None):
            strategy_entries = list(audit.strategy_signals or [])
        else:
            try:
                strategy_payload = self.alpha_engine.get_latest_strategy_signal_payload()
                if isinstance(strategy_payload, dict) and strategy_payload.get("strategies"):
                    strategy_entries = list(strategy_payload.get("strategies") or [])
            except Exception:
                strategy_entries = []
            if not strategy_entries:
                try:
                    strategy_file = self.alpha_engine.strategy_signals_path()
                    if strategy_file is not None and strategy_file.exists():
                        payload = json.loads(strategy_file.read_text(encoding="utf-8"))
                        if isinstance(payload, dict) and payload.get("strategies"):
                            strategy_entries = list(payload.get("strategies") or [])
                except Exception:
                    strategy_entries = []

        return self._strategy_entries_to_lookup(strategy_entries)

    @staticmethod
    def _strategy_entries_to_lookup(strategy_entries: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        lookup: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for entry in strategy_entries:
            strategy_name = str((entry or {}).get("strategy", "") or "").strip()
            if not strategy_name:
                continue
            sym_map: Dict[str, Dict[str, Any]] = {}
            for signal in (entry or {}).get("signals", []) or []:
                symbol = str((signal or {}).get("symbol", "") or "").strip()
                if not symbol:
                    continue
                sym_map[symbol] = dict(signal or {})
            lookup[strategy_name] = sym_map
        return lookup

    @staticmethod
    def _strategy_entries_from_obj(obj: Any) -> List[Dict[str, Any]]:
        if not isinstance(obj, dict):
            return []
        entries = obj.get("strategy_signals")
        if isinstance(entries, list) and entries:
            return [dict(item or {}) for item in entries if isinstance(item, dict)]
        entries = obj.get("strategies")
        if isinstance(entries, list) and entries:
            return [dict(item or {}) for item in entries if isinstance(item, dict)]
        return []

    @staticmethod
    def _signal_score(signal: Optional[Dict[str, Any]]) -> Optional[float]:
        if not isinstance(signal, dict):
            return None
        for key in ("score", "raw_score"):
            value = signal.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except Exception:
                continue
        return None

    @staticmethod
    def _signal_side(signal: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(signal, dict):
            return None
        side = str(signal.get("side", "") or "").strip().lower()
        return side or None

    @staticmethod
    def _alpha6_rsi_confirm(signal: Optional[Dict[str, Any]]) -> Optional[float]:
        if not isinstance(signal, dict):
            return None
        metadata = signal.get("metadata") if isinstance(signal, dict) else None
        metadata = metadata if isinstance(metadata, dict) else {}
        for bucket in ("z_factors", "raw_factors"):
            values = metadata.get(bucket)
            if not isinstance(values, dict):
                continue
            if "f5_rsi_trend_confirm" not in values:
                continue
            try:
                return float(values.get("f5_rsi_trend_confirm"))
            except Exception:
                continue
        return None

    @staticmethod
    def _alpha6_volume_confirm(signal: Optional[Dict[str, Any]]) -> Optional[float]:
        if not isinstance(signal, dict):
            return None
        metadata = signal.get("metadata") if isinstance(signal, dict) else None
        metadata = metadata if isinstance(metadata, dict) else {}
        for bucket in ("z_factors", "raw_factors"):
            values = metadata.get(bucket)
            if not isinstance(values, dict):
                continue
            if "f4_volume_expansion" not in values:
                continue
            try:
                return float(values.get("f4_volume_expansion"))
            except Exception:
                continue
        return None

    @staticmethod
    def _alpha6_volume_expansion(signal: Optional[Dict[str, Any]]) -> Optional[float]:
        return V5Pipeline._alpha6_volume_confirm(signal)

    @staticmethod
    def _rolling_close_high_excluding_latest(
        series: Optional[MarketSeries],
        lookback_hours: int,
    ) -> Optional[float]:
        if series is None:
            return None
        values = []
        for value in list(getattr(series, "close", []) or []):
            try:
                parsed = float(value)
            except Exception:
                continue
            if parsed > 0:
                values.append(parsed)
        if len(values) < 2:
            return None
        bars = max(1, int(lookback_hours or 1))
        prior_values = values[-(bars + 1):-1] if len(values) > bars else values[:-1]
        if not prior_values:
            return None
        return float(max(prior_values))

    def _btc_leadership_probe_active_cooldown(
        self,
        symbol: str,
        cooldown_hours: int,
    ) -> Optional[Dict[str, Any]]:
        try:
            hours = int(cooldown_hours or 0)
        except Exception:
            hours = 0
        if hours <= 0:
            return None
        try:
            if not self._runtime_order_store_path.exists():
                return None
            from src.execution.order_store import OrderStore

            now_ms = int(self.clock.now().timestamp() * 1000)
            cooldown_ms = int(hours * 3600 * 1000)
            since_ts = now_ms - cooldown_ms
            store = OrderStore(path=str(self._runtime_order_store_path))
            row = store.get_latest_filled(
                inst_id=str(symbol).replace("/", "-"),
                side="buy",
                intent="OPEN_LONG",
                since_ts=since_ts,
            )
            if row is None:
                return None
            event_ts = int(row.updated_ts or row.created_ts or 0)
            return {
                "latest_filled_cl_ord_id": str(row.cl_ord_id),
                "latest_filled_run_id": str(row.run_id),
                "latest_filled_event_ts": event_ts,
                "cooldown_hours": int(hours),
                "remain_seconds": max(0.0, (event_ts + cooldown_ms - now_ms) / 1000.0),
            }
        except Exception:
            return None

    def _btc_leadership_probe_negative_bypass_allowed(self, stats: Optional[Dict[str, Any]]) -> bool:
        if not bool(getattr(self.cfg.execution, "btc_leadership_probe_allow_single_negative_cycle_bypass", True)):
            return False
        closed_cycles = int((stats or {}).get("closed_cycles") or 0)
        max_cycles = int(getattr(self.cfg.execution, "btc_leadership_probe_max_negative_cycles_to_bypass", 1) or 0)
        floor_bps = float(
            _coalesce(
                getattr(self.cfg.execution, "btc_leadership_probe_min_net_expectancy_bps_to_bypass", -120.0),
                -120.0,
            )
        )
        expectancy_bps = self._negative_expectancy_bps(stats or {})
        return closed_cycles <= max_cycles and float(expectancy_bps) >= float(floor_bps)

    @staticmethod
    def _position_tags(position: Position) -> Dict[str, Any]:
        try:
            obj = json.loads(str(getattr(position, "tags_json", "{}") or "{}"))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def _probe_metadata_for_position(
        self,
        position: Position,
        *,
        probe_state: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        tags = self._position_tags(position)
        state = (getattr(self.profit_taking, "positions", {}) or {}).get(getattr(position, "symbol", ""))
        market_probe_payload = None
        if probe_state is not None:
            candidate_payload = probe_state.get(getattr(position, "symbol", ""))
            if isinstance(candidate_payload, dict):
                market_probe_payload = candidate_payload
        state_probe_type = str(getattr(state, "probe_type", "") or "").strip() if state is not None else ""
        state_entry_reason = str(getattr(state, "entry_reason", "") or "").strip() if state is not None else ""
        payload_probe_type = probe_type_from_meta(market_probe_payload) if market_probe_payload is not None else None
        probe_type = probe_type_from_meta(tags)
        if probe_type is None and state_probe_type in PROBE_POSITION_TYPES:
            probe_type = state_probe_type
        if probe_type is None and state_entry_reason in PROBE_POSITION_TYPES:
            probe_type = state_entry_reason
        if probe_type is None and payload_probe_type in PROBE_POSITION_TYPES:
            probe_type = payload_probe_type
        if probe_type is None and market_probe_payload is not None:
            probe_type = "market_impulse_probe"
        if probe_type is None:
            return None

        entry_px = _float_or_none(tags.get("entry_px"))
        if entry_px is None and state is not None:
            entry_px = _float_or_none(getattr(state, "entry_price", None))
        if entry_px is None and market_probe_payload is not None:
            entry_px = _float_or_none(market_probe_payload.get("entry_px"))
        if entry_px is None:
            entry_px = _float_or_none(getattr(position, "avg_px", None))
        if entry_px is None or entry_px <= 0:
            return None

        entry_ts = str(tags.get("entry_ts") or "").strip()
        if not entry_ts and state is not None and getattr(state, "entry_time", None) is not None:
            entry_ts = getattr(state, "entry_time").isoformat()
        if not entry_ts and market_probe_payload is not None:
            entry_ts = str(market_probe_payload.get("entry_ts") or "").strip()
            if not entry_ts:
                entry_ts_ms = int(market_probe_payload.get("entry_ts_ms") or 0)
                if entry_ts_ms > 0:
                    entry_ts = datetime.fromtimestamp(
                        entry_ts_ms / 1000.0,
                        tz=timezone.utc,
                    ).isoformat().replace("+00:00", "Z")
        if not entry_ts:
            entry_ts = str(getattr(position, "entry_ts", "") or "").strip()

        target_w = _float_or_none(tags.get("target_w"))
        if target_w is None and state is not None:
            target_w = _float_or_none(getattr(state, "target_w", None))
        if target_w is None and market_probe_payload is not None:
            target_w = _float_or_none(market_probe_payload.get("target_w"))
        highest_net_bps = _float_or_none(tags.get("highest_net_bps"))
        if state is not None:
            state_highest_net_bps = _float_or_none(getattr(state, "highest_net_bps", None))
            if state_highest_net_bps is not None:
                highest_net_bps = max(float(highest_net_bps or 0.0), float(state_highest_net_bps))
        if highest_net_bps is None and market_probe_payload is not None:
            highest_net_bps = _float_or_none(market_probe_payload.get("highest_net_bps"))

        payload_entry_reason = (
            str(market_probe_payload.get("entry_reason") or "").strip()
            if market_probe_payload is not None
            else ""
        )
        return {
            "probe_type": probe_type,
            "entry_reason": str(
                tags.get("entry_reason")
                or getattr(state, "entry_reason", None)
                or payload_entry_reason
                or probe_type
            ),
            "entry_px": float(entry_px),
            "entry_ts": entry_ts,
            "target_w": target_w,
            "highest_net_bps": float(highest_net_bps or 0.0),
        }

    def _probe_net_bps(self, *, entry_px: float, current_px: float) -> float:
        if entry_px <= 0 or current_px <= 0:
            return 0.0
        gross_bps = (float(current_px) / float(entry_px) - 1.0) * 10000.0
        fee_bps = float(getattr(self.cfg.execution, "fee_bps", 0.0) or 0.0)
        slippage_bps = float(getattr(self.cfg.execution, "slippage_bps", 0.0) or 0.0)
        return float(gross_bps - 2.0 * (fee_bps + slippage_bps))

    def _protect_entry_signal_values(self, signal: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "alpha6_score": self._signal_score(signal),
            "alpha6_side": self._signal_side(signal),
            "f4_volume_expansion": self._alpha6_volume_confirm(signal),
            "f5_rsi_trend_confirm": self._alpha6_rsi_confirm(signal),
        }

    def _protect_entry_signal_meets_normal_confirmation(self, signal: Optional[Dict[str, Any]]) -> bool:
        values = self._protect_entry_signal_values(signal)
        if values.get("alpha6_side") != "buy":
            return False
        score = values.get("alpha6_score")
        f5 = values.get("f5_rsi_trend_confirm")
        f4 = values.get("f4_volume_expansion")
        if score is None or f5 is None or f4 is None:
            return False
        alpha6_min_score = float(getattr(self.cfg.execution, "protect_entry_alpha6_min_score", 0.40) or 0.0)
        min_f5 = float(getattr(self.cfg.execution, "protect_entry_min_f5_rsi_trend_confirm", 0.30) or 0.0)
        min_f4 = float(getattr(self.cfg.execution, "protect_entry_min_f4_volume_expansion", 0.0) or 0.0)
        return float(score) >= alpha6_min_score and float(f5) >= min_f5 and float(f4) >= min_f4

    def _protect_entry_signal_meets_strong_confirmation(self, signal: Optional[Dict[str, Any]]) -> bool:
        values = self._protect_entry_signal_values(signal)
        if values.get("alpha6_side") != "buy":
            return False
        score = values.get("alpha6_score")
        f5 = values.get("f5_rsi_trend_confirm")
        f4 = values.get("f4_volume_expansion")
        if score is None or f5 is None or f4 is None:
            return False
        strong_score = float(
            getattr(self.cfg.execution, "protect_entry_single_round_strong_alpha6_score", 0.55) or 0.0
        )
        strong_f5 = float(getattr(self.cfg.execution, "protect_entry_single_round_strong_f5", 0.45) or 0.0)
        min_f4 = float(getattr(self.cfg.execution, "protect_entry_min_f4_volume_expansion", 0.0) or 0.0)
        return float(score) >= strong_score and float(f5) >= strong_f5 and float(f4) >= min_f4

    def _load_protect_entry_history_alpha6_signals(
        self,
        *,
        symbol: str,
        now_utc: datetime,
        current_run_id: str,
    ) -> List[Optional[Dict[str, Any]]]:
        max_hours = int(getattr(self.cfg.execution, "protect_entry_confirm_memory_hours", 6) or 6)
        cutoff = now_utc - timedelta(hours=max_hours)
        try:
            runs_dir = derive_runtime_runs_dir(getattr(self.cfg.execution, "order_store_path", "reports/orders.sqlite"))
            run_dirs = [p for p in Path(runs_dir).iterdir() if p.is_dir()]
        except Exception:
            return []

        def _run_time(run_dir: Path) -> datetime:
            audit_path = run_dir / "decision_audit.json"
            try:
                if audit_path.exists():
                    obj = json.loads(audit_path.read_text(encoding="utf-8"))
                    for key in ("window_end_ts", "now_ts"):
                        raw = obj.get(key)
                        if raw is not None:
                            value = float(raw)
                            if value > 10_000_000_000:
                                value /= 1000.0
                            return datetime.fromtimestamp(value, tz=timezone.utc)
            except Exception:
                pass
            try:
                return datetime.fromtimestamp(float(run_dir.stat().st_mtime), tz=timezone.utc)
            except Exception:
                return datetime.fromtimestamp(0, tz=timezone.utc)

        signals: List[Optional[Dict[str, Any]]] = []
        for run_dir in sorted(run_dirs, key=_run_time, reverse=True):
            if str(run_dir.name) == str(current_run_id):
                continue
            run_time = _run_time(run_dir)
            if run_time < cutoff:
                continue
            entries: List[Dict[str, Any]] = []
            for file_name in ("decision_audit.json", "strategy_signals.json"):
                path = run_dir / file_name
                if not path.exists():
                    continue
                try:
                    entries = self._strategy_entries_from_obj(json.loads(path.read_text(encoding="utf-8")))
                except Exception:
                    entries = []
                if entries:
                    break
            if not entries:
                continue
            alpha6_signal = (self._strategy_entries_to_lookup(entries).get("Alpha6Factor") or {}).get(symbol)
            signals.append(alpha6_signal if isinstance(alpha6_signal, dict) else None)
        return signals

    def _evaluate_protect_entry_confirmation_debounce(
        self,
        *,
        symbol: str,
        alpha6_signal: Optional[Dict[str, Any]],
        now_utc: datetime,
        current_run_id: str,
    ) -> Optional[Dict[str, Any]]:
        required_rounds = int(getattr(self.cfg.execution, "protect_entry_confirm_rounds", 2) or 2)
        values = self._protect_entry_signal_values(alpha6_signal)
        if required_rounds <= 1 or self._protect_entry_signal_meets_strong_confirmation(alpha6_signal):
            return None

        observed_rounds = 1 if self._protect_entry_signal_meets_normal_confirmation(alpha6_signal) else 0
        for previous_signal in self._load_protect_entry_history_alpha6_signals(
            symbol=symbol,
            now_utc=now_utc,
            current_run_id=current_run_id,
        ):
            if not self._protect_entry_signal_meets_normal_confirmation(previous_signal):
                break
            observed_rounds += 1
            if observed_rounds >= required_rounds:
                return None

        return {
            "reason": "protect_entry_confirmation_not_stable",
            "current_alpha6_score": values.get("alpha6_score"),
            "current_f5": values.get("f5_rsi_trend_confirm"),
            "current_f4": values.get("f4_volume_expansion"),
            "confirm_rounds_observed": int(observed_rounds),
            "required_confirm_rounds": int(required_rounds),
        }

    def _evaluate_protect_entry_gate(
        self,
        *,
        symbol: str,
        strategy_signal_lookup: Dict[str, Dict[str, Dict[str, Any]]],
        current_auto_risk_level: Optional[str],
        now_utc: Optional[datetime] = None,
        current_run_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        if str(current_auto_risk_level or "").upper() != "PROTECT":
            return None

        trend_signal = (strategy_signal_lookup.get("TrendFollowing") or {}).get(symbol)
        alpha6_signal = (strategy_signal_lookup.get("Alpha6Factor") or {}).get(symbol)
        trend_score = self._signal_score(trend_signal)
        alpha6_score = self._signal_score(alpha6_signal)
        alpha6_side = self._signal_side(alpha6_signal)
        trend_side = self._signal_side(trend_signal)
        rsi_confirm = self._alpha6_rsi_confirm(alpha6_signal)
        volume_confirm = self._alpha6_volume_confirm(alpha6_signal)

        if bool(getattr(self.cfg.execution, "protect_entry_block_trend_only", True)):
            if trend_side == "buy" and alpha6_signal is None:
                return {
                    "reason": "protect_entry_trend_only",
                    "trend_score": trend_score,
                    "alpha6_score": alpha6_score,
                    "alpha6_side": alpha6_side,
                    "f4_volume_expansion": volume_confirm,
                    "f5_rsi_trend_confirm": rsi_confirm,
                }

        if bool(getattr(self.cfg.execution, "protect_entry_require_alpha6_confirmation", True)):
            if alpha6_side != "buy":
                return {
                    "reason": "protect_entry_no_alpha6_confirmation",
                    "trend_score": trend_score,
                    "alpha6_score": alpha6_score,
                    "alpha6_side": alpha6_side,
                    "f4_volume_expansion": volume_confirm,
                    "f5_rsi_trend_confirm": rsi_confirm,
                }

        protect_min_rsi = float(getattr(self.cfg.execution, "protect_entry_min_f5_rsi_trend_confirm", 0.30) or 0.0)
        if bool(getattr(self.cfg.execution, "protect_entry_require_alpha6_rsi_confirm_positive", True)):
            if rsi_confirm is None or float(rsi_confirm) < float(protect_min_rsi):
                return {
                    "reason": "protect_entry_rsi_confirm_too_weak",
                    "trend_score": trend_score,
                    "alpha6_score": alpha6_score,
                    "alpha6_side": alpha6_side,
                    "f4_volume_expansion": volume_confirm,
                    "f5_rsi_trend_confirm": rsi_confirm,
                }

        alpha6_min_score = float(getattr(self.cfg.execution, "protect_entry_alpha6_min_score", 0.40) or 0.0)
        if alpha6_score is None or float(alpha6_score) < float(alpha6_min_score):
            return {
                "reason": "protect_entry_alpha6_score_too_low",
                "trend_score": trend_score,
                "alpha6_score": alpha6_score,
                "alpha6_side": alpha6_side,
                "f4_volume_expansion": volume_confirm,
                "f5_rsi_trend_confirm": rsi_confirm,
            }

        if bool(getattr(self.cfg.execution, "protect_entry_require_volume_confirm", True)):
            min_f4 = float(getattr(self.cfg.execution, "protect_entry_min_f4_volume_expansion", 0.0) or 0.0)
            if volume_confirm is None or float(volume_confirm) < float(min_f4):
                return {
                    "reason": "protect_entry_volume_confirm_negative",
                    "trend_score": trend_score,
                    "alpha6_score": alpha6_score,
                    "alpha6_side": alpha6_side,
                    "f4_volume_expansion": volume_confirm,
                    "f5_rsi_trend_confirm": rsi_confirm,
                }

        debounce_block = self._evaluate_protect_entry_confirmation_debounce(
            symbol=symbol,
            alpha6_signal=alpha6_signal,
            now_utc=now_utc or datetime.now(timezone.utc),
            current_run_id=current_run_id,
        )
        if debounce_block is not None:
            debounce_block.update(
                {
                    "trend_score": trend_score,
                    "alpha6_score": alpha6_score,
                    "alpha6_side": alpha6_side,
                    "f4_volume_expansion": volume_confirm,
                    "f5_rsi_trend_confirm": rsi_confirm,
                }
            )
            return debounce_block

        return None

    @staticmethod
    def _record_target_zero_reason(
        target_zero_reason_by_symbol: Dict[str, str],
        *,
        audit: Optional[DecisionAudit],
        symbol: str,
        reason: str,
    ) -> None:
        sym = str(symbol or "").strip()
        if not sym:
            return
        if sym not in target_zero_reason_by_symbol:
            target_zero_reason_by_symbol[sym] = str(reason)
        if audit is None:
            return
        audit.record_count("target_zero_after_regime_count", symbol=sym)
        if str(reason) == "risk_off_pos_mult_zero":
            audit.record_count("risk_off_suppressed_count", symbol=sym)

    @staticmethod
    def _replacement_block_reason_allowed(reason: str) -> bool:
        norm = str(reason or "").strip()
        return norm.startswith("protect_entry_") or norm in {
            "cost_aware_edge",
            "negative_expectancy_cooldown",
            "negative_expectancy_open_block",
            "negative_expectancy_fast_fail_open_block",
            "min_notional",
            "insufficient_cash",
        }

    def _held_symbol_has_negative_expectancy_hard_block(self, symbol: str) -> bool:
        try:
            if not any(
                [
                    bool(getattr(self.cfg.execution, "negative_expectancy_cooldown_enabled", False)),
                    bool(getattr(self.cfg.execution, "negative_expectancy_open_block_enabled", False)),
                    bool(getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_enabled", False)),
                ]
            ):
                return False
            blocked = self.negative_expectancy_cooldown.is_blocked(symbol)
            if blocked:
                return True
            stat = self.negative_expectancy_cooldown.get_symbol_stats(symbol) or {}
            if bool(getattr(self.cfg.execution, "negative_expectancy_open_block_enabled", False)):
                min_cycles = int(getattr(self.cfg.execution, "negative_expectancy_open_block_min_closed_cycles", 2) or 2)
                floor_bps = float(_coalesce(getattr(self.cfg.execution, "negative_expectancy_open_block_floor_bps", 5.0), 5.0))
                if int(stat.get("closed_cycles") or 0) >= min_cycles and self._negative_expectancy_bps(stat) < floor_bps:
                    return True
            if bool(getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_enabled", False)):
                ff_min_cycles = int(
                    getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_min_closed_cycles", 2) or 2
                )
                ff_floor_bps = float(
                    _coalesce(getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_floor_bps", 0.0), 0.0)
                )
                if int(stat.get("fast_fail_closed_cycles") or 0) >= ff_min_cycles and self._negative_expectancy_bps(stat, fast_fail=True) < ff_floor_bps:
                    return True
        except Exception:
            return False
        return False

    @staticmethod
    def _record_replacement_block(
        *,
        audit: Optional[DecisionAudit],
        blocked_replacement_reasons: Dict[str, str],
        symbol: str,
        reason: str,
    ) -> None:
        sym = str(symbol or "").strip()
        norm_reason = str(reason or "").strip()
        if not sym or not norm_reason:
            return
        blocked_replacement_reasons[sym] = norm_reason
        if audit is not None:
            audit.record_count("replacement_blocked_count", symbol=sym)

    def _load_market_impulse_probe_state(self) -> Dict[str, Any]:
        try:
            path = Path(self.market_impulse_probe_state_path)
            if not path.exists():
                return {}
            obj = json.loads(path.read_text(encoding="utf-8"))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def _write_market_impulse_probe_state(self, state: Dict[str, Any]) -> None:
        path = Path(self.market_impulse_probe_state_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    @staticmethod
    def _json_state_file_has_symbol(path: str | Path, symbol: str) -> bool:
        try:
            p = Path(path)
            if not p.exists():
                return False
            obj = json.loads(p.read_text(encoding="utf-8"))
            return isinstance(obj, dict) and str(symbol) in obj
        except Exception:
            return False

    @staticmethod
    def _remove_symbol_from_json_state_file(path: str | Path, symbol: str) -> bool:
        try:
            p = Path(path)
            if not p.exists():
                return False
            obj = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(obj, dict) or str(symbol) not in obj:
                return False
            obj.pop(str(symbol), None)
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(p)
            return True
        except Exception:
            return False

    def _highest_px_state_path(self) -> Path:
        try:
            from src.execution.highest_px_tracker import derive_tracker_state_path

            return derive_tracker_state_path(derive_position_store_path(self._runtime_order_store_path))
        except Exception:
            path = Path(self._runtime_order_store_path)
            if path.name == "orders.sqlite":
                return path.with_name("highest_px_state.json")
            if "orders" in path.stem:
                return path.with_name(path.name.replace("orders", "highest_px_state", 1)).with_suffix(".json")
            return path.with_name("highest_px_state.json")

    def _market_impulse_probe_state_has_active_position(self, symbol: str) -> bool:
        payload = self._load_market_impulse_probe_state().get(str(symbol))
        if not isinstance(payload, dict):
            return False
        has_active_fields = bool(
            int(payload.get("entry_ts_ms") or 0) > 0
            or str(payload.get("entry_ts") or "").strip()
            or str(payload.get("source_cl_ord_id") or "").strip()
        )
        if payload.get("active_position") is False and not has_active_fields:
            return False
        return bool(has_active_fields)

    def _deactivate_market_impulse_probe_active_state(self, symbol: str) -> bool:
        state = self._load_market_impulse_probe_state()
        payload = state.get(str(symbol))
        if not isinstance(payload, dict):
            return False
        had_active = self._market_impulse_probe_state_has_active_position(str(symbol))
        if not had_active:
            return False

        now_ms = int(self.clock.now().timestamp() * 1000)
        cooldown_until_ms = int(payload.get("cooldown_until_ms") or 0)
        if cooldown_until_ms > now_ms:
            next_payload: Dict[str, Any] = {
                "symbol": str(payload.get("symbol") or symbol),
                "active_position": False,
                "closed_ts_ms": now_ms,
                "cooldown_until_ms": cooldown_until_ms,
            }
            if payload.get("cooldown_until") is not None:
                next_payload["cooldown_until"] = payload.get("cooldown_until")
            state[str(symbol)] = next_payload
        else:
            state.pop(str(symbol), None)
        self._write_market_impulse_probe_state(state)
        return True

    def _symbol_has_active_position_state(self, symbol: str) -> bool:
        sym = str(symbol)
        try:
            if sym in (getattr(self.profit_taking, "positions", {}) or {}):
                return True
        except Exception:
            pass
        try:
            if sym in (getattr(self.stop_loss_manager, "positions", {}) or {}):
                return True
        except Exception:
            pass
        try:
            if sym in (getattr(self.fixed_stop_loss, "entry_prices", {}) or {}):
                return True
        except Exception:
            pass
        try:
            if self._json_state_file_has_symbol(self._highest_px_state_path(), sym):
                return True
        except Exception:
            pass
        return self._market_impulse_probe_state_has_active_position(sym)

    def _evaluate_same_symbol_reentry_guard(
        self,
        *,
        symbol: str,
        latest_px: float,
        entry_kind: str,
        audit: Optional[DecisionAudit] = None,
    ) -> Dict[str, Any]:
        result = evaluate_same_symbol_reentry_guard(
            path=self.same_symbol_reentry_memory_path,
            symbol=symbol,
            latest_px=float(latest_px or 0.0),
            config=self.cfg,
            entry_kind=entry_kind,
            now_ms=int(self.clock.now().timestamp() * 1000),
        )
        if audit and bool(result.get("breakout_exception_met", False)):
            audit.record_count("same_symbol_reentry_breakout_bypass_count", symbol=symbol)
            audit.add_note(
                "Same-symbol reentry breakout bypass: "
                f"{symbol} kind={entry_kind} last_exit={result.get('last_exit_reason')} "
                f"latest_px={result.get('latest_px')} exit_px={result.get('last_exit_px')} "
                f"highest={result.get('highest_px_before_exit')}"
            )
        return result

    def _same_symbol_reentry_block_decision(self, symbol: str, guard: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "action": "skip",
            "reason": "same_symbol_reentry_cooldown",
            "last_exit_reason": guard.get("last_exit_reason"),
            "last_exit_px": guard.get("last_exit_px"),
            "highest_px_before_exit": guard.get("highest_px_before_exit"),
            "elapsed_hours": guard.get("elapsed_hours"),
            "required_cooldown_hours": guard.get("required_cooldown_hours"),
            "breakout_exception_met": bool(guard.get("breakout_exception_met", False)),
            "latest_px": guard.get("latest_px"),
            "net_bps": guard.get("net_bps"),
        }

    def _clear_active_position_state_for_symbol(self, symbol: str) -> List[str]:
        sym = str(symbol)
        cleared: List[str] = []

        profit_state_path = getattr(self.profit_taking, "state_file", None)
        had_profit = sym in (getattr(self.profit_taking, "positions", {}) or {}) or (
            profit_state_path is not None and self._json_state_file_has_symbol(profit_state_path, sym)
        )
        if had_profit:
            try:
                self.profit_taking.clear_position(sym)
            except Exception:
                self._remove_symbol_from_json_state_file(profit_state_path, sym)
            if profit_state_path is not None:
                self._remove_symbol_from_json_state_file(profit_state_path, sym)
            cleared.append("profit_taking_state")

        stop_state_path = getattr(self.stop_loss_manager, "state_file", None)
        had_stop = sym in (getattr(self.stop_loss_manager, "positions", {}) or {}) or (
            stop_state_path is not None and self._json_state_file_has_symbol(stop_state_path, sym)
        )
        if had_stop:
            try:
                self.stop_loss_manager.remove_position(sym)
            except Exception:
                self._remove_symbol_from_json_state_file(stop_state_path, sym)
            if stop_state_path is not None:
                self._remove_symbol_from_json_state_file(stop_state_path, sym)
            cleared.append("stop_loss_state")

        fixed_state_path = getattr(self.fixed_stop_loss, "state_file", None)
        had_fixed = sym in (getattr(self.fixed_stop_loss, "entry_prices", {}) or {}) or (
            fixed_state_path is not None and self._json_state_file_has_symbol(fixed_state_path, sym)
        )
        if had_fixed:
            try:
                self.fixed_stop_loss.clear_position(sym)
            except Exception:
                self._remove_symbol_from_json_state_file(fixed_state_path, sym)
            if fixed_state_path is not None:
                self._remove_symbol_from_json_state_file(fixed_state_path, sym)
            cleared.append("fixed_stop_loss_state")

        highest_state_path = self._highest_px_state_path()
        if self._json_state_file_has_symbol(highest_state_path, sym):
            try:
                from src.execution.highest_px_tracker import get_highest_price_tracker

                get_highest_price_tracker(highest_state_path).clear_symbol(sym)
            except Exception:
                self._remove_symbol_from_json_state_file(highest_state_path, sym)
            cleared.append("highest_px_state")

        if self._deactivate_market_impulse_probe_active_state(sym):
            cleared.append("market_impulse_probe_state_active")

        return cleared

    def _position_value_for_cleanup(self, position: Position, prices: Dict[str, float]) -> tuple[float, float]:
        px = float(prices.get(position.symbol, 0.0) or 0.0)
        if px <= 0.0:
            px = float(getattr(position, "last_mark_px", 0.0) or 0.0)
        if px <= 0.0:
            px = float(getattr(position, "avg_px", 0.0) or 0.0)
        qty = float(getattr(position, "qty", 0.0) or 0.0)
        return px, max(0.0, qty * px)

    def cleanup_stale_position_state_for_dust_positions(
        self,
        positions: List[Position],
        *,
        prices: Dict[str, float],
        audit: Optional[DecisionAudit] = None,
    ) -> tuple[List[Position], List[Dict[str, Any]], set[str]]:
        active_positions: List[Position] = []
        router_decisions: List[Dict[str, Any]] = []
        dust_flat_symbols: set[str] = set()

        for position in positions or []:
            qty = float(getattr(position, "qty", 0.0) or 0.0)
            if qty <= 0.0:
                continue
            px, remaining_value = self._position_value_for_cleanup(position, prices)
            if px <= 0.0:
                active_positions.append(position)
                continue

            dust_threshold = self._dust_position_threshold_usdt(symbol=position.symbol, px=px)
            if remaining_value >= float(dust_threshold):
                active_positions.append(position)
                continue

            cleared_keys = self._clear_active_position_state_for_symbol(position.symbol)
            if not cleared_keys:
                active_positions.append(position)
                continue

            dust_flat_symbols.add(str(position.symbol))
            if audit:
                audit.record_count("stale_position_state_detected_count", symbol=position.symbol)
                audit.record_count("position_state_cleared_after_close_count", symbol=position.symbol)
                audit.add_note(
                    "Position state cleared after close: "
                    f"{position.symbol} remaining_value={remaining_value:.8f} "
                    f"dust_threshold={float(dust_threshold):.6f} cleared={cleared_keys}"
                )

            router_decisions.append(
                {
                    "symbol": position.symbol,
                    "action": "state_cleanup",
                    "reason": "position_state_cleared_after_close",
                    "position_state_cleared_after_close": True,
                    "remaining_value_usdt": float(remaining_value),
                    "dust_threshold_usdt": float(dust_threshold),
                    "cleared_state_keys": list(cleared_keys),
                }
            )

        return active_positions, router_decisions, dust_flat_symbols

    def _sync_market_impulse_probe_state_with_positions(self, positions: List[Position]) -> Dict[str, Any]:
        state = self._load_market_impulse_probe_state()
        if not state:
            return {}
        now_ms = int(self.clock.now().timestamp() * 1000)
        held_symbols = {
            str(getattr(position, "symbol", "") or "")
            for position in (positions or [])
            if float(getattr(position, "qty", 0.0) or 0.0) > 0.0
        }
        changed = False
        for symbol in list(state.keys()):
            payload = state.get(symbol)
            if not isinstance(payload, dict):
                state.pop(symbol, None)
                changed = True
                continue
            cooldown_until_ms = int(payload.get("cooldown_until_ms") or 0)
            if symbol not in held_symbols and cooldown_until_ms <= now_ms:
                state.pop(symbol, None)
                changed = True
        if changed:
            self._write_market_impulse_probe_state(state)
        return state

    def _market_impulse_probe_context(
        self,
        *,
        strategy_signal_lookup: Dict[str, Dict[str, Dict[str, Any]]],
        current_auto_risk_level: Optional[str],
        regime_state_str: str,
        require_feature_enabled: bool = True,
    ) -> Dict[str, Any]:
        enabled = bool(getattr(self.cfg.execution, "market_impulse_probe_enabled", True))
        if require_feature_enabled and not enabled:
            return {"active": False, "trend_buy_count": 0, "btc_trend_score": None, "candidates": []}

        if bool(getattr(self.cfg.execution, "market_impulse_probe_only_in_protect", True)):
            if str(current_auto_risk_level or "").upper() != "PROTECT":
                return {"active": False, "trend_buy_count": 0, "btc_trend_score": None, "candidates": []}

        if str(regime_state_str or "") in {"Risk-Off", "Risk_Off", "RiskOff"}:
            return {"active": False, "trend_buy_count": 0, "btc_trend_score": None, "candidates": []}

        trend_lookup = strategy_signal_lookup.get("TrendFollowing") or {}
        min_symbol_trend_score = float(
            getattr(self.cfg.execution, "market_impulse_probe_min_symbol_trend_score", 0.60) or 0.0
        )
        whitelist = list(self._live_symbol_whitelist or set(getattr(self.cfg, "symbols", []) or []))
        allowed_symbols = {str(symbol) for symbol in whitelist if str(symbol).strip()}

        trend_buy_candidates = []
        btc_trend_score = None
        btc_signal = trend_lookup.get("BTC/USDT")
        btc_side = self._signal_side(btc_signal)
        if btc_side == "buy":
            btc_trend_score = self._signal_score(btc_signal)

        for symbol, signal in trend_lookup.items():
            if allowed_symbols and str(symbol) not in allowed_symbols:
                continue
            if self._signal_side(signal) != "buy":
                continue
            trend_score = self._signal_score(signal)
            if trend_score is None or float(trend_score) < float(min_symbol_trend_score):
                continue
            trend_buy_candidates.append({"symbol": str(symbol), "trend_score": float(trend_score)})

        if len(trend_buy_candidates) < int(getattr(self.cfg.execution, "market_impulse_probe_min_trend_buy_count", 3) or 3):
            return {
                "active": False,
                "trend_buy_count": len(trend_buy_candidates),
                "btc_trend_score": btc_trend_score,
                "candidates": trend_buy_candidates,
            }

        if bool(getattr(self.cfg.execution, "market_impulse_probe_require_btc_trend_buy", True)):
            min_btc_score = float(getattr(self.cfg.execution, "market_impulse_probe_min_btc_trend_score", 0.60) or 0.0)
            if btc_side != "buy" or btc_trend_score is None or float(btc_trend_score) < float(min_btc_score):
                return {
                    "active": False,
                    "trend_buy_count": len(trend_buy_candidates),
                    "btc_trend_score": btc_trend_score,
                    "candidates": trend_buy_candidates,
                }

        priority_order = {"BTC/USDT": 0, "ETH/USDT": 1, "SOL/USDT": 2, "BNB/USDT": 3}
        trend_buy_candidates.sort(
            key=lambda item: (
                priority_order.get(str(item.get("symbol") or ""), 99),
                -float(item.get("trend_score") or 0.0),
                str(item.get("symbol") or ""),
            )
        )
        return {
            "active": True,
            "trend_buy_count": len(trend_buy_candidates),
            "btc_trend_score": btc_trend_score,
            "candidates": trend_buy_candidates,
        }

    def _should_soften_fast_fail_with_market_impulse(
        self,
        *,
        symbol: str,
        stat: Dict[str, Any],
        strategy_signal_lookup: Dict[str, Dict[str, Dict[str, Any]]],
        current_auto_risk_level: Optional[str],
        regime_state_str: str,
    ) -> tuple[bool, Dict[str, Any]]:
        if not bool(getattr(self.cfg.execution, "negative_expectancy_fast_fail_market_aware", True)):
            return False, {}
        if not bool(getattr(self.cfg.execution, "negative_expectancy_fast_fail_bypass_when_market_impulse", True)):
            return False, {}
        if str(current_auto_risk_level or "").upper() != "PROTECT":
            return False, {}
        if str(regime_state_str or "") in {"Risk-Off", "Risk_Off", "RiskOff"}:
            return False, {}
        if self.negative_expectancy_cooldown.is_blocked(symbol):
            return False, {}

        closed_cycles = int((stat or {}).get("closed_cycles") or 0)
        if closed_cycles >= 2:
            return False, {}

        ff_cycles = int((stat or {}).get("fast_fail_closed_cycles") or 0)
        max_cycles = int(getattr(self.cfg.execution, "negative_expectancy_fast_fail_bypass_max_cycles", 1) or 1)
        if ff_cycles <= 0 or ff_cycles > max_cycles:
            return False, {}

        net_expectancy_bps = self._negative_expectancy_bps(stat or {})
        min_net_bps = float(
            getattr(self.cfg.execution, "negative_expectancy_fast_fail_bypass_min_net_bps", -80.0) or -80.0
        )
        if net_expectancy_bps < min_net_bps:
            return False, {}

        impulse = self._market_impulse_probe_context(
            strategy_signal_lookup=strategy_signal_lookup,
            current_auto_risk_level=current_auto_risk_level,
            regime_state_str=regime_state_str,
            require_feature_enabled=False,
        )
        candidate_symbols = {str(item.get("symbol") or "") for item in (impulse.get("candidates") or [])}
        if not bool(impulse.get("active")) or str(symbol) not in candidate_symbols:
            return False, impulse

        return True, {
            "trend_buy_count": int(impulse.get("trend_buy_count") or 0),
            "btc_trend_score": impulse.get("btc_trend_score"),
            "net_expectancy_bps": float(net_expectancy_bps),
            "fast_fail_closed_cycles": int(ff_cycles),
        }

    def _market_impulse_probe_negexp_gate(
        self,
        *,
        symbol: str,
    ) -> tuple[bool, Optional[str], Optional[str]]:
        try:
            if bool(getattr(self.cfg.execution, "market_impulse_probe_disallow_active_cooldown", True)):
                blocked = self.negative_expectancy_cooldown.is_blocked(symbol)
                if blocked:
                    return False, "negative_expectancy_cooldown", None

            stat = self.negative_expectancy_cooldown.get_symbol_stats(symbol) or {}
            if bool(getattr(self.cfg.execution, "negative_expectancy_open_block_enabled", False)):
                min_cycles = int(getattr(self.cfg.execution, "negative_expectancy_open_block_min_closed_cycles", 2) or 2)
                floor_bps = float(_coalesce(getattr(self.cfg.execution, "negative_expectancy_open_block_floor_bps", 5.0), 5.0))
                if int(stat.get("closed_cycles") or 0) >= min_cycles and self._negative_expectancy_bps(stat) < floor_bps:
                    return False, "negative_expectancy_open_block", None

            if bool(getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_enabled", False)):
                ff_min_cycles = int(
                    getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_min_closed_cycles", 2) or 2
                )
                ff_floor_bps = float(
                    getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_floor_bps", 0.0) or 0.0
                )
                ff_cycles = int(stat.get("fast_fail_closed_cycles") or 0)
                ff_expectancy_bps = self._negative_expectancy_bps(stat, fast_fail=True)
                if ff_cycles >= ff_min_cycles and ff_expectancy_bps < ff_floor_bps:
                    allow_bypass = bool(
                        getattr(self.cfg.execution, "market_impulse_probe_allow_single_fast_fail_bypass", True)
                    )
                    max_cycles = int(
                        getattr(self.cfg.execution, "market_impulse_probe_max_fast_fail_cycles_to_bypass", 1) or 1
                    )
                    min_bypass_bps = float(
                        getattr(self.cfg.execution, "market_impulse_probe_min_net_expectancy_bps_to_bypass", -80.0)
                        or -80.0
                    )
                    if allow_bypass and ff_cycles <= max_cycles and ff_expectancy_bps >= min_bypass_bps:
                        return True, None, "negative_expectancy_fast_fail_open_block"
                    return False, "negative_expectancy_fast_fail_open_block", None
        except Exception:
            return True, None, None
        return True, None, None

    def _market_impulse_probe_sizing(
        self,
        *,
        symbol: str,
        px: float,
        equity: float,
    ) -> Dict[str, float]:
        configured_target_w = float(getattr(self.cfg.execution, "market_impulse_probe_target_w", 0.06) or 0.0)
        dynamic_enabled = bool(getattr(self.cfg.execution, "market_impulse_probe_dynamic_sizing_enabled", True))
        min_trade_value_usdt = float(getattr(self.cfg.execution, "min_trade_value_usdt", 0.0) or 0.0)
        budget_min_notional = float(getattr(self.cfg.budget, "min_trade_notional_base", 0.0) or 0.0)
        exchange_min_notional_with_slack = 0.0

        if bool(getattr(self.cfg.budget, "exchange_min_notional_enabled", True)):
            try:
                from src.data.okx_instruments import OKXSpotInstrumentsCache

                spec = OKXSpotInstrumentsCache().get_spec(str(symbol).replace("/", "-"))
                if spec is not None:
                    min_sz = float(spec.min_sz or 0.0)
                    slack = float(getattr(self.cfg.budget, "exchange_min_notional_slack_multiplier", 1.05) or 1.05)
                    if min_sz > 0.0 and float(px) > 0.0:
                        exchange_min_notional_with_slack = float(min_sz) * float(px) * float(slack)
            except Exception:
                exchange_min_notional_with_slack = 0.0

        min_executable_notional = max(
            float(min_trade_value_usdt),
            float(budget_min_notional),
            float(exchange_min_notional_with_slack),
        )
        min_executable_buffer = float(
            getattr(self.cfg.execution, "market_impulse_probe_min_executable_buffer", 1.05) or 1.05
        )
        min_executable_target_w = (
            float(min_executable_notional) / float(equity) * float(min_executable_buffer)
            if float(equity) > 0.0
            else float("inf")
        )
        effective_probe_target_w = float(configured_target_w)
        if dynamic_enabled:
            effective_probe_target_w = max(float(configured_target_w), float(min_executable_target_w))

        return {
            "configured_target_w": float(configured_target_w),
            "min_executable_notional": float(min_executable_notional),
            "exchange_min_notional_with_slack": float(exchange_min_notional_with_slack),
            "min_executable_target_w": float(min_executable_target_w),
            "effective_probe_target_w": float(effective_probe_target_w),
            "probe_notional": float(effective_probe_target_w) * float(equity),
        }

    def _exchange_min_notional_with_slack(self, *, symbol: str, px: float) -> float:
        if float(px) <= 0.0 or not bool(getattr(self.cfg.budget, "exchange_min_notional_enabled", True)):
            return 0.0
        try:
            from src.data.okx_instruments import OKXSpotInstrumentsCache

            spec = OKXSpotInstrumentsCache().get_spec(str(symbol).replace("/", "-"))
            if spec is None:
                return 0.0
            min_sz = float(spec.min_sz or 0.0)
            if min_sz <= 0.0:
                return 0.0
            slack = float(getattr(self.cfg.budget, "exchange_min_notional_slack_multiplier", 1.05) or 1.05)
            return float(min_sz) * float(px) * float(slack)
        except Exception:
            return 0.0

    def _dust_position_threshold_usdt(self, *, symbol: str, px: float) -> float:
        exchange_min = self._exchange_min_notional_with_slack(symbol=symbol, px=px)
        exchange_rules = {"min_notional_usdt": exchange_min} if float(exchange_min or 0.0) > 0.0 else None
        return dust_position_threshold_usdt(
            symbol=symbol,
            qty=0.0,
            value_usdt=0.0,
            config=self.cfg,
            exchange_rules=exchange_rules,
        )

    def mark_to_market(self, store, market_data_1h: Dict[str, MarketSeries]) -> None:
        """按市值计价更新持仓
        
        Args:
            store: 持仓存储
            market_data_1h: 市场数据
        """
        now_ts = self.clock.now().isoformat().replace("+00:00", "Z")
        for p in store.list():
            s = market_data_1h.get(p.symbol)
            if not s or not s.close:
                continue
            s = _normalize_market_series(s)
            if not s.close:
                continue
            mark = float(s.close[-1])
            hi = float(s.high[-1]) if s.high else mark
            store.mark_position(symbol=p.symbol, now_ts=now_ts, mark_px=mark, high_px=hi)

    def compute_equity(self, cash_usdt: float, positions: List[Position], market_data_1h: Dict[str, MarketSeries]) -> float:
        """计算总权益
        
        Args:
            cash_usdt: 现金余额
            positions: 持仓列表
            market_data_1h: 市场数据
            
        Returns:
            总权益 (现金 + 持仓市值)
        """
        eq = float(cash_usdt)
        for p in positions:
            s = market_data_1h.get(p.symbol)
            if not s or not s.close:
                continue
            s = _normalize_market_series(s)
            if not s.close:
                continue
            eq += float(p.qty) * float(s.close[-1])
        return float(eq)

    def _resolve_ml_snapshot_timestamp_ms(
        self,
        *,
        audit: Optional[DecisionAudit],
    ) -> int:
        if audit is not None:
            try:
                window_end_ts = getattr(audit, "window_end_ts", None)
                if window_end_ts is not None:
                    return int(window_end_ts) * 1000
            except Exception:
                pass

        now_ms = int(self.clock.now().timestamp() * 1000)
        hour_ms = 3600 * 1000
        return now_ms - (now_ms % hour_ms)

    def _resolve_ml_research_universe_path(self) -> Optional[Path]:
        raw_path = (
            getattr(self.cfg.execution, "ml_research_universe_path", None)
            or getattr(getattr(self.cfg, "universe", None), "cache_path", None)
        )
        if not raw_path:
            return None
        path = Path(str(raw_path))
        if path.is_absolute():
            return path
        return (REPORTS_DIR.parent / path).resolve()

    def _load_ml_research_symbols(self) -> list[str]:
        symbols: list[str] = []

        explicit = [
            str(sym).strip()
            for sym in (getattr(self.cfg.execution, "ml_research_symbols", []) or [])
            if str(sym).strip()
        ]
        if explicit:
            symbols.extend(explicit)
        else:
            universe_path = self._resolve_ml_research_universe_path()
            if universe_path is not None and universe_path.exists():
                try:
                    payload = json.loads(universe_path.read_text(encoding="utf-8"))
                    cached_symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
                    symbols.extend(str(sym).strip() for sym in cached_symbols if str(sym).strip())
                except Exception:
                    pass

        if bool(getattr(self.cfg.execution, "ml_research_include_config_symbols", True)):
            symbols.extend(str(sym).strip() for sym in (getattr(self.cfg, "symbols", []) or []) if str(sym).strip())

        out: list[str] = []
        seen: set[str] = set()
        for raw in symbols:
            sym = str(raw).strip()
            if not sym:
                continue
            key = sym.upper()
            if key in seen:
                continue
            seen.add(key)
            out.append(sym)
        return out

    @staticmethod
    def _market_series_to_ml_payload(symbol: str, series: MarketSeries) -> Optional[Dict[str, Any]]:
        close = list(getattr(series, "close", []) or [])
        if len(close) < 2:
            return None
        return {
            "symbol": str(symbol),
            "ts": list(getattr(series, "ts", []) or []),
            "open": list(getattr(series, "open", []) or close),
            "high": list(getattr(series, "high", []) or close),
            "low": list(getattr(series, "low", []) or close),
            "close": close,
            "volume": list(getattr(series, "volume", []) or [0.0] * len(close)),
        }

    def _resolve_ml_collection_payloads(
        self,
        market_data_1h: Dict[str, MarketSeries],
        *,
        snapshot_ts: int,
    ) -> tuple[Dict[str, Dict[str, Any]], list[str]]:
        use_stable_universe = bool(getattr(self.cfg.execution, "ml_research_use_stable_universe", False))
        lookback_bars = int(getattr(self.cfg.execution, "ml_research_lookback_bars", 600) or 600)
        target_symbols = (
            self._load_ml_research_symbols()
            if use_stable_universe
            else sorted(str(sym) for sym in market_data_1h.keys())
        )
        if not target_symbols:
            target_symbols = sorted(str(sym) for sym in market_data_1h.keys())

        payloads: Dict[str, Dict[str, Any]] = {}
        missing_symbols: list[str] = []
        for sym in target_symbols:
            payload = None
            series = market_data_1h.get(sym)
            if series is not None:
                payload = self._market_series_to_ml_payload(sym, series)
            if payload is None:
                payload = self.data_collector.load_market_data_for_feature_snapshot(
                    sym,
                    end_timestamp=int(snapshot_ts),
                    lookback_bars=lookback_bars,
                )
            if payload is None:
                missing_symbols.append(sym)
                continue
            payloads[sym] = payload
        return payloads, missing_symbols

    def _refresh_negative_expectancy_state(self, audit: Optional[DecisionAudit] = None) -> Dict[str, Any]:
        return self._refresh_negative_expectancy_state_with_scope(
            audit=audit,
            positions=None,
            managed_symbols=None,
        )

    def _negative_expectancy_config_fingerprint(self) -> str:
        return negative_expectancy_config_fingerprint(self.cfg)

    def _refresh_negative_expectancy_state_with_scope(
        self,
        *,
        audit: Optional[DecisionAudit] = None,
        positions: Optional[List[Any]] = None,
        managed_symbols: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        neg_feedback_enabled = any(
            [
                bool(getattr(self.cfg.execution, 'negative_expectancy_cooldown_enabled', False)),
                bool(getattr(self.cfg.execution, 'negative_expectancy_score_penalty_enabled', False)),
                bool(getattr(self.cfg.execution, 'negative_expectancy_open_block_enabled', False)),
                bool(getattr(self.cfg.execution, 'negative_expectancy_fast_fail_open_block_enabled', False)),
            ]
        )
        if not neg_feedback_enabled:
            return {}
        try:
            whitelist_symbols = sorted(self._live_symbol_whitelist)
            open_position_symbols = sorted(
                {
                    str(getattr(pos, "symbol", "") or "").strip()
                    for pos in (positions or [])
                    if str(getattr(pos, "symbol", "") or "").strip()
                }
            )
            normalized_managed_symbols = sorted(
                {
                    str(sym or "").strip()
                    for sym in (managed_symbols or [])
                    if str(sym or "").strip()
                }
            )
            self.negative_expectancy_cooldown.set_scope(
                whitelist_symbols=whitelist_symbols,
                open_position_symbols=open_position_symbols,
                managed_symbols=normalized_managed_symbols,
                config_fingerprint=self._negative_expectancy_config_fingerprint(),
            )
            state = self.negative_expectancy_cooldown.refresh(force=False) or {}
            if audit:
                blocked_n = len((state.get('symbols') or {}))
                stats_n = len((state.get('stats') or {}))
                release_start_ts = state.get("release_start_ts", "not_observable")
                release_warnings = [
                    str(item)
                    for item in (state.get("warnings") or [])
                    if str(item).strip()
                ]
                audit.negative_expectancy_state = {
                    "config_fingerprint": str(state.get("config_fingerprint") or ""),
                    "release_start_ts": release_start_ts,
                    "release_start_ts_status": str(state.get("release_start_ts_status") or ""),
                    "release_start_ts_warning": "; ".join(release_warnings),
                    "state_path": str(getattr(self.negative_expectancy_cooldown.cfg, "state_path", "") or ""),
                    "stats_count": stats_n,
                    "cooldown_active_count": blocked_n,
                }
                audit.add_note(
                    "NegativeExpectancy refresh: "
                    f"stats={stats_n}, cooldown_active={blocked_n}, "
                    f"scope={len((state.get('scope_symbols') or []))}, "
                    f"release_start_ts={release_start_ts}"
                )
                for warning in release_warnings:
                    audit.add_note(f"NegativeExpectancy warning: {warning}")
            return state
        except Exception as e:
            if audit:
                audit.add_note(f"NegativeExpectancy refresh error: {e}")
            return {}

    @staticmethod
    def _negative_expectancy_bps(stat: Dict[str, Any], *, fast_fail: bool = False) -> float:
        if not isinstance(stat, dict):
            return 0.0
        keys = (
            ("fast_fail_net_expectancy_bps", "fast_fail_expectancy_bps", "fast_fail_gross_expectancy_bps")
            if fast_fail
            else ("net_expectancy_bps", "expectancy_bps", "gross_expectancy_bps")
        )
        for key in keys:
            if key in stat and stat.get(key) is not None:
                try:
                    return float(stat.get(key) or 0.0)
                except Exception:
                    continue
        return 0.0

    @staticmethod
    def _negative_expectancy_usdt(stat: Dict[str, Any]) -> float:
        if not isinstance(stat, dict):
            return 0.0
        for key in ("net_expectancy_usdt", "expectancy_usdt", "gross_expectancy_usdt"):
            if key in stat and stat.get(key) is not None:
                try:
                    return float(stat.get(key) or 0.0)
                except Exception:
                    continue
        return 0.0

    def _apply_negative_expectancy_score_penalty(
        self,
        alpha: AlphaSnapshot,
        neg_cd_state: Dict[str, Any],
        audit: Optional[DecisionAudit] = None,
    ) -> AlphaSnapshot:
        if not getattr(alpha, 'scores', None):
            return alpha
        if not bool(getattr(self.cfg.execution, 'negative_expectancy_score_penalty_enabled', False)):
            return alpha

        stats_map = (neg_cd_state.get('stats') or {}) if isinstance(neg_cd_state, dict) else {}
        if not stats_map:
            return alpha

        min_cycles = int(getattr(self.cfg.execution, 'negative_expectancy_score_penalty_min_closed_cycles', 2) or 2)
        floor_bps = float(
            _coalesce(getattr(self.cfg.execution, 'negative_expectancy_score_penalty_floor_bps', 5.0), 5.0)
        )
        penalty_per_bps = float(
            _coalesce(getattr(self.cfg.execution, 'negative_expectancy_score_penalty_per_bps', 0.015), 0.015)
        )
        penalty_cap = float(
            _coalesce(getattr(self.cfg.execution, 'negative_expectancy_score_penalty_max', 0.60), 0.60)
        )
        ff_min_cycles = int(
            getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_min_closed_cycles", 2) or 2
        )
        ff_floor_bps = float(
            getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_floor_bps", 0.0) or 0.0
        )

        adjusted_scores = dict(alpha.scores or {})
        penalized = []
        for sym, raw_score in list(adjusted_scores.items()):
            score = float(raw_score)
            stat = stats_map.get(sym)
            if not isinstance(stat, dict):
                continue
            closed_cycles = int(stat.get('closed_cycles') or 0)
            expectancy_bps = self._negative_expectancy_bps(stat)
            shortfall_bps = 0.0
            if closed_cycles >= min_cycles:
                shortfall_bps = max(0.0, float(floor_bps) - expectancy_bps)

            ff_closed_cycles = int(stat.get("fast_fail_closed_cycles") or 0)
            ff_expectancy_bps = self._negative_expectancy_bps(stat, fast_fail=True)
            ff_shortfall_bps = 0.0
            if ff_closed_cycles >= ff_min_cycles:
                ff_shortfall_bps = max(0.0, float(ff_floor_bps) - ff_expectancy_bps)

            total_shortfall_bps = shortfall_bps + ff_shortfall_bps
            if total_shortfall_bps <= 0.0:
                continue
            penalty = min(float(penalty_cap), float(total_shortfall_bps) * float(penalty_per_bps))
            if penalty <= 0.0:
                continue
            adjusted_scores[sym] = score - penalty
            penalized.append(
                (
                    sym,
                    score,
                    adjusted_scores[sym],
                    expectancy_bps,
                    closed_cycles,
                    ff_expectancy_bps,
                    ff_closed_cycles,
                    penalty,
                )
            )

            raw_bucket = alpha.raw_factors.setdefault(sym, {})
            raw_bucket["negative_expectancy_gross_bps"] = float(stat.get("gross_expectancy_bps", stat.get("expectancy_bps") or 0.0))
            raw_bucket["negative_expectancy_net_bps"] = float(stat.get("net_expectancy_bps", expectancy_bps))
            raw_bucket["negative_expectancy_bps"] = expectancy_bps
            raw_bucket["negative_expectancy_closed_cycles"] = float(closed_cycles)
            raw_bucket["negative_expectancy_fast_fail_gross_bps"] = float(
                stat.get("fast_fail_gross_expectancy_bps", stat.get("fast_fail_expectancy_bps") or 0.0)
            )
            raw_bucket["negative_expectancy_fast_fail_net_bps"] = float(
                stat.get("fast_fail_net_expectancy_bps", ff_expectancy_bps)
            )
            raw_bucket["negative_expectancy_fast_fail_bps"] = ff_expectancy_bps
            raw_bucket["negative_expectancy_fast_fail_closed_cycles"] = float(ff_closed_cycles)
            z_bucket = alpha.z_factors.setdefault(sym, {})
            z_bucket["negative_expectancy_score_penalty"] = -float(penalty)

        if penalized and audit:
            for (
                sym,
                score_before,
                score_after,
                expectancy_bps,
                closed_cycles,
                ff_expectancy_bps,
                ff_closed_cycles,
                penalty,
            ) in penalized:
                audit.record_count("negative_expectancy_score_penalty", symbol=sym)
                audit.add_note(
                    "NegativeExpectancy penalty: "
                    f"{sym} cycles={closed_cycles} expectancy_bps={expectancy_bps:.2f} "
                    f"fast_fail_cycles={ff_closed_cycles} fast_fail_bps={ff_expectancy_bps:.2f} "
                    f"penalty={penalty:.4f} score={score_before:.4f}->{score_after:.4f}"
                )

        alpha.scores = adjusted_scores
        return alpha

    def _apply_negative_expectancy_rank_guard(
        self,
        alpha: AlphaSnapshot,
        neg_cd_state: Dict[str, Any],
        *,
        positions: List[Position],
        current_auto_risk_level: Optional[str] = None,
        regime_state_str: Optional[str] = None,
        audit: Optional[DecisionAudit] = None,
    ) -> AlphaSnapshot:
        if not getattr(alpha, 'scores', None):
            return alpha

        stats_map = (neg_cd_state.get('stats') or {}) if isinstance(neg_cd_state, dict) else {}
        cooldown_map = (neg_cd_state.get('symbols') or {}) if isinstance(neg_cd_state, dict) else {}
        if not stats_map and not cooldown_map:
            return alpha

        held_symbols = {
            str(getattr(p, 'symbol', '') or '')
            for p in (positions or [])
            if float(getattr(p, 'qty', 0.0) or 0.0) > 0.0
        }
        if current_auto_risk_level is None:
            current_auto_risk_level = self._load_current_auto_risk_level()
        regime_state = str(regime_state_str or (audit.regime if audit is not None else "") or "")
        strategy_signal_lookup = self._resolve_strategy_signal_lookup(audit)
        adjusted_scores = dict(alpha.scores or {})
        if not adjusted_scores:
            return alpha

        cooldown_enabled = bool(getattr(self.cfg.execution, "negative_expectancy_cooldown_enabled", False))
        open_block_enabled = bool(getattr(self.cfg.execution, "negative_expectancy_open_block_enabled", False))
        ff_block_enabled = bool(getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_enabled", False))
        if not any([cooldown_enabled, open_block_enabled, ff_block_enabled]):
            return alpha

        base_min_score = min(float(v) for v in adjusted_scores.values())
        demoted = []
        demote_index = 0
        for sym, raw_score in list(adjusted_scores.items()):
            if sym in held_symbols:
                continue

            reason = None
            stat = stats_map.get(sym) if isinstance(stats_map.get(sym), dict) else {}
            cooldown_active = cooldown_enabled and isinstance(cooldown_map.get(sym), dict)
            if cooldown_active:
                reason = "negative_expectancy_cooldown"
            else:
                if open_block_enabled:
                    min_cycles = int(
                        getattr(self.cfg.execution, "negative_expectancy_open_block_min_closed_cycles", 2) or 2
                    )
                    floor_bps = float(
                        _coalesce(getattr(self.cfg.execution, "negative_expectancy_open_block_floor_bps", 5.0), 5.0)
                    )
                    closed_cycles = int((stat or {}).get("closed_cycles") or 0)
                    expectancy_bps = self._negative_expectancy_bps(stat or {})
                    if closed_cycles >= min_cycles and expectancy_bps < floor_bps:
                        reason = "negative_expectancy_open_block"
                if reason is None and ff_block_enabled:
                    ff_min_cycles = int(
                        getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_min_closed_cycles", 2) or 2
                    )
                    ff_floor_bps = float(
                        getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_floor_bps", 0.0) or 0.0
                    )
                    ff_closed_cycles = int((stat or {}).get("fast_fail_closed_cycles") or 0)
                    ff_expectancy_bps = self._negative_expectancy_bps(stat or {}, fast_fail=True)
                    if ff_closed_cycles >= ff_min_cycles and ff_expectancy_bps < ff_floor_bps:
                        reason = "negative_expectancy_fast_fail_open_block"

            if reason is None:
                continue

            if reason == "negative_expectancy_fast_fail_open_block":
                soften, soften_ctx = self._should_soften_fast_fail_with_market_impulse(
                    symbol=sym,
                    stat=stat or {},
                    strategy_signal_lookup=strategy_signal_lookup,
                    current_auto_risk_level=current_auto_risk_level,
                    regime_state_str=regime_state,
                )
                if soften:
                    if audit:
                        audit.record_count("negative_expectancy_fast_fail_softened_count", symbol=sym)
                        audit.add_note(
                            "negative_expectancy_fast_fail_softened_by_market_impulse: "
                            f"{sym} trend_buy_count={int(soften_ctx.get('trend_buy_count') or 0)} "
                            f"btc_trend_score={soften_ctx.get('btc_trend_score')} "
                            f"fast_fail_closed_cycles={int(soften_ctx.get('fast_fail_closed_cycles') or 0)} "
                            f"net_expectancy_bps={float(soften_ctx.get('net_expectancy_bps') or 0.0):.2f}"
                        )
                    continue

            demoted_score = min(float(raw_score) - 1.0, base_min_score - 0.05 - demote_index * 0.001)
            adjusted_scores[sym] = demoted_score
            demote_index += 1
            demoted.append((sym, float(raw_score), float(demoted_score), reason))
            if reason == "negative_expectancy_fast_fail_open_block" and audit:
                audit.record_count("negative_expectancy_fast_fail_hard_block_count", symbol=sym)

            raw_bucket = alpha.raw_factors.setdefault(sym, {})
            raw_bucket["negative_expectancy_rank_guard_reason"] = reason
            z_bucket = alpha.z_factors.setdefault(sym, {})
            z_bucket["negative_expectancy_rank_guard"] = -1.0

        if demoted and audit:
            for sym, before, after, reason in demoted:
                audit.record_gate(reason, symbol=sym)
                audit.add_note(
                    "NegativeExpectancy rank-guard: "
                    f"{sym} reason={reason} score={before:.4f}->{after:.4f}"
                )

        alpha.scores = adjusted_scores
        return alpha

    @staticmethod
    def _score_rank_map(scores: Dict[str, float]) -> Dict[str, int]:
        ranked = sorted(
            ((str(sym), float(score or 0.0)) for sym, score in (scores or {}).items()),
            key=lambda item: (item[1], item[0]),
            reverse=True,
        )
        return {sym: idx + 1 for idx, (sym, _) in enumerate(ranked)}

    @staticmethod
    def _top_score_rows(scores: Dict[str, float], rank_map: Dict[str, int], limit: int) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for sym, _ in sorted(
            ((str(sym), float(score or 0.0)) for sym, score in (scores or {}).items()),
            key=lambda item: (item[1], item[0]),
            reverse=True,
        )[: max(int(limit or 0), 0)]:
            rows.append(
                {
                    "symbol": sym,
                    "score": float(scores.get(sym, 0.0) or 0.0),
                    "rank": int(rank_map.get(sym, 0) or 0),
                }
            )
        return rows

    @staticmethod
    def _mean_return_bps(symbols: List[str], returns_by_symbol: Dict[str, float]) -> Optional[float]:
        vals = [float(returns_by_symbol[sym]) for sym in symbols if sym in returns_by_symbol]
        if not vals:
            return None
        return float(sum(vals) / len(vals) * 10000.0)

    @staticmethod
    def _impact_tone(value_bps: Optional[float], *, positive_th: float = 5.0, negative_th: float = -5.0) -> str:
        if value_bps is None:
            return "insufficient"
        if value_bps >= positive_th:
            return "positive"
        if value_bps <= negative_th:
            return "negative"
        return "mixed"

    def _resolve_ml_impact_path(self, attr_name: str, default_name: str) -> Path:
        ml_cfg = getattr(self.cfg.alpha, "ml_factor", None)
        raw_path = getattr(ml_cfg, attr_name, None) if ml_cfg is not None else None
        raw = str(raw_path or "").strip()
        if not raw or raw == default_name:
            order_store_path = Path(
                str(getattr(self.cfg.execution, "order_store_path", "reports/orders.sqlite"))
            )
            if not order_store_path.is_absolute():
                order_store_path = (REPORTS_DIR.parent / order_store_path).resolve()
            name = Path(default_name).name
            suffix = ".jsonl" if name.endswith(".jsonl") else Path(name).suffix
            base_name = name[: -len(suffix)] if suffix else name
            return derive_runtime_named_artifact_path(order_store_path, base_name, suffix).resolve()

        path = Path(raw)
        if not path.is_absolute():
            path = (REPORTS_DIR.parent / path).resolve()
        return path

    def _build_ml_audit_overview(
        self,
        alpha: AlphaSnapshot,
        *,
        impact_summary: Optional[Dict[str, Any]] = None,
        limit: int = 3,
    ) -> Dict[str, Any]:
        ml_runtime = dict(getattr(alpha, "ml_runtime", {}) or {})
        base_scores = dict(getattr(alpha, "base_scores", {}) or {})
        final_scores = dict(getattr(alpha, "ml_attribution_scores", {}) or getattr(alpha, "scores", {}) or {})
        overlay_scores = dict(getattr(alpha, "ml_overlay_scores", {}) or {})
        overlay_raw_scores = dict(getattr(alpha, "ml_overlay_raw_scores", {}) or {})

        configured_enabled = bool(ml_runtime.get("configured_enabled", False))
        promoted = bool(ml_runtime.get("promotion_passed", False))
        live_active = bool(ml_runtime.get("used_in_latest_snapshot", False))
        overlay_mode = str(ml_runtime.get("overlay_mode") or "disabled")
        prediction_count = int(ml_runtime.get("prediction_count", 0) or 0)
        if not configured_enabled and not promoted and not live_active and not overlay_scores and not overlay_raw_scores:
            return {}

        base_rank = self._score_rank_map(base_scores)
        final_rank = self._score_rank_map(final_scores)
        symbols = sorted(set(final_scores.keys()) | set(base_scores.keys()) | set(overlay_scores.keys()) | set(overlay_raw_scores.keys()))

        top_contributors: List[Dict[str, Any]] = []
        top_promoted: List[Dict[str, Any]] = []
        top_suppressed: List[Dict[str, Any]] = []
        for sym in symbols:
            base_score = float(base_scores.get(sym, 0.0) or 0.0)
            final_score = float(final_scores.get(sym, 0.0) or 0.0)
            delta = float(final_score - base_score)
            raw_z = float(overlay_raw_scores.get(sym, 0.0) or 0.0)
            overlay_score = float(overlay_scores.get(sym, 0.0) or 0.0)
            base_pos = int(base_rank.get(sym, len(base_rank) + 1 if base_rank else 0) or 0)
            final_pos = int(final_rank.get(sym, len(final_rank) + 1 if final_rank else 0) or 0)
            rank_delta = int(base_pos - final_pos) if base_pos and final_pos else 0

            if abs(raw_z) > 1e-9 or abs(overlay_score) > 1e-9 or abs(delta) > 1e-9:
                top_contributors.append(
                    {
                        "symbol": sym,
                        "ml_zscore": round(raw_z, 4),
                        "ml_overlay_score": round(overlay_score, 4),
                        "base_score": round(base_score, 4),
                        "final_score": round(final_score, 4),
                        "score_delta": round(delta, 4),
                        "base_rank": base_pos,
                        "final_rank": final_pos,
                        "rank_delta": rank_delta,
                    }
                )
            if rank_delta > 0:
                top_promoted.append(
                    {
                        "symbol": sym,
                        "base_rank": base_pos,
                        "final_rank": final_pos,
                        "rank_delta": rank_delta,
                        "score_delta": round(delta, 4),
                        "ml_overlay_score": round(overlay_score, 4),
                    }
                )
            elif rank_delta < 0:
                top_suppressed.append(
                    {
                        "symbol": sym,
                        "base_rank": base_pos,
                        "final_rank": final_pos,
                        "rank_delta": rank_delta,
                        "score_delta": round(delta, 4),
                        "ml_overlay_score": round(overlay_score, 4),
                    }
                )

        top_contributors.sort(
            key=lambda item: (
                abs(float(item.get("score_delta", 0.0) or 0.0)),
                abs(float(item.get("ml_zscore", 0.0) or 0.0)),
                str(item.get("symbol") or ""),
            ),
            reverse=True,
        )
        top_promoted.sort(
            key=lambda item: (
                int(item.get("rank_delta", 0) or 0),
                abs(float(item.get("score_delta", 0.0) or 0.0)),
                str(item.get("symbol") or ""),
            ),
            reverse=True,
        )
        top_suppressed.sort(
            key=lambda item: (
                abs(int(item.get("rank_delta", 0) or 0)),
                abs(float(item.get("score_delta", 0.0) or 0.0)),
                str(item.get("symbol") or ""),
            ),
            reverse=True,
        )

        last_step = (impact_summary or {}).get("last_step") or {}
        rolling = (impact_summary or {}).get("rolling_24h") or {}
        return {
            "configured_enabled": configured_enabled,
            "promoted": promoted,
            "live_active": live_active,
            "prediction_count": prediction_count,
            "active_symbols": int(len(overlay_scores) or prediction_count or 0),
            "coverage_count": int(len(overlay_scores) or prediction_count or 0),
            "ml_weight": float(ml_runtime.get("ml_weight", 0.0) or 0.0),
            "configured_ml_weight": float(ml_runtime.get("configured_ml_weight", ml_runtime.get("ml_weight", 0.0)) or 0.0),
            "effective_ml_weight": float(ml_runtime.get("effective_ml_weight", ml_runtime.get("ml_weight", 0.0)) or 0.0),
            "overlay_mode": overlay_mode,
            "online_control_reason": str(ml_runtime.get("online_control_reason") or ""),
            "reason": str(ml_runtime.get("reason") or ""),
            "last_update": ml_runtime.get("ts"),
            "overlay_transform": ml_runtime.get("overlay_transform"),
            "overlay_transform_scale": ml_runtime.get("overlay_transform_scale"),
            "overlay_transform_max_abs": ml_runtime.get("overlay_transform_max_abs"),
            "overlay_score_max_abs": ml_runtime.get("overlay_score_max_abs"),
            "lifted_into_top3": int(sum(1 for item in top_promoted if int(item.get("final_rank", 99) or 99) <= 3)),
            "pushed_out_of_top3": int(sum(1 for item in top_suppressed if int(item.get("base_rank", 99) or 99) <= 3)),
            "top_contributors": top_contributors[:limit],
            "top_promoted": top_promoted[:limit],
            "top_suppressed": top_suppressed[:limit],
            "impact_status": str((rolling.get("status") or last_step.get("status") or "insufficient")),
            "last_step": last_step,
            "rolling_24h": rolling,
            "rolling_48h": impact_summary.get("rolling_48h", {}) if isinstance(impact_summary, dict) else {},
        }

    def _update_ml_impact_monitor(
        self,
        alpha: AlphaSnapshot,
        market_data_1h: Dict[str, MarketSeries],
        *,
        snapshot_ts_ms: int,
    ) -> Dict[str, Any]:
        ml_cfg = getattr(self.cfg.alpha, "ml_factor", None)
        final_scores = dict(getattr(alpha, "ml_attribution_scores", {}) or getattr(alpha, "scores", {}) or {})
        base_scores = dict(getattr(alpha, "base_scores", {}) or {})
        overlay_scores = dict(getattr(alpha, "ml_overlay_scores", {}) or {})
        overlay_raw_scores = dict(getattr(alpha, "ml_overlay_raw_scores", {}) or {})
        if ml_cfg is None or not final_scores or not base_scores or not (overlay_scores or overlay_raw_scores):
            return {}

        state_path = self._resolve_ml_impact_path("impact_state_path", "reports/ml_overlay_impact_state.json")
        history_path = self._resolve_ml_impact_path("impact_history_path", "reports/ml_overlay_impact_history.jsonl")
        summary_path = self._resolve_ml_impact_path("impact_summary_path", "reports/ml_overlay_impact.json")
        top_n = int(getattr(ml_cfg, "impact_eval_top_n", 3) or 3)

        closes: Dict[str, float] = {}
        for sym, series in (market_data_1h or {}).items():
            if not getattr(series, "close", None):
                continue
            normalized_series = _normalize_market_series(series)
            if not normalized_series.close:
                continue
            closes[str(sym)] = float(normalized_series.close[-1])
        base_rank = self._score_rank_map(base_scores)
        final_rank = self._score_rank_map(final_scores)

        current_state = {
            "ts_ms": int(snapshot_ts_ms),
            "base_scores": {str(sym): float(val) for sym, val in base_scores.items()},
            "final_scores": {str(sym): float(val) for sym, val in final_scores.items()},
            "base_rank": {str(sym): int(rank) for sym, rank in base_rank.items()},
            "final_rank": {str(sym): int(rank) for sym, rank in final_rank.items()},
            "overlay_scores": {str(sym): float(val) for sym, val in overlay_scores.items()},
            "overlay_raw_scores": {str(sym): float(val) for sym, val in overlay_raw_scores.items()},
            "closes": {str(sym): float(px) for sym, px in closes.items()},
        }

        previous_state: Dict[str, Any] = {}
        if state_path.exists():
            try:
                previous_state = json.loads(state_path.read_text(encoding="utf-8"))
            except Exception:
                previous_state = {}

        step_summary: Dict[str, Any] = {}
        try:
            prev_ts = int(previous_state.get("ts_ms") or 0)
            if prev_ts > 0 and prev_ts < int(snapshot_ts_ms):
                prev_closes = {
                    str(sym): float(px)
                    for sym, px in ((previous_state.get("closes") or {}).items())
                    if float(px or 0.0) > 0.0
                }
                returns_by_symbol = {
                    sym: float(closes[sym]) / float(prev_px) - 1.0
                    for sym, prev_px in prev_closes.items()
                    if sym in closes and float(prev_px) > 0.0 and float(closes[sym]) > 0.0
                }
                prev_base_scores = {
                    str(sym): float(score)
                    for sym, score in ((previous_state.get("base_scores") or {}).items())
                }
                prev_final_scores = {
                    str(sym): float(score)
                    for sym, score in ((previous_state.get("final_scores") or {}).items())
                }
                prev_base_rank = {
                    str(sym): int(rank)
                    for sym, rank in ((previous_state.get("base_rank") or {}).items())
                } or self._score_rank_map(prev_base_scores)
                prev_final_rank = {
                    str(sym): int(rank)
                    for sym, rank in ((previous_state.get("final_rank") or {}).items())
                } or self._score_rank_map(prev_final_scores)

                base_top = [
                    row["symbol"]
                    for row in self._top_score_rows(prev_base_scores, prev_base_rank, top_n)
                    if row["symbol"] in returns_by_symbol
                ]
                final_top = [
                    row["symbol"]
                    for row in self._top_score_rows(prev_final_scores, prev_final_rank, top_n)
                    if row["symbol"] in returns_by_symbol
                ]

                promoted = []
                suppressed = []
                for sym in sorted(set(prev_base_scores.keys()) | set(prev_final_scores.keys())):
                    if sym not in returns_by_symbol:
                        continue
                    base_pos = int(prev_base_rank.get(sym, 0) or 0)
                    final_pos = int(prev_final_rank.get(sym, 0) or 0)
                    if base_pos <= 0 or final_pos <= 0:
                        continue
                    rank_delta = int(base_pos - final_pos)
                    row = {
                        "symbol": sym,
                        "base_rank": base_pos,
                        "final_rank": final_pos,
                        "rank_delta": rank_delta,
                        "return_bps": round(float(returns_by_symbol[sym]) * 10000.0, 2),
                    }
                    if rank_delta > 0:
                        promoted.append(row)
                    elif rank_delta < 0:
                        suppressed.append(row)

                promoted.sort(key=lambda item: (int(item["rank_delta"]), float(item["return_bps"])), reverse=True)
                suppressed.sort(key=lambda item: (abs(int(item["rank_delta"])), abs(float(item["return_bps"]))), reverse=True)

                base_top_return_bps = self._mean_return_bps(base_top, returns_by_symbol)
                final_top_return_bps = self._mean_return_bps(final_top, returns_by_symbol)
                promoted_return_bps = self._mean_return_bps([row["symbol"] for row in promoted[:top_n]], returns_by_symbol)
                suppressed_return_bps = self._mean_return_bps([row["symbol"] for row in suppressed[:top_n]], returns_by_symbol)
                delta_bps = (
                    float(final_top_return_bps - base_top_return_bps)
                    if final_top_return_bps is not None and base_top_return_bps is not None
                    else None
                )
                promoted_minus_suppressed_bps = (
                    float(promoted_return_bps - suppressed_return_bps)
                    if promoted_return_bps is not None and suppressed_return_bps is not None
                    else None
                )

                step_summary = {
                    "from_ts_ms": prev_ts,
                    "to_ts_ms": int(snapshot_ts_ms),
                    "top_n": top_n,
                    "base_top_symbols": base_top,
                    "final_top_symbols": final_top,
                    "base_top_return_bps": round(float(base_top_return_bps), 2) if base_top_return_bps is not None else None,
                    "final_top_return_bps": round(float(final_top_return_bps), 2) if final_top_return_bps is not None else None,
                    "delta_bps": round(float(delta_bps), 2) if delta_bps is not None else None,
                    "promoted_symbols": promoted[:top_n],
                    "suppressed_symbols": suppressed[:top_n],
                    "promoted_return_bps": round(float(promoted_return_bps), 2) if promoted_return_bps is not None else None,
                    "suppressed_return_bps": round(float(suppressed_return_bps), 2) if suppressed_return_bps is not None else None,
                    "promoted_minus_suppressed_bps": round(float(promoted_minus_suppressed_bps), 2)
                    if promoted_minus_suppressed_bps is not None
                    else None,
                }
                step_summary["status"] = self._impact_tone(
                    step_summary.get("delta_bps"),
                )

                history_path.parent.mkdir(parents=True, exist_ok=True)
                with history_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(step_summary, ensure_ascii=False) + "\n")
        except Exception:
            step_summary = {}

        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(current_state, ensure_ascii=False, indent=2), encoding="utf-8")

        history_rows: List[Dict[str, Any]] = []
        if history_path.exists():
            try:
                for line in history_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    if int(row.get("to_ts_ms") or 0) >= int(snapshot_ts_ms) - 48 * 3600 * 1000:
                        history_rows.append(row)
            except Exception:
                history_rows = []

        def _rolling_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
            delta_vals = [
                float(row.get("delta_bps"))
                for row in rows
                if row.get("delta_bps") is not None
            ]
            promoted_vals = [
                float(row.get("promoted_minus_suppressed_bps"))
                for row in rows
                if row.get("promoted_minus_suppressed_bps") is not None
            ]
            rolling_delta = float(sum(delta_vals) / len(delta_vals)) if delta_vals else None
            rolling_promoted = float(sum(promoted_vals) / len(promoted_vals)) if promoted_vals else None
            positive_ratio = (
                float(sum(1 for row in rows if float(row.get("delta_bps") or 0.0) > 0.0) / len(rows))
                if rows
                else None
            )
            from_candidates = [int(row.get("from_ts_ms") or 0) for row in rows if int(row.get("from_ts_ms") or 0) > 0]
            to_candidates = [int(row.get("to_ts_ms") or 0) for row in rows if int(row.get("to_ts_ms") or 0) > 0]
            coverage_hours = (
                max(0.0, (max(to_candidates) - min(from_candidates)) / 3_600_000.0)
                if from_candidates and to_candidates and max(to_candidates) >= min(from_candidates)
                else 0.0
            )
            return {
                "points": len(rows),
                "topn_delta_mean_bps": round(float(rolling_delta), 2) if rolling_delta is not None else None,
                "promoted_minus_suppressed_mean_bps": round(float(rolling_promoted), 2)
                if rolling_promoted is not None
                else None,
                "positive_ratio": round(float(positive_ratio), 4) if positive_ratio is not None else None,
                "coverage_hours": round(float(coverage_hours), 2),
                "status": self._impact_tone(rolling_delta),
            }

        rolling_24h_rows = [
            row for row in history_rows
            if int(row.get("to_ts_ms") or 0) >= int(snapshot_ts_ms) - 24 * 3600 * 1000
        ]
        rolling_24h = _rolling_stats(rolling_24h_rows)
        rolling_48h = _rolling_stats(history_rows)
        ml_runtime = dict(getattr(alpha, "ml_runtime", {}) or {})
        summary = {
            "updated_at": utc_now_iso(),
            "last_step": step_summary,
            "rolling_24h": rolling_24h,
            "rolling_48h": rolling_48h,
            "overlay_mode": str(ml_runtime.get("overlay_mode") or ""),
            "configured_ml_weight": float(ml_runtime.get("configured_ml_weight", ml_runtime.get("ml_weight", 0.0)) or 0.0),
            "effective_ml_weight": float(ml_runtime.get("effective_ml_weight", ml_runtime.get("ml_weight", 0.0)) or 0.0),
            "online_control_reason": str(ml_runtime.get("online_control_reason") or ""),
        }
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary

    @staticmethod
    def _rebalance_turnover_priority(order: Order) -> Tuple[int, float, float, str]:
        drift = abs(float(((order.meta or {}).get("drift", 0.0) or 0.0)))
        notional = abs(float(order.notional_usdt or 0.0))
        side = str(order.side or "").lower()
        intent = str(order.intent or "").upper()
        # For buys, preserve top-ranked symbols first, then prefer fresh opens over add-ons.
        score_rank = int(((order.meta or {}).get("score_rank", 1_000_000) or 1_000_000))
        open_rank = 0 if side == "buy" and intent == "OPEN_LONG" else 1
        if side == "buy":
            return (score_rank, open_rank, -drift, notional, str(order.symbol))
        return (open_rank, -drift, notional, str(order.symbol))

    def _cap_rebalance_side(
        self,
        orders: List[Order],
        *,
        cap_notional: float,
    ) -> Tuple[List[Order], List[Order], float, List[Order]]:
        if not orders:
            return [], [], 0.0, []

        ranked = sorted(orders, key=self._rebalance_turnover_priority)
        kept_ranked: List[Order] = []
        clipped_ranked: List[Order] = []
        used = 0.0
        for order in ranked:
            notional = abs(float(order.notional_usdt or 0.0))
            remaining = float(cap_notional) - float(used)
            if remaining <= 0.0:
                continue
            if (used + notional) <= cap_notional:
                kept_ranked.append(order)
                used += notional
                continue

            side = str(order.side or "").lower()
            intent = str(order.intent or "").upper()
            clip_floor = float(
                ((order.meta or {}).get("clip_min_notional", getattr(self.cfg.budget, "min_trade_notional_base", 0.0)) or 0.0)
            )
            if (
                side == "buy"
                and intent == "OPEN_LONG"
                and remaining >= clip_floor
                and remaining < notional
            ):
                meta = dict(order.meta or {})
                meta["turnover_cap_clipped"] = True
                meta["turnover_cap_original_notional"] = float(order.notional_usdt or 0.0)
                meta["turnover_cap_clipped_notional"] = float(remaining)
                order.meta = meta
                order.notional_usdt = float(remaining)
                kept_ranked.append(order)
                clipped_ranked.append(order)
                used += float(remaining)

        keep_ids = {id(order) for order in kept_ranked}
        kept = [order for order in orders if id(order) in keep_ids]
        dropped = [order for order in orders if id(order) not in keep_ids]
        return kept, dropped, float(used), clipped_ranked

    def _apply_rebalance_turnover_cap(
        self,
        rebalance_orders: List[Order],
        *,
        equity_raw: float,
    ) -> Tuple[List[Order], List[Order], Dict[str, float]]:
        protected_orders = [
            order
            for order in (rebalance_orders or [])
            if bool(((order.meta or {}).get("bypass_turnover_cap_for_exit", False)))
        ]
        cap_eligible_orders = [
            order
            for order in (rebalance_orders or [])
            if not bool(((order.meta or {}).get("bypass_turnover_cap_for_exit", False)))
        ]
        max_rb_turnover = getattr(self.cfg.execution, "max_rebalance_turnover_per_cycle", None)
        if (
            max_rb_turnover is None
            or float(max_rb_turnover) <= 0.0
            or float(equity_raw) <= 0.0
            or not cap_eligible_orders
        ):
            return rebalance_orders, [], {}

        cap_notional = float(max_rb_turnover) * float(equity_raw)
        buy_orders = [order for order in cap_eligible_orders if str(order.side or "").lower() == "buy"]
        sell_orders = [order for order in cap_eligible_orders if str(order.side or "").lower() == "sell"]
        total_buy = float(sum(abs(float(order.notional_usdt or 0.0)) for order in buy_orders))
        total_sell = float(sum(abs(float(order.notional_usdt or 0.0)) for order in sell_orders))
        effective_turnover = float(max(total_buy, total_sell))
        stats = {
            "cap_notional": float(cap_notional),
            "total_buy_notional": float(total_buy),
            "total_sell_notional": float(total_sell),
            "effective_turnover_notional": float(effective_turnover),
            "bypassed_exit_count": float(len(protected_orders)),
            "bypassed_exit_notional": float(
                sum(abs(float(order.notional_usdt or 0.0)) for order in protected_orders)
            ),
        }

        if effective_turnover <= cap_notional:
            return rebalance_orders, [], stats

        kept_buys, dropped_buys, kept_buy, clipped_buys = self._cap_rebalance_side(
            buy_orders,
            cap_notional=cap_notional,
        )
        kept_sells, dropped_sells, kept_sell, clipped_sells = self._cap_rebalance_side(
            sell_orders,
            cap_notional=cap_notional,
        )
        keep_ids = {id(order) for order in (protected_orders + kept_buys + kept_sells)}
        protected_ids = {id(order) for order in protected_orders}
        kept_orders = [order for order in rebalance_orders if id(order) in keep_ids]
        dropped_orders = [
            order
            for order in rebalance_orders
            if id(order) not in keep_ids and id(order) not in protected_ids
        ]
        clipped_orders = list(clipped_buys) + list(clipped_sells)
        stats.update(
            {
                "kept_buy_notional": float(kept_buy),
                "kept_sell_notional": float(kept_sell),
                "dropped_count": float(len(dropped_orders)),
                "dropped_buy_count": float(len(dropped_buys)),
                "dropped_sell_count": float(len(dropped_sells)),
                "clipped_count": float(len(clipped_orders)),
                "clipped_buy_count": float(len(clipped_buys)),
                "clipped_sell_count": float(len(clipped_sells)),
            }
        )
        return kept_orders, dropped_orders, stats

    def run(
        self,
        market_data_1h: Dict[str, MarketSeries],
        positions: List[Position],
        cash_usdt: float,
        equity_peak_usdt: float,
        run_logger=None,
        audit: Optional[DecisionAudit] = None,
        precomputed_alpha: Optional[AlphaSnapshot] = None,
        precomputed_regime: Optional[RegimeResult] = None,
    ) -> PipelineOutput:
        """运行完整的交易流水线
        
        Pipeline流程:
        1. 市场状态检测 (Regime)
        2. Alpha因子计算
        3. 投资组合分配
        4. 风控检查
        5. 退出策略评估
        6. 订单生成
        
        Args:
            market_data_1h: 1小时K线数据
            positions: 当前持仓
            cash_usdt: 现金余额
            equity_peak_usdt: 权益峰值
            run_logger: 运行日志记录器
            audit: 决策审计对象
            
        Returns:
            流水线输出 (Alpha, Regime, Portfolio, Orders)
        """
        # mark first
        store = None
        # 严谨的类型检查：确保positions是列表且元素有symbol属性
        if positions is not None and isinstance(positions, (list, tuple)) and len(positions) > 0:
            first_pos = positions[0]
            if hasattr(first_pos, 'symbol'):
                pass  # 正常情况
            elif isinstance(first_pos, dict) and 'symbol' in first_pos:
                pass  # dict格式也接受
            else:
                # 类型不匹配，记录警告
                if run_logger:
                    run_logger.warning(f"[Pipeline] positions格式异常: {type(first_pos)}")
        # caller can pass store via run_logger hook if desired; for now, marking is done by main.

        run_id = ""
        if run_logger is not None:
            try:
                run_id = Path(getattr(run_logger, 'run_dir', '')).name
            except Exception:
                run_id = ""
        if not run_id and audit is not None:
            try:
                run_id = str(getattr(audit, 'run_id', '') or '').strip()
            except Exception:
                run_id = ""
        if not run_id:
            run_id = str(os.getenv("V5_RUN_ID", "") or "").strip()
        self.alpha_engine.set_run_id(run_id)
        self.portfolio_engine.set_run_id(run_id)

        if self._live_symbol_whitelist:
            dropped_market_symbols = sorted(
                sym for sym in market_data_1h.keys() if sym not in self._live_symbol_whitelist
            )
            if dropped_market_symbols:
                self._record_live_whitelist_drop(
                    audit=audit,
                    stage="market_data",
                    dropped_symbols=dropped_market_symbols,
                )
            market_data_1h = {
                sym: series
                for sym, series in (market_data_1h or {}).items()
                if sym in self._live_symbol_whitelist
            }

            dropped_position_symbols = sorted(
                {
                    str(getattr(p, "symbol", "") or "")
                    for p in (positions or [])
                    if str(getattr(p, "symbol", "") or "").strip()
                    and str(getattr(p, "symbol", "") or "") not in self._live_symbol_whitelist
                }
            )
            if dropped_position_symbols:
                self._record_live_whitelist_drop(
                    audit=audit,
                    stage="positions",
                    dropped_symbols=dropped_position_symbols,
                )
            positions = [
                p
                for p in (positions or [])
                if str(getattr(p, "symbol", "") or "").strip() in self._live_symbol_whitelist
            ]

        market_data_1h = {
            sym: _normalize_market_series(series)
            for sym, series in (market_data_1h or {}).items()
        }

        # 1) Regime detection (needed early if we want regime-aware alpha weights)
        # Regime检测后审计（显式处理空行情，避免 StopIteration）
        if not market_data_1h:
            if audit:
                audit.reject("no_market_data")
                audit.add_note("market_data_1h is empty; cannot run pipeline")
            raise ValueError("market_data_1h is empty")

        if precomputed_regime is not None:
            regime = precomputed_regime
        else:
            btc = market_data_1h.get("BTC/USDT")
            if btc is None:
                btc = next(iter(market_data_1h.values()))
            regime = self.regime_engine.detect(btc)
        
        # 2) Alpha计算（用于短线覆盖判断）
        regime_key = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
        self.alpha_engine.set_regime_context(regime_key)
        alpha = precomputed_alpha if precomputed_alpha is not None else self.alpha_engine.compute_snapshot(market_data_1h)
        alpha = self._filter_live_alpha_snapshot(alpha, audit=audit)
        neg_feedback_enabled = any(
            [
                bool(getattr(self.cfg.execution, 'negative_expectancy_cooldown_enabled', False)),
                bool(getattr(self.cfg.execution, 'negative_expectancy_score_penalty_enabled', False)),
                bool(getattr(self.cfg.execution, 'negative_expectancy_open_block_enabled', False)),
                bool(getattr(self.cfg.execution, 'negative_expectancy_fast_fail_open_block_enabled', False)),
            ]
        )
        neg_cd_enabled = bool(getattr(self.cfg.execution, 'negative_expectancy_cooldown_enabled', False))
        neg_cd_state = self._refresh_negative_expectancy_state_with_scope(
            audit=audit,
            positions=positions,
            managed_symbols=list(alpha.scores.keys()),
        ) if neg_feedback_enabled else {}
        alpha = self._apply_negative_expectancy_score_penalty(alpha, neg_cd_state, audit=audit)
        alpha = self._apply_negative_expectancy_rank_guard(
            alpha,
            neg_cd_state,
            positions=positions,
            current_auto_risk_level=self._load_current_auto_risk_level(),
            regime_state_str=str(regime.state.value if hasattr(regime.state, "value") else regime.state),
            audit=audit,
        )
        
        # 3) 短线交易增强：Risk-Off 机会覆盖 (已禁用 - HMM标签已修复)
        # 当Alpha评分很高时，覆盖Risk-Off状态，允许短线交易
        # 注意：此功能已禁用，因为HMM模型已修复(TrendingUp标签)
        # 如果市场确实是Risk-Off(TrendingDown)，不应该强行买入
        """
        if regime.state.value == "Risk-Off":
            try:
                from src.regime.short_term_override import check_short_term_opportunity
                override = check_short_term_opportunity(
                    alpha_scores=alpha.scores
                )
                if override.should_override:
                    from configs.schema import RegimeState
                    from dataclasses import replace
                    old_state = regime.state
                    regime = replace(regime, state=RegimeState.SIDEWAYS, multiplier=override.new_multiplier)
                    if audit:
                        audit.add_note(f"[ShortTermOverride] {old_state.value} → Sideways: {override.reason}")
                        audit.regime_override = {
                            'from': 'Risk-Off',
                            'to': 'Sideways',
                            'reason': override.reason,
                            'confidence': override.confidence,
                            'new_multiplier': override.new_multiplier
                        }
            except Exception as e:
                if audit:
                    audit.add_note(f"[ShortTermOverride] error: {e}")
        """
        
        if audit:
            audit.regime = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
            audit.regime_multiplier = regime.multiplier
            # 保存Ensemble详情（如果可用）
            if hasattr(regime, 'votes') and regime.votes:
                audit.regime_details = {
                    'method': 'EnsembleRegimeEngine',
                    'votes': regime.votes,
                    'final_score': getattr(regime, 'final_score', 0),
                    'hmm_weight': getattr(self.cfg.regime, 'hmm_weight', 0),
                    'funding_weight': getattr(self.cfg.regime, 'funding_weight', 0),
                    'rss_weight': getattr(self.cfg.regime, 'rss_weight', 0),
                }

        ml_snapshot_ts = self._resolve_ml_snapshot_timestamp_ms(audit=audit)
        ml_impact_summary = self._update_ml_impact_monitor(
            alpha,
            market_data_1h,
            snapshot_ts_ms=ml_snapshot_ts,
        )

        # 2) Alpha计算后审计 (alpha已在前面计算)
        if audit:
            sorted_scores = sorted(alpha.scores.items(), key=lambda x: x[1], reverse=True)
            raw_scores = dict(getattr(alpha, "raw_scores", {}) or {})
            base_scores = dict(getattr(alpha, "base_scores", {}) or {})
            ml_overlay_scores = dict(getattr(alpha, "ml_overlay_scores", {}) or {})
            ml_overlay_raw_scores = dict(getattr(alpha, "ml_overlay_raw_scores", {}) or {})
            base_rank_map = self._score_rank_map(base_scores)
            final_rank_map = self._score_rank_map(alpha.scores)
            audit.ml_signal_overview = self._build_ml_audit_overview(
                alpha,
                impact_summary=ml_impact_summary,
            )
            
            # Add strategy signal audit if multi-strategy is used
            if hasattr(self.alpha_engine, 'use_multi_strategy') and self.alpha_engine.use_multi_strategy:
                try:
                    strategy_payload = self.alpha_engine.get_latest_strategy_signal_payload()
                    strategy_audit_file = self.alpha_engine.strategy_signals_path()
                    strategy_source = "missing"
                    if isinstance(strategy_payload, dict) and strategy_payload.get('strategies'):
                        strategy_source = "memory"
                        if strategy_audit_file is not None and not strategy_audit_file.exists():
                            try:
                                strategy_audit_file.parent.mkdir(parents=True, exist_ok=True)
                                tmp_file = strategy_audit_file.with_suffix('.tmp')
                                tmp_file.write_text(
                                    json.dumps(strategy_payload, indent=2, ensure_ascii=False, default=str),
                                    encoding='utf-8',
                                )
                                tmp_file.replace(strategy_audit_file)
                                strategy_source = "memory_backfill"
                            except Exception as e:
                                audit.add_note(f"Strategy signal file backfill failed: {str(e)[:80]}")
                    else:
                        if strategy_audit_file is not None and strategy_audit_file.exists():
                            strategy_payload = json.loads(strategy_audit_file.read_text(encoding='utf-8'))
                            if isinstance(strategy_payload, dict) and strategy_payload.get('strategies'):
                                strategy_source = "file"
                    audit.strategy_signals = (
                        strategy_payload.get('strategies', [])
                        if isinstance(strategy_payload, dict)
                        else []
                    )
                    total_signals = sum(int(s.get('total_signals', 0) or 0) for s in audit.strategy_signals)
                    audit.add_note(
                        f"Multi-strategy audit: source={strategy_source}, strategies={len(audit.strategy_signals)}, total_signals={total_signals}"
                    )
                    for s in audit.strategy_signals:
                        audit.add_note(
                            f"  {s['strategy']}: {s['total_signals']} signals ({s['buy_signals']} buy, {s['sell_signals']} sell)"
                        )
                except Exception as e:
                    audit.add_note(f"Strategy signal audit error: {str(e)[:50]}")
            
            audit.top_scores = [
                {
                    "symbol": sym,
                    "score": score,
                    "display_score": score,
                    "raw_score": float(raw_scores.get(sym, score)),
                    "base_score": float(base_scores.get(sym, 0.0)),
                    "ml_overlay_score": float(ml_overlay_scores.get(sym, 0.0)),
                    "ml_pred_zscore": float(ml_overlay_raw_scores.get(sym, 0.0)),
                    "score_delta": float(score - base_scores.get(sym, 0.0)),
                    "base_rank": int(base_rank_map.get(sym, idx + 1)),
                    "rank": idx + 1,
                    "rank_delta": int(base_rank_map.get(sym, idx + 1)) - int(final_rank_map.get(sym, idx + 1)),
                }
                for idx, (sym, score) in enumerate(sorted_scores[:10])
            ]
            audit.counts["scored"] = len(alpha.scores)

        # Compute *raw* equity (for reporting / performance).
        equity_raw = self.compute_equity(cash_usdt=cash_usdt, positions=positions, market_data_1h=market_data_1h)
        cash_raw = float(cash_usdt)

        # Live small-budget safety: cap sizing equity if configured.
        # IMPORTANT: this cap is for *order sizing only*; it must not pollute reporting.
        equity = float(equity_raw)
        cash_usdt = float(cash_raw)

        cap_eq = getattr(self.cfg.budget, "live_equity_cap_usdt", None)
        if cap_eq is not None:
            try:
                cap_eq_f = float(cap_eq)
                if cap_eq_f >= 0:
                    equity = min(float(equity), cap_eq_f)
                    cash_usdt = min(float(cash_usdt), cap_eq_f)
            except Exception:
                pass

        # Risk: drawdown-based exposure multiplier
        # IMPORTANT: drawdown must be computed on *raw* equity (accounting truth), not capped sizing equity.
        # Otherwise small-budget equity caps (e.g. 20U) will create a fake massive drawdown and permanently throttle.
        # ALSO: track scale_basis for proper drawdown calculation when budget changes.
        from src.portfolio.portfolio_state import PortfolioState
        from src.execution.account_store import AccountStore

        # 获取资金规模基准（优先从数据库读取历史记录）
        cap_eq = getattr(self.cfg.budget, "live_equity_cap_usdt", None)
        
        # 读取数据库中的历史 scale_basis
        runtime_order_store_path = Path(
            str(getattr(self.cfg.execution, "order_store_path", "reports/orders.sqlite"))
        )
        if not runtime_order_store_path.is_absolute():
            runtime_order_store_path = (REPORTS_DIR.parent / runtime_order_store_path).resolve()
        acc_store = AccountStore(path=str(derive_position_store_path(runtime_order_store_path).resolve()))
        acc_state = acc_store.get()
        old_scale_basis = float(acc_state.scale_basis_usdt or 0)
        
        # 如果数据库没有记录，使用当前 budget_cap 或 peak
        if old_scale_basis <= 0:
            old_scale_basis = float(cap_eq) if cap_eq else float(equity_peak_usdt)
        
        # 新的 scale_basis 来自配置
        scale_basis = float(cap_eq) if cap_eq else old_scale_basis
        
        # 检测资金规模变化
        if scale_basis > 0 and old_scale_basis > 0:
            scale_ratio = scale_basis / old_scale_basis
            if scale_ratio < 0.5 or scale_ratio > 2.0:
                # 资金规模变化超过2倍，按比例调整峰值
                new_peak = float(equity_peak_usdt) * scale_ratio
                if audit:
                    audit.add_note(f"Scale basis changed: {old_scale_basis:.2f} -> {scale_basis:.2f}, "
                                 f"peak adjusted: {equity_peak_usdt:.2f} -> {new_peak:.2f}")
                equity_peak_usdt = new_peak
                
                # 更新数据库中的 scale_basis
                acc_store.update_scale_basis(scale_basis, propagate_to_peak=False)
            elif abs(scale_ratio - 1.0) < 0.01:
                # scale_basis 没有变化，确保数据库记录正确
                if acc_state.scale_basis_usdt != scale_basis:
                    acc_store.update_scale_basis(scale_basis, propagate_to_peak=False)

        pst = PortfolioState(
            cash_usdt=float(cash_raw),
            equity_usdt=float(equity_raw),
            peak_equity_usdt=float(equity_peak_usdt),
            scale_basis_usdt=scale_basis,
        )
        pst.update_equity(equity_raw)
        dd_mult = self.risk_engine.exposure_multiplier(pst.drawdown_pct)
        
        # 3. DD multiplier审计
        if audit and dd_mult < 1.0:
            audit.reject("dd_throttle")
            audit.add_note(f"DD multiplier: {dd_mult} (drawdown: {pst.drawdown_pct:.2%})")

        # Define prices early for use in minSz filtering and later logic
        prices = {s: float(market_data_1h[s].close[-1]) for s in market_data_1h.keys() if market_data_1h[s].close}
        active_positions, position_state_cleanup_router_decisions, dust_flat_symbols = (
            self.cleanup_stale_position_state_for_dust_positions(
                positions,
                prices=prices,
                audit=audit,
            )
        )

        # qlib hold-threshold migration: compute holding minutes once (best effort).
        now_utc = self.clock.now().astimezone(timezone.utc)
        held_minutes_by_symbol: Dict[str, float] = {}
        for p in active_positions:
            hm = _holding_minutes(getattr(p, 'entry_ts', None), now_utc)
            if hm is not None:
                held_minutes_by_symbol[p.symbol] = hm
        probe_state = self._sync_market_impulse_probe_state_with_positions(active_positions)
        current_auto_risk_level = self._load_current_auto_risk_level()

        # 4. Portfolio分配后审计
        portfolio = self.portfolio_engine.allocate(
            scores=alpha.scores, 
            market_data=market_data_1h, 
            regime_mult=regime.multiplier,
            audit=audit
        )

        if self._live_symbol_whitelist:
            dropped_selected = [
                str(sym)
                for sym in (portfolio.selected or [])
                if str(sym) not in self._live_symbol_whitelist
            ]
            self._record_live_whitelist_drop(
                audit=audit,
                stage="portfolio_selected",
                dropped_symbols=dropped_selected,
            )
            portfolio.selected = [
                sym for sym in (portfolio.selected or []) if sym in self._live_symbol_whitelist
            ]
            target_weights = dict(portfolio.target_weights or {})
            dropped_targets = [str(sym) for sym in target_weights.keys() if str(sym) not in self._live_symbol_whitelist]
            self._record_live_whitelist_drop(
                audit=audit,
                stage="portfolio_targets",
                dropped_symbols=dropped_targets,
            )
            portfolio.target_weights = {
                sym: weight
                for sym, weight in target_weights.items()
                if sym in self._live_symbol_whitelist
            }
            if hasattr(portfolio, "entry_candidates") and getattr(portfolio, "entry_candidates", None) is not None:
                portfolio.entry_candidates = [
                    sym
                    for sym in (getattr(portfolio, "entry_candidates", None) or [])
                    if sym in self._live_symbol_whitelist
                ]

        # Filter out symbols that don't meet OKX minSz requirement
        # to avoid DUST_SKIP rejection at execution time
        from src.data.okx_instruments import OKXSpotInstrumentsCache
        instrument_cache = OKXSpotInstrumentsCache()
        filtered_selected = []
        skipped_for_minsz = []
        for sym in (portfolio.selected or []):
            inst_id = sym.replace("/", "-")
            spec = instrument_cache.get_spec(inst_id)
            px = float(prices.get(sym, 0.0) or 0.0)
            if spec and px > 0:
                weight = portfolio.target_weights.get(sym, 0)
                notional = weight * float(equity_raw)
                min_sz = float(spec.min_sz or 0)
                est_qty = notional / px if px > 0 else 0
                if min_sz > 0 and est_qty < min_sz:
                    skipped_for_minsz.append(f"{sym}: est_qty={est_qty:.4f} < minSz={min_sz}")
                    continue
            filtered_selected.append(sym)
        
        if skipped_for_minsz and audit:
            audit.add_note(f"minSz_skip: {', '.join(skipped_for_minsz)}")
        
        # Update portfolio.selected with filtered list
        portfolio.selected = filtered_selected
        
        target0 = dict(portfolio.target_weights or {})
        if audit:
            audit.targets_pre_risk = target0
            audit.counts["targets_pre_risk"] = len(target0)
            audit.counts["selected"] = len(portfolio.selected)
            # 从portfolio_debug获取更多信息
            if hasattr(audit, 'portfolio_debug') and audit.portfolio_debug:
                audit.portfolio_debug = audit.portfolio_debug
        
        # 5. 风险缩放后审计
        target = self.portfolio_engine.scale_targets(target0, dd_mult)
        if audit:
            audit.targets_post_risk = target
        eligible_buy_symbols = set(
            getattr(portfolio, "entry_candidates", None) or list(portfolio.selected or [])
        )
        regime_state_str = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
        risk_off_mult = float(getattr(self.cfg.regime, 'pos_mult_risk_off', 0.0) or 0.0)
        is_risk_off_close_only = (
            regime_state_str in ("Risk-Off", "Risk_Off", "RiskOff") and risk_off_mult <= 0.0
        )
        target_hold_eps = float(_coalesce(getattr(self.cfg.rebalance, "close_only_weight_eps", None), 0.001))
        target_zero_reason_by_symbol: Dict[str, str] = {}
        if abs(float(regime.multiplier or 0.0)) <= target_hold_eps:
            zero_after_regime_reason = "risk_off_pos_mult_zero" if is_risk_off_close_only else "regime_mult_zero"
            candidate_zero_symbols = sorted(
                set(list(target0.keys()) + list(portfolio.selected or []) + list(eligible_buy_symbols or []))
            )
            for sym in candidate_zero_symbols:
                if abs(float(target0.get(sym, 0.0) or 0.0)) <= target_hold_eps:
                    self._record_target_zero_reason(
                        target_zero_reason_by_symbol,
                        audit=audit,
                        symbol=sym,
                        reason=zero_after_regime_reason,
                    )
        if abs(float(dd_mult)) <= target_hold_eps:
            for sym, pre_risk_target in (target0 or {}).items():
                if abs(float(pre_risk_target or 0.0)) <= target_hold_eps:
                    continue
                if abs(float(target.get(sym, 0.0) or 0.0)) > target_hold_eps:
                    continue
                if sym not in target_zero_reason_by_symbol:
                    target_zero_reason_by_symbol[sym] = "dd_throttle_zero"
                if audit:
                    audit.record_count("target_zero_after_dd_throttle_count", symbol=sym)

        btc_leadership_probe_meta_by_symbol: Dict[str, Dict[str, Any]] = {}
        btc_leadership_probe_router_decisions: List[Dict[str, Any]] = []
        protect_entry_gate_active = str(current_auto_risk_level or "").upper() == "PROTECT"
        btc_probe_enabled = bool(getattr(self.cfg.execution, "btc_leadership_probe_enabled", True))
        btc_probe_configured_target_w = float(
            _coalesce(getattr(self.cfg.execution, "btc_leadership_probe_target_w", 0.08), 0.08)
        )
        btc_probe_max_target_w = float(
            _coalesce(getattr(self.cfg.execution, "btc_leadership_probe_max_target_w", 0.10), 0.10)
        )
        btc_probe_breakout_buffer_bps = float(
            _coalesce(getattr(self.cfg.execution, "btc_leadership_probe_breakout_buffer_bps", 15.0), 15.0)
        )
        btc_probe_min_alpha6_score = float(
            _coalesce(getattr(self.cfg.execution, "btc_leadership_probe_min_alpha6_score", 0.30), 0.30)
        )
        btc_probe_min_f5_rsi = float(
            _coalesce(getattr(self.cfg.execution, "btc_leadership_probe_min_f5_rsi", 0.30), 0.30)
        )
        btc_probe_min_f4_volume = float(
            _coalesce(getattr(self.cfg.execution, "btc_leadership_probe_min_f4_volume", -0.10), -0.10)
        )
        strategy_signal_lookup = (
            self._resolve_strategy_signal_lookup(audit)
            if protect_entry_gate_active or btc_probe_enabled
            else {}
        )

        def _has_effective_non_dust_position() -> bool:
            for pos in positions:
                qty_pos = float(getattr(pos, "qty", 0.0) or 0.0)
                if qty_pos <= 0:
                    continue
                px_pos = float(prices.get(pos.symbol, 0.0) or 0.0)
                if px_pos <= 0:
                    px_pos = float(getattr(pos, "last_mark_px", 0.0) or 0.0)
                if px_pos <= 0:
                    px_pos = float(getattr(pos, "avg_px", 0.0) or 0.0)
                if px_pos <= 0:
                    return True
                position_value = qty_pos * px_pos
                if position_value >= self._dust_position_threshold_usdt(symbol=pos.symbol, px=px_pos):
                    return True
            return False

        def _btc_probe_audit_fields(**fields: Any) -> Dict[str, Any]:
            payload = {
                "enabled": bool(btc_probe_enabled),
                "rolling_high": None,
                "latest_px": None,
                "breakout_buffer_bps": float(btc_probe_breakout_buffer_bps),
                "breakout_met": False,
                "min_alpha6_score": float(btc_probe_min_alpha6_score),
                "actual_alpha6_score": None,
                "min_f4_volume": float(btc_probe_min_f4_volume),
                "actual_f4_volume": None,
                "min_f5_rsi": float(btc_probe_min_f5_rsi),
                "actual_f5_rsi": None,
                "blocked_reason": None,
                "configured_target_w": float(btc_probe_configured_target_w),
                "effective_target_w": None,
                "max_target_w": float(btc_probe_max_target_w),
            }
            payload.update(fields)
            return payload

        def _btc_probe_block(reason: str, **fields: Any) -> None:
            if not audit:
                return
            audit.record_count(
                "btc_leadership_probe_blocked_count",
                symbol=f"BTC/USDT:{str(reason)}",
            )
            decision = {
                "symbol": "BTC/USDT",
                "action": "skip",
                "reason": str(reason),
                "btc_leadership_probe": True,
            }
            block_fields = dict(fields)
            block_fields["blocked_reason"] = str(reason)
            decision.update(_btc_probe_audit_fields(**block_fields))
            btc_leadership_probe_router_decisions.append(decision)

        if btc_probe_enabled:
            btc_symbol = "BTC/USDT"
            if audit:
                audit.record_count("btc_leadership_probe_candidate_count", symbol=btc_symbol)

            only_in_protect = bool(getattr(self.cfg.execution, "btc_leadership_probe_only_in_protect", True))
            probe_regime_state = str(regime.state.value if hasattr(regime.state, "value") else regime.state)
            if self._live_symbol_whitelist and btc_symbol not in self._live_symbol_whitelist:
                _btc_probe_block("btc_leadership_probe_not_whitelisted")
            elif only_in_protect and not protect_entry_gate_active:
                _btc_probe_block(
                    "btc_leadership_probe_not_protect",
                    current_level=current_auto_risk_level,
                )
            elif bool(getattr(self.cfg.execution, "btc_leadership_probe_require_regime_not_risk_off", True)) and probe_regime_state in (
                "Risk-Off",
                "Risk_Off",
                "RiskOff",
            ):
                _btc_probe_block(
                    "btc_leadership_probe_risk_off",
                    regime=probe_regime_state,
                )
            elif btc_symbol not in market_data_1h or not getattr(market_data_1h.get(btc_symbol), "close", None):
                _btc_probe_block("btc_leadership_probe_missing_market_data")
            elif _has_effective_non_dust_position():
                _btc_probe_block("btc_leadership_probe_not_flat")
            else:
                active_neg_cooldown = None
                try:
                    active_neg_cooldown = self.negative_expectancy_cooldown.is_blocked(btc_symbol)
                except Exception:
                    active_neg_cooldown = None

                probe_cooldown = self._btc_leadership_probe_active_cooldown(
                    btc_symbol,
                    int(getattr(self.cfg.execution, "btc_leadership_probe_cooldown_hours", 8) or 0),
                )
                alpha6_signal = (strategy_signal_lookup.get("Alpha6Factor") or {}).get(btc_symbol)
                alpha6_score = self._signal_score(alpha6_signal)
                alpha6_side = self._signal_side(alpha6_signal)
                f5_rsi = self._alpha6_rsi_confirm(alpha6_signal)
                f4_volume = self._alpha6_volume_expansion(alpha6_signal)
                lookback_hours = int(getattr(self.cfg.execution, "btc_leadership_probe_lookback_hours", 24) or 24)
                rolling_high = self._rolling_close_high_excluding_latest(
                    market_data_1h.get(btc_symbol),
                    lookback_hours,
                )
                last_px = float(prices.get(btc_symbol, 0.0) or 0.0)
                breakout_buffer_bps = float(btc_probe_breakout_buffer_bps)
                min_alpha6_score = float(btc_probe_min_alpha6_score)
                min_f5_rsi = float(btc_probe_min_f5_rsi)
                min_f4_volume = float(btc_probe_min_f4_volume)
                breakout_price = None
                breakout_met = False
                if rolling_high is not None and last_px > 0:
                    breakout_price = float(rolling_high) * (1.0 + float(breakout_buffer_bps) / 10000.0)
                    breakout_met = float(last_px) >= float(breakout_price)
                base_probe_fields = _btc_probe_audit_fields(
                    rolling_high=_float_or_none(rolling_high),
                    latest_px=float(last_px),
                    last_px=float(last_px),
                    breakout_buffer_bps=float(breakout_buffer_bps),
                    breakout_price=breakout_price,
                    breakout_met=bool(breakout_met),
                    min_alpha6_score=float(min_alpha6_score),
                    actual_alpha6_score=_float_or_none(alpha6_score),
                    alpha6_score=alpha6_score,
                    min_f4_volume=float(min_f4_volume),
                    actual_f4_volume=_float_or_none(f4_volume),
                    f4_volume_expansion=f4_volume,
                    min_f5_rsi=float(min_f5_rsi),
                    actual_f5_rsi=_float_or_none(f5_rsi),
                    f5_rsi_trend_confirm=f5_rsi,
                )

                if active_neg_cooldown:
                    if audit:
                        audit.record_gate("negative_expectancy_cooldown", symbol=btc_symbol)
                    _btc_probe_block(
                        "negative_expectancy_cooldown",
                        **base_probe_fields,
                        remain_seconds=float(active_neg_cooldown.get("remain_seconds") or 0.0),
                    )
                elif probe_cooldown:
                    _btc_probe_block(
                        "btc_leadership_probe_cooldown",
                        **base_probe_fields,
                        **probe_cooldown,
                    )
                elif alpha6_side != "buy":
                    _btc_probe_block(
                        "btc_leadership_probe_no_alpha6_buy",
                        **base_probe_fields,
                        alpha6_side=alpha6_side,
                    )
                elif alpha6_score is None or float(alpha6_score) < min_alpha6_score:
                    _btc_probe_block(
                        "btc_leadership_probe_alpha6_score_too_low",
                        **base_probe_fields,
                    )
                elif f5_rsi is None or float(f5_rsi) < min_f5_rsi:
                    _btc_probe_block(
                        "btc_leadership_probe_f5_rsi_too_low",
                        **base_probe_fields,
                    )
                elif f4_volume is None or float(f4_volume) < min_f4_volume:
                    _btc_probe_block(
                        "btc_leadership_probe_f4_volume_too_low",
                        **base_probe_fields,
                    )
                elif rolling_high is None or last_px <= 0:
                    _btc_probe_block(
                        "btc_leadership_probe_missing_rolling_high",
                        **base_probe_fields,
                    )
                elif not bool(breakout_met):
                    _btc_probe_block(
                        "btc_leadership_probe_no_breakout",
                        **base_probe_fields,
                    )
                else:
                    target_w = float(btc_probe_configured_target_w)
                    max_target_w = float(btc_probe_max_target_w)
                    target_w = min(float(target_w), float(max_target_w))
                    required_notional = max(
                        float(getattr(self.cfg.budget, "min_trade_notional_base", 0.0) or 0.0),
                        float(self._exchange_min_notional_with_slack(symbol=btc_symbol, px=last_px) or 0.0),
                    )
                    if bool(getattr(self.cfg.execution, "btc_leadership_probe_dynamic_sizing_enabled", True)):
                        if equity_raw > 0 and required_notional > 0:
                            target_w = max(float(target_w), float(required_notional) / float(equity_raw))
                        target_w = min(float(target_w), float(max_target_w))
                    sized_probe_fields = dict(base_probe_fields)
                    sized_probe_fields.update(
                        {
                            "effective_target_w": float(target_w),
                            "target_w": float(target_w),
                            "max_target_w": float(max_target_w),
                        }
                    )

                    if target_w <= 0:
                        _btc_probe_block(
                            "btc_leadership_probe_zero_target",
                            **sized_probe_fields,
                        )
                    elif equity_raw > 0 and required_notional > 0 and float(target_w) * float(equity_raw) < float(required_notional):
                        _btc_probe_block(
                            "btc_leadership_probe_min_notional_unreachable",
                            **sized_probe_fields,
                            required_notional_usdt=float(required_notional),
                            target_notional_usdt=float(target_w) * float(equity_raw),
                        )
                    else:
                        target[btc_symbol] = max(float(target.get(btc_symbol, 0.0) or 0.0), float(target_w))
                        eligible_buy_symbols.add(btc_symbol)
                        btc_leadership_probe_meta_by_symbol[btc_symbol] = {
                            "btc_leadership_probe": True,
                            "entry_reason": "btc_leadership_probe",
                            "probe_type": "btc_leadership_probe",
                            **sized_probe_fields,
                            "rolling_high": float(rolling_high),
                            "breakout_buffer_bps": float(breakout_buffer_bps),
                            "alpha6_score": float(alpha6_score),
                            "f4_volume_expansion": float(f4_volume),
                            "f5_rsi_trend_confirm": float(f5_rsi),
                            "blocked_reason": None,
                            "bypassed_negative_expectancy": False,
                            "target_w": float(target_w),
                            "btc_leadership_probe_time_stop_hours": int(
                                getattr(self.cfg.execution, "btc_leadership_probe_time_stop_hours", 8) or 0
                            ),
                        }
                        if audit:
                            audit.add_note(
                                "BTC leadership probe armed: "
                                f"last_px={last_px:.4f}, rolling_high={float(rolling_high):.4f}, "
                                f"buffer_bps={float(breakout_buffer_bps):.2f}, target_w={float(target_w):.4f}"
                            )
                            audit.targets_post_risk = target

        # 4.4 确保已有持仓都注册到止损/利润管理（避免重启后状态丢失）
        for p in active_positions:
            if float(p.qty) <= 0:
                continue
            px = float(prices.get(p.symbol, 0.0) or 0.0)
            if px <= 0:
                continue
            series = market_data_1h.get(p.symbol)
            candle_high = float(series.high[-1]) if series and getattr(series, "high", None) else px
            entry_ref = float(p.avg_px) if float(getattr(p, 'avg_px', 0.0) or 0.0) > 0 else px
            if p.symbol not in self.fixed_stop_loss.entry_prices:
                self.fixed_stop_loss.register_position(p.symbol, entry_ref)
            # profit_taking 自带“入场价漂移>1%自动重置”逻辑，需每轮同步一次
            probe_state_meta = self._probe_metadata_for_position(p, probe_state=probe_state)
            probe_register_kwargs = (
                {
                    "entry_reason": probe_state_meta.get("entry_reason"),
                    "probe_type": probe_state_meta.get("probe_type"),
                    "entry_ts": probe_state_meta.get("entry_ts"),
                    "target_w": probe_state_meta.get("target_w"),
                    "highest_net_bps": self._probe_net_bps(
                        entry_px=float(probe_state_meta.get("entry_px") or 0.0),
                        current_px=max(float(getattr(p, 'highest_px', 0.0) or 0.0), candle_high, px),
                    ),
                }
                if probe_state_meta is not None
                else {}
            )
            self.profit_taking.register_position(
                p.symbol,
                entry_ref,
                current_price=px,
                highest_price_hint=max(float(getattr(p, 'highest_px', 0.0) or 0.0), candle_high),
                **probe_register_kwargs,
            )

        # 4.5 Profit-first exit priority (profit-taking > fixed stop > rank exit)
        probe_exit_orders = []
        protect_profit_lock_orders = []
        protect_profit_lock_router_decisions = []
        profit_orders = []
        fixed_stop_orders = []
        profit_symbols = set()  # Track symbols already handled by profit-taking

        if bool(getattr(self.cfg.execution, "probe_exit_enabled", True)):
            take_profit_bps = float(_coalesce(getattr(self.cfg.execution, "probe_take_profit_net_bps", None), 80.0))
            stop_loss_bps = float(_coalesce(getattr(self.cfg.execution, "probe_stop_loss_net_bps", None), -50.0))
            trailing_enable_bps = float(
                _coalesce(getattr(self.cfg.execution, "probe_trailing_enable_after_net_bps", None), 50.0)
            )
            trailing_gap_bps = float(_coalesce(getattr(self.cfg.execution, "probe_trailing_gap_bps", None), 25.0))
            time_stop_hours = float(_coalesce(getattr(self.cfg.execution, "probe_time_stop_hours", None), 8))
            time_stop_min_bps = float(_coalesce(getattr(self.cfg.execution, "probe_time_stop_min_net_bps", None), 10.0))

            for p in active_positions:
                if float(getattr(p, "qty", 0.0) or 0.0) <= 0:
                    continue
                probe_meta = self._probe_metadata_for_position(p, probe_state=probe_state)
                if probe_meta is None:
                    continue
                s = market_data_1h.get(p.symbol)
                if not s or not s.close:
                    continue
                current_price = float(s.close[-1])
                if current_price <= 0:
                    continue
                entry_px = float(probe_meta["entry_px"])
                net_bps = self._probe_net_bps(entry_px=entry_px, current_px=current_price)

                state = (getattr(self.profit_taking, "positions", {}) or {}).get(p.symbol)
                highest_price = max(
                    current_price,
                    float(s.high[-1]) if getattr(s, "high", None) else current_price,
                    float(getattr(p, "highest_px", 0.0) or 0.0),
                    float(getattr(state, "highest_price", 0.0) or 0.0) if state is not None else 0.0,
                )
                high_net_bps = self._probe_net_bps(entry_px=entry_px, current_px=highest_price)
                highest_net_bps = max(float(high_net_bps), float(probe_meta.get("highest_net_bps") or 0.0))
                entry_dt = _parse_iso_utc(str(probe_meta.get("entry_ts") or ""))
                held_hours = (
                    max(0.0, (now_utc - entry_dt).total_seconds() / 3600.0)
                    if entry_dt is not None
                    else None
                )

                reason = None
                if net_bps <= stop_loss_bps:
                    reason = "probe_stop_loss"
                elif net_bps >= take_profit_bps:
                    reason = "probe_take_profit"
                elif highest_net_bps >= trailing_enable_bps and (highest_net_bps - net_bps) >= trailing_gap_bps:
                    reason = "probe_trailing_stop"
                elif held_hours is not None and held_hours >= time_stop_hours and net_bps < time_stop_min_bps:
                    reason = "probe_time_stop"

                if reason is None:
                    continue

                probe_exit_orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * current_price,
                        signal_price=current_price,
                        meta={
                            "reason": reason,
                            "exit_reason": reason,
                            "probe_exit": True,
                            "probe_exit_policy_active": True,
                            "probe_type": probe_meta.get("probe_type"),
                            "entry_reason": probe_meta.get("entry_reason"),
                            "entry_px": float(entry_px),
                            "entry_ts": probe_meta.get("entry_ts"),
                            "target_w": probe_meta.get("target_w"),
                            "net_bps": float(net_bps),
                            "high_net_bps": float(high_net_bps),
                            "highest_net_bps": float(highest_net_bps),
                            "highest_px_before_exit": float(highest_price),
                            "held_hours": held_hours,
                            "hold_hours": held_hours,
                            "probe_take_profit_net_bps": float(take_profit_bps),
                            "probe_stop_loss_net_bps": float(stop_loss_bps),
                            "probe_trailing_enable_after_net_bps": float(trailing_enable_bps),
                            "probe_trailing_gap_bps": float(trailing_gap_bps),
                            "probe_time_stop_hours": float(time_stop_hours),
                            "probe_time_stop_min_net_bps": float(time_stop_min_bps),
                            "bypass_turnover_cap_for_exit": True,
                            "turnover_cap_bypass_reason": "probe_exit",
                        },
                    )
                )
                profit_symbols.add(p.symbol)
                if audit:
                    audit.record_count(f"{reason}_count", symbol=p.symbol)
                    audit.add_note(
                        f"Probe exit: {p.symbol} reason={reason} net={net_bps:.2f}bps "
                        f"high={high_net_bps:.2f}bps held_hours={held_hours}"
                    )

        protect_profit_lock_enabled = bool(getattr(self.cfg.execution, "protect_profit_lock_enabled", True))
        protect_profit_lock_active = (
            protect_profit_lock_enabled
            and str(current_auto_risk_level or "").upper() == "PROTECT"
        )
        if protect_profit_lock_active:
            min_net_bps = float(
                _coalesce(getattr(self.cfg.execution, "protect_profit_lock_min_net_bps", None), 100.0)
            )
            breakeven_plus_bps = float(
                _coalesce(getattr(self.cfg.execution, "protect_profit_lock_breakeven_plus_bps", None), 20.0)
            )
            trailing_start_bps = float(
                _coalesce(getattr(self.cfg.execution, "protect_profit_lock_trailing_start_net_bps", None), 150.0)
            )
            trailing_gap_bps = float(
                _coalesce(getattr(self.cfg.execution, "protect_profit_lock_trailing_gap_bps", None), 60.0)
            )
            strong_start_bps = float(
                _coalesce(getattr(self.cfg.execution, "protect_profit_lock_strong_start_net_bps", None), 200.0)
            )
            strong_gap_bps = float(
                _coalesce(getattr(self.cfg.execution, "protect_profit_lock_strong_trailing_gap_bps", None), 50.0)
            )
            rt_cost_bps = 2.0 * (
                float(getattr(self.cfg.execution, "fee_bps", 0.0) or 0.0)
                + float(getattr(self.cfg.execution, "slippage_bps", 0.0) or 0.0)
            )

            for p in active_positions:
                if float(getattr(p, "qty", 0.0) or 0.0) <= 0:
                    continue
                if p.symbol in profit_symbols:
                    continue
                if self._probe_metadata_for_position(p, probe_state=probe_state) is not None:
                    continue

                s = market_data_1h.get(p.symbol)
                if not s or not s.close:
                    continue
                current_price = float(s.close[-1])
                if current_price <= 0:
                    continue

                state = (getattr(self.profit_taking, "positions", {}) or {}).get(p.symbol)
                entry_px = (
                    float(getattr(state, "entry_price", 0.0) or 0.0)
                    if state is not None
                    else 0.0
                )
                if entry_px <= 0:
                    entry_px = float(getattr(p, "avg_px", 0.0) or 0.0)
                if entry_px <= 0:
                    continue

                observed_high_price = max(
                    current_price,
                    float(s.high[-1]) if getattr(s, "high", None) else current_price,
                    float(getattr(p, "highest_px", 0.0) or 0.0),
                    float(getattr(state, "highest_price", 0.0) or 0.0) if state is not None else 0.0,
                )
                gross_bps = (float(current_price) / float(entry_px) - 1.0) * 10000.0
                net_bps = float(gross_bps - rt_cost_bps)
                high_gross_bps = (float(observed_high_price) / float(entry_px) - 1.0) * 10000.0
                high_net_bps = float(high_gross_bps - rt_cost_bps)
                stored_highest_net_bps = (
                    float(getattr(state, "highest_net_bps", 0.0) or 0.0)
                    if state is not None
                    else 0.0
                )
                highest_net_bps = max(float(high_net_bps), stored_highest_net_bps)

                current_stop_px = (
                    float(getattr(state, "current_stop", 0.0) or 0.0)
                    if state is not None
                    else 0.0
                )
                effective_stop_px = current_stop_px
                stop_raised = False
                state_changed = False

                if state is not None and highest_net_bps > float(getattr(state, "highest_net_bps", 0.0) or 0.0) + 1e-9:
                    state.highest_net_bps = float(highest_net_bps)
                    state_changed = True

                if net_bps + 1e-12 >= min_net_bps:
                    breakeven_stop_px = float(entry_px) * (1.0 + (rt_cost_bps + breakeven_plus_bps) / 10000.0)
                    effective_stop_px = max(float(effective_stop_px or 0.0), breakeven_stop_px)
                    if state is not None and effective_stop_px > float(getattr(state, "current_stop", 0.0) or 0.0) + 1e-9:
                        state.current_stop = float(effective_stop_px)
                        state.current_action = "protect_profit_lock"
                        stop_raised = True
                        state_changed = True

                if state_changed and state is not None:
                    try:
                        self.profit_taking._save_state()
                    except Exception:
                        pass

                trailing_gap = None
                trailing_mode = None
                bps_epsilon = 1e-9
                if highest_net_bps + bps_epsilon >= strong_start_bps:
                    trailing_gap = strong_gap_bps
                    trailing_mode = "strong"
                elif highest_net_bps + bps_epsilon >= trailing_start_bps:
                    trailing_gap = trailing_gap_bps
                    trailing_mode = "normal"

                lock_engaged = (
                    net_bps + bps_epsilon >= min_net_bps
                    or trailing_gap is not None
                )
                if not lock_engaged:
                    continue

                exit_reason = None
                if trailing_gap is not None and (highest_net_bps - net_bps) + bps_epsilon >= float(trailing_gap):
                    exit_reason = "protect_profit_lock_trailing"

                payload = {
                    "protect_profit_lock_active": True,
                    "symbol": p.symbol,
                    "entry_px": float(entry_px),
                    "current_px": float(current_price),
                    "gross_bps": float(gross_bps),
                    "roundtrip_cost_bps": float(rt_cost_bps),
                    "net_bps": float(net_bps),
                    "highest_net_bps": float(highest_net_bps),
                    "highest_px_before_exit": float(observed_high_price),
                    "effective_stop_px": float(effective_stop_px or 0.0),
                    "exit_reason": exit_reason,
                    "trailing_mode": trailing_mode,
                    "trailing_gap_bps": float(trailing_gap) if trailing_gap is not None else None,
                }

                if audit:
                    audit.record_count("protect_profit_lock_active_count", symbol=p.symbol)
                    if stop_raised:
                        audit.record_count("protect_profit_lock_stop_raised_count", symbol=p.symbol)

                if exit_reason is None:
                    protect_profit_lock_router_decisions.append(
                        {
                            "symbol": p.symbol,
                            "action": "hold",
                            "reason": "protect_profit_lock_stop_raised" if stop_raised else "protect_profit_lock_active",
                            **payload,
                        }
                    )
                    continue

                protect_profit_lock_orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * current_price,
                        signal_price=current_price,
                        meta={
                            "reason": exit_reason,
                            "exit_reason": exit_reason,
                            "protect_profit_lock_exit": True,
                            **payload,
                            "bypass_turnover_cap_for_exit": True,
                            "turnover_cap_bypass_reason": "protect_profit_lock",
                        },
                    )
                )
                profit_symbols.add(p.symbol)
                if audit:
                    audit.record_count("protect_profit_lock_trailing_exit_count", symbol=p.symbol)
                    audit.add_note(
                        f"Protect profit lock trailing: {p.symbol} net={net_bps:.2f}bps "
                        f"high={highest_net_bps:.2f}bps gap={float(trailing_gap):.2f}bps mode={trailing_mode}"
                    )
        
        for p in active_positions:
            if p.symbol in profit_symbols:
                continue
            s = market_data_1h.get(p.symbol)
            if not s or not s.close:
                continue
            current_price = float(s.close[-1])
            observed_low_price = float(s.low[-1]) if getattr(s, "low", None) else current_price
            observed_high_price = max(
                current_price,
                float(s.high[-1]) if getattr(s, "high", None) else current_price,
                float(getattr(p, 'highest_px', 0.0) or 0.0),
            )
            
            # 1st priority: 程序化利润管理（利润回撤锁盈）
            action, value, reason = self.profit_taking.evaluate(
                p.symbol,
                current_price,
                observed_low_price=observed_low_price,
                observed_high_price=observed_high_price,
            )
            
            if action in {'sell_all', 'sell_partial'} and float(p.qty) > 0:
                sell_fraction = 1.0 if action == 'sell_all' else max(0.0, min(float(value or 0.0), 1.0))
                if sell_fraction <= 0:
                    continue
                is_full_exit = sell_fraction >= 0.999
                exit_reason = f"profit_taking_{reason}" if is_full_exit else f"profit_partial_{reason}"
                profit_orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG" if is_full_exit else "REBALANCE",
                        notional_usdt=float(p.qty) * current_price * sell_fraction,
                        signal_price=current_price,
                        meta={
                            "reason": exit_reason,
                            "action": action,
                            "value": value,
                            "sell_fraction": sell_fraction,
                        },
                    )
                )
                profit_symbols.add(p.symbol)
                if audit:
                    audit.add_note(
                        f"Profit taking: {p.symbol} {reason}, sell_fraction={sell_fraction:.2f}"
                    )
                continue  # Skip other exit checks for this symbol
            
            # 2nd priority: 多级动态止损（取代固定止损，更智能）
            # 每轮同步动态止损状态，避免旧仓位状态污染新开仓
            entry_ref = float(p.avg_px) if float(getattr(p, 'avg_px', 0)) > 0 else current_price
            self.stop_loss_manager.register_position(p.symbol, entry_ref)

            should_stop, stop_price, stop_type, profit_pct = self.stop_loss_manager.evaluate_stop(
                p.symbol, current_price
            )
            
            if should_stop and float(p.qty) > 0:
                fixed_stop_orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * current_price,
                        signal_price=current_price,
                        meta={
                            "reason": f"dynamic_stop_{stop_type}",
                            "stop_price": stop_price,
                            "profit_pct": profit_pct,
                            "stop_type": stop_type,
                        },
                    )
                )
                profit_symbols.add(p.symbol)  # Also skip rank exit
                if audit:
                    audit.add_note(f"Dynamic stop: {p.symbol} {stop_type}, profit {profit_pct*100:.1f}%")
                continue  # Skip rank exit for this symbol
            
            # 3rd priority: 固定止损（备用，当动态止损未触发但固定止损条件满足时）
            should_stop_fixed, stop_price_fixed, loss_pct = self.fixed_stop_loss.should_stop_loss(
                p.symbol, current_price
            )
            
            if should_stop_fixed and float(p.qty) > 0:
                fixed_stop_orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * current_price,
                        signal_price=current_price,
                        meta={
                            "reason": "fixed_stop_loss",
                            "entry_price": self.fixed_stop_loss.entry_prices.get(p.symbol, p.avg_px),
                            "stop_price": stop_price_fixed,
                            "loss_pct": loss_pct,
                        },
                    )
                )
                profit_symbols.add(p.symbol)  # Also skip rank exit
                if audit:
                    audit.add_note(f"Fixed stop loss: {p.symbol} loss {loss_pct*100:.1f}%")
                continue  # Skip rank exit for this symbol
        
        # 3rd priority: 排名退出（只在未被利润/止损处理时）
        # IMPORTANT:
        # - 排名来源要与选币/定仓尽量同源（fused 优先）
        # - 若本轮该币目标仓位仍>0，则不应触发 rank_exit，避免“同轮又买又卖”
        ranking_exit_orders = []
        rank_scores = dict(getattr(alpha, 'scores', {}) or {})
        rank_source = 'alpha'
        symbol_ranks: Dict[str, int] = {}

        try:
            use_fused_for_weighting = bool(getattr(self.cfg.alpha, 'use_fused_score_for_weighting', True))
            if use_fused_for_weighting:
                fused_rank_scores = self.portfolio_engine._load_fused_signals()
                if fused_rank_scores:
                    rank_scores = dict(fused_rank_scores)
                    rank_source = 'fused'
        except Exception:
            pass

        if rank_scores:
            sorted_scores = sorted(rank_scores.items(), key=lambda x: x[1], reverse=True)
            symbol_ranks = {sym: idx + 1 for idx, (sym, _) in enumerate(sorted_scores)}
            target_hold_eps = float(_coalesce(getattr(self.cfg.rebalance, 'close_only_weight_eps', None), 0.001))
            rank_exit_max_rank = int(getattr(self.cfg.execution, 'rank_exit_max_rank', 3) or 3)
            rank_exit_confirm_rounds = int(getattr(self.cfg.execution, 'rank_exit_confirm_rounds', 2) or 2)
            rank_exit_strict_mode = bool(getattr(self.cfg.execution, 'rank_exit_strict_mode', False))
            rank_exit_require_zero_target = bool(
                getattr(self.cfg.execution, 'rank_exit_require_zero_target', True)
            )
            rank_exit_buffer_positions = int(
                getattr(self.cfg.execution, 'rank_exit_buffer_positions', 0) or 0
            )

            if audit:
                audit.add_note(
                    f"Rank exit source: {rank_source}, candidates={len(symbol_ranks)}, max_rank={rank_exit_max_rank}, "
                    f"confirm_rounds={rank_exit_confirm_rounds}, require_zero_target={rank_exit_require_zero_target}, "
                    f"buffer_positions={rank_exit_buffer_positions}"
                )

            for p in active_positions:
                if p.qty <= 0 or p.symbol in profit_symbols:
                    continue  # Skip if already handled by profit-taking or stop loss

                current_rank = symbol_ranks.get(p.symbol, 999)
                tw = float(target.get(p.symbol, 0.0) or 0.0)
                if rank_exit_require_zero_target and tw > target_hold_eps:
                    if audit:
                        audit.add_note(
                            f"rank_exit_target_still_positive: {p.symbol} target_w={tw:.4f} > eps={target_hold_eps:.4f}, "
                            f"rank={current_rank}, source={rank_source}"
                        )
                    continue

                should_exit, reason = self.profit_taking.should_exit_by_rank(
                    p.symbol,
                    current_rank,
                    max_rank=rank_exit_max_rank,
                    confirm_rounds=rank_exit_confirm_rounds,
                    buffer_positions=rank_exit_buffer_positions,
                )
                if not should_exit:
                    if audit and str(reason).startswith("rank_exit_pending"):
                        audit.add_note(
                            f"Rank exit pending: {p.symbol} rank {current_rank}, {reason}, source={rank_source}"
                        )
                    elif audit and str(reason).startswith("rank_exit_buffered"):
                        audit.add_note(
                            f"Rank exit buffered: {p.symbol} rank {current_rank}, {reason}, source={rank_source}"
                        )
                    continue

                if not rank_exit_strict_mode and tw > target_hold_eps:
                    if audit:
                        audit.add_note(
                            f"Rank exit skipped: {p.symbol} target_w={tw:.4f} > eps={target_hold_eps:.4f}"
                        )
                    continue
                if rank_exit_strict_mode and tw > target_hold_eps and audit:
                    audit.add_note(
                        f"Rank exit strict mode: {p.symbol} ignoring target_w={tw:.4f} > eps={target_hold_eps:.4f}"
                    )

                # qlib hold-threshold migration: do not rank-exit too soon after entry.
                min_hold_rank_exit = int(getattr(self.cfg.execution, 'min_hold_minutes_before_rank_exit', 0) or 0)
                if min_hold_rank_exit > 0:
                    held_min = held_minutes_by_symbol.get(p.symbol)
                    if held_min is not None and held_min < float(min_hold_rank_exit):
                        if audit:
                            audit.reject('min_hold_rank_exit')
                            audit.add_note(
                                f"Rank exit blocked by min-hold: {p.symbol} held={held_min:.1f}m < {min_hold_rank_exit}m"
                            )
                        continue

                s = market_data_1h.get(p.symbol)
                if s and s.close:
                    current_price = float(s.close[-1])
                    ranking_exit_orders.append(
                        Order(
                            symbol=p.symbol,
                            side="sell",
                            intent="CLOSE_LONG",
                            notional_usdt=float(p.qty) * current_price,
                            signal_price=current_price,
                            meta={
                                "reason": f"rank_exit_{reason}",
                                "current_rank": current_rank,
                                "rank_source": rank_source,
                                "target_w": tw,
                                "confirm_rounds": rank_exit_confirm_rounds,
                                "max_rank": rank_exit_max_rank,
                                "buffer_positions": rank_exit_buffer_positions,
                            },
                        )
                    )
                if audit:
                    audit.add_note(
                        f"Rank exit: {p.symbol} rank {current_rank}, {reason}, source={rank_source}"
                    )

        # Market impulse probe time-stop exits.
        market_impulse_time_stop_orders: List[Order] = []
        if probe_state and not bool(getattr(self.cfg.execution, "probe_exit_enabled", True)):
            probe_strategy_signal_lookup = self._resolve_strategy_signal_lookup(audit)
            probe_context = self._market_impulse_probe_context(
                strategy_signal_lookup=probe_strategy_signal_lookup,
                current_auto_risk_level=current_auto_risk_level,
                regime_state_str=str(regime.state.value if hasattr(regime.state, "value") else regime.state),
            )
            impulse_still_active = bool(probe_context.get("active"))
            for p in active_positions:
                probe_payload = probe_state.get(p.symbol)
                if not isinstance(probe_payload, dict):
                    continue
                entry_ts_ms = int(probe_payload.get("entry_ts_ms") or 0)
                if entry_ts_ms <= 0:
                    continue
                time_stop_hours = int(
                    probe_payload.get("time_stop_hours")
                    or getattr(self.cfg.execution, "market_impulse_probe_time_stop_hours", 4)
                    or 4
                )
                held_hours = max(0.0, (int(now_utc.timestamp() * 1000) - entry_ts_ms) / 3_600_000.0)
                if held_hours < float(time_stop_hours):
                    continue
                pnl_pct = float(getattr(p, "unrealized_pnl_pct", 0.0) or 0.0)
                if pnl_pct > 0.0 and impulse_still_active:
                    continue
                current_price = float(prices.get(p.symbol, 0.0) or 0.0)
                if current_price <= 0.0:
                    continue
                market_impulse_time_stop_orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(getattr(p, "qty", 0.0) or 0.0) * current_price,
                        signal_price=current_price,
                        meta={
                            "reason": "market_impulse_probe_time_stop",
                            "held_hours": held_hours,
                            "time_stop_hours": float(time_stop_hours),
                            "unrealized_pnl_pct": pnl_pct,
                            "impulse_still_active": impulse_still_active,
                        },
                    )
                )
                if audit:
                    audit.record_count("market_impulse_probe_time_stop_count", symbol=p.symbol)
                    audit.add_note(
                        f"Market impulse probe time stop: {p.symbol} held_hours={held_hours:.2f} pnl_pct={pnl_pct:.4f} impulse_active={impulse_still_active}"
                    )
        
        # 4. Policy-based exits (if not already handled)
        exit_orders = self.exit_policy.evaluate(
            positions=active_positions,
            market_data=market_data_1h,
            regime_state=str(regime.state.value if hasattr(regime.state, 'value') else regime.state),
        )
        # Filter out symbols already handled by profit/stop
        exit_orders = [o for o in exit_orders if o.symbol not in profit_symbols]

        # qlib hold-threshold migration: optional minimum hold before regime-exit.
        min_hold_regime_exit = int(getattr(self.cfg.execution, 'min_hold_minutes_before_regime_exit', 0) or 0)
        if min_hold_regime_exit > 0 and exit_orders:
            filtered_exit_orders = []
            for eo in exit_orders:
                reason = str((eo.meta or {}).get('reason', '') or '')
                if reason == 'regime_exit':
                    held_min = held_minutes_by_symbol.get(eo.symbol)
                    if held_min is not None and held_min < float(min_hold_regime_exit):
                        if audit:
                            audit.reject('min_hold_regime_exit')
                            audit.add_note(
                                f"Regime exit blocked by min-hold: {eo.symbol} held={held_min:.1f}m < {min_hold_regime_exit}m"
                            )
                        continue
                filtered_exit_orders.append(eo)
            exit_orders = filtered_exit_orders
        
        # Merge all exit orders: probe first, then protect profit-lock, profit, stop, rank, impulse time-stop, then policy
        exit_orders = (
            probe_exit_orders
            + protect_profit_lock_orders
            + profit_orders
            + fixed_stop_orders
            + ranking_exit_orders
            + market_impulse_time_stop_orders
            + exit_orders
        )

        # Deduplicate: keep only one exit per symbol per round, priority: sell_all > dynamic_stop > fixed_stop > atr > partial
        if exit_orders:
            prio_map = {
                'probe_take_profit': 110,
                'probe_stop_loss': 110,
                'probe_trailing_stop': 110,
                'probe_time_stop': 110,
                'protect_profit_lock_trailing': 105,
                'profit_taking_stop_loss_hit': 100,
                'dynamic_stop': 95,  # 动态止损优先级高于固定止损
                'profit_taking_': 93,
                'fixed_stop_loss': 90,
                'atr_trailing': 80,
                'rank_exit_': 75,
                'regime_exit': 70,
                'profit_partial': 60,
            }
            best = {}
            for o in exit_orders:
                reason = str((o.meta or {}).get('reason', ''))
                prio = 10
                for k, v in prio_map.items():
                    if reason.startswith(k):
                        prio = v
                        break
                cur = best.get(o.symbol)
                if cur is None or prio > cur[0]:
                    best[o.symbol] = (prio, o)
            exit_orders = [v[1] for v in best.values()]

        exit_router_decisions = []
        if exit_orders:
            filtered_exit_orders = []
            for order in exit_orders:
                px_exit = float(getattr(order, "signal_price", 0.0) or 0.0)
                if px_exit <= 0.0:
                    try:
                        px_exit = float(prices.get(order.symbol, 0.0) or 0.0)
                    except Exception:
                        px_exit = 0.0
                dust_threshold = self._dust_position_threshold_usdt(symbol=order.symbol, px=px_exit)
                notional_exit = float(getattr(order, "notional_usdt", 0.0) or 0.0)
                if str(getattr(order, "side", "")).lower() == "sell" and 0.0 < notional_exit < dust_threshold:
                    if audit:
                        audit.record_count("dust_residual_no_close_order_count", symbol=order.symbol)
                        exit_router_decisions.append(
                            {
                                "symbol": order.symbol,
                                "action": "skip",
                                "reason": "dust_residual_no_close_order",
                                "source_reason": (order.meta or {}).get("reason"),
                                "held_value_usdt": float(notional_exit),
                                "raw_held_value_usdt": float(notional_exit),
                                "effective_held_value_usdt": 0.0,
                                "dust_threshold_usdt": float(dust_threshold),
                            }
                        )
                    continue
                filtered_exit_orders.append(order)
            exit_orders = filtered_exit_orders

        for order in exit_orders:
            meta = dict(order.meta or {})
            meta["bypass_turnover_cap_for_exit"] = True
            meta.setdefault("turnover_cap_bypass_reason", "exit_signal_priority")
            order.meta = meta
            router_payload = {
                "symbol": order.symbol,
                "action": "create",
                "reason": "exit_signal_priority",
                "source_reason": meta.get("reason"),
                "side": order.side,
                "intent": order.intent,
                "notional": float(order.notional_usdt or 0.0),
                "bypass_turnover_cap_for_exit": True,
            }
            if bool(meta.get("probe_exit", False)):
                router_payload.update(
                    {
                        "probe_exit_policy_active": True,
                        "probe_type": meta.get("probe_type"),
                        "entry_reason": meta.get("entry_reason"),
                        "net_bps": meta.get("net_bps"),
                        "highest_net_bps": meta.get("highest_net_bps"),
                        "hold_hours": meta.get("hold_hours", meta.get("held_hours")),
                        "exit_reason": meta.get("exit_reason", meta.get("reason")),
                    }
                )
            if bool(meta.get("protect_profit_lock_exit", False)):
                router_payload.update(
                    {
                        "protect_profit_lock_active": True,
                        "entry_px": meta.get("entry_px"),
                        "current_px": meta.get("current_px"),
                        "net_bps": meta.get("net_bps"),
                        "highest_net_bps": meta.get("highest_net_bps"),
                        "effective_stop_px": meta.get("effective_stop_px"),
                        "exit_reason": meta.get("exit_reason", meta.get("reason")),
                    }
                )
            exit_router_decisions.append(router_payload)
        exit_symbols = {o.symbol for o in exit_orders}
        
        if audit:
            audit.counts["orders_exit"] = len(exit_orders)
            # capture detailed exit reasons for explainability
            xs = []
            for o in exit_orders:
                meta = o.meta or {}
                xs.append(
                    {
                        "symbol": o.symbol,
                        "side": o.side,
                        "intent": o.intent,
                        "reason": meta.get("reason"),
                        "last": meta.get("last") or o.signal_price,
                        "stop": meta.get("stop"),
                        "highest": meta.get("highest"),
                        "atr": meta.get("atr"),
                        "atr_mult": meta.get("atr_mult"),
                        "atr_n": meta.get("atr_n"),
                        "bypass_turnover_cap_for_exit": bool(meta.get("bypass_turnover_cap_for_exit", False)),
                        "turnover_cap_bypass_reason": meta.get("turnover_cap_bypass_reason"),
                        "probe_exit_policy_active": bool(meta.get("probe_exit_policy_active", False)),
                        "probe_type": meta.get("probe_type"),
                        "protect_profit_lock_active": bool(meta.get("protect_profit_lock_active", False)),
                        "entry_px": meta.get("entry_px"),
                        "current_px": meta.get("current_px"),
                        "net_bps": meta.get("net_bps"),
                        "highest_net_bps": meta.get("highest_net_bps"),
                        "effective_stop_px": meta.get("effective_stop_px"),
                        "hold_hours": meta.get("hold_hours", meta.get("held_hours")),
                        "exit_reason": meta.get("exit_reason", meta.get("reason")),
                    }
                )
            audit.exit_signals = xs

        # 7. Rebalance orders生成（deadband + 拒绝原因审计）
        rebalance_orders: List[Order] = []
        router_decisions = (
            list(exit_router_decisions)
            + list(position_state_cleanup_router_decisions)
            + list(btc_leadership_probe_router_decisions)
            + list(protect_profit_lock_router_decisions)
        )
        invalid_price_warnings: List[Dict] = []  # 记录价格无效的告警

        # Risk-Off 下是否进入 close-only：
        # 仅当策略明确将 risk_off 仓位倍数设为 0 时，才强制禁止 rebalance buy。
        # 这样可支持“Risk-Off 试探仓”（例如 pos_mult_risk_off=0.2）。
        regime_state_str = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
        risk_off_mult = float(getattr(self.cfg.regime, 'pos_mult_risk_off', 0.0))
        is_risk_off_close_only = (
            regime_state_str in ("Risk-Off", "Risk_Off", "RiskOff") and risk_off_mult <= 0.0
        )
        if is_risk_off_close_only and audit:
            audit.add_note("Risk-Off close-only: rebalance buy suppressed (pos_mult_risk_off<=0)")

        # deadband: adapt by regime
        rstate = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
        if rstate == "Trending":
            deadband_base = float(self.cfg.rebalance.deadband_trending)
        elif rstate in ("Risk-Off", "Risk_Off", "RiskOff"):
            deadband_base = float(self.cfg.rebalance.deadband_riskoff)
        else:
            deadband_base = float(self.cfg.rebalance.deadband_sideways)

        deadband = _effective_deadband(deadband_base, self.cfg, audit)
        if audit:
            audit.rebalance_deadband_pct = deadband
            # record budget action (F3.1)
            b = audit.budget or {}
            if b.get("exceeded") and self.cfg.budget.action_enabled:
                audit.budget_action = {
                    "enabled": True,
                    "trigger": b.get("reason") or "unknown",
                    "deadband_base": deadband_base,
                    "deadband_multiplier": float(self.cfg.budget.deadband_multiplier_exceeded),
                    "deadband_cap": float(self.cfg.budget.deadband_cap),
                    "deadband_effective": deadband,
                    "min_trade_notional_multiplier": 1.0,
                    "min_trade_notional_effective": None,
                    "suppressed_orders_count": 0,
                    "suppressed_reasons": [],
                }
            else:
                audit.budget_action = {"enabled": False}

        # current weights (with dust filtering)
        current_w: Dict[str, float] = {}
        dust_position_info_by_symbol: Dict[str, Dict[str, float]] = {}
        # Small-account-safe dust thresholds:
        # - value threshold is primary
        # - qty threshold only applies to tiny-value positions (to avoid wiping valid low-price holdings)
        DUST_QTY_THRESHOLD = float(_coalesce(getattr(self.cfg.execution, 'dust_qty_threshold', None), 1e-6))

        if equity > 0:
            for p in positions:
                pxp = float(prices.get(p.symbol, 0.0) or 0.0)
                if pxp <= 0:
                    continue

                qty = float(p.qty or 0.0)
                position_value = qty * pxp
                dust_threshold_usdt = self._dust_position_threshold_usdt(symbol=p.symbol, px=pxp)

                # Treat as dust only when value is truly tiny.
                # qty gate is secondary and only meaningful in tiny-value zone.
                is_dust = position_value < dust_threshold_usdt
                if is_dust:
                    dust_position_info_by_symbol[p.symbol] = {
                        "held_value_usdt": float(position_value),
                        "raw_held_value_usdt": float(position_value),
                        "effective_held_value_usdt": 0.0,
                        "dust_threshold_usdt": float(dust_threshold_usdt),
                    }
                    if audit and qty > 0:
                        audit.add_note(
                            f"Dust filter: {p.symbol} qty={qty:.8f} value=${position_value:.4f} "
                            f"(qty_th={DUST_QTY_THRESHOLD}, value_th={dust_threshold_usdt:.4f}) treated as 0"
                        )
                    continue

                current_w[p.symbol] = position_value / float(equity)

        cash_remaining = float(cash_usdt)

        # Rebalance should also handle symbols currently held but removed from target universe.
        # Iterate union(current positions, target weights). For symbols not in target, desired weight = 0.
        held_symbols = {p.symbol for p in positions if float(getattr(p, 'qty', 0.0) or 0.0) > 0}
        symbols_all = sorted(set(current_w.keys()) | set(target.keys()) | held_symbols)

        # Optional hard rule: force-close held symbols that are no longer in current scored universe.
        scored_symbols = set(alpha.scores.keys()) if alpha and getattr(alpha, 'scores', None) else set()
        force_close_unscored = bool(getattr(self.cfg.execution, 'force_close_unscored_positions', False))
        target_hold_eps = float(_coalesce(getattr(self.cfg.rebalance, "close_only_weight_eps", None), 0.001))

        # Optional hard rule: require fused strategy signals for any buy order (disable alpha fallback buys).
        require_fused_buy = bool(getattr(self.cfg.execution, 'require_fused_signals_for_buy', False))
        fused_buy_symbols = set()
        protect_entry_gate_active = str(current_auto_risk_level or "").upper() == "PROTECT"
        protect_entry_require_alpha6_confirmation = bool(
            getattr(self.cfg.execution, "protect_entry_require_alpha6_confirmation", True)
        )
        protect_entry_block_trend_only = bool(
            getattr(self.cfg.execution, "protect_entry_block_trend_only", True)
        )
        protect_entry_require_alpha6_rsi_confirm_positive = bool(
            getattr(self.cfg.execution, "protect_entry_require_alpha6_rsi_confirm_positive", True)
        )
        protect_entry_alpha6_min_score = float(
            getattr(self.cfg.execution, "protect_entry_alpha6_min_score", 0.40) or 0.0
        )
        protect_entry_require_volume_confirm = bool(
            getattr(self.cfg.execution, "protect_entry_require_volume_confirm", True)
        )
        protect_entry_min_f4_volume_expansion = float(
            getattr(self.cfg.execution, "protect_entry_min_f4_volume_expansion", 0.0) or 0.0
        )
        protect_entry_min_f5_rsi_trend_confirm = float(
            getattr(self.cfg.execution, "protect_entry_min_f5_rsi_trend_confirm", 0.30) or 0.0
        )
        protect_replacement_close_guard_enabled = bool(
            getattr(self.cfg.execution, "protect_replacement_close_guard_enabled", True)
        )
        protect_hold_current_when_replacement_blocked = bool(
            getattr(self.cfg.execution, "protect_hold_current_when_replacement_blocked", True)
        )
        protect_replacement_hold_min_score = float(
            getattr(self.cfg.execution, "protect_replacement_hold_min_score", 0.10) or 0.0
        )
        strategy_signal_lookup = (
            self._resolve_strategy_signal_lookup(audit)
            if protect_entry_gate_active
            else {}
        )
        if audit:
            audit.protect_entry_gate_active = bool(protect_entry_gate_active)
            audit.protect_entry_require_alpha6_confirmation = protect_entry_require_alpha6_confirmation
            audit.protect_entry_block_trend_only = protect_entry_block_trend_only
            audit.protect_entry_require_alpha6_rsi_confirm_positive = protect_entry_require_alpha6_rsi_confirm_positive
            audit.protect_entry_alpha6_min_score = protect_entry_alpha6_min_score
        if protect_entry_gate_active and audit:
            audit.add_note(
                "PROTECT entry gate active: "
                f"require_alpha6_confirmation={protect_entry_require_alpha6_confirmation}, "
                f"block_trend_only={protect_entry_block_trend_only}, "
                f"require_alpha6_rsi_confirm_positive={protect_entry_require_alpha6_rsi_confirm_positive}, "
                f"alpha6_min_score={protect_entry_alpha6_min_score:.2f}, "
                f"require_volume_confirm={protect_entry_require_volume_confirm}, "
                f"min_f4_volume_expansion={protect_entry_min_f4_volume_expansion:.2f}, "
                f"min_f5_rsi_trend_confirm={protect_entry_min_f5_rsi_trend_confirm:.2f}, "
                f"replacement_close_guard_enabled={protect_replacement_close_guard_enabled}, "
                f"hold_current_when_replacement_blocked={protect_hold_current_when_replacement_blocked}, "
                f"replacement_hold_min_score={protect_replacement_hold_min_score:.2f}"
            )
        if require_fused_buy:
            try:
                import json as _json

                strategy_file = self.alpha_engine.strategy_signals_path()
                if strategy_file is not None and strategy_file.exists():
                    obj = _json.loads(strategy_file.read_text(encoding='utf-8'))
                    fused = obj.get('fused')
                    if isinstance(fused, dict) and fused:
                        for fsym, sig in fused.items():
                            if str((sig or {}).get('direction', '')).lower() == 'buy':
                                fused_buy_symbols.add(str(fsym))
            except Exception:
                fused_buy_symbols = set()

            if audit:
                audit.add_note(f"require_fused_signals_for_buy enabled: fused_buy_symbols={len(fused_buy_symbols)}")

        # 预收集所有买入候选，用于比例现金分配
        buy_candidates = []
        for sym in symbols_all:
            tw = float(target.get(sym, 0.0))
            cw = float(current_w.get(sym, 0.0))
            drift = float(tw) - cw
            if drift > 0:
                buy_candidates.append((sym, drift, tw))
        
        # 计算总买入权重，用于比例分配
        total_buy_drift = sum(d for _, d, _ in buy_candidates) if buy_candidates else 0.0
        replacement_open_candidates: set[str] = set()
        blocked_replacement_reasons: Dict[str, str] = {}
        successful_replacement_symbols: set[str] = set()
        pending_zero_target_close_candidates: list[Dict[str, Any]] = []

        for sym in symbols_all:
            tw = float(target.get(sym, 0.0))
            # deadband check on weight drift with banding: new position vs existing
            cw = float(current_w.get(sym, 0.0))
            drift = float(tw) - cw

            held = next((p for p in positions if p.symbol == sym and float(getattr(p, 'qty', 0.0) or 0.0) > 0), None)
            dust_position_info = dust_position_info_by_symbol.get(sym)
            held_is_dust = held is not None and dust_position_info is not None
            active_held = None if held_is_dust else held
            btc_probe_meta = btc_leadership_probe_meta_by_symbol.get(sym)

            if sym in exit_symbols:
                if audit:
                    audit.add_note(f"Rebalance skipped due to exit order: {sym}")
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "exit_order_selected",
                    })
                continue

            if held_is_dust and abs(float(tw)) <= target_hold_eps:
                if audit and self._symbol_has_active_position_state(sym):
                    audit.record_count("dust_residual_no_close_order_count", symbol=sym)
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "dust_residual_no_close_order",
                        "held_value_usdt": float(dust_position_info.get("held_value_usdt") or 0.0),
                        "raw_held_value_usdt": float(dust_position_info.get("raw_held_value_usdt") or dust_position_info.get("held_value_usdt") or 0.0),
                        "effective_held_value_usdt": 0.0,
                        "dust_threshold_usdt": float(dust_position_info.get("dust_threshold_usdt") or 0.0),
                        "target_w": float(tw),
                    })
                continue

            # User hard rule: if symbol is held but absent from scoring list, force CLOSE_LONG.
            if force_close_unscored and active_held is not None and scored_symbols and sym not in scored_symbols:
                px_fs = float(prices.get(sym, 0.0) or 0.0)
                if px_fs <= 0:
                    px_fs = float(getattr(active_held, 'last_mark_px', 0.0) or 0.0)
                if px_fs > 0:
                    notional_fs = max(0.0, float(getattr(active_held, 'qty', 0.0) or 0.0) * px_fs)
                    if notional_fs > 0:
                        rebalance_orders.append(
                            Order(
                                symbol=sym,
                                side='sell',
                                intent='CLOSE_LONG',
                                notional_usdt=notional_fs,
                                signal_price=px_fs,
                                meta={
                                    'reason': 'force_close_unscored',
                                    'bypass_turnover_cap_for_exit': True,
                                    'turnover_cap_bypass_reason': 'zero_target_close',
                                },
                            )
                        )
                        if audit:
                            audit.add_note(f"Force close unscored: {sym} qty={float(getattr(active_held,'qty',0.0)):.8f}")
                            router_decisions.append(
                                {
                                    'symbol': sym,
                                    'action': 'close',
                                    'reason': 'zero_target_close',
                                    'source_reason': 'force_close_unscored',
                                    'bypass_turnover_cap_for_exit': True,
                                }
                            )
                        continue
            
            # Banding 逻辑：新建仓阈值 > 维持仓阈值
            # 判断是否是新建仓（当前权重接近0）
            eps = float(_coalesce(getattr(self.cfg.rebalance, "new_position_weight_eps", None), 0.001))
            is_new_position = cw < eps

            # 调整 deadband：新建仓需要更大的信号强度；清仓（tw≈0）允许更小 deadband 以加速清理
            effective_deadband = deadband
            if is_new_position:
                mult = float(getattr(self.cfg.rebalance, "new_position_deadband_multiplier", 2.0) or 2.0)
                effective_deadband = deadband * mult
                if audit:
                    audit.add_note(f"Banding: {sym} is new position, deadband {deadband}→{effective_deadband:.3f}")

            # If target weight is ~0 (close-only), shrink deadband (but keep sells allowed) to avoid stuck dust positions.
            try:
                if abs(float(tw)) <= target_hold_eps and abs(float(cw)) > target_hold_eps:
                    # 清仓模式：死区大幅降低，确保能卖出
                    cm = float(_coalesce(getattr(self.cfg.rebalance, "close_only_deadband_multiplier", None), 0.1))
                    effective_deadband = min(float(effective_deadband), float(deadband) * float(cm))
                    if audit:
                        audit.add_note(f"Close-only: {sym} tw≈0, deadband {deadband}→{effective_deadband:.3f} (force exit)")
            except Exception:
                pass
            
            if audit:
                audit.rebalance_drift_by_symbol[sym] = drift
                audit.rebalance_effective_deadband_by_symbol[sym] = effective_deadband

            target_zero_reason = target_zero_reason_by_symbol.get(sym)
            if held is None and abs(float(tw)) <= target_hold_eps:
                if audit:
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "target_zero_no_order",
                        "target_zero_reason": target_zero_reason or "zero_target",
                        "drift": drift,
                        "deadband": effective_deadband,
                        "is_new_position": is_new_position,
                    })
                continue

            if abs(drift) <= effective_deadband:
                if audit:
                    audit.rebalance_skipped_deadband_count += 1
                    audit.rebalance_skipped_deadband_by_symbol[sym] = abs(drift)
                    audit.reject("deadband_skip")
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "deadband",
                        "drift": drift,
                        "deadband": effective_deadband,
                        "is_new_position": is_new_position,
                    })
                continue

            px = float(prices.get(sym, 0.0) or 0.0)
            if px <= 0:
                if audit:
                    audit.reject("no_closed_bar")
                # 记录价格无效告警
                invalid_price_warnings.append({
                    "symbol": sym,
                    "timestamp": utc_now_iso(),
                    "reason": "price_invalid_or_missing"
                })
                continue
            
            # P0 FIX: Risk-Off close-only 模式：跳过所有买入型的 rebalance
            if is_risk_off_close_only and drift > 0:
                if audit:
                    audit.record_count("risk_off_suppressed_count", symbol=sym)
                    audit.reject("risk_off_close_only")
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "risk_off_close_only",
                        "drift": drift,
                    })
                continue

            # If symbol is removed from target universe but currently held, generate a sell to reduce drift.
            # P0 FIX: 统一逻辑：根据 drift 符号决定买卖方向
            if drift < 0:
                # 需要减仓/清仓
                side = "sell"
                intent = "REBALANCE"
                # P0 FIX: notional 用 delta 计算
                notional = abs(float(drift)) * float(equity)
                if notional <= 0:
                    continue
            else:
                # drift > 0，需要加仓
                side = "buy"
                intent = "OPEN_LONG" if active_held is None else "REBALANCE"
                # FIX: 按比例分配现金，而不是使用 drift * equity
                # 这样可以避免第一个标的全额买入导致后续标的无法建仓
                desired_notional = abs(float(drift)) * float(equity)
                if total_buy_drift > 0 and cash_usdt > 0:
                    # 按 drift 比例分配可用现金
                    total_desired_buy_notional = float(total_buy_drift) * float(equity)
                    if total_desired_buy_notional > float(cash_usdt):
                        drift_ratio = abs(float(drift)) / total_buy_drift
                        notional = drift_ratio * float(cash_usdt)
                    else:
                        notional = desired_notional
                else:
                    notional = desired_notional
                if notional <= 0:
                    continue

            if side == "buy" and intent == "OPEN_LONG":
                replacement_open_candidates.add(sym)

            bypass_turnover_cap_for_exit = bool(
                side == "sell" and active_held is not None and abs(float(tw)) <= float(target_hold_eps)
            )

            if (
                side == "sell"
                and active_held is not None
                and abs(float(tw)) <= float(target_hold_eps)
                and protect_entry_gate_active
                and protect_replacement_close_guard_enabled
                and protect_hold_current_when_replacement_blocked
                and not is_risk_off_close_only
            ):
                held_score = None
                try:
                    held_score = float((alpha.scores or {}).get(sym))
                except Exception:
                    held_score = None
                pending_zero_target_close_candidates.append(
                    {
                        "symbol": sym,
                        "order": Order(
                            symbol=sym,
                            side=side,
                            intent=intent,
                            notional_usdt=notional,
                            signal_price=px,
                            meta={},
                        ),
                        "held_score": held_score,
                    }
                )
                continue

            if side == "buy" and eligible_buy_symbols and sym not in eligible_buy_symbols:
                if audit:
                    audit.reject("off_ranking_buy_block")
                    router_decisions.append(
                        {
                            "symbol": sym,
                            "action": "skip",
                            "reason": "off_ranking_buy_block",
                            "eligible_buy_symbols": sorted(eligible_buy_symbols),
                        }
                    )
                continue

            if side == "buy" and intent == "OPEN_LONG" and protect_entry_gate_active and btc_probe_meta is None:
                protect_block = self._evaluate_protect_entry_gate(
                    symbol=sym,
                    strategy_signal_lookup=strategy_signal_lookup,
                    current_auto_risk_level=current_auto_risk_level,
                    now_utc=now_utc,
                    current_run_id=run_id,
                )
                if protect_block is not None:
                    self._record_replacement_block(
                        audit=audit,
                        blocked_replacement_reasons=blocked_replacement_reasons,
                        symbol=sym,
                        reason=str(protect_block.get("reason") or "protect_entry_no_alpha6_confirmation"),
                    )
                    if audit:
                        audit.record_count("protect_entry_block_count", symbol=sym)
                        if str(protect_block.get("reason") or "") == "protect_entry_trend_only":
                            audit.record_count("protect_entry_trend_only_block_count", symbol=sym)
                        if str(protect_block.get("reason") or "") == "protect_entry_alpha6_score_too_low":
                            audit.record_count("protect_entry_alpha6_score_too_low_count", symbol=sym)
                        if str(protect_block.get("reason") or "") == "protect_entry_volume_confirm_negative":
                            audit.record_count("protect_entry_volume_confirm_negative_count", symbol=sym)
                        if str(protect_block.get("reason") or "") == "protect_entry_rsi_confirm_too_weak":
                            audit.record_count("protect_entry_rsi_confirm_too_weak_count", symbol=sym)
                            audit.record_count("protect_entry_alpha6_rsi_block_count", symbol=sym)
                        if str(protect_block.get("reason") or "") == "protect_entry_alpha6_rsi_confirm_negative":
                            audit.record_count("protect_entry_alpha6_rsi_block_count", symbol=sym)
                        if str(protect_block.get("reason") or "") == "protect_entry_confirmation_not_stable":
                            audit.record_count("protect_entry_confirmation_not_stable_count", symbol=sym)
                        audit.record_gate(str(protect_block.get("reason") or "protect_entry_no_alpha6_confirmation"), symbol=sym)
                        if btc_probe_meta is not None:
                            audit.record_count(
                                "btc_leadership_probe_blocked_count",
                                symbol=f"{sym}:{str(protect_block.get('reason') or 'protect_entry_no_alpha6_confirmation')}",
                            )
                        decision = {
                            "symbol": sym,
                            "action": "skip",
                            "reason": str(protect_block.get("reason") or "protect_entry_no_alpha6_confirmation"),
                            "trend_score": protect_block.get("trend_score"),
                            "alpha6_score": protect_block.get("alpha6_score"),
                            "alpha6_side": protect_block.get("alpha6_side"),
                            "f4_volume_expansion": protect_block.get("f4_volume_expansion"),
                            "f5_rsi_trend_confirm": protect_block.get("f5_rsi_trend_confirm"),
                            "current_alpha6_score": protect_block.get("current_alpha6_score"),
                            "current_f5": protect_block.get("current_f5"),
                            "current_f4": protect_block.get("current_f4"),
                            "confirm_rounds_observed": protect_block.get("confirm_rounds_observed"),
                            "required_confirm_rounds": protect_block.get("required_confirm_rounds"),
                            "current_level": current_auto_risk_level,
                        }
                        if btc_probe_meta is not None:
                            decision.update(btc_probe_meta)
                            decision["blocked_reason"] = str(decision.get("reason") or "")
                        router_decisions.append(decision)
                    continue

            neg_stats = None
            if side == "buy" and neg_feedback_enabled:
                try:
                    neg_stats = self.negative_expectancy_cooldown.get_symbol_stats(sym)
                except Exception:
                    neg_stats = None

            # Negative expectancy cooldown gate
            if side == "buy" and neg_cd_enabled:
                blocked = self.negative_expectancy_cooldown.is_blocked(sym)
                if blocked:
                    if intent == "OPEN_LONG":
                        self._record_replacement_block(
                            audit=audit,
                            blocked_replacement_reasons=blocked_replacement_reasons,
                            symbol=sym,
                            reason="negative_expectancy_cooldown",
                        )
                    if audit:
                        audit.record_gate("negative_expectancy_cooldown", symbol=sym)
                        router_decisions.append(
                            {
                                "symbol": sym,
                                "action": "skip",
                                "reason": "negative_expectancy_cooldown",
                                "expectancy_usdt": self._negative_expectancy_usdt(blocked),
                                "net_expectancy_bps": self._negative_expectancy_bps(blocked),
                                "closed_cycles": int(blocked.get("closed_cycles") or 0),
                                "remain_seconds": float(blocked.get("remain_seconds") or 0.0),
                            }
                        )
                    continue

            if (
                side == "buy"
                and intent == "OPEN_LONG"
                and neg_feedback_enabled
                and bool(getattr(self.cfg.execution, "negative_expectancy_open_block_enabled", False))
            ):
                min_cycles = int(
                    getattr(self.cfg.execution, "negative_expectancy_open_block_min_closed_cycles", 2) or 2
                )
                floor_bps = float(
                    _coalesce(getattr(self.cfg.execution, "negative_expectancy_open_block_floor_bps", 5.0), 5.0)
                )
                closed_cycles = int((neg_stats or {}).get("closed_cycles") or 0)
                expectancy_bps = self._negative_expectancy_bps(neg_stats or {})
                if closed_cycles >= min_cycles and expectancy_bps < floor_bps:
                    if btc_probe_meta is not None and self._btc_leadership_probe_negative_bypass_allowed(neg_stats):
                        btc_probe_meta["bypassed_negative_expectancy"] = True
                        if audit:
                            audit.record_count("btc_leadership_probe_negative_expectancy_bypass_count", symbol=sym)
                            audit.add_note(
                                "BTC leadership probe bypassed negative expectancy: "
                                f"{sym} closed_cycles={closed_cycles} net_expectancy_bps={expectancy_bps:.2f}"
                            )
                    else:
                        if intent == "OPEN_LONG":
                            self._record_replacement_block(
                                audit=audit,
                                blocked_replacement_reasons=blocked_replacement_reasons,
                                symbol=sym,
                                reason="negative_expectancy_open_block",
                            )
                        if audit:
                            audit.record_gate("negative_expectancy_open_block", symbol=sym)
                            if btc_probe_meta is not None:
                                audit.record_count("btc_leadership_probe_blocked_count", symbol=f"{sym}:negative_expectancy_open_block")
                            decision = {
                                "symbol": sym,
                                "action": "skip",
                                "reason": "negative_expectancy_open_block",
                                "expectancy_bps": expectancy_bps,
                                "closed_cycles": closed_cycles,
                                "required_expectancy_bps": floor_bps,
                            }
                            if btc_probe_meta is not None:
                                decision.update(btc_probe_meta)
                                decision["blocked_reason"] = "negative_expectancy_open_block"
                            router_decisions.append(decision)
                        continue

            if (
                side == "buy"
                and intent == "OPEN_LONG"
                and neg_feedback_enabled
                and bool(getattr(self.cfg.execution, "negative_expectancy_fast_fail_open_block_enabled", False))
            ):
                ff_min_cycles = int(
                    getattr(
                        self.cfg.execution,
                        "negative_expectancy_fast_fail_open_block_min_closed_cycles",
                        2,
                    )
                    or 2
                )
                ff_floor_bps = float(
                    getattr(
                        self.cfg.execution,
                        "negative_expectancy_fast_fail_open_block_floor_bps",
                        0.0,
                    )
                    or 0.0
                )
                ff_hold_minutes = int(
                    getattr(
                        self.cfg.execution,
                        "negative_expectancy_fast_fail_max_hold_minutes",
                        120,
                    )
                    or 120
                )
                ff_closed_cycles = int((neg_stats or {}).get("fast_fail_closed_cycles") or 0)
                ff_expectancy_bps = self._negative_expectancy_bps(neg_stats or {}, fast_fail=True)
                ff_avg_hold_minutes = float((neg_stats or {}).get("fast_fail_avg_hold_minutes") or 0.0)
                if ff_closed_cycles >= ff_min_cycles and ff_expectancy_bps < ff_floor_bps:
                    if btc_probe_meta is not None and self._btc_leadership_probe_negative_bypass_allowed(neg_stats):
                        already_bypassed = bool(btc_probe_meta.get("bypassed_negative_expectancy", False))
                        btc_probe_meta["bypassed_negative_expectancy"] = True
                        if audit and not already_bypassed:
                            audit.record_count("btc_leadership_probe_negative_expectancy_bypass_count", symbol=sym)
                            audit.add_note(
                                "BTC leadership probe bypassed fast-fail negative expectancy: "
                                f"{sym} fast_fail_closed_cycles={ff_closed_cycles} net_expectancy_bps={ff_expectancy_bps:.2f}"
                            )
                    else:
                        soften_fast_fail, soften_ctx = self._should_soften_fast_fail_with_market_impulse(
                            symbol=sym,
                            stat=neg_stats or {},
                            strategy_signal_lookup=strategy_signal_lookup,
                            current_auto_risk_level=current_auto_risk_level,
                            regime_state_str=regime_state_str,
                        )
                        if soften_fast_fail:
                            if audit:
                                audit.record_count("negative_expectancy_fast_fail_softened_count", symbol=sym)
                                audit.add_note(
                                    "negative_expectancy_fast_fail_softened_by_market_impulse: "
                                    f"{sym} trend_buy_count={int(soften_ctx.get('trend_buy_count') or 0)} "
                                    f"btc_trend_score={soften_ctx.get('btc_trend_score')} "
                                    f"fast_fail_closed_cycles={int(soften_ctx.get('fast_fail_closed_cycles') or 0)} "
                                    f"net_expectancy_bps={float(soften_ctx.get('net_expectancy_bps') or 0.0):.2f}"
                                )
                            # keep score penalty path only; do not hard-block OPEN_LONG
                            pass
                        else:
                            if intent == "OPEN_LONG":
                                self._record_replacement_block(
                                    audit=audit,
                                    blocked_replacement_reasons=blocked_replacement_reasons,
                                    symbol=sym,
                                    reason="negative_expectancy_fast_fail_open_block",
                                )
                            if audit:
                                audit.record_gate("negative_expectancy_fast_fail_open_block", symbol=sym)
                                audit.record_count("negative_expectancy_fast_fail_hard_block_count", symbol=sym)
                                if btc_probe_meta is not None:
                                    audit.record_count("btc_leadership_probe_blocked_count", symbol=f"{sym}:negative_expectancy_fast_fail_open_block")
                                decision = {
                                    "symbol": sym,
                                    "action": "skip",
                                    "reason": "negative_expectancy_fast_fail_open_block",
                                    "fast_fail_expectancy_bps": ff_expectancy_bps,
                                    "fast_fail_closed_cycles": ff_closed_cycles,
                                    "fast_fail_avg_hold_minutes": ff_avg_hold_minutes,
                                    "fast_fail_max_hold_minutes": ff_hold_minutes,
                                    "required_expectancy_bps": ff_floor_bps,
                                }
                                if btc_probe_meta is not None:
                                    decision.update(btc_probe_meta)
                                    decision["blocked_reason"] = "negative_expectancy_fast_fail_open_block"
                                router_decisions.append(decision)
                            continue
            
            # Require fused signals for buys (no alpha-fallback buys).
            if side == "buy" and require_fused_buy:
                if not fused_buy_symbols:
                    if audit:
                        audit.reject("require_fused_missing")
                        router_decisions.append(
                            {
                                "symbol": sym,
                                "action": "skip",
                                "reason": "require_fused_missing",
                            }
                        )
                    continue
                if sym not in fused_buy_symbols:
                    if audit:
                        audit.reject("require_fused_symbol")
                        router_decisions.append(
                            {
                                "symbol": sym,
                                "action": "skip",
                                "reason": "require_fused_symbol",
                            }
                        )
                    continue

            # Hard budget buy-block: when equity hits configured cap, force sell-only.
            # Prefer live equity from main(audit.budget.current_equity_usdt) to avoid local-state drift bypass.
            if side == "buy" and bool(getattr(self.cfg.budget, "hard_buy_block_on_cap", False)):
                try:
                    cap_raw = getattr(self.cfg.budget, "live_equity_cap_usdt", None)
                    cap_ratio = float(getattr(self.cfg.budget, "hard_buy_block_cap_ratio", 1.0) or 1.0)
                    if cap_raw is not None and float(cap_raw) > 0:
                        hard_cap = float(cap_raw) * cap_ratio

                        equity_ref = float(equity_raw)
                        try:
                            if audit and isinstance(getattr(audit, "budget", None), dict):
                                eq_live = audit.budget.get("current_equity_usdt")
                                if eq_live is not None:
                                    equity_ref = float(eq_live)
                        except Exception:
                            pass

                        if float(equity_ref) >= float(hard_cap):
                            if audit:
                                audit.reject("budget_hard_buy_block")
                                router_decisions.append(
                                    {
                                        "symbol": sym,
                                        "action": "skip",
                                        "reason": "budget_hard_buy_block",
                                        "equity_ref": float(equity_ref),
                                        "hard_cap": float(hard_cap),
                                    }
                                )
                            continue
                except Exception:
                    pass

            dust_add_size_ignore_info: Dict[str, float] = {}
            if side == "buy" and held_is_dust:
                dust_add_size_ignore_info = {
                    "dust_position_ignored_for_add_size": True,
                    "held_value_usdt": float(dust_position_info.get("held_value_usdt") or 0.0),
                    "raw_held_value_usdt": float(dust_position_info.get("raw_held_value_usdt") or dust_position_info.get("held_value_usdt") or 0.0),
                    "effective_held_value_usdt": 0.0,
                    "dust_threshold_usdt": float(dust_position_info.get("dust_threshold_usdt") or 0.0),
                }
                if audit:
                    audit.record_count("dust_position_ignored_for_add_size_count", symbol=sym)
                    audit.add_note(
                        "Dust residual ignored for anti_chase_add_size: "
                        f"{sym} held_value=${float(dust_add_size_ignore_info['held_value_usdt']):.6f} "
                        f"threshold=${float(dust_add_size_ignore_info['dust_threshold_usdt']):.4f}"
                    )

            # Anti-chase for existing positions: avoid buying far above own entry and avoid oversized add-ons.
            if side == "buy" and active_held is not None and bool(getattr(self.cfg.execution, "anti_chase_enabled", False)):
                try:
                    entry_px = float(getattr(active_held, "avg_px", 0.0) or 0.0)
                    held_qty = float(getattr(active_held, "qty", 0.0) or 0.0)
                    held_value = held_qty * float(px)
                    premium = (float(px) / entry_px - 1.0) if entry_px > 0 else 0.0
                    max_premium = float(
                        _coalesce(getattr(self.cfg.execution, "anti_chase_max_entry_premium_pct", 0.015), 0.015)
                    )
                    max_add_ratio = float(
                        _coalesce(getattr(self.cfg.execution, "anti_chase_max_add_notional_ratio", 0.25), 0.25)
                    )

                    if entry_px > 0 and premium > max_premium:
                        if audit:
                            audit.reject("anti_chase_premium")
                            router_decisions.append(
                                {
                                    "symbol": sym,
                                    "action": "skip",
                                    "reason": "anti_chase_premium",
                                    "entry_px": float(entry_px),
                                    "px": float(px),
                                    "premium": float(premium),
                                    "max_premium": float(max_premium),
                                }
                            )
                        continue

                    if held_value > 0 and float(notional) > float(held_value) * float(max_add_ratio):
                        if audit:
                            audit.reject("anti_chase_add_size")
                            router_decisions.append(
                                {
                                    "symbol": sym,
                                    "action": "skip",
                                    "reason": "anti_chase_add_size",
                                    "notional": float(notional),
                                    "held_value": float(held_value),
                                    "effective_held_value_usdt": float(held_value),
                                    "max_add_ratio": float(max_add_ratio),
                                }
                            )
                        continue
                except Exception:
                    pass

            # qlib migration: cost-aware entry gate (score as edge proxy).
            if side == "buy" and bool(getattr(self.cfg.execution, "cost_aware_entry_enabled", False)):
                try:
                    score_sym = None
                    try:
                        score_sym = float(rank_scores.get(sym)) if sym in rank_scores else None
                    except Exception:
                        score_sym = None
                    if score_sym is None:
                        try:
                            score_sym = float((alpha.scores or {}).get(sym))
                        except Exception:
                            score_sym = None

                    if score_sym is not None:
                        fee_bps = float(getattr(self.cfg.execution, "fee_bps", 0.0) or 0.0)
                        slippage_bps = float(getattr(self.cfg.execution, "slippage_bps", 0.0) or 0.0)
                        rt_cost_bps_cfg = getattr(self.cfg.execution, "cost_aware_roundtrip_cost_bps", None)
                        rt_cost_bps = float(rt_cost_bps_cfg) if rt_cost_bps_cfg is not None else 2.0 * (fee_bps + slippage_bps)
                        score_per_bps = float(
                            _coalesce(getattr(self.cfg.execution, "cost_aware_score_per_bps", 0.0025), 0.0025)
                        )
                        score_floor = float(
                            _coalesce(getattr(self.cfg.execution, "cost_aware_min_score_floor", 0.08), 0.08)
                        )
                        low_price_guard_enabled = bool(
                            getattr(self.cfg.execution, "low_price_entry_guard_enabled", False)
                        )
                        low_price_threshold = float(
                            _coalesce(getattr(self.cfg.execution, "low_price_entry_threshold_usdt", 0.05), 0.05)
                        )
                        low_price_extra_floor = float(
                            _coalesce(getattr(self.cfg.execution, "low_price_entry_extra_score_floor", 0.0), 0.0)
                        )
                        low_price_extra_cost_bps = float(
                            _coalesce(getattr(self.cfg.execution, "low_price_entry_extra_cost_bps", 0.0), 0.0)
                        )
                        if low_price_guard_enabled and float(px) > 0 and float(px) <= low_price_threshold:
                            rt_cost_bps += low_price_extra_cost_bps
                            score_floor += low_price_extra_floor
                        alpha_floor = float(getattr(self.cfg.alpha, "min_score_threshold", 0.0) or 0.0)
                        required_score = max(alpha_floor, score_floor + rt_cost_bps * score_per_bps)

                        if float(score_sym) < float(required_score):
                            if intent == "OPEN_LONG":
                                self._record_replacement_block(
                                    audit=audit,
                                    blocked_replacement_reasons=blocked_replacement_reasons,
                                    symbol=sym,
                                    reason="cost_aware_edge",
                                )
                            if audit:
                                audit.reject("cost_edge_insufficient")
                                router_decisions.append(
                                    {
                                        "symbol": sym,
                                        "action": "skip",
                                        "reason": "cost_aware_edge",
                                        "score": float(score_sym),
                                        "required_score": float(required_score),
                                        "rt_cost_bps": float(rt_cost_bps),
                                        "low_price_guard_applied": bool(
                                            low_price_guard_enabled and float(px) > 0 and float(px) <= low_price_threshold
                                        ),
                                        "px": float(px),
                                    }
                                )
                            continue
                except Exception:
                    pass

            # Router check: min_notional (base + F3.2 stage-2)
            min_notional = float(self.cfg.budget.min_trade_notional_base)
            clip_min_notional = float(min_notional)
            if audit and (audit.budget or {}).get("exceeded") and self.cfg.budget.action_enabled:
                try:
                    from src.core.budget_action import effective_min_trade_notional

                    eff, patch = effective_min_trade_notional(self.cfg, audit)
                    min_notional = float(eff)
                    clip_min_notional = float(min_notional)
                    # merge patch into budget_action
                    ba = audit.budget_action or {}
                    ba.update(patch)
                    audit.budget_action = ba
                except Exception:
                    pass

            # Borrow-prevention filter (live): skip opening high-risk low-price meme coins.
            # - allow sells to exit/clean up positions
            if side == "buy" and bool(getattr(self.cfg.execution, "borrow_prevention", False)):
                rules = _load_borrow_prevention_rules(str(getattr(self.cfg.execution, "high_risk_blacklist_path", "configs/borrow_prevention_rules.json")))
                mp = _min_price_usdt(rules=rules)
                if _is_high_risk_symbol(sym, rules=rules):
                    if audit:
                        audit.reject("high_risk_symbol")
                        router_decisions.append({"symbol": sym, "action": "skip", "reason": "high_risk_symbol"})
                    continue
                if mp is not None and float(px) < float(mp):
                    if audit:
                        audit.reject("min_price")
                        router_decisions.append({"symbol": sym, "action": "skip", "reason": f"min_price<{mp}", "px": px})
                    continue

            # Exchange min-order filter (symbol-specific): avoid placing orders that the exchange will reject.
            # Uses OKX instrument minSz (base qty) to estimate a minimum USDT notional.
            if side == "buy" and bool(getattr(self.cfg.budget, "exchange_min_notional_enabled", True)):
                try:
                    from src.data.okx_instruments import OKXSpotInstrumentsCache

                    spec = OKXSpotInstrumentsCache().get_spec(symbol_to_inst_id(sym))
                    if spec is not None:
                        min_sz = float(spec.min_sz or 0.0)
                        # Estimate min notional requirement from base minSz.
                        min_notional_ex = float(min_sz) * float(px)
                        slack = float(getattr(self.cfg.budget, "exchange_min_notional_slack_multiplier", 1.05) or 1.05)
                        if min_notional_ex > 0:
                            clip_min_notional = max(float(clip_min_notional), float(min_notional_ex) * float(slack))
                        if min_notional_ex > 0 and float(notional) < float(min_notional_ex) * slack:
                            if audit:
                                audit.reject("exchange_min_notional")
                                router_decisions.append(
                                    {
                                        "symbol": sym,
                                        "action": "skip",
                                        "reason": "exchange_min_notional",
                                        "notional": float(notional),
                                        "min_notional_ex": float(min_notional_ex),
                                        "min_sz": float(min_sz),
                                        "px": float(px),
                                        "slack": float(slack),
                                    }
                                )
                            continue
                except Exception:
                    pass

            # Min-notional filter: apply to buys; allow sells (especially for removed symbols) to reduce drift.
            if side == "buy" and notional < float(min_notional):
                if intent == "OPEN_LONG":
                    self._record_replacement_block(
                        audit=audit,
                        blocked_replacement_reasons=blocked_replacement_reasons,
                        symbol=sym,
                        reason="min_notional",
                    )
                if audit:
                    audit.reject("min_notional")
                    router_decisions.append(
                        {
                            "symbol": sym,
                            "action": "skip",
                            "reason": "min_notional",
                            "notional": notional,
                            "min_notional": float(min_notional),
                        }
                    )
                    # budget_action suppression stats
                    try:
                        ba = audit.budget_action or {}
                        if ba.get("enabled"):
                            ba.setdefault("suppressed_reasons", [])
                            if "min_notional" not in ba["suppressed_reasons"]:
                                ba["suppressed_reasons"].append("min_notional")
                            ba["suppressed_orders_count"] = int(ba.get("suppressed_orders_count") or 0) + 1
                            sbs = ba.get("suppressed_by_symbol") or {}
                            sbs[sym] = float(notional)
                            ba["suppressed_by_symbol"] = sbs
                            audit.budget_action = ba
                    except Exception:
                        pass
                continue
            
            # 检查cash是否足够（按批次累计扣减，避免多单同时通过导致超额下单）
            if side == "buy" and notional > cash_remaining:
                if intent == "OPEN_LONG":
                    self._record_replacement_block(
                        audit=audit,
                        blocked_replacement_reasons=blocked_replacement_reasons,
                        symbol=sym,
                        reason="insufficient_cash",
                    )
                if audit:
                    audit.reject("insufficient_cash")
                    router_decisions.append(
                        {
                            "symbol": sym,
                            "action": "skip",
                            "reason": "insufficient_cash",
                            "notional": notional,
                            "cash_available": cash_remaining,
                            "cash_initial": float(cash_usdt),
                        }
                    )
                continue
            
            # 如果通过所有检查，生成订单
            if side == "buy" and intent == "OPEN_LONG":
                entry_kind = "btc_leadership_probe" if btc_probe_meta is not None else "normal_entry"
                reentry_guard = self._evaluate_same_symbol_reentry_guard(
                    symbol=sym,
                    latest_px=px,
                    entry_kind=entry_kind,
                    audit=audit,
                )
                if bool(reentry_guard.get("blocked", False)):
                    self._record_replacement_block(
                        audit=audit,
                        blocked_replacement_reasons=blocked_replacement_reasons,
                        symbol=sym,
                        reason="same_symbol_reentry_cooldown",
                    )
                    if audit:
                        audit.record_count("same_symbol_reentry_cooldown_count", symbol=sym)
                        if btc_probe_meta is not None:
                            audit.record_count(
                                "btc_leadership_probe_blocked_count",
                                symbol=f"{sym}:same_symbol_reentry_cooldown",
                            )
                        decision = self._same_symbol_reentry_block_decision(sym, reentry_guard)
                        if btc_probe_meta is not None:
                            decision.update(btc_probe_meta)
                            decision["btc_leadership_probe"] = True
                            decision["blocked_reason"] = "same_symbol_reentry_cooldown"
                        router_decisions.append(decision)
                    continue

            meta = {
                "target_w": tw,
                "dd_mult": dd_mult,
                "score_rank": int(symbol_ranks.get(sym, 1_000_000)),
            }
            if dust_add_size_ignore_info:
                meta.update(dust_add_size_ignore_info)
            if side == "buy":
                meta["clip_min_notional"] = float(clip_min_notional)
                if btc_probe_meta is not None:
                    meta.update(btc_probe_meta)
                probe_tags = probe_tags_from_order_meta(meta, entry_px=px, entry_ts=utc_now_iso())
                if probe_tags is not None:
                    meta.update(probe_tags)
            if bypass_turnover_cap_for_exit:
                meta["bypass_turnover_cap_for_exit"] = True
                meta["turnover_cap_bypass_reason"] = "zero_target_close"
                meta["target_hold_eps"] = float(target_hold_eps)
            if audit:
                meta.update(
                    {
                        "regime": audit.regime,
                        "window_start_ts": audit.window_start_ts,
                        "window_end_ts": audit.window_end_ts,
                        "deadband_pct": audit.rebalance_deadband_pct,
                        "drift": drift,
                    }
                )

            rebalance_orders.append(
                Order(
                    symbol=sym,
                    side=side,
                    intent=intent,
                    notional_usdt=notional,
                    signal_price=px,
                    meta=meta,
                )
            )

            # 买入订单：注册止损和利润管理
            if side == "buy":
                self.fixed_stop_loss.register_position(sym, px)
                probe_tags = probe_tags_from_order_meta(meta, entry_px=px, entry_ts=utc_now_iso())
                probe_kwargs = (
                    {
                        "entry_reason": probe_tags.get("entry_reason"),
                        "probe_type": probe_tags.get("probe_type"),
                        "target_w": probe_tags.get("target_w"),
                    }
                    if probe_tags is not None
                    else {}
                )
                if probe_kwargs:
                    self.profit_taking.register_position(sym, px, **probe_kwargs)
                self.profit_taking.register_position(sym, px)  # 注册利润管理
                if audit:
                    stop_pct = self.fixed_stop_loss.config.get_stop_pct(sym)
                    audit.add_note(f"Fixed stop registered: {sym} @ {px:.4f}, stop @ {px*(1-stop_pct):.4f}")

            # Update batch cash budget.
            if side == "buy":
                cash_remaining -= float(notional)
                if intent == "OPEN_LONG":
                    successful_replacement_symbols.add(sym)
            else:
                cash_remaining += float(notional)

            if audit:
                decision_reason = "zero_target_close" if bypass_turnover_cap_for_exit else "ok"
                decision = {
                    "symbol": sym,
                    "action": "create",
                    "reason": decision_reason,
                    "side": side,
                    "notional": notional,
                    "cash_after": cash_remaining,
                    "bypass_turnover_cap_for_exit": bool(bypass_turnover_cap_for_exit),
                    **dust_add_size_ignore_info,
                }
                if side == "buy" and btc_probe_meta is not None:
                    audit.record_count("btc_leadership_probe_open_count", symbol=sym)
                    decision.update(btc_probe_meta)
                router_decisions.append(decision)

        market_impulse_probe_context = self._market_impulse_probe_context(
            strategy_signal_lookup=strategy_signal_lookup,
            current_auto_risk_level=current_auto_risk_level,
            regime_state_str=regime_state_str,
        )
        if audit:
            audit.counts["market_impulse_probe_candidate_count"] = int(
                len(market_impulse_probe_context.get("candidates") or [])
            )

        if (
            bool(getattr(self.cfg.execution, "market_impulse_probe_enabled", True))
            and (not bool(getattr(self.cfg.execution, "market_impulse_probe_only_in_protect", True)) or protect_entry_gate_active)
            and not is_risk_off_close_only
            and not active_positions
            and not any(str(getattr(order, "side", "")).lower() == "buy" for order in rebalance_orders)
            and bool(market_impulse_probe_context.get("active"))
        ):
            probe_cooldown_until_ms = max(
                [int((payload or {}).get("cooldown_until_ms") or 0) for payload in (probe_state or {}).values()]
                or [0]
            )
            now_ms = int(now_utc.timestamp() * 1000)
            if probe_cooldown_until_ms > now_ms:
                if audit:
                    audit.counts["market_impulse_probe_blocked_count"] = int(
                        audit.counts.get("market_impulse_probe_blocked_count", 0) or 0
                    ) + 1
                    audit.add_note(
                        f"Market impulse probe blocked by cooldown until {probe_cooldown_until_ms}"
                    )
            else:
                selected_probe_symbols = 0
                max_probe_symbols = int(getattr(self.cfg.execution, "market_impulse_probe_max_symbols", 1) or 1)
                configured_probe_target_w = float(getattr(self.cfg.execution, "market_impulse_probe_target_w", 0.06) or 0.0)
                max_probe_target_w = float(getattr(self.cfg.execution, "market_impulse_probe_max_target_w", 0.10) or 0.10)
                probe_cooldown_hours = int(getattr(self.cfg.execution, "market_impulse_probe_cooldown_hours", 8) or 0)
                probe_time_stop_hours = int(getattr(self.cfg.execution, "market_impulse_probe_time_stop_hours", 4) or 4)
                probe_candidates = list((market_impulse_probe_context.get("candidates") or [])[:max_probe_symbols])
                for candidate in probe_candidates:
                    if selected_probe_symbols >= max_probe_symbols:
                        break
                    sym = str(candidate.get("symbol") or "")
                    trend_score = float(candidate.get("trend_score") or 0.0)
                    allowed_probe, probe_block_reason, bypass_reason = self._market_impulse_probe_negexp_gate(symbol=sym)
                    if not allowed_probe:
                        if audit:
                            audit.counts["market_impulse_probe_blocked_count"] = int(
                                audit.counts.get("market_impulse_probe_blocked_count", 0) or 0
                            ) + 1
                            audit.add_note(
                                f"Market impulse probe blocked: {sym} reason={probe_block_reason}"
                            )
                        continue

                    px = float(prices.get(sym, 0.0) or 0.0)
                    if px <= 0.0:
                        continue
                    sizing = self._market_impulse_probe_sizing(
                        symbol=sym,
                        px=float(px),
                        equity=float(equity),
                    )
                    effective_probe_target_w = float(sizing.get("effective_probe_target_w") or 0.0)
                    probe_notional = float(sizing.get("probe_notional") or 0.0)
                    min_executable_notional = float(sizing.get("min_executable_notional") or 0.0)
                    if effective_probe_target_w > float(max_probe_target_w):
                        if audit:
                            audit.counts["market_impulse_probe_blocked_count"] = int(
                                audit.counts.get("market_impulse_probe_blocked_count", 0) or 0
                            ) + 1
                            audit.counts["market_impulse_probe_unexecutable_notional_count"] = int(
                                audit.counts.get("market_impulse_probe_unexecutable_notional_count", 0) or 0
                            ) + 1
                            router_decisions.append(
                                {
                                    "symbol": sym,
                                    "action": "skip",
                                    "reason": "market_impulse_probe_unexecutable_notional",
                                    "probe_notional": float(probe_notional),
                                    "min_executable_notional": float(min_executable_notional),
                                    "effective_probe_target_w": float(effective_probe_target_w),
                                    "market_impulse_probe_max_target_w": float(max_probe_target_w),
                                    "configured_target_w": float(configured_probe_target_w),
                                    "min_executable_target_w": float(sizing.get("min_executable_target_w") or 0.0),
                                }
                            )
                            audit.add_note(
                                f"Market impulse probe blocked: {sym} reason=market_impulse_probe_unexecutable_notional "
                                f"probe_notional={probe_notional:.4f} min_executable_notional={min_executable_notional:.4f} "
                                f"effective_probe_target_w={effective_probe_target_w:.4f} max_target_w={max_probe_target_w:.4f}"
                            )
                        continue
                    if probe_notional > cash_remaining:
                        if audit:
                            audit.counts["market_impulse_probe_blocked_count"] = int(
                                audit.counts.get("market_impulse_probe_blocked_count", 0) or 0
                            ) + 1
                            audit.add_note(
                                f"Market impulse probe blocked: {sym} reason=insufficient_cash probe_notional={probe_notional:.4f} cash_remaining={cash_remaining:.4f}"
                            )
                        continue
                    reentry_guard = self._evaluate_same_symbol_reentry_guard(
                        symbol=sym,
                        latest_px=px,
                        entry_kind="market_impulse_probe",
                        audit=audit,
                    )
                    if bool(reentry_guard.get("blocked", False)):
                        if audit:
                            audit.counts["market_impulse_probe_blocked_count"] = int(
                                audit.counts.get("market_impulse_probe_blocked_count", 0) or 0
                            ) + 1
                            audit.record_count("same_symbol_reentry_cooldown_count", symbol=sym)
                            router_decisions.append(
                                {
                                    **self._same_symbol_reentry_block_decision(sym, reentry_guard),
                                    "market_impulse_probe": True,
                                    "trend_buy_count": int(market_impulse_probe_context.get("trend_buy_count") or 0),
                                    "btc_trend_score": market_impulse_probe_context.get("btc_trend_score"),
                                    "trend_score": trend_score,
                                }
                            )
                            audit.add_note(
                                "Market impulse probe blocked by same-symbol reentry guard: "
                                f"{sym} last_exit={reentry_guard.get('last_exit_reason')} "
                                f"elapsed_hours={float(reentry_guard.get('elapsed_hours') or 0.0):.2f}"
                            )
                        continue
                    cooldown_until_ms = now_ms + probe_cooldown_hours * 3600 * 1000
                    meta = {
                        "target_w": float(effective_probe_target_w),
                        "market_impulse_probe": True,
                        "market_impulse_probe_target_w": float(effective_probe_target_w),
                        "market_impulse_probe_configured_target_w": float(configured_probe_target_w),
                        "market_impulse_probe_min_executable_notional": float(min_executable_notional),
                        "market_impulse_probe_min_executable_target_w": float(sizing.get("min_executable_target_w") or 0.0),
                        "market_impulse_probe_trend_buy_count": int(
                            market_impulse_probe_context.get("trend_buy_count") or 0
                        ),
                        "market_impulse_probe_btc_trend_score": market_impulse_probe_context.get("btc_trend_score"),
                        "market_impulse_probe_bypassed_negative_expectancy_reason": bypass_reason,
                        "market_impulse_probe_cooldown_until_ms": int(cooldown_until_ms),
                        "market_impulse_probe_cooldown_hours": int(probe_cooldown_hours),
                        "market_impulse_probe_time_stop_hours": int(probe_time_stop_hours),
                        "reason": "market_impulse_probe",
                    }
                    probe_tags = probe_tags_from_order_meta(meta, entry_px=px, entry_ts=utc_now_iso())
                    if probe_tags is not None:
                        meta.update(probe_tags)
                    rebalance_orders.append(
                        Order(
                            symbol=sym,
                            side="buy",
                            intent="OPEN_LONG",
                            notional_usdt=float(probe_notional),
                            signal_price=px,
                            meta=meta,
                        )
                    )
                    selected_probe_symbols += 1
                    cash_remaining -= float(probe_notional)
                    if audit:
                        audit.counts["market_impulse_probe_open_count"] = int(
                            audit.counts.get("market_impulse_probe_open_count", 0) or 0
                        ) + 1
                        audit.add_note(
                            "Market impulse probe open: "
                            f"symbol={sym} trend_buy_count={int(market_impulse_probe_context.get('trend_buy_count') or 0)} "
                            f"btc_trend_score={market_impulse_probe_context.get('btc_trend_score')} "
                            f"target_w={effective_probe_target_w:.4f} configured_target_w={configured_probe_target_w:.4f} "
                            f"min_executable_notional={min_executable_notional:.4f} "
                            f"bypassed_negative_expectancy_reason={bypass_reason or ''} "
                            f"cooldown_until={cooldown_until_ms}"
                        )
                        router_decisions.append(
                            {
                                "symbol": sym,
                                "action": "create",
                                "reason": "market_impulse_probe",
                                "side": "buy",
                                "intent": "OPEN_LONG",
                                "notional": float(probe_notional),
                                "cash_after": cash_remaining,
                                "market_impulse_probe": True,
                                "trend_buy_count": int(market_impulse_probe_context.get("trend_buy_count") or 0),
                                "btc_trend_score": market_impulse_probe_context.get("btc_trend_score"),
                                "selected_symbol": sym,
                                "bypassed_negative_expectancy_reason": bypass_reason,
                                "target_w": float(effective_probe_target_w),
                                "configured_target_w": float(configured_probe_target_w),
                                "min_executable_notional": float(min_executable_notional),
                                "effective_probe_target_w": float(effective_probe_target_w),
                                "cooldown_until": int(cooldown_until_ms),
                                "trend_score": trend_score,
                            }
                        )

        if pending_zero_target_close_candidates:
            replacement_candidate_symbols = set(replacement_open_candidates or set())
            blocked_candidate_symbols = set(blocked_replacement_reasons.keys())
            all_replacements_blocked = (
                bool(replacement_candidate_symbols)
                and not bool(replacement_candidate_symbols & successful_replacement_symbols)
                and replacement_candidate_symbols.issubset(blocked_candidate_symbols)
            )

            for pending in pending_zero_target_close_candidates:
                sym = str(pending.get("symbol") or "")
                order = pending.get("order")
                held_score = pending.get("held_score")

                safe_to_hold = (
                    all_replacements_blocked
                    and held_score is not None
                    and float(held_score) >= float(protect_replacement_hold_min_score)
                    and not self._held_symbol_has_negative_expectancy_hard_block(sym)
                )

                if safe_to_hold:
                    if audit:
                        audit.record_count("hold_current_no_valid_replacement_count", symbol=sym)
                        audit.add_note(
                            f"Hold current instead of zero_target_close: {sym} replacements_blocked={sorted(blocked_candidate_symbols)}"
                        )
                        router_decisions.append(
                            {
                                "symbol": sym,
                                "action": "skip",
                                "reason": "hold_current_no_valid_replacement",
                                "held_symbol": sym,
                                "blocked_replacement_symbols": sorted(blocked_candidate_symbols),
                                "blocked_replacement_reasons": {
                                    candidate: blocked_replacement_reasons[candidate]
                                    for candidate in sorted(blocked_candidate_symbols)
                                },
                                "held_symbol_score": float(held_score),
                                "protect_replacement_hold_min_score": float(protect_replacement_hold_min_score),
                            }
                        )
                    continue

                if order is None:
                    continue
                meta = dict(getattr(order, "meta", {}) or {})
                meta["target_w"] = 0.0
                meta["bypass_turnover_cap_for_exit"] = True
                meta["turnover_cap_bypass_reason"] = "zero_target_close"
                order.meta = meta
                rebalance_orders.append(order)
                cash_remaining += float(getattr(order, "notional_usdt", 0.0) or 0.0)
                if audit:
                    router_decisions.append(
                        {
                            "symbol": order.symbol,
                            "action": "create",
                            "reason": "zero_target_close",
                            "side": order.side,
                            "notional": float(order.notional_usdt or 0.0),
                            "cash_after": cash_remaining,
                            "bypass_turnover_cap_for_exit": True,
                        }
                    )

        # qlib migration: proactive per-cycle rebalance turnover cap.
        try:
            kept_rebalance_orders, dropped_rebalance_orders, turnover_cap_stats = self._apply_rebalance_turnover_cap(
                rebalance_orders,
                equity_raw=float(equity_raw),
            )
            clipped_rebalance_orders = [
                order
                for order in kept_rebalance_orders
                if bool(((order.meta or {}).get("turnover_cap_clipped", False)))
            ]
            if dropped_rebalance_orders or clipped_rebalance_orders:
                rebalance_orders = kept_rebalance_orders
                if audit:
                    if dropped_rebalance_orders:
                        audit.reject("turnover_cap")
                    for _ in clipped_rebalance_orders:
                        audit.reject("cap_clipped")
                    audit.add_note(
                        "Rebalance turnover capped: "
                        f"buy=${float(turnover_cap_stats.get('total_buy_notional', 0.0)):.2f}, "
                        f"sell=${float(turnover_cap_stats.get('total_sell_notional', 0.0)):.2f}, "
                        f"effective=${float(turnover_cap_stats.get('effective_turnover_notional', 0.0)):.2f} "
                        f"> cap=${float(turnover_cap_stats.get('cap_notional', 0.0)):.2f}, "
                        f"bypassed_exit_count={int(turnover_cap_stats.get('bypassed_exit_count', 0.0))}, "
                        f"kept={len(kept_rebalance_orders)}, dropped={len(dropped_rebalance_orders)}, "
                        f"clipped={len(clipped_rebalance_orders)}"
                    )
                    for order in clipped_rebalance_orders[:12]:
                        meta = order.meta or {}
                        router_decisions.append(
                            {
                                "symbol": order.symbol,
                                "action": "clip",
                                "reason": "turnover_cap",
                                "side": order.side,
                                "intent": order.intent,
                                "notional": float(meta.get("turnover_cap_clipped_notional", order.notional_usdt or 0.0)),
                                "original_notional": float(meta.get("turnover_cap_original_notional", order.notional_usdt or 0.0)),
                                "turnover_cap_notional": float(turnover_cap_stats.get("cap_notional", 0.0)),
                            }
                        )
                    for order in dropped_rebalance_orders[:12]:
                        router_decisions.append(
                            {
                                "symbol": order.symbol,
                                "action": "skip",
                                "reason": "turnover_cap",
                                "side": order.side,
                                "intent": order.intent,
                                "notional": float(order.notional_usdt or 0.0),
                                "turnover_cap_notional": float(turnover_cap_stats.get("cap_notional", 0.0)),
                            }
                        )
        except Exception:
            pass

        if self._live_symbol_whitelist:
            leaked_router_symbols = [
                str(d.get("symbol") or "").strip()
                for d in router_decisions
                if str(d.get("symbol") or "").strip()
                and str(d.get("symbol") or "").strip() not in self._live_symbol_whitelist
            ]
            if leaked_router_symbols:
                self._record_live_whitelist_drop(
                    audit=audit,
                    stage="router_decisions",
                    dropped_symbols=leaked_router_symbols,
                )
                router_decisions = [
                    d
                    for d in router_decisions
                    if str(d.get("symbol") or "").strip() in self._live_symbol_whitelist
                ]

            leaked_rebalance_symbols = [
                str(order.symbol)
                for order in rebalance_orders
                if str(order.symbol) not in self._live_symbol_whitelist
            ]
            if leaked_rebalance_symbols:
                self._record_live_whitelist_drop(
                    audit=audit,
                    stage="rebalance_orders",
                    dropped_symbols=leaked_rebalance_symbols,
                )
                rebalance_orders = [
                    order for order in rebalance_orders if str(order.symbol) in self._live_symbol_whitelist
                ]

            leaked_exit_symbols = [
                str(order.symbol)
                for order in exit_orders
                if str(order.symbol) not in self._live_symbol_whitelist
            ]
            if leaked_exit_symbols:
                self._record_live_whitelist_drop(
                    audit=audit,
                    stage="exit_orders",
                    dropped_symbols=leaked_exit_symbols,
                )
                exit_orders = [
                    order for order in exit_orders if str(order.symbol) in self._live_symbol_whitelist
                ]

        if audit:
            audit.router_decisions = router_decisions
            audit.counts["orders_rebalance"] = len(rebalance_orders)
            # fill budget_action suppression stats (F3.1)
            try:
                ba = audit.budget_action or {}
                if ba.get("enabled"):
                    ba["suppressed_orders_count"] = int(audit.rebalance_skipped_deadband_count)
                    ba["suppressed_reasons"] = ["deadband"] if audit.rebalance_skipped_deadband_count > 0 else []
                    audit.budget_action = ba
            except Exception:
                pass

        if run_logger is not None:
            try:
                now_ts = self.clock.now().isoformat().replace("+00:00", "Z")
                run_logger.log_equity({
                    "ts": now_ts,
                    # Reporting (raw) vs sizing (capped)
                    "cash": float(cash_raw),
                    "equity": float(equity_raw),
                    "cash_sizing": float(cash_usdt),
                    "equity_sizing": float(equity),
                    "equity_cap_usdt": float(getattr(self.cfg.budget, "live_equity_cap_usdt", 0.0) or 0.0) if getattr(self.cfg.budget, "live_equity_cap_usdt", None) is not None else None,
                    "peak": float(pst.peak_equity_usdt),
                    "dd": float(pst.drawdown_pct),
                    "exposure_mult": float(dd_mult),
                })
                for p in positions:
                    run_logger.log_position({
                        "ts": now_ts,
                        "symbol": p.symbol,
                        "qty": float(p.qty),
                        "avg_px": float(p.avg_px),
                        "mark_px": float(getattr(p, 'last_mark_px', 0.0) or prices.get(p.symbol, 0.0)),
                        "highest_px": float(getattr(p, 'highest_px', 0.0)),
                        "unrealized_pnl_pct": float(getattr(p, 'unrealized_pnl_pct', 0.0)),
                    })
            except Exception:
                pass

        # Phase 3: ML数据收集
        # 收集特征快照用于训练ML模型
        try:
            if bool(getattr(self.cfg.execution, "collect_ml_training_data", True)):
                current_ts = int(self.clock.now().timestamp() * 1000)
                snapshot_ts = self._resolve_ml_snapshot_timestamp_ms(audit=audit)
                ml_payloads, missing_symbols = self._resolve_ml_collection_payloads(
                    market_data_1h,
                    snapshot_ts=snapshot_ts,
                )
                for sym, payload in ml_payloads.items():
                    close = list(payload.get("close", []) or [])
                    px = float(close[-1]) if close else float(prices.get(sym, 0.0) or 0.0)
                    if px <= 0:
                        continue
                    self.data_collector.collect_features(
                        timestamp=snapshot_ts,
                        symbol=sym,
                        market_data=payload,
                        regime=str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
                    )
                if audit and missing_symbols:
                    audit.add_note(f"ML data collection missing {len(missing_symbols)} stable-universe symbols")
            
            # 回填6小时前的标签
                filled_count = self.data_collector.fill_labels(current_ts)
                if audit and filled_count > 0:
                    audit.add_note(f"ML data: filled {filled_count} labels")
        except Exception as e:
            # 数据收集失败不应影响交易
            if audit:
                audit.add_note(f"ML data collection skipped: {str(e)[:50]}")

        orders = exit_orders + rebalance_orders
        
        # 输出价格无效警告（如果有）
        if invalid_price_warnings and run_logger:
            run_logger.warning(f"[Pipeline] {len(invalid_price_warnings)} symbols have invalid prices: " + 
                             ", ".join([w['symbol'] for w in invalid_price_warnings[:5]]))
        
        return PipelineOutput(alpha=alpha, regime=regime, portfolio=portfolio, orders=orders)
