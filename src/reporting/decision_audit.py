from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.utils.time import utc_now_iso, utc_now_timestamp

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_run_dir(run_dir: str | Path) -> Path:
    resolved = Path(run_dir)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


@dataclass
class DecisionAudit:
    """决策审计记录，用于解释'为什么0单/为什么被拒绝'"""
    
    run_id: str
    now_ts: int = field(default_factory=utc_now_timestamp)
    window_start_ts: Optional[int] = None
    window_end_ts: Optional[int] = None
    
    # 状态信息
    regime: str = "Unknown"
    regime_multiplier: float = 1.0
    
    # 计数信息
    counts: Dict[str, int] = field(default_factory=lambda: {
        "universe": 0,
        "scored": 0,
        "selected": 0,
        "targets_pre_risk": 0,
        "orders_exit": 0,
        "orders_rebalance": 0,
        "risk_off_suppressed_count": 0,
        "target_zero_after_regime_count": 0,
        "target_zero_after_dd_throttle_count": 0,
        "protect_entry_block_count": 0,
        "protect_entry_trend_only_block_count": 0,
        "protect_entry_alpha6_rsi_block_count": 0,
        "protect_entry_alpha6_score_too_low_count": 0,
        "protect_entry_volume_confirm_negative_count": 0,
        "protect_entry_rsi_confirm_too_weak_count": 0,
        "protect_entry_confirmation_not_stable_count": 0,
        "hold_current_no_valid_replacement_count": 0,
        "replacement_blocked_count": 0,
        "market_impulse_probe_candidate_count": 0,
        "market_impulse_probe_open_count": 0,
        "market_impulse_probe_blocked_count": 0,
        "market_impulse_probe_unexecutable_notional_count": 0,
        "market_impulse_probe_time_stop_count": 0,
        "negative_expectancy_score_penalty": 0,
        "negative_expectancy_cooldown": 0,
        "negative_expectancy_open_block": 0,
        "negative_expectancy_fast_fail_open_block": 0,
        "negative_expectancy_fast_fail_softened_count": 0,
        "negative_expectancy_fast_fail_hard_block_count": 0,
        "dust_position_ignored_for_add_size_count": 0,
        "dust_residual_no_close_order_count": 0,
        "btc_leadership_probe_candidate_count": 0,
        "btc_leadership_probe_open_count": 0,
        "btc_leadership_probe_blocked_count": 0,
        "btc_leadership_probe_negative_expectancy_bypass_count": 0,
        "probe_take_profit_count": 0,
        "probe_stop_loss_count": 0,
        "probe_trailing_stop_count": 0,
        "probe_time_stop_count": 0,
        "protect_profit_lock_active_count": 0,
        "protect_profit_lock_stop_raised_count": 0,
        "protect_profit_lock_trailing_exit_count": 0,
        "same_symbol_reentry_cooldown_count": 0,
        "same_symbol_reentry_breakout_bypass_count": 0,
        "position_state_cleared_after_close_count": 0,
        "stale_position_state_detected_count": 0,
    })
    
    # 详细数据
    top_scores: List[Dict[str, Any]] = field(default_factory=list)
    targets_pre_risk: Dict[str, float] = field(default_factory=dict)
    targets_post_risk: Dict[str, float] = field(default_factory=dict)
    
    # Portfolio调试信息
    portfolio_debug: Dict[str, Any] = field(default_factory=dict)
    
    # Rebalance deadband info
    rebalance_deadband_pct: Optional[float] = None
    rebalance_skipped_deadband_count: int = 0
    rebalance_skipped_deadband_by_symbol: Dict[str, float] = field(default_factory=dict)  # sym -> abs(drift)
    rebalance_drift_by_symbol: Dict[str, float] = field(default_factory=dict)  # sym -> signed drift
    rebalance_effective_deadband_by_symbol: Dict[str, float] = field(default_factory=dict)  # sym -> effective deadband (with banding)

    # 路由决策
    router_decisions: List[Dict[str, Any]] = field(default_factory=list)
    
    # 拒绝原因计数
    rejects: Dict[str, int] = field(default_factory=lambda: {
        "no_closed_bar": 0,
        "min_notional": 0,
        "spread_gate": 0,
        "dd_throttle": 0,
        "cap_clipped": 0,
        "insufficient_cash": 0,
        "deadband_skip": 0,
        "provider_error": 0,
        "exchange_min_notional": 0,
        "cooldown_hit": 0,
        "cost_edge_insufficient": 0,
        "confirmation_pending": 0,
        "negative_expectancy_cooldown": 0,
        "negative_expectancy_open_block": 0,
        "negative_expectancy_fast_fail_open_block": 0,
    })
    
    # Budget (F3)
    budget: Dict[str, Any] = field(default_factory=dict)
    budget_action: Dict[str, Any] = field(default_factory=dict)

    # Universe配置信息
    universe_config: Dict[str, Any] = field(default_factory=dict)
    
    # Exit signals (why exits happened)
    exit_signals: List[Dict[str, Any]] = field(default_factory=list)

    # 备注
    notes: List[str] = field(default_factory=list)
    
    # Ensemble regime详细信息
    regime_details: Dict[str, Any] = field(default_factory=dict)
    
    # 多策略信号详情
    strategy_signals: List[Dict[str, Any]] = field(default_factory=list)
    ml_signal_overview: Dict[str, Any] = field(default_factory=dict)
    negative_expectancy_state: Dict[str, Any] = field(default_factory=dict)
    protect_entry_gate_active: bool = False
    protect_entry_require_alpha6_confirmation: bool = True
    protect_entry_block_trend_only: bool = True
    protect_entry_require_alpha6_rsi_confirm_positive: bool = True
    protect_entry_alpha6_min_score: float = 0.40
    
    def reject(self, reason: str) -> None:
        """记录拒绝原因"""
        if reason in self.rejects:
            self.rejects[reason] += 1
        else:
            self.rejects[reason] = 1

    def record_count(self, reason: str, *, symbol: str | None = None, also_reject: bool = False) -> None:
        """Record a per-symbol reason count, optionally mirroring it into rejects."""
        norm_reason = str(reason or "").strip()
        if not norm_reason:
            return

        seen = getattr(self, "_count_seen", None)
        if seen is None:
            seen = set()
            setattr(self, "_count_seen", seen)

        dedupe_key = (norm_reason, str(symbol or "").strip())
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)

        self.counts[norm_reason] = int(self.counts.get(norm_reason, 0) or 0) + 1
        if also_reject:
            self.reject(norm_reason)

    def record_gate(self, reason: str, *, symbol: str | None = None) -> None:
        """Record a gate/blocker once per symbol+reason and surface it in both counts and rejects."""
        self.record_count(reason, symbol=symbol, also_reject=True)
    
    def add_note(self, note: str) -> None:
        """添加备注"""
        self.notes.append(f"{utc_now_iso()}: {note}")
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    def save(self, run_dir: str) -> None:
        """保存到文件"""
        path = _resolve_run_dir(run_dir) / "decision_audit.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        
        data = self.to_dict()
        # 确保所有字段都是JSON可序列化的
        for key, value in data.items():
            if isinstance(value, (datetime, Path)):
                data[key] = str(value)
        
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_decision_audit(run_dir: str) -> Optional[DecisionAudit]:
    """从文件加载DecisionAudit"""
    path = _resolve_run_dir(run_dir) / "decision_audit.json"
    if not path.exists():
        return None
    
    data = json.loads(path.read_text(encoding="utf-8"))
    
    # 重新创建对象
    audit = DecisionAudit(
        run_id=data["run_id"],
        now_ts=data.get("now_ts", 0),
        window_start_ts=data.get("window_start_ts"),
        window_end_ts=data.get("window_end_ts"),
        regime=data.get("regime", "Unknown"),
        regime_multiplier=data.get("regime_multiplier", 1.0),
    )
    
    # 恢复其他字段
    audit.counts = data.get("counts", {})
    audit.top_scores = data.get("top_scores", [])
    audit.targets_pre_risk = data.get("targets_pre_risk", {})
    audit.targets_post_risk = data.get("targets_post_risk", {})
    audit.router_decisions = data.get("router_decisions", [])
    audit.rejects = data.get("rejects", {})
    audit.budget = data.get("budget", {})
    audit.budget_action = data.get("budget_action", {})
    audit.universe_config = data.get("universe_config", {})
    audit.exit_signals = data.get("exit_signals", [])
    audit.notes = data.get("notes", [])
    audit.regime_details = data.get("regime_details", {})
    audit.strategy_signals = data.get("strategy_signals", [])
    audit.ml_signal_overview = data.get("ml_signal_overview", {})
    audit.negative_expectancy_state = data.get("negative_expectancy_state", {})
    audit.protect_entry_gate_active = bool(data.get("protect_entry_gate_active", False))
    audit.protect_entry_require_alpha6_confirmation = bool(
        data.get("protect_entry_require_alpha6_confirmation", True)
    )
    audit.protect_entry_block_trend_only = bool(data.get("protect_entry_block_trend_only", True))
    audit.protect_entry_require_alpha6_rsi_confirm_positive = bool(
        data.get("protect_entry_require_alpha6_rsi_confirm_positive", True)
    )
    audit.protect_entry_alpha6_min_score = float(data.get("protect_entry_alpha6_min_score", 0.40) or 0.0)
    
    return audit
