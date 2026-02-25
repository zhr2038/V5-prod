from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional


@dataclass
class DecisionAudit:
    """决策审计记录，用于解释'为什么0单/为什么被拒绝'"""
    
    run_id: str
    now_ts: int = field(default_factory=lambda: int(datetime.utcnow().timestamp()))
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
    
    def reject(self, reason: str) -> None:
        """记录拒绝原因"""
        if reason in self.rejects:
            self.rejects[reason] += 1
        else:
            self.rejects[reason] = 1
    
    def add_note(self, note: str) -> None:
        """添加备注"""
        self.notes.append(f"{datetime.utcnow().isoformat()}: {note}")
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    def save(self, run_dir: str) -> None:
        """保存到文件"""
        path = Path(run_dir) / "decision_audit.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        
        data = self.to_dict()
        # 确保所有字段都是JSON可序列化的
        for key, value in data.items():
            if isinstance(value, (datetime, Path)):
                data[key] = str(value)
        
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_decision_audit(run_dir: str) -> Optional[DecisionAudit]:
    """从文件加载DecisionAudit"""
    path = Path(run_dir) / "decision_audit.json"
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
    
    return audit