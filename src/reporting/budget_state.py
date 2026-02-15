from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _utc_yyyymmdd_from_epoch_sec(ts: int) -> str:
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


@dataclass
class BudgetState:
    ymd_utc: str

    turnover_budget_per_day: Optional[float] = None
    cost_budget_bps_per_day: Optional[float] = None

    turnover_used: float = 0.0
    cost_used_usdt: float = 0.0

    # optional diagnostic denominator
    avg_equity_est: Optional[float] = None

    # keep per-run contributions to avoid double count
    runs: Dict[str, Dict[str, float]] = field(default_factory=dict)  # run_id -> {turnover, cost_usdt}

    def cost_used_bps(self) -> Optional[float]:
        if not self.avg_equity_est or self.avg_equity_est <= 0:
            return None
        return float(self.cost_used_usdt) / float(self.avg_equity_est) * 10_000.0

    def exceeded(self) -> bool:
        if self.turnover_budget_per_day is not None and self.turnover_used > float(self.turnover_budget_per_day):
            return True
        bps = self.cost_used_bps()
        if self.cost_budget_bps_per_day is not None and bps is not None and bps > float(self.cost_budget_bps_per_day):
            return True
        return False

    def reason(self) -> Optional[str]:
        reasons = []
        if self.turnover_budget_per_day is not None and self.turnover_used > float(self.turnover_budget_per_day):
            reasons.append("exceeded_turnover")
        bps = self.cost_used_bps()
        if self.cost_budget_bps_per_day is not None and bps is not None and bps > float(self.cost_budget_bps_per_day):
            reasons.append("exceeded_cost")
        if not reasons:
            return None
        return "+".join(reasons)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["cost_used_bps"] = self.cost_used_bps()
        d["exceeded"] = self.exceeded()
        d["reason"] = self.reason()
        return d


def load_budget_state(path: str) -> Optional[BudgetState]:
    p = Path(path)
    if not p.exists():
        return None
    data = json.loads(p.read_text(encoding="utf-8"))
    st = BudgetState(ymd_utc=str(data.get("ymd_utc")))
    st.turnover_budget_per_day = data.get("turnover_budget_per_day")
    st.cost_budget_bps_per_day = data.get("cost_budget_bps_per_day")
    st.turnover_used = _safe_float(data.get("turnover_used"), 0.0)
    st.cost_used_usdt = _safe_float(data.get("cost_used_usdt"), 0.0)
    st.avg_equity_est = data.get("avg_equity_est")
    st.runs = data.get("runs") or {}
    return st


def update_daily_budget_state(
    *,
    base_dir: str = "reports/budget_state",
    ymd_utc: str,
    run_id: str,
    turnover_inc: float,
    cost_inc_usdt: float,
    avg_equity: Optional[float],
    turnover_budget_per_day: Optional[float],
    cost_budget_bps_per_day: Optional[float],
) -> BudgetState:
    out_dir = Path(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{ymd_utc}.json"

    st = load_budget_state(str(path))
    if st is None:
        st = BudgetState(ymd_utc=ymd_utc)

    # budgets are part of state (latest config wins)
    st.turnover_budget_per_day = turnover_budget_per_day
    st.cost_budget_bps_per_day = cost_budget_bps_per_day

    # update avg equity estimate (use latest non-null)
    if avg_equity is not None and float(avg_equity) > 0:
        st.avg_equity_est = float(avg_equity)

    # idempotent per run_id
    prev = st.runs.get(run_id)
    if prev is None:
        st.runs[run_id] = {"turnover": float(turnover_inc), "cost_usdt": float(cost_inc_usdt)}
        st.turnover_used += float(turnover_inc)
        st.cost_used_usdt += float(cost_inc_usdt)
    else:
        # if re-run with changed numbers, adjust delta
        prev_turn = _safe_float(prev.get("turnover"), 0.0)
        prev_cost = _safe_float(prev.get("cost_usdt"), 0.0)
        st.runs[run_id] = {"turnover": float(turnover_inc), "cost_usdt": float(cost_inc_usdt)}
        st.turnover_used += float(turnover_inc) - prev_turn
        st.cost_used_usdt += float(cost_inc_usdt) - prev_cost

    path.write_text(json.dumps(st.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return st


def derive_ymd_utc_from_summary(summary: Dict[str, Any]) -> str:
    # Prefer window_end_ts if present else end_ts else now.
    ts = summary.get("window_end_ts") or summary.get("end_ts")
    if ts is None:
        ts = int(datetime.now(timezone.utc).timestamp())
    return _utc_yyyymmdd_from_epoch_sec(int(ts))
