from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _utc_yyyymmdd_from_epoch_sec(ts: int) -> str:
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _resolve_budget_state_dir(base_dir: str | Path = "reports/budget_state") -> Path:
    path = Path(base_dir)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


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
    # run_id -> {turnover, cost_usdt, fills_count, notionals_json}
    runs: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Derived daily stats for F3.2 trigger (cached for pipeline consumption)
    fills_count_today: int = 0
    median_notional_usdt_today: Optional[float] = None
    p25_notional_usdt_today: Optional[float] = None
    p75_notional_usdt_today: Optional[float] = None
    small_trade_ratio_today: Optional[float] = None
    small_trade_notional_cutoff: Optional[float] = None

    def turnover_used_usdt(self) -> float:
        return float(self.turnover_used)

    def turnover_budget_ratio(self) -> Optional[float]:
        if self.turnover_budget_per_day is None:
            return None
        return float(self.turnover_budget_per_day)

    def turnover_used_ratio(self) -> Optional[float]:
        if not self.avg_equity_est or float(self.avg_equity_est) <= 0:
            return None
        return float(self.turnover_used_usdt()) / float(self.avg_equity_est)

    def turnover_budget_usdt(self) -> Optional[float]:
        budget_ratio = self.turnover_budget_ratio()
        if budget_ratio is None or not self.avg_equity_est or float(self.avg_equity_est) <= 0:
            return None
        return float(budget_ratio) * float(self.avg_equity_est)

    def turnover_exceeded(self) -> bool:
        used_ratio = self.turnover_used_ratio()
        budget_ratio = self.turnover_budget_ratio()
        if used_ratio is None or budget_ratio is None:
            return False
        return float(used_ratio) > float(budget_ratio)

    def cost_exceeded(self) -> bool:
        bps = self.cost_used_bps()
        if self.cost_budget_bps_per_day is None or bps is None:
            return False
        return float(bps) > float(self.cost_budget_bps_per_day)

    def cost_used_bps(self) -> Optional[float]:
        if not self.avg_equity_est or self.avg_equity_est <= 0:
            return None
        return float(self.cost_used_usdt) / float(self.avg_equity_est) * 10_000.0

    def exceeded(self) -> bool:
        return self.turnover_exceeded() or self.cost_exceeded()

    def reason(self) -> Optional[str]:
        reasons = []
        if self.turnover_exceeded():
            reasons.append("exceeded_turnover")
        if self.cost_exceeded():
            reasons.append("exceeded_cost")
        if not reasons:
            return None
        return "+".join(reasons)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["turnover_used_usdt"] = self.turnover_used_usdt()
        d["turnover_used_ratio"] = self.turnover_used_ratio()
        d["turnover_budget_ratio"] = self.turnover_budget_ratio()
        d["turnover_budget_usdt"] = self.turnover_budget_usdt()
        d["turnover_exceeded"] = self.turnover_exceeded()
        d["cost_used_bps"] = self.cost_used_bps()
        d["cost_exceeded"] = self.cost_exceeded()
        d["exceeded"] = self.exceeded()
        d["reason"] = self.reason()
        return d


def load_budget_state(path: str) -> Optional[BudgetState]:
    p = Path(path)
    if not p.is_absolute():
        p = (PROJECT_ROOT / p).resolve()
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

    st.fills_count_today = int(data.get("fills_count_today") or 0)
    st.median_notional_usdt_today = data.get("median_notional_usdt_today")
    st.p25_notional_usdt_today = data.get("p25_notional_usdt_today")
    st.p75_notional_usdt_today = data.get("p75_notional_usdt_today")
    st.small_trade_ratio_today = data.get("small_trade_ratio_today")
    st.small_trade_notional_cutoff = data.get("small_trade_notional_cutoff")
    return st


def update_daily_budget_state(
    *,
    base_dir: str = "reports/budget_state",
    ymd_utc: str,
    run_id: str,
    turnover_inc: float,
    cost_inc_usdt: float,
    fills_count_inc: int,
    notionals_inc: list,
    avg_equity: Optional[float],
    turnover_budget_per_day: Optional[float],
    cost_budget_bps_per_day: Optional[float],
    small_trade_notional_cutoff: Optional[float] = None,
) -> BudgetState:
    out_dir = _resolve_budget_state_dir(base_dir)
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
    record = {
        "turnover": float(turnover_inc),
        "cost_usdt": float(cost_inc_usdt),
        "fills_count": int(fills_count_inc),
        # store notionals as JSON list for easy recompute
        "notionals": [float(x) for x in (notionals_inc or [])],
    }
    if prev is None:
        st.runs[run_id] = record
        st.turnover_used += float(turnover_inc)
        st.cost_used_usdt += float(cost_inc_usdt)
    else:
        # if re-run with changed numbers, adjust delta
        prev_turn = _safe_float(prev.get("turnover"), 0.0)
        prev_cost = _safe_float(prev.get("cost_usdt"), 0.0)
        st.runs[run_id] = record
        st.turnover_used += float(turnover_inc) - prev_turn
        st.cost_used_usdt += float(cost_inc_usdt) - prev_cost

    # recompute derived notionals stats from per-run storage (small daily size)
    all_notionals = []
    fills_count = 0
    for rr in (st.runs or {}).values():
        try:
            xs = rr.get("notionals") or []
            all_notionals.extend([float(x) for x in xs])
            fills_count += int(rr.get("fills_count") or len(xs) or 0)
        except Exception:
            pass

    all_notionals = [float(x) for x in all_notionals if float(x) > 0]
    all_notionals.sort()

    st.fills_count_today = int(fills_count if fills_count else len(all_notionals))
    if all_notionals:
        mid = len(all_notionals) // 2
        if len(all_notionals) % 2 == 1:
            st.median_notional_usdt_today = float(all_notionals[mid])
        else:
            st.median_notional_usdt_today = float((all_notionals[mid - 1] + all_notionals[mid]) / 2.0)
        st.p25_notional_usdt_today = float(all_notionals[int(0.25 * (len(all_notionals) - 1))])
        st.p75_notional_usdt_today = float(all_notionals[int(0.75 * (len(all_notionals) - 1))])
    else:
        st.median_notional_usdt_today = None
        st.p25_notional_usdt_today = None
        st.p75_notional_usdt_today = None

    if small_trade_notional_cutoff is not None and all_notionals:
        cutoff = float(small_trade_notional_cutoff)
        st.small_trade_notional_cutoff = cutoff
        small_cnt = sum(1 for x in all_notionals if float(x) < cutoff)
        st.small_trade_ratio_today = float(small_cnt) / float(len(all_notionals))
    else:
        st.small_trade_notional_cutoff = small_trade_notional_cutoff
        st.small_trade_ratio_today = None

    path.write_text(json.dumps(st.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return st


def derive_ymd_utc_from_summary(summary: Dict[str, Any]) -> str:
    # Prefer window_end_ts if present else end_ts else now.
    ts = summary.get("window_end_ts") or summary.get("end_ts")
    if ts is None:
        ts = int(datetime.now(timezone.utc).timestamp())
    return _utc_yyyymmdd_from_epoch_sec(int(ts))
