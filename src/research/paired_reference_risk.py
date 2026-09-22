"""The existing V5 risk transition rules with independent paper evidence."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from src.reporting.participation_runtime import _save_report
from src.risk.auto_risk_guard import get_auto_risk_guard


def update_risk(account, rows, now, *, observation_gap):
    """Evaluate before each common hourly decision using prior 12-hour audits.

Drawdown, conversion, rejection and realized exit-order PnL come exclusively
from this account. No real-account history, equity endpoint or recovery state
can enter either cohort. The same AutoRiskGuard.evaluate owns thresholds and
one-step recovery rules used by V5 production.
    """
    book = account["book"]
    histories = [row for row in account.get("risk_history", []) if now - 12 * 3600 <= row["now_ts"] < now]
    account["risk_history"] = histories
    since = min((row["now_ts"] for row in histories), default=now)
    selected = sum(int(row["selected"]) for row in histories)
    rejected = sum(int(row["dust_rejected"]) for row in histories)
    filled = {(fill["symbol"], fill["decision_ts"]) for fill in book.fills
              if fill["side"] == "buy" and since <= float(fill["observed_at"]) <= now
              and any(row["now_ts"] == fill["decision_ts"] for row in histories)}
    conversion = min(1, len(filled) / selected) if selected else None
    submitted = sum(row.get("submitted_orders", 0) for row in histories)
    no_opportunities = selected == 0 and submitted == 0
    pnls = [float(lot["net_pnl_usdt"]) for lot in book.closed_lots if since <= float(lot["exit_ts"]) <= now]
    consecutive = 0
    for pnl in reversed(pnls):
        if pnl >= 0:
            break
        consecutive += 1
    trend = "flat"
    if len(pnls) >= 6:
        recent, prior = sum(pnls[-3:]), sum(pnls[-6:-3])
        tolerance = max(abs(recent), abs(prior)) * .05
        trend = "up" if recent > prior + tolerance else "down" if recent < prior - tolerance else "flat"
    mark = book.mark(rows, now=now)
    # A whole missed hour remains a recovery blocker until it leaves the 12h
    # evidence window. This is stricter evidence admission, not a new threshold.
    history_complete = all(b["bar_ts"] - a["bar_ts"] == 3600 for a, b in zip(histories, histories[1:]))
    latest_fresh = bool(histories and 0 < now - histories[-1]["now_ts"] <= 3600 + 120)
    candidate_evidence = all(row.get("selected_count_observed", False) for row in histories)
    opportunity_evidence = selected > 0 or no_opportunities
    recovery_ok = len(histories) >= 3 and history_complete and latest_fresh and not observation_gap and candidate_evidence and opportunity_evidence
    metrics = {"dd_pct": float(mark["drawdown_fraction"]), "conversion_rate": conversion,
               "dust_reject_rate": rejected / (selected + rejected) if selected + rejected else 0,
               "pnl_trend": trend, "consecutive_losses": consecutive, "sample_size": len(histories),
               "selected_candidates": selected, "filled_opportunities": len(filled),
               "opportunity_status": "no_opportunities" if no_opportunities else "observed",
               "pnl_source": "own_paper_realized_exit_allocations", "pnl_observation_count": len(pnls),
               "drawdown_source": "own_paper_net_equity_and_historical_peak", "recovery_evidence_ok": recovery_ok,
               "history_complete": history_complete, "latest_audit_fresh": latest_fresh,
               "candidate_evidence_observed": candidate_evidence,
               "paper_only": True, "live_order_effect": "none"}
    runtime = account["adapter"].root / "reports"
    path = runtime / "auto_risk_guard.json"
    stamp = datetime.fromtimestamp(now, timezone.utc).isoformat()
    with patch("src.risk.auto_risk_guard.utc_now_iso", lambda: stamp):
        guard = get_auto_risk_guard(str(path))
        _, _, reason = guard.evaluate(dd_pct=metrics["dd_pct"], conversion_rate=conversion,
                                      dust_reject_rate=metrics["dust_reject_rate"], recent_pnl_trend=trend,
                                      consecutive_losses=consecutive, no_trade_opportunities=no_opportunities,
                                      recovery_evidence_ok=recovery_ok)
        if len(histories) < 3:
            reason += f"; cold_start:{len(histories)}_prior_audits_downgrades_only_no_recovery"
    # Existing guard persistence catches IO errors; the research ledger must not
    # silently proceed after a failed state publication.
    _save_report(path, {"current_level": guard.current_level, "current_config": guard.get_current_config(),
                        "metrics": guard.metrics, "history": guard.history[-50:], "last_update": stamp})
    snapshot = {"ts": stamp, "current_level": guard.current_level, "config": guard.get_current_config(),
                "metrics": metrics, "reason": reason, "history": guard.history[-5:],
                "evaluation_clock": "before_common_hourly_decision; prior_12h_own_audits", "paper_only": True}
    _save_report(runtime / "auto_risk_eval.json", snapshot)
    account["risk_snapshot"] = snapshot
    return snapshot


def record_risk_audit(account, audit, *, now, bar, decisions):
    counts, rejects = audit.get("counts", {}), audit.get("rejects", {})
    rejected = int(rejects.get("min_notional", 0) or 0) + int(rejects.get("exchange_min_notional", 0) or 0)
    router_dust = sum(row.get("reason") in {"min_notional", "exchange_min_notional"} for row in audit.get("router_decisions", []))
    account.setdefault("risk_history", []).append({"now_ts": now, "bar_ts": bar,
                                                   "selected": int(counts.get("selected", 0) or 0),
                                                   "selected_count_observed": "selected" in counts,
                                                   "dust_rejected": max(rejected, router_dust),
                                                   "submitted_orders": sum(row["action"] == "original_order_intent" for row in decisions)})
