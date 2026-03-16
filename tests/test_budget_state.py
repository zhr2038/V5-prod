from __future__ import annotations

from src.reporting.budget_state import update_daily_budget_state


def test_budget_state_idempotent_per_run(tmp_path):
    base = tmp_path / "budget"

    st1 = update_daily_budget_state(
        base_dir=str(base),
        ymd_utc="20260101",
        run_id="r1",
        turnover_inc=100.0,
        cost_inc_usdt=1.0,
        fills_count_inc=2,
        notionals_inc=[40.0, 60.0],
        avg_equity=1000.0,
        turnover_budget_per_day=500.0,
        cost_budget_bps_per_day=50.0,
        small_trade_notional_cutoff=25.0,
    )
    assert st1.turnover_used == 100.0
    assert st1.cost_used_usdt == 1.0

    # same run_id should not double count
    st2 = update_daily_budget_state(
        base_dir=str(base),
        ymd_utc="20260101",
        run_id="r1",
        turnover_inc=100.0,
        cost_inc_usdt=1.0,
        fills_count_inc=2,
        notionals_inc=[40.0, 60.0],
        avg_equity=1000.0,
        turnover_budget_per_day=500.0,
        cost_budget_bps_per_day=50.0,
        small_trade_notional_cutoff=25.0,
    )
    assert st2.turnover_used == 100.0
    assert st2.cost_used_usdt == 1.0

    # changed numbers adjust delta
    st3 = update_daily_budget_state(
        base_dir=str(base),
        ymd_utc="20260101",
        run_id="r1",
        turnover_inc=120.0,
        cost_inc_usdt=2.0,
        fills_count_inc=2,
        notionals_inc=[50.0, 70.0],
        avg_equity=1000.0,
        turnover_budget_per_day=500.0,
        cost_budget_bps_per_day=50.0,
        small_trade_notional_cutoff=25.0,
    )
    assert st3.turnover_used == 120.0
    assert st3.cost_used_usdt == 2.0
