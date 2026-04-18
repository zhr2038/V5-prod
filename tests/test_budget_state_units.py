from __future__ import annotations

from src.reporting.budget_state import BudgetState


def test_turnover_budget_not_exceeded_when_used_usdt_is_below_ratio_budget() -> None:
    st = BudgetState(
        ymd_utc="20260418",
        turnover_budget_per_day=0.60,
        cost_budget_bps_per_day=40.0,
        turnover_used=16.0,
        cost_used_usdt=0.0,
        avg_equity_est=107.0,
    )

    assert st.turnover_used_usdt() == 16.0
    assert st.turnover_used_ratio() == 16.0 / 107.0
    assert st.turnover_budget_ratio() == 0.60
    assert st.turnover_budget_usdt() == 0.60 * 107.0
    assert st.turnover_exceeded() is False
    assert st.exceeded() is False
    assert st.reason() is None


def test_turnover_budget_exceeded_when_used_usdt_is_above_ratio_budget() -> None:
    st = BudgetState(
        ymd_utc="20260418",
        turnover_budget_per_day=0.60,
        turnover_budget_unit="ratio",
        cost_budget_bps_per_day=40.0,
        turnover_used=70.0,
        cost_used_usdt=0.0,
        avg_equity_est=107.0,
    )

    assert st.turnover_used_ratio() == 70.0 / 107.0
    assert st.turnover_budget_usdt() == 0.60 * 107.0
    assert st.turnover_exceeded() is True
    assert st.exceeded() is True
    assert st.reason() == "exceeded_turnover"


def test_cost_budget_can_exceed_independently_of_turnover_budget() -> None:
    st = BudgetState(
        ymd_utc="20260418",
        turnover_budget_per_day=0.60,
        cost_budget_bps_per_day=40.0,
        turnover_used=16.0,
        cost_used_usdt=1.0,
        avg_equity_est=107.0,
    )

    assert st.turnover_exceeded() is False
    assert st.cost_used_bps() is not None and st.cost_used_bps() > 40.0
    assert st.cost_exceeded() is True
    assert st.exceeded() is True
    assert st.reason() == "exceeded_cost"


def test_legacy_absolute_turnover_budget_remains_usdt_compatible() -> None:
    st = BudgetState(
        ymd_utc="20260418",
        turnover_budget_per_day=1000.0,
        turnover_budget_unit=None,
        cost_budget_bps_per_day=40.0,
        turnover_used=160.0,
        cost_used_usdt=0.0,
        avg_equity_est=200.0,
    )

    assert st.turnover_budget_usdt() == 1000.0
    assert st.turnover_budget_ratio() == 5.0
    assert st.turnover_used_ratio() == 0.8
    assert st.turnover_exceeded() is False


def test_ratio_budget_above_one_is_not_misread_when_unit_is_explicit() -> None:
    st = BudgetState(
        ymd_utc="20260419",
        turnover_budget_per_day=1.5,
        turnover_budget_unit="ratio",
        cost_budget_bps_per_day=40.0,
        turnover_used=160.0,
        cost_used_usdt=0.0,
        avg_equity_est=200.0,
    )

    assert st.turnover_budget_ratio() == 1.5
    assert st.turnover_budget_usdt() == 300.0
    assert st.turnover_exceeded() is False
