from __future__ import annotations

from types import SimpleNamespace

import main


class _Engine:
    def __init__(self) -> None:
        self.run_id = ""

    def set_run_id(self, run_id: str) -> None:
        self.run_id = run_id


def test_prime_pipeline_run_context_sets_alpha_and_portfolio_run_ids() -> None:
    alpha_engine = _Engine()
    portfolio_engine = _Engine()
    pipe = SimpleNamespace(alpha_engine=alpha_engine, portfolio_engine=portfolio_engine)

    main._prime_pipeline_run_context(pipe, " 20260427_19 ")

    assert alpha_engine.run_id == "20260427_19"
    assert portfolio_engine.run_id == "20260427_19"


def test_prime_pipeline_run_context_ignores_empty_run_id() -> None:
    alpha_engine = _Engine()
    pipe = SimpleNamespace(alpha_engine=alpha_engine)

    main._prime_pipeline_run_context(pipe, " ")

    assert alpha_engine.run_id == ""


def test_skipped_candidate_tracker_skips_flat_close_only_risk_off() -> None:
    cfg = SimpleNamespace(regime=SimpleNamespace(pos_mult_risk_off=0.0))
    state = SimpleNamespace(name="RISK_OFF", value="Risk-Off")

    assert main._should_update_skipped_candidate_tracker(cfg, state, []) is False


def test_skipped_candidate_tracker_runs_when_risk_off_still_has_positions() -> None:
    cfg = SimpleNamespace(regime=SimpleNamespace(pos_mult_risk_off=0.0))
    state = SimpleNamespace(name="RISK_OFF", value="Risk-Off")
    positions = [SimpleNamespace(symbol="BTC/USDT", qty=0.001)]

    assert main._should_update_skipped_candidate_tracker(cfg, state, positions) is True


def test_skipped_candidate_tracker_runs_outside_close_only_risk_off() -> None:
    cfg = SimpleNamespace(regime=SimpleNamespace(pos_mult_risk_off=0.0))
    state = SimpleNamespace(name="SIDEWAYS", value="Sideways")

    assert main._should_update_skipped_candidate_tracker(cfg, state, []) is True


def test_order_arbitration_summary_warns_in_live_mode() -> None:
    calls: list[tuple[str, str]] = []
    log = SimpleNamespace(
        warning=lambda message: calls.append(("warning", message)),
        info=lambda message: calls.append(("info", message)),
    )
    cfg = SimpleNamespace(execution=SimpleNamespace(mode="live"))

    main._log_order_arbitration_summary(log, cfg, "ORDER_ARBITRATION: before=2 after=1 blocked=1")

    assert calls == [("warning", "ORDER_ARBITRATION: before=2 after=1 blocked=1")]


def test_order_arbitration_summary_is_info_in_dry_run_mode() -> None:
    calls: list[tuple[str, str]] = []
    log = SimpleNamespace(
        warning=lambda message: calls.append(("warning", message)),
        info=lambda message: calls.append(("info", message)),
    )
    cfg = SimpleNamespace(execution=SimpleNamespace(mode="dry_run"))

    main._log_order_arbitration_summary(log, cfg, "ORDER_ARBITRATION: before=2 after=1 blocked=1")

    assert calls == [("info", "ORDER_ARBITRATION: before=2 after=1 blocked=1")]
