from __future__ import annotations

import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import main
from configs.schema import ExecutionConfig
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


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


def test_sync_live_fills_before_routing_records_reentry_memory() -> None:
    class _Client:
        def __init__(self) -> None:
            self.calls = 0

        def get_fills(self, *, after=None, limit=100):
            self.calls += 1
            if self.calls > 1:
                return SimpleNamespace(data={"data": []})
            return SimpleNamespace(
                data={
                    "data": [
                        {
                            "instId": "SOL-USDT",
                            "tradeId": "fill-1",
                            "ts": "123456",
                            "ordId": "3001",
                            "clOrdId": "PL1",
                            "side": "sell",
                            "fillPx": "102",
                            "fillSz": "1.0",
                            "fee": "0",
                            "feeCcy": "USDT",
                        }
                    ]
                }
            )

    with tempfile.TemporaryDirectory() as td:
        cfg = SimpleNamespace(
            execution=ExecutionConfig(
                mode="live",
                order_store_path=f"{td}/orders.sqlite",
                reconcile_status_path=f"{td}/reconcile_status.json",
                kill_switch_path=f"{td}/kill_switch.json",
            )
        )
        store = OrderStore(path=f"{td}/orders.sqlite")
        positions = PositionStore(path=f"{td}/positions.sqlite")
        positions.upsert_buy("SOL/USDT", qty=1.0, px=100.0)
        positions.mark_position("SOL/USDT", now_ts="2026-05-09T05:00:00Z", mark_px=102.0, high_px=103.0)
        store.upsert_new(
            cl_ord_id="PL1",
            run_id="r",
            inst_id="SOL-USDT",
            side="sell",
            intent="CLOSE_LONG",
            decision_hash="pl-h",
            td_mode="cash",
            ord_type="market",
            notional_usdt=102.0,
            req={
                "_v5_reason": "protect_profit_lock_trailing",
                "_v5_order_meta": {
                    "reason": "protect_profit_lock_trailing",
                    "exit_reason": "protect_profit_lock_trailing",
                    "highest_px_before_exit": 103.0,
                    "net_bps": 128.0,
                },
            },
        )
        store.update_state("PL1", new_state="OPEN", ord_id="3001")
        audit_notes = []
        result = main._sync_live_fills_before_routing(
            cfg,
            client=_Client(),
            position_store=positions,
            audit=SimpleNamespace(add_note=audit_notes.append),
            log_obj=SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None),
        )

        assert result["enabled"] is True
        assert result["new_fills"] == 1
        assert result["updated_orders"] == 1
        assert any("LIVE_FILL_SYNC_PRE_ROUTE" in note for note in audit_notes)
        memory_path = Path(td) / "same_symbol_reentry_exit_memory.json"
        payload = json.loads(memory_path.read_text(encoding="utf-8"))
        rec = payload["symbols"]["SOL/USDT"]
        assert rec["exit_reason"] == "protect_profit_lock_trailing"
        assert rec["exit_ts_ms"] == 123456
