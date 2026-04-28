from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.reporting.probe_lifecycle import build_probe_lifecycle_reports


def _write_trade(
    bundle_root: Path,
    run_id: str,
    *,
    ts: str,
    symbol: str,
    intent: str,
    side: str,
    qty: float,
    price: float,
    fee_usdt: float,
) -> None:
    run_dir = bundle_root / "raw" / "recent_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "trades.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ts",
                "run_id",
                "symbol",
                "intent",
                "side",
                "qty",
                "price",
                "notional_usdt",
                "fee_usdt",
                "slippage_usdt",
                "realized_pnl_usdt",
                "realized_pnl_pct",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ts": ts,
                "run_id": run_id,
                "symbol": symbol,
                "intent": intent,
                "side": side,
                "qty": f"{qty:.12g}",
                "price": f"{price:.12g}",
                "notional_usdt": f"{qty * price:.12g}",
                "fee_usdt": f"{fee_usdt:.12g}",
                "slippage_usdt": "",
                "realized_pnl_usdt": "",
                "realized_pnl_pct": "",
            }
        )


def _write_audit(bundle_root: Path, run_id: str, router_decisions: list[dict]) -> None:
    run_dir = bundle_root / "raw" / "recent_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "router_decisions": router_decisions,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_probe_lifecycle_pairs_latest_btc_market_impulse_case(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    entry_px = 78021.7
    exit_px = 78207.2
    qty = 0.0002
    gross_bps = (exit_px / entry_px - 1.0) * 10000.0
    entry_notional = qty * entry_px
    total_fee = (gross_bps - 3.75) * entry_notional / 10000.0

    _write_trade(
        bundle,
        "20260426_16",
        ts="2026-04-26T16:00:00Z",
        symbol="BTC/USDT",
        intent="OPEN_LONG",
        side="buy",
        qty=qty,
        price=entry_px,
        fee_usdt=total_fee / 2.0,
    )
    _write_trade(
        bundle,
        "20260426_21",
        ts="2026-04-26T21:00:00Z",
        symbol="BTC/USDT",
        intent="CLOSE_LONG",
        side="sell",
        qty=qty,
        price=exit_px,
        fee_usdt=total_fee / 2.0,
    )
    _write_audit(
        bundle,
        "20260426_16",
        [
            {
                "symbol": "BTC/USDT",
                "action": "create",
                "reason": "market_impulse_probe",
                "side": "buy",
                "intent": "OPEN_LONG",
                "market_impulse_probe": True,
            }
        ],
    )
    _write_audit(
        bundle,
        "20260426_21",
        [
            {
                "symbol": "BTC/USDT",
                "action": "create",
                "reason": "exit_signal_priority",
                "source_reason": "market_impulse_probe_time_stop",
                "side": "sell",
                "intent": "CLOSE_LONG",
            }
        ],
    )
    state_dir = bundle / "raw" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "profit_taking_state.json").write_text(
        json.dumps({"BTC/USDT": {"entry_price": entry_px}}, ensure_ascii=False),
        encoding="utf-8",
    )

    result = build_probe_lifecycle_reports(bundle)

    assert result["roundtrip_rows"] == 1
    assert result["probe_lifecycle_rows"] == 1
    roundtrip = _read_csv(bundle / "summaries" / "trades_roundtrips.csv")[0]
    assert roundtrip["entry_reason"] == "market_impulse_probe"
    assert roundtrip["exit_reason"] == "market_impulse_probe_time_stop"
    assert roundtrip["probe_type"] == "market_impulse_probe"
    assert float(roundtrip["hold_minutes"]) == pytest.approx(300.0)
    assert float(roundtrip["gross_bps"]) == pytest.approx(23.775424, abs=0.01)
    assert float(roundtrip["net_bps"]) == pytest.approx(3.75, abs=0.01)

    lifecycle = _read_csv(bundle / "summaries" / "probe_lifecycle_audit.csv")[0]
    assert lifecycle["probe_type"] == "market_impulse_probe"
    assert lifecycle["entry_ts"] == "2026-04-26T16:00:00Z"
    assert lifecycle["exit_ts"] == "2026-04-26T21:00:00Z"
    assert lifecycle["exit_reason"] == "market_impulse_probe_time_stop"
    assert lifecycle["state_still_present_after_close"] == "True"
    assert float(lifecycle["gross_bps"]) == pytest.approx(23.775424, abs=0.01)
    assert float(lifecycle["net_bps"]) == pytest.approx(3.75, abs=0.01)

    summary = json.loads((bundle / "summaries" / "window_summary.json").read_text(encoding="utf-8"))
    assert summary["probe_lifecycle_rows"] == 1
    assert summary["probe_trade_gross_bps"]["avg"] == pytest.approx(23.775424, abs=0.01)
    assert summary["probe_trade_net_bps"]["avg"] == pytest.approx(3.75, abs=0.01)


def test_probe_lifecycle_writes_high_issue_when_buy_sell_cannot_pair(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    _write_trade(
        bundle,
        "20260426_21",
        ts="2026-04-26T21:00:00Z",
        symbol="BTC/USDT",
        intent="CLOSE_LONG",
        side="sell",
        qty=0.1,
        price=101.0,
        fee_usdt=0.0,
    )
    _write_trade(
        bundle,
        "20260426_22",
        ts="2026-04-26T22:00:00Z",
        symbol="BTC/USDT",
        intent="OPEN_LONG",
        side="buy",
        qty=0.1,
        price=100.0,
        fee_usdt=0.0,
    )

    result = build_probe_lifecycle_reports(bundle)

    assert result["roundtrip_rows"] == 0
    issues = json.loads((bundle / "summaries" / "issues_to_fix.json").read_text(encoding="utf-8"))
    assert issues["high_issue_count"] >= 1
    assert any(issue["code"] == "raw_trades_buy_sell_not_paired" for issue in issues["issues"])
