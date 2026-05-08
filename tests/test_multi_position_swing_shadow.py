from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.multi_position_swing_shadow import update_multi_position_swing_shadow_evaluator


def _ts_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _series(symbol: str, timestamps_ms: list[int], closes: list[float]) -> MarketSeries:
    return MarketSeries(
        symbol=symbol,
        timeframe="1h",
        ts=timestamps_ms,
        open=closes,
        high=closes,
        low=closes,
        close=closes,
        volume=[1000.0 for _ in closes],
    )


def _write_cache_csv(cache_dir: Path, symbol: str, rows: list[tuple[str, float]]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{symbol.replace('/', '_')}_1H_test.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for ts, close in rows:
            writer.writerow(
                {
                    "timestamp": ts,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": 1000.0,
                }
            )


def _cfg() -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"])
    cfg.diagnostics.multi_position_swing_shadow_enabled = True
    cfg.diagnostics.multi_position_swing_shadow_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
    cfg.diagnostics.multi_position_swing_shadow_min_final_score = 0.30
    cfg.diagnostics.multi_position_swing_shadow_horizons_hours = [24, 48, 72]
    cfg.diagnostics.multi_position_swing_shadow_rt_cost_bps = 30.0
    return cfg


def _audit(run_id: str, entry_ts_ms: int, *, risk_off: bool = False) -> DecisionAudit:
    audit = DecisionAudit(run_id=run_id)
    audit.now_ts = entry_ts_ms // 1000
    audit.regime = "Risk-Off" if risk_off else "Trending"
    audit.top_scores = [
        {"symbol": "ETH/USDT", "score": 0.90, "rank": 1},
        {"symbol": "BTC/USDT", "score": 0.80, "rank": 2},
        {"symbol": "SOL/USDT", "score": 0.70, "rank": 3},
        {"symbol": "BNB/USDT", "score": 0.20, "rank": 4},
    ]
    audit.target_execution_explain = [
        {"symbol": "ETH/USDT", "entry_px": 100.0, "final_score": 0.90},
        {"symbol": "BTC/USDT", "entry_px": 200.0, "final_score": 0.80},
        {"symbol": "SOL/USDT", "entry_px": 50.0, "final_score": 0.70},
    ]
    return audit


def test_top2_shadow_is_generated_without_orders(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260508_08"
    run_dir.mkdir(parents=True, exist_ok=True)
    entry_ts_ms = _ts_ms("2026-05-08T08:00:00Z")

    result = update_multi_position_swing_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260508_08", entry_ts_ms),
        market_data_1h={},
        cfg=_cfg(),
        current_level="PROTECT",
        cache_dir=tmp_path / "data" / "cache",
        ohlcv_provider=None,
    )

    assert result["new_records"] == 3
    assert "orders" not in result
    labels = [
        json.loads(line)
        for line in (tmp_path / "reports" / "multi_position_swing_shadow_labels.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    top2 = next(row for row in labels if row["k"] == 2)
    assert top2["symbols"] == ["ETH/USDT", "BTC/USDT"]
    assert top2["equal_weight"] == 0.5
    assert top2["entry_px"] == {"ETH/USDT": 100.0, "BTC/USDT": 200.0}

    outcomes = list(
        csv.DictReader((tmp_path / "reports" / "summaries" / "multi_position_swing_shadow_outcomes.csv").read_text(encoding="utf-8").splitlines())
    )
    assert any(row["k"] == "2" and "ETH/USDT" in row["symbols"] for row in outcomes)


def test_matured_horizons_fill_portfolio_labels(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260508_08"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    entry_ts_ms = _ts_ms("2026-05-08T08:00:00Z")
    cfg = _cfg()

    update_multi_position_swing_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260508_08", entry_ts_ms),
        market_data_1h={},
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
        ohlcv_provider=None,
    )
    _write_cache_csv(
        cache_dir,
        "ETH/USDT",
        [("2026-05-08T08:00:00Z", 100.0), ("2026-05-09T08:00:00Z", 104.0), ("2026-05-10T08:00:00Z", 106.0)],
    )
    _write_cache_csv(
        cache_dir,
        "BTC/USDT",
        [("2026-05-08T08:00:00Z", 200.0), ("2026-05-09T08:00:00Z", 202.0), ("2026-05-10T08:00:00Z", 204.0)],
    )
    _write_cache_csv(
        cache_dir,
        "SOL/USDT",
        [("2026-05-08T08:00:00Z", 50.0), ("2026-05-09T08:00:00Z", 49.0), ("2026-05-10T08:00:00Z", 51.0)],
    )

    later = _audit("20260510_12", _ts_ms("2026-05-10T12:00:00Z"))
    later.top_scores = []
    later.target_execution_explain = []
    result = update_multi_position_swing_shadow_evaluator(
        run_dir=tmp_path / "reports" / "runs" / "20260510_12",
        audit=later,
        market_data_1h={},
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
        ohlcv_provider=None,
    )

    assert result["total_records"] == 3
    labels = [
        json.loads(line)
        for line in (tmp_path / "reports" / "multi_position_swing_shadow_labels.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    top2 = next(row for row in labels if row["k"] == 2)
    assert top2["label_24h_status"] == "complete"
    assert top2["label_24h_portfolio_avg_net_bps"] == 220.0
    assert top2["label_24h_worst_symbol_net_bps"] == 70.0
    assert top2["label_24h_win_count"] == 2
    assert top2["label_48h_status"] == "complete"
    assert top2["label_72h_status"] == "pending"

    by_k = list(
        csv.DictReader((tmp_path / "reports" / "summaries" / "multi_position_swing_shadow_by_k.csv").read_text(encoding="utf-8").splitlines())
    )
    k2 = next(row for row in by_k if row["k"] == "2")
    assert k2["avg_24h_net_bps"] == "220.0"
    assert k2["win_rate"] == "1.0"
    assert k2["worst_avg"] == "70.0"


def test_unmatured_horizon_stays_pending(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260508_08"
    run_dir.mkdir(parents=True, exist_ok=True)
    entry_ts_ms = _ts_ms("2026-05-08T08:00:00Z")

    update_multi_position_swing_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260508_08", entry_ts_ms),
        market_data_1h={},
        cfg=_cfg(),
        current_level="PROTECT",
        cache_dir=tmp_path / "data" / "cache",
        ohlcv_provider=None,
    )

    labels = [
        json.loads(line)
        for line in (tmp_path / "reports" / "multi_position_swing_shadow_labels.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert all(row["label_status"] == "pending" for row in labels)
    assert all(row["label_24h_status"] == "pending" for row in labels)


def test_risk_off_generates_no_shadow_labels(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260508_08"
    run_dir.mkdir(parents=True, exist_ok=True)
    entry_ts_ms = _ts_ms("2026-05-08T08:00:00Z")

    result = update_multi_position_swing_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260508_08", entry_ts_ms, risk_off=True),
        market_data_1h={},
        cfg=_cfg(),
        current_level="PROTECT",
        cache_dir=tmp_path / "data" / "cache",
        ohlcv_provider=None,
    )

    assert result["new_records"] == 0
    assert not (tmp_path / "reports" / "multi_position_swing_shadow_labels.jsonl").exists()
