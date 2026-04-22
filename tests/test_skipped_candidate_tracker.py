from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.skipped_candidate_tracker import update_skipped_candidate_tracker


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


def test_protect_entry_trend_only_skip_is_written(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_00"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4]

    audit = DecisionAudit(run_id="20260421_00")
    audit.regime = "Trending"
    audit.router_decisions = [
        {
            "symbol": "BTC/USDT",
            "action": "skip",
            "reason": "protect_entry_trend_only",
            "trend_score": 0.92,
            "current_level": "PROTECT",
        }
    ]
    market_data = {
        "BTC/USDT": _series(
            "BTC/USDT",
            [1_710_000_000_000],
            [100.0],
        )
    }

    result = update_skipped_candidate_tracker(
        run_dir=run_dir,
        audit=audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
    )

    assert result["new_records"] == 1
    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["skip_reason"] == "protect_entry_trend_only"
    assert rows[0]["trend_score"] == 0.92
    assert rows[0]["label_status"] == "pending"


def test_cost_aware_edge_skip_gets_forward_label_when_horizon_available(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_01"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4]
    cfg.diagnostics.skipped_candidate_roundtrip_cost_bps = 30.0

    audit = DecisionAudit(run_id="20260421_01")
    audit.regime = "Trending"
    audit.router_decisions = [
        {
            "symbol": "BTC/USDT",
            "action": "skip",
            "reason": "cost_aware_edge",
            "score": 0.12,
            "required_score": 0.18,
            "rt_cost_bps": 30.0,
            "px": 100.0,
        }
    ]
    entry_ts_ms = 1_710_000_000_000
    market_data = {"BTC/USDT": _series("BTC/USDT", [entry_ts_ms], [100.0])}
    _write_cache_csv(
        cache_dir,
        "BTC/USDT",
        [
            ("2024-03-09T16:00:00Z", 100.0),
            ("2024-03-09T20:00:00Z", 101.0),
        ],
    )

    update_skipped_candidate_tracker(
        run_dir=run_dir,
        audit=audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="NEUTRAL",
        cache_dir=cache_dir,
    )

    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["skip_reason"] == "cost_aware_edge"
    assert rows[0]["label_4h_net_bps"] == 70.0
    assert rows[0]["label_status"] == "complete"


def test_unreached_horizon_stays_pending(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_02"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4, 8]

    audit = DecisionAudit(run_id="20260421_02")
    audit.regime = "Trending"
    audit.router_decisions = [
        {
            "symbol": "BTC/USDT",
            "action": "skip",
            "reason": "cost_aware_edge",
            "score": 0.12,
            "required_score": 0.18,
            "px": 100.0,
        }
    ]
    entry_ts_ms = 1_710_000_000_000
    market_data = {"BTC/USDT": _series("BTC/USDT", [entry_ts_ms], [100.0])}
    _write_cache_csv(
        cache_dir,
        "BTC/USDT",
        [
            ("2024-03-09T16:00:00Z", 100.0),
            ("2024-03-09T20:00:00Z", 100.5),
        ],
    )

    update_skipped_candidate_tracker(
        run_dir=run_dir,
        audit=audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="NEUTRAL",
        cache_dir=cache_dir,
    )

    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[0]["label_4h_net_bps"] == 20.0
    assert rows[0]["label_8h_net_bps"] is None
    assert rows[0]["label_status"] == "pending"


def test_tracker_disabled_writes_no_files(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_03"
    run_dir.mkdir(parents=True, exist_ok=True)
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_label_enabled = False
    audit = DecisionAudit(run_id="20260421_03")
    audit.router_decisions = [
        {"symbol": "BTC/USDT", "action": "skip", "reason": "protect_entry_trend_only"}
    ]
    market_data = {"BTC/USDT": _series("BTC/USDT", [1_710_000_000_000], [100.0])}

    result = update_skipped_candidate_tracker(
        run_dir=run_dir,
        audit=audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=tmp_path / "data" / "cache",
    )

    assert result["enabled"] is False
    assert not (tmp_path / "reports" / "skipped_candidate_labels.jsonl").exists()


def test_load_cache_ohlcv_prefers_logically_newer_file_for_duplicate_timestamp(tmp_path: Path) -> None:
    from src.reporting.skipped_candidate_tracker import _load_cache_ohlcv

    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    (cache_dir / "BTC_USDT_1H_20260101.csv").write_text(
        "\n".join(
            [
                "timestamp,open,high,low,close,volume",
                "2026-01-01T00:00:00Z,100,100,100,100,1000",
                "2026-01-01T01:00:00Z,101,101,101,101,1000",
            ]
        ),
        encoding="utf-8",
    )
    (cache_dir / "BTC_USDT_1H_2026-01-01_2026-01-02.csv").write_text(
        "\n".join(
            [
                "timestamp,open,high,low,close,volume",
                "2026-01-01T01:00:00Z,999,999,999,999,1000",
                "2026-01-01T02:00:00Z,103,103,103,103,1000",
            ]
        ),
        encoding="utf-8",
    )

    series = _load_cache_ohlcv(cache_dir, "BTC/USDT")

    assert [row["timestamp_ms"] for row in series] == [
        int(datetime.fromisoformat("2026-01-01T00:00:00+00:00").timestamp() * 1000),
        int(datetime.fromisoformat("2026-01-01T01:00:00+00:00").timestamp() * 1000),
        int(datetime.fromisoformat("2026-01-01T02:00:00+00:00").timestamp() * 1000),
    ]
    assert [row["close"] for row in series] == [100.0, 999.0, 103.0]
