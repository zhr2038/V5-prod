from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
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


class _FakeProvider:
    def __init__(self, series_by_symbol: dict[str, MarketSeries]):
        self.series_by_symbol = series_by_symbol
        self.calls: list[dict[str, object]] = []

    def fetch_ohlcv(self, symbols, timeframe="1h", limit=200, end_ts_ms=None):
        self.calls.append(
            {
                "symbols": list(symbols or []),
                "timeframe": timeframe,
                "limit": int(limit),
                "end_ts_ms": end_ts_ms,
            }
        )
        return {symbol: self.series_by_symbol[symbol] for symbol in symbols if symbol in self.series_by_symbol}


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

    entry_ts_ms = 1_710_000_000_000
    audit = DecisionAudit(run_id="20260421_00")
    audit.now_ts = entry_ts_ms // 1000
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
    market_data = {"BTC/USDT": _series("BTC/USDT", [entry_ts_ms], [100.0])}

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
    audit.now_ts = (1_710_000_000_000 // 1000) + 8 * 3600
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
    assert rows[0]["label_4h_gross_bps"] == 100.0
    assert rows[0]["label_4h_net_bps"] == 70.0
    assert rows[0]["label_4h_would_have_won_net"] is True
    assert rows[0]["label_4h_status"] == "complete"
    assert rows[0]["label_status"] == "complete"


def test_existing_ts_utc_record_over_horizon_gets_labeled(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260422_00"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4]
    cfg.diagnostics.skipped_candidate_roundtrip_cost_bps = 30.0

    entry_ts_ms = int(datetime.fromisoformat("2026-04-21T14:00:00+00:00").timestamp() * 1000)
    labels_path.write_text(
        json.dumps(
            {
                "ts_utc": "2026-04-21T14:00:00Z",
                "run_id": "20260421_14",
                "symbol": "BTC/USDT",
                "intended_side": "buy",
                "skip_reason": "cost_aware_edge",
                "entry_px": 100.0,
                "rt_cost_bps": 30.0,
                "label_status": "pending",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    audit = DecisionAudit(run_id="20260422_00")
    audit.now_ts = int(datetime.fromisoformat("2026-04-22T00:00:00+00:00").timestamp())
    market_data = {
        "BTC/USDT": _series(
            "BTC/USDT",
            [
                entry_ts_ms,
                entry_ts_ms + 4 * 3600 * 1000,
            ],
            [100.0, 101.0],
        )
    }

    update_skipped_candidate_tracker(
        run_dir=run_dir,
        audit=audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="NEUTRAL",
        cache_dir=cache_dir,
    )

    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["entry_ts_ms"] == entry_ts_ms
    assert rows[0]["label_4h_status"] == "complete"
    assert rows[0]["label_4h_gross_bps"] == 100.0
    assert rows[0]["label_4h_net_bps"] == 70.0
    assert rows[0]["label_status"] == "complete"


def test_unreached_horizon_stays_pending_even_if_future_cache_exists(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_02"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4, 8]

    entry_ts_ms = 1_710_000_000_000
    audit = DecisionAudit(run_id="20260421_02")
    audit.now_ts = entry_ts_ms // 1000
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
    assert rows[0]["label_4h_net_bps"] is None
    assert rows[0]["label_4h_status"] == "pending"
    assert rows[0]["label_8h_net_bps"] is None
    assert rows[0]["label_8h_status"] == "pending"
    assert rows[0]["label_status"] == "pending"


def test_matured_record_fetches_provider_ohlcv_when_local_cache_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260422_00"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4]
    cfg.diagnostics.skipped_candidate_roundtrip_cost_bps = 30.0

    entry_ts_ms = int(datetime.fromisoformat("2026-04-21T14:00:00+00:00").timestamp() * 1000)
    labels_path.write_text(
        json.dumps(
            {
                "ts_utc": "2026-04-21T14:00:00Z",
                "entry_ts_ms": entry_ts_ms,
                "run_id": "20260421_14",
                "symbol": "BTC/USDT",
                "intended_side": "buy",
                "skip_reason": "cost_aware_edge",
                "entry_px": 100.0,
                "rt_cost_bps": 30.0,
                "label_status": "pending",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    audit = DecisionAudit(run_id="20260422_00")
    audit.now_ts = int(datetime.fromisoformat("2026-04-22T00:00:00+00:00").timestamp())
    provider = _FakeProvider(
        {
            "BTC/USDT": _series(
                "BTC/USDT",
                [entry_ts_ms, entry_ts_ms + 4 * 3600 * 1000],
                [100.0, 102.0],
            )
        }
    )

    update_skipped_candidate_tracker(
        run_dir=run_dir,
        audit=audit,
        market_data_1h={},
        cfg=cfg,
        current_level="NEUTRAL",
        cache_dir=cache_dir,
        ohlcv_provider=provider,
    )

    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(provider.calls) == 1
    assert rows[0]["label_4h_status"] == "complete"
    assert rows[0]["label_4h_gross_bps"] == 200.0
    assert rows[0]["label_4h_net_bps"] == 170.0
    assert rows[0]["label_status"] == "complete"


def test_4h_8h_12h_24h_horizons_mature_independently(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260422_16"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4, 8, 12, 24]
    cfg.diagnostics.skipped_candidate_roundtrip_cost_bps = 30.0

    entry_ts_ms = int(datetime.fromisoformat("2026-04-21T14:00:00+00:00").timestamp() * 1000)
    labels_path.write_text(
        json.dumps(
            {
                "ts_utc": "2026-04-21T14:00:00Z",
                "entry_ts_ms": entry_ts_ms,
                "run_id": "20260421_14",
                "symbol": "BTC/USDT",
                "intended_side": "buy",
                "skip_reason": "cost_aware_edge",
                "entry_px": 100.0,
                "rt_cost_bps": 30.0,
                "label_status": "pending",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    audit = DecisionAudit(run_id="20260421_14")
    audit.now_ts = int(datetime.fromisoformat("2026-04-22T16:00:00+00:00").timestamp())
    market_data = {
        "BTC/USDT": _series(
            "BTC/USDT",
            [
                entry_ts_ms,
                entry_ts_ms + 4 * 3600 * 1000,
                entry_ts_ms + 8 * 3600 * 1000,
                entry_ts_ms + 12 * 3600 * 1000,
                entry_ts_ms + 24 * 3600 * 1000,
            ],
            [100.0, 101.0, 102.0, 103.0, 104.0],
        )
    }

    update_skipped_candidate_tracker(
        run_dir=run_dir,
        audit=audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="NEUTRAL",
        cache_dir=cache_dir,
    )

    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    row = rows[0]
    assert row["label_4h_status"] == "complete"
    assert row["label_8h_status"] == "complete"
    assert row["label_12h_status"] == "complete"
    assert row["label_24h_status"] == "complete"
    assert row["label_4h_net_bps"] == 70.0
    assert row["label_8h_net_bps"] == 170.0
    assert row["label_12h_net_bps"] == 270.0
    assert row["label_24h_net_bps"] == 370.0

    by_reason_path = tmp_path / "reports" / "summaries" / "skipped_candidate_outcomes_by_reason.csv"
    with by_reason_path.open("r", encoding="utf-8") as f:
        summary_rows = list(csv.DictReader(f))
    assert summary_rows[0]["complete_count"] == "1"
    assert summary_rows[0]["pending_count"] == "0"
    assert summary_rows[0]["not_observable_count"] == "0"
    assert float(summary_rows[0]["avg_4h_net_bps"]) == 70.0
    assert float(summary_rows[0]["avg_8h_net_bps"]) == 170.0
    assert float(summary_rows[0]["avg_12h_net_bps"]) == 270.0
    assert float(summary_rows[0]["avg_24h_net_bps"]) == 370.0
    assert float(summary_rows[0]["win_rate_4h"]) == 1.0
    assert float(summary_rows[0]["win_rate_8h"]) == 1.0
    assert float(summary_rows[0]["win_rate_12h"]) == 1.0
    assert float(summary_rows[0]["win_rate_24h"]) == 1.0


def test_matured_record_uses_current_market_data_when_cache_is_stale(tmp_path: Path) -> None:
    entry_run_dir = tmp_path / "reports" / "runs" / "20260421_14"
    entry_run_dir.mkdir(parents=True, exist_ok=True)
    update_run_dir = tmp_path / "reports" / "runs" / "20260422_11"
    update_run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    cfg = AppConfig(symbols=["BNB/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4, 8, 12, 24]
    cfg.diagnostics.skipped_candidate_roundtrip_cost_bps = 30.0

    entry_ts_ms = int(datetime.fromisoformat("2026-04-21T14:00:00+00:00").timestamp() * 1000)
    entry_audit = DecisionAudit(run_id="20260421_14")
    entry_audit.now_ts = entry_ts_ms // 1000
    entry_audit.regime = "Trending"
    entry_audit.router_decisions = [
        {
            "symbol": "BNB/USDT",
            "action": "skip",
            "reason": "all_scores_below_threshold",
            "score": -1.0,
            "required_score": 0.1,
            "px": 630.5,
        }
    ]
    entry_market_data = {"BNB/USDT": _series("BNB/USDT", [entry_ts_ms], [630.5])}
    update_skipped_candidate_tracker(
        run_dir=entry_run_dir,
        audit=entry_audit,
        market_data_1h=entry_market_data,
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
    )

    update_audit = DecisionAudit(run_id="20260422_11")
    update_audit.now_ts = int(datetime.fromisoformat("2026-04-22T11:00:00+00:00").timestamp())
    update_audit.regime = "Trending"
    series_ts = [
        int(datetime.fromisoformat(v).replace(tzinfo=timezone.utc).timestamp() * 1000)
        for v in [
            "2026-04-21T14:00:00",
            "2026-04-21T18:00:00",
            "2026-04-21T22:00:00",
            "2026-04-22T02:00:00",
            "2026-04-22T11:00:00",
        ]
    ]
    market_data = {"BNB/USDT": _series("BNB/USDT", series_ts, [630.5, 632.0, 633.0, 635.0, 634.0])}

    update_skipped_candidate_tracker(
        run_dir=update_run_dir,
        audit=update_audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
    )

    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[0]["label_4h_status"] == "complete"
    assert rows[0]["label_8h_status"] == "complete"
    assert rows[0]["label_12h_status"] == "complete"
    assert rows[0]["label_24h_status"] == "pending"
    assert rows[0]["label_status"] == "complete"

    by_reason_path = tmp_path / "reports" / "summaries" / "skipped_candidate_outcomes_by_reason.csv"
    with by_reason_path.open("r", encoding="utf-8") as f:
        summary_rows = list(csv.DictReader(f))
    assert summary_rows[0]["complete_count"] == "1"
    assert summary_rows[0]["pending_count"] == "0"


def test_matured_without_future_price_becomes_not_observable(tmp_path: Path) -> None:
    entry_run_dir = tmp_path / "reports" / "runs" / "20260421_00"
    entry_run_dir.mkdir(parents=True, exist_ok=True)
    update_run_dir = tmp_path / "reports" / "runs" / "20260422_12"
    update_run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_horizons_hours = [4]

    entry_ts_ms = 1_710_000_000_000
    entry_audit = DecisionAudit(run_id="20260421_00")
    entry_audit.now_ts = entry_ts_ms // 1000
    entry_audit.regime = "Trending"
    entry_audit.router_decisions = [
        {
            "symbol": "BTC/USDT",
            "action": "skip",
            "reason": "cost_aware_edge",
            "score": 0.12,
            "required_score": 0.18,
            "px": 100.0,
        }
    ]
    entry_market_data = {"BTC/USDT": _series("BTC/USDT", [entry_ts_ms], [100.0])}
    update_skipped_candidate_tracker(
        run_dir=entry_run_dir,
        audit=entry_audit,
        market_data_1h=entry_market_data,
        cfg=cfg,
        current_level="NEUTRAL",
        cache_dir=cache_dir,
    )

    _write_cache_csv(
        cache_dir,
        "BTC/USDT",
        [
            ("2024-03-09T16:00:00Z", 100.0),
            ("2024-03-09T19:00:00Z", 101.0),
        ],
    )

    update_audit = DecisionAudit(run_id="20260422_12")
    update_audit.now_ts = (entry_ts_ms // 1000) + 8 * 3600
    update_audit.regime = "Trending"
    market_data = {"BTC/USDT": _series("BTC/USDT", [entry_ts_ms], [100.0])}

    update_skipped_candidate_tracker(
        run_dir=update_run_dir,
        audit=update_audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="NEUTRAL",
        cache_dir=cache_dir,
    )

    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[0]["label_4h_status"] == "not_observable"
    assert "missing_price_at_or_after_" in rows[0]["label_4h_reason"]
    assert rows[0]["label_status"] == "not_observable"


def test_tracker_disabled_writes_no_files(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_03"
    run_dir.mkdir(parents=True, exist_ok=True)
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.diagnostics.skipped_candidate_label_enabled = False
    audit = DecisionAudit(run_id="20260421_03")
    audit.router_decisions = [{"symbol": "BTC/USDT", "action": "skip", "reason": "protect_entry_trend_only"}]
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


def test_all_scores_below_threshold_uses_latest_market_bar_when_series_is_unsorted(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_04"
    run_dir.mkdir(parents=True, exist_ok=True)
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.alpha.min_score_threshold = 0.2

    audit = DecisionAudit(run_id="20260421_04")
    audit.regime = "Trending"
    audit.top_scores = [{"symbol": "BTC/USDT", "score": 0.1}]
    market_data = {
        "BTC/USDT": _series(
            "BTC/USDT",
            [1_710_003_600_000, 1_710_000_000_000],
            [120.0, 100.0],
        )
    }

    result = update_skipped_candidate_tracker(
        run_dir=run_dir,
        audit=audit,
        market_data_1h=market_data,
        cfg=cfg,
        current_level="NEUTRAL",
        cache_dir=tmp_path / "data" / "cache",
    )

    assert result["new_records"] == 1
    labels_path = tmp_path / "reports" / "skipped_candidate_labels.jsonl"
    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["skip_reason"] == "all_scores_below_threshold"
    assert rows[0]["entry_px"] == 120.0
    assert rows[0]["ts_utc"] == "2024-03-09T17:00:00Z"


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
