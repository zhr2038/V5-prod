from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.reporting.alt_impulse_shadow import update_alt_impulse_shadow_evaluator
from src.reporting.decision_audit import DecisionAudit


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
    cfg.diagnostics.alt_impulse_shadow_enabled = True
    cfg.diagnostics.alt_impulse_shadow_symbols = ["ETH/USDT", "SOL/USDT", "BNB/USDT"]
    cfg.diagnostics.alt_impulse_shadow_min_final_score = 0.80
    cfg.diagnostics.alt_impulse_shadow_min_trend_score = 0.80
    cfg.diagnostics.alt_impulse_shadow_require_btc_positive_4h = True
    cfg.diagnostics.alt_impulse_shadow_require_broad_market_positive_count = 2
    cfg.diagnostics.alt_impulse_shadow_rt_cost_bps = 30.0
    return cfg


def _audit(run_id: str, entry_ts_ms: int, explain: list[dict], *, regime: str = "Trending") -> DecisionAudit:
    audit = DecisionAudit(run_id=run_id)
    audit.now_ts = entry_ts_ms // 1000
    audit.regime = regime
    audit.target_execution_explain = explain
    return audit


def _eth_trend_only_explain(*, regime: str = "Trending", volatility_bucket: str = "medium") -> list[dict]:
    return [
        {
            "symbol": "ETH/USDT",
            "target_w": 0.15,
            "final_score": 1.0,
            "trend_score": 1.0,
            "trend_side": "buy",
            "alpha6_score": None,
            "alpha6_side": None,
            "router_action": "skip",
            "router_reason": "protect_entry_trend_only",
            "current_level": "PROTECT",
            "regime": regime,
            "funding_state": "neutral",
            "volatility_bucket": volatility_bucket,
        }
    ]


def test_alt_impulse_shadow_writes_eth_label_when_btc_4h_positive(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_14"
    run_dir.mkdir(parents=True, exist_ok=True)
    entry_ts_ms = _ts_ms("2026-04-21T14:00:00Z")
    prior_ts_ms = entry_ts_ms - 4 * 3600 * 1000

    result = update_alt_impulse_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260421_14", entry_ts_ms, _eth_trend_only_explain()),
        market_data_1h={
            "BTC/USDT": _series("BTC/USDT", [prior_ts_ms, entry_ts_ms], [100.0, 101.0]),
            "ETH/USDT": _series("ETH/USDT", [prior_ts_ms, entry_ts_ms], [98.0, 100.0]),
            "SOL/USDT": _series("SOL/USDT", [prior_ts_ms, entry_ts_ms], [50.0, 51.0]),
        },
        cfg=_cfg(),
        current_level="PROTECT",
        cache_dir=tmp_path / "data" / "cache",
        ohlcv_provider=None,
    )

    assert result["new_records"] == 1
    labels_path = tmp_path / "reports" / "alt_impulse_shadow_labels.jsonl"
    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "ETH/USDT"
    assert row["skip_reason"] == "protect_entry_trend_only"
    assert row["entry_px"] == 100.0
    assert row["btc_4h_ret_bps"] == 100.0
    assert row["btc_trend_state"] == "positive_4h"
    assert row["whitelist_positive_4h_count"] == 3
    assert row["broad_market_positive_count"] == 3
    assert row["regime_state"] == "Trending"
    assert row["risk_level"] == "PROTECT"
    assert row["funding_state"] == "neutral"
    assert row["volatility_bucket"] == "medium"
    assert row["shadow_decision"] == "REGIME_SHADOW"
    assert row["alpha_discovery_board_status"] == "REGIME_SHADOW"
    assert row["paper_ready_allowed"] is False
    assert row["live_ready_allowed"] is False
    assert row["label_status"] == "pending"


def test_alt_impulse_shadow_skips_when_btc_4h_negative(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_14"
    run_dir.mkdir(parents=True, exist_ok=True)
    entry_ts_ms = _ts_ms("2026-04-21T14:00:00Z")
    prior_ts_ms = entry_ts_ms - 4 * 3600 * 1000

    result = update_alt_impulse_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260421_14", entry_ts_ms, _eth_trend_only_explain()),
        market_data_1h={
            "BTC/USDT": _series("BTC/USDT", [prior_ts_ms, entry_ts_ms], [101.0, 100.0]),
            "ETH/USDT": _series("ETH/USDT", [prior_ts_ms, entry_ts_ms], [98.0, 100.0]),
            "SOL/USDT": _series("SOL/USDT", [prior_ts_ms, entry_ts_ms], [50.0, 51.0]),
        },
        cfg=_cfg(),
        current_level="PROTECT",
        cache_dir=tmp_path / "data" / "cache",
        ohlcv_provider=None,
    )

    assert result["new_records"] == 0
    assert not (tmp_path / "reports" / "alt_impulse_shadow_labels.jsonl").exists()


def test_alt_impulse_shadow_matures_forward_labels(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_14"
    run_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "data" / "cache"
    entry_ts_ms = _ts_ms("2026-04-21T14:00:00Z")
    prior_ts_ms = entry_ts_ms - 4 * 3600 * 1000
    cfg = _cfg()
    cfg.diagnostics.extended_label_horizons_hours = [4, 24, 48, 72]

    update_alt_impulse_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260421_14", entry_ts_ms, _eth_trend_only_explain()),
        market_data_1h={
            "BTC/USDT": _series("BTC/USDT", [prior_ts_ms, entry_ts_ms], [100.0, 101.0]),
            "ETH/USDT": _series("ETH/USDT", [prior_ts_ms, entry_ts_ms], [98.0, 100.0]),
            "SOL/USDT": _series("SOL/USDT", [prior_ts_ms, entry_ts_ms], [50.0, 51.0]),
        },
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
        ohlcv_provider=None,
    )
    labels_path = tmp_path / "reports" / "alt_impulse_shadow_labels.jsonl"
    initial_rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines()]
    initial_rows[0]["label_not_observable_reason"] = "missing_entry_px"
    labels_path.write_text("\n".join(json.dumps(row) for row in initial_rows) + "\n", encoding="utf-8")

    _write_cache_csv(
        cache_dir,
        "ETH/USDT",
        [
            ("2026-04-21T14:00:00Z", 100.0),
            ("2026-04-21T18:00:00Z", 101.0),
            ("2026-04-21T22:00:00Z", 102.0),
            ("2026-04-22T02:00:00Z", 103.0),
            ("2026-04-22T14:00:00Z", 104.0),
            ("2026-04-23T14:00:00Z", 105.0),
        ],
    )

    later_audit = _audit("20260424_02", entry_ts_ms + 60 * 3600 * 1000, [])
    result = update_alt_impulse_shadow_evaluator(
        run_dir=tmp_path / "reports" / "runs" / "20260424_02",
        audit=later_audit,
        market_data_1h={},
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
        ohlcv_provider=None,
    )

    assert result["total_records"] == 1
    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["label_status"] == "complete"
    assert rows[0]["label_not_observable_reason"] == ""
    assert rows[0]["label_4h_net_bps"] == 70.0
    assert rows[0]["label_4h_reason"] == ""
    assert rows[0]["label_24h_net_bps"] == 370.0
    assert rows[0]["label_48h_net_bps"] == 470.0
    assert rows[0]["label_72h_status"] == "pending"

    by_symbol_path = tmp_path / "reports" / "summaries" / "alt_impulse_shadow_outcomes_by_symbol.csv"
    with by_symbol_path.open("r", encoding="utf-8") as handle:
        by_symbol = list(csv.DictReader(handle))
    assert by_symbol[0]["symbol"] == "ETH/USDT"
    assert by_symbol[0]["skip_reason"] == "protect_entry_trend_only"
    assert by_symbol[0]["avg_4h_net_bps"] == "70.0"
    assert by_symbol[0]["avg_48h_net_bps"] == "470.0"
    assert by_symbol[0]["win_rate_4h"] == "1.0"

    by_horizon_path = tmp_path / "reports" / "summaries" / "alt_impulse_shadow_outcomes_by_horizon.csv"
    with by_horizon_path.open("r", encoding="utf-8") as handle:
        by_horizon = {row["horizon_hours"]: row for row in csv.DictReader(handle)}
    assert by_horizon["48"]["avg_net_bps"] == "470.0"
    assert by_horizon["48"]["complete_count"] == "1"
    assert by_horizon["72"]["pending_count"] == "1"

    by_regime_path = tmp_path / "reports" / "summaries" / "alt_impulse_shadow_by_regime.csv"
    with by_regime_path.open("r", encoding="utf-8") as handle:
        by_regime = {row["regime_state"]: row for row in csv.DictReader(handle)}
    assert by_regime["Trending"]["avg_48h_net_bps"] == "470.0"
    assert by_regime["Trending"]["win_rate_48h"] == "1.0"

    by_symbol_regime_horizon_path = (
        tmp_path / "reports" / "summaries" / "alt_impulse_shadow_by_symbol_regime_horizon.csv"
    )
    with by_symbol_regime_horizon_path.open("r", encoding="utf-8") as handle:
        by_symbol_regime_horizon = {
            (row["symbol"], row["regime_state"], row["horizon_hours"]): row
            for row in csv.DictReader(handle)
        }
    assert by_symbol_regime_horizon[("ETH/USDT", "Trending", "48")]["avg_net_bps"] == "470.0"
    assert by_symbol_regime_horizon[("ETH/USDT", "Trending", "48")]["complete_count"] == "1"
    assert by_symbol_regime_horizon[("ETH/USDT", "Trending", "48")]["shadow_decision"] == "REGIME_SHADOW"


def test_alt_impulse_shadow_splits_outcomes_by_regime(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data" / "cache"
    cfg = _cfg()
    cfg.diagnostics.extended_label_horizons_hours = [24]

    trending_ts_ms = _ts_ms("2026-04-21T14:00:00Z")
    sideways_ts_ms = _ts_ms("2026-04-23T14:00:00Z")

    update_alt_impulse_shadow_evaluator(
        run_dir=tmp_path / "reports" / "runs" / "r_trending",
        audit=_audit("r_trending", trending_ts_ms, _eth_trend_only_explain(regime="Trending"), regime="Trending"),
        market_data_1h={
            "BTC/USDT": _series("BTC/USDT", [trending_ts_ms - 4 * 3600 * 1000, trending_ts_ms], [100.0, 101.0]),
            "ETH/USDT": _series("ETH/USDT", [trending_ts_ms - 4 * 3600 * 1000, trending_ts_ms], [98.0, 100.0]),
            "SOL/USDT": _series("SOL/USDT", [trending_ts_ms - 4 * 3600 * 1000, trending_ts_ms], [50.0, 51.0]),
        },
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
        ohlcv_provider=None,
    )
    update_alt_impulse_shadow_evaluator(
        run_dir=tmp_path / "reports" / "runs" / "r_sideways",
        audit=_audit("r_sideways", sideways_ts_ms, _eth_trend_only_explain(regime="Sideways", volatility_bucket="high"), regime="Sideways"),
        market_data_1h={
            "BTC/USDT": _series("BTC/USDT", [sideways_ts_ms - 4 * 3600 * 1000, sideways_ts_ms], [101.0, 102.0]),
            "ETH/USDT": _series("ETH/USDT", [sideways_ts_ms - 4 * 3600 * 1000, sideways_ts_ms], [196.0, 200.0]),
            "SOL/USDT": _series("SOL/USDT", [sideways_ts_ms - 4 * 3600 * 1000, sideways_ts_ms], [51.0, 52.0]),
        },
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
        ohlcv_provider=None,
    )

    _write_cache_csv(
        cache_dir,
        "ETH/USDT",
        [
            ("2026-04-21T14:00:00Z", 100.0),
            ("2026-04-22T14:00:00Z", 102.0),
            ("2026-04-23T14:00:00Z", 200.0),
            ("2026-04-24T14:00:00Z", 198.0),
        ],
    )
    result = update_alt_impulse_shadow_evaluator(
        run_dir=tmp_path / "reports" / "runs" / "r_mature",
        audit=_audit("r_mature", sideways_ts_ms + 30 * 3600 * 1000, [], regime="Sideways"),
        market_data_1h={},
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=cache_dir,
        ohlcv_provider=None,
    )

    assert result["total_records"] == 2
    by_regime_path = tmp_path / "reports" / "summaries" / "alt_impulse_shadow_by_regime.csv"
    with by_regime_path.open("r", encoding="utf-8") as handle:
        by_regime = {row["regime_state"]: row for row in csv.DictReader(handle)}
    assert by_regime["Trending"]["avg_24h_net_bps"] == "170.0"
    assert by_regime["Trending"]["win_rate_24h"] == "1.0"
    assert by_regime["Sideways"]["avg_24h_net_bps"] == "-130.0"
    assert by_regime["Sideways"]["win_rate_24h"] == "0.0"
    assert {row["shadow_decision"] for row in by_regime.values()} == {"REGIME_SHADOW"}
    assert {row["alpha_discovery_board_status"] for row in by_regime.values()} == {"REGIME_SHADOW"}
    assert {row["paper_ready_allowed"] for row in by_regime.values()} == {"False"}
    assert {row["live_ready_allowed"] for row in by_regime.values()} == {"False"}

    outcomes_path = tmp_path / "reports" / "summaries" / "alt_impulse_shadow_outcomes.csv"
    with outcomes_path.open("r", encoding="utf-8") as handle:
        outcomes = list(csv.DictReader(handle))
    assert {row["btc_trend_state"] for row in outcomes} == {"positive_4h"}
    assert {row["risk_level"] for row in outcomes} == {"PROTECT"}
    assert {row["broad_market_positive_count"] for row in outcomes} == {"3"}
    assert {row["volatility_bucket"] for row in outcomes} == {"medium", "high"}


def test_alt_impulse_shadow_missing_entry_keeps_global_reason_when_all_not_observable(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260422_02"
    run_dir.mkdir(parents=True, exist_ok=True)
    labels_path = tmp_path / "reports" / "alt_impulse_shadow_labels.jsonl"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    entry_ts_ms = _ts_ms("2026-04-21T14:00:00Z")
    labels_path.write_text(
        json.dumps(
            {
                "ts_utc": "2026-04-21T14:00:00Z",
                "entry_ts_ms": entry_ts_ms,
                "run_id": "20260421_14",
                "symbol": "ETH/USDT",
                "skip_reason": "protect_entry_trend_only",
                "rt_cost_bps": 30.0,
                "label_status": "pending",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    cfg = _cfg()
    cfg.diagnostics.extended_label_horizons_hours = [4, 8]

    update_alt_impulse_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260422_02", entry_ts_ms + 12 * 3600 * 1000, []),
        market_data_1h={},
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=tmp_path / "data" / "cache",
        ohlcv_provider=None,
    )

    rows = [json.loads(line) for line in labels_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["label_status"] == "not_observable"
    assert rows[0]["label_not_observable_reason"] == "missing_entry_px"
    assert rows[0]["label_4h_status"] == "not_observable"
    assert rows[0]["label_4h_reason"] == "missing_entry_px"


def test_alt_impulse_shadow_disabled_writes_no_files(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260421_14"
    run_dir.mkdir(parents=True, exist_ok=True)
    entry_ts_ms = _ts_ms("2026-04-21T14:00:00Z")
    cfg = _cfg()
    cfg.diagnostics.alt_impulse_shadow_enabled = False

    result = update_alt_impulse_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("20260421_14", entry_ts_ms, _eth_trend_only_explain()),
        market_data_1h={},
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=tmp_path / "data" / "cache",
        ohlcv_provider=None,
    )

    assert result["enabled"] is False
    assert not (tmp_path / "reports" / "alt_impulse_shadow_labels.jsonl").exists()
    assert not (tmp_path / "reports" / "summaries" / "alt_impulse_shadow_outcomes.csv").exists()
