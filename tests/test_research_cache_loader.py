from __future__ import annotations

import json

import pytest

from src.research.cache_loader import load_cached_market_data, summarize_market_data
from src.research.task_runner import run_walk_forward_task


def test_load_cached_market_data_aligns_symbols_and_applies_limit(tmp_path) -> None:
    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True)

    (cache_dir / "BTC_USDT_1H_2026-01-01_2026-01-02.csv").write_text(
        "\n".join(
            [
                "timestamp,open,high,low,close,volume,symbol",
                "2026-01-01 00:00:00,100,101,99,100,10,BTC/USDT",
                "2026-01-01 01:00:00,101,102,100,101,11,BTC/USDT",
                "2026-01-01 02:00:00,102,103,101,102,12,BTC/USDT",
                "2026-01-01 03:00:00,103,104,102,103,13,BTC/USDT",
            ]
        ),
        encoding="utf-8",
    )
    (cache_dir / "ETH_USDT_1H_2026-01-01_2026-01-02.csv").write_text(
        "\n".join(
            [
                "timestamp,open,high,low,close,volume,symbol",
                "2026-01-01 01:00:00,200,201,199,200,20,ETH/USDT",
                "2026-01-01 02:00:00,201,202,200,201,21,ETH/USDT",
                "2026-01-01 03:00:00,202,203,201,202,22,ETH/USDT",
            ]
        ),
        encoding="utf-8",
    )

    market_data = load_cached_market_data(cache_dir, ["BTC/USDT", "ETH/USDT"], "1h", limit=2)
    summary = summarize_market_data(market_data, source="cache", source_path=str(cache_dir))

    assert sorted(market_data) == ["BTC/USDT", "ETH/USDT"]
    assert market_data["BTC/USDT"].close == [102.0, 103.0]
    assert market_data["ETH/USDT"].close == [201.0, 202.0]
    assert all(ts > 1_000_000_000_000 for ts in market_data["BTC/USDT"].ts)
    assert summary["bars"] == 2
    assert summary["source"] == "cache"
    assert summary["time_range"]["start_iso"] == "2026-01-01T02:00:00+00:00"


def test_load_cached_market_data_prefers_logically_newer_cache_file_for_duplicate_timestamp(tmp_path) -> None:
    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True)

    # Lexicographically, the daily file sorts after the range file, but the range file is logically newer.
    (cache_dir / "BTC_USDT_1H_20260101.csv").write_text(
        "\n".join(
            [
                "timestamp,open,high,low,close,volume,symbol",
                "2026-01-01 00:00:00,100,101,99,100,10,BTC/USDT",
                "2026-01-01 01:00:00,101,102,100,101,11,BTC/USDT",
            ]
        ),
        encoding="utf-8",
    )
    (cache_dir / "BTC_USDT_1H_2026-01-01_2026-01-02.csv").write_text(
        "\n".join(
            [
                "timestamp,open,high,low,close,volume,symbol",
                "2026-01-01 01:00:00,101,102,100,999,11,BTC/USDT",
                "2026-01-01 02:00:00,102,103,101,103,12,BTC/USDT",
            ]
        ),
        encoding="utf-8",
    )

    market_data = load_cached_market_data(cache_dir, ["BTC/USDT"], "1h")

    assert market_data["BTC/USDT"].close == [100.0, 999.0, 103.0]


def test_run_walk_forward_task_supports_cache_provider(monkeypatch, tmp_path) -> None:
    project_root = tmp_path
    cache_dir = project_root / "data" / "cache"
    cache_dir.mkdir(parents=True)
    config_path = project_root / "configs" / "live_prod.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        "\n".join(
            [
                "symbols:",
                "  - BTC/USDT",
                "  - ETH/USDT",
                "timeframe_main: 1h",
                "timeframe_aux: 4h",
            ]
        ),
        encoding="utf-8",
    )

    for symbol, base in [("BTC", 100), ("ETH", 200)]:
        (cache_dir / f"{symbol}_USDT_1H_2026-01-01_2026-01-02.csv").write_text(
            "\n".join(
                [
                    "timestamp,open,high,low,close,volume,symbol",
                    f"2026-01-01 00:00:00,{base},{base+1},{base-1},{base},10,{symbol}/USDT",
                    f"2026-01-01 01:00:00,{base+1},{base+2},{base},{base+1},11,{symbol}/USDT",
                    f"2026-01-01 02:00:00,{base+2},{base+3},{base+1},{base+2},12,{symbol}/USDT",
                ]
            ),
            encoding="utf-8",
        )

    captured: dict[str, object] = {}

    def _fake_run_walk_forward(market_data, folds, cfg, data_provider=None):
        captured["symbols"] = sorted(market_data)
        captured["bars"] = min(len(series.close) for series in market_data.values())
        captured["provider"] = data_provider
        return []

    monkeypatch.setattr("src.backtest.walk_forward.run_walk_forward", _fake_run_walk_forward)

    task_config = {
        "task": {"name": "walk_forward_prod_cache"},
        "paths": {
            "runs_dir": "reports/runs",
            "output_report_path": "reports/research/walk_forward/prod_cache_live_prod.json",
        },
        "walk_forward": {
            "config_path": "configs/live_prod.yaml",
            "provider": "cache",
            "cache_dir": "data/cache",
            "collect_ml_training_data": False,
            "ohlcv_limit": 2,
            "folds": 1,
        },
    }

    result = run_walk_forward_task(project_root=project_root, task_config=task_config)
    output_path = project_root / "reports" / "research" / "walk_forward" / "prod_cache_live_prod.json"
    output = json.loads(output_path.read_text(encoding="utf-8"))

    assert result["exit_code"] == 0
    assert captured == {"symbols": ["BTC/USDT", "ETH/USDT"], "bars": 2, "provider": None}
    assert output["cost_assumption_meta"]["provider"] == "cache"


def test_run_walk_forward_task_finalizes_failed_run(monkeypatch, tmp_path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)
            self.run_id = "run_123"

        def write_json(self, relative_path: str, payload):
            finalized.setdefault("writes", []).append((relative_path, payload))
            return self.run_dir / relative_path

    class FakeRecorder:
        def __init__(self, *args, **kwargs):
            pass

        def start_run(self, **kwargs):
            finalized["task_name"] = kwargs["task_name"]
            return FakeRun()

        def finalize_run(self, run, *, status: str, summary):
            finalized["status"] = status
            finalized["summary"] = summary
            return run.run_dir / "meta.json"

    monkeypatch.setattr("src.research.task_runner.ResearchRecorder", FakeRecorder)
    monkeypatch.setattr("configs.loader.load_config", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("walk data failed")))

    with pytest.raises(RuntimeError, match="walk data failed"):
        run_walk_forward_task(
            project_root=tmp_path,
            task_config={
                "task": {"name": "walk_forward"},
                "paths": {},
                "walk_forward": {"provider": "mock"},
            },
        )

    assert finalized["task_name"] == "walk_forward"
    assert finalized["status"] == "failed"
    assert finalized["summary"] == {
        "reason": "walk_forward_failed",
        "error_type": "RuntimeError",
        "error": "walk data failed",
    }
