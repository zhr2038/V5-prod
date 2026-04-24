from __future__ import annotations

from pathlib import Path

import pytest

from src.core.models import MarketSeries
import src.research.window_diagnostics as wd


def test_run_window_diagnostic_task_finalizes_failed_run_when_no_evaluations(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

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
            finalized["finalize_calls"] = int(finalized.get("finalize_calls", 0)) + 1
            finalized["status"] = status
            finalized["summary"] = summary
            return run.run_dir / "meta.json"

    monkeypatch.setattr(
        wd,
        "load_task_config",
        lambda path: {
            "task": {"name": "window_diagnostics"},
            "paths": {},
            "experiment": {"symbols": ["BTC/USDT"], "evaluations": []},
        },
    )
    monkeypatch.setattr(wd, "ResearchRecorder", FakeRecorder)

    with pytest.raises(ValueError, match="requires at least one evaluation"):
        wd.run_window_diagnostic_task(project_root=tmp_path, task_config_path="task.yaml")

    assert finalized["task_name"] == "window_diagnostics"
    assert finalized["finalize_calls"] == 1
    assert finalized["status"] == "failed"
    assert finalized["summary"] == {
        "reason": "window_diagnostics_failed",
        "error_type": "ValueError",
        "error": "window diagnostics requires at least one evaluation",
    }


def test_run_window_diagnostic_task_finalizes_failed_run_when_job_raises(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

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
            finalized["finalize_calls"] = int(finalized.get("finalize_calls", 0)) + 1
            finalized["status"] = status
            finalized["summary"] = summary
            return run.run_dir / "meta.json"

    monkeypatch.setattr(
        wd,
        "load_task_config",
        lambda path: {
            "task": {"name": "window_diagnostics"},
            "paths": {},
            "experiment": {
                "symbols": ["BTC/USDT"],
                "evaluations": [{"name": "window_1", "ohlcv_limit": 24, "window_shift_bars": 0}],
                "workers": 1,
            },
        },
    )
    monkeypatch.setattr(wd, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(wd, "_run_window_job", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("window task failed")))

    with pytest.raises(RuntimeError, match="window task failed"):
        wd.run_window_diagnostic_task(project_root=tmp_path, task_config_path="task.yaml")

    assert finalized["task_name"] == "window_diagnostics"
    assert finalized["finalize_calls"] == 1
    assert finalized["status"] == "failed"
    assert finalized["summary"] == {
        "reason": "window_diagnostics_failed",
        "error_type": "RuntimeError",
        "error": "window task failed",
    }


def test_run_window_diagnostic_uses_min_max_timestamps_when_series_is_unsorted(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, cfg, clock=None, data_provider=None):
            self.clock = clock

        def run(self, market_data, **kwargs):
            audit = kwargs["audit"]
            if "window_end_ts" not in captured:
                captured["clock_ts_ms"] = int(self.clock.now().timestamp() * 1000)
                captured["window_start_ts"] = audit.window_start_ts
                captured["window_end_ts"] = audit.window_end_ts
            return type(
                "Out",
                (),
                {
                    "regime": type("Regime", (), {"state": "SIDEWAYS", "multiplier": 1.0, "atr_pct": 0.0, "ma20": 0.0, "ma60": 0.0})(),
                    "orders": [],
                },
            )()

    monkeypatch.setattr(wd, "make_cost_model_from_cfg", lambda cfg: (None, {}))
    monkeypatch.setattr(wd, "V5Pipeline", FakePipeline)
    monkeypatch.setattr(wd, "RunLogger", lambda *args, **kwargs: object())

    cfg = type(
        "Cfg",
        (),
        {
            "backtest": type("Backtest", (), {"initial_equity_usdt": 100.0, "fee_bps": 0.0, "slippage_bps": 0.0})(),
        },
    )()
    base_ts = 1_710_000_000_000
    unsorted_ts = [base_ts + 3_600_000 * i for i in range(80)]
    unsorted_ts[0], unsorted_ts[60] = unsorted_ts[60], unsorted_ts[0]
    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=unsorted_ts,
            open=[100.0 + i for i in range(80)],
            high=[101.0 + i for i in range(80)],
            low=[99.0 + i for i in range(80)],
            close=[100.5 + i for i in range(80)],
            volume=[1.0 for _ in range(80)],
        )
    }

    result = wd.run_window_diagnostic(
        market_data=market_data,
        cfg=cfg,
        window_name="window_1",
        output_dir=tmp_path / "out",
    )

    assert result["status"] == "completed"
    assert captured["window_start_ts"] == base_ts
    assert captured["window_end_ts"] == base_ts + 60 * 3_600_000
    assert captured["clock_ts_ms"] == base_ts + 60 * 3_600_000
