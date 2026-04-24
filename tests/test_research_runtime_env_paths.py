from __future__ import annotations

from pathlib import Path

from configs.schema import AppConfig
import src.research.latest_signal_monitor as latest_signal_monitor
import src.research.task_runner as task_runner
import src.research.walk_forward_optimizer as walk_forward_optimizer
import src.research.window_diagnostics as window_diagnostics


def test_window_diagnostics_resolves_env_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)

        def write_json(self, relative_path: str, payload):
            return self.run_dir / relative_path

        def write_text(self, relative_path: str, content: str):
            return self.run_dir / relative_path

    class FakeRecorder:
        def __init__(self, *args, **kwargs):
            pass

        def start_run(self, **kwargs):
            return FakeRun()

        def finalize_run(self, run, *, status: str, summary):
            return run.run_dir / "meta.json"

    monkeypatch.setattr(
        window_diagnostics,
        "load_task_config",
        lambda path: {
            "task": {"name": "window_diagnostics"},
            "paths": {},
            "experiment": {
                "symbols": ["BTC/USDT"],
                "evaluations": [{"name": "window_1", "ohlcv_limit": 24, "window_shift_bars": 0}],
                "env_path": ".env.runtime",
                "workers": 1,
            },
        },
    )
    monkeypatch.setattr(window_diagnostics, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(
        window_diagnostics,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )
    monkeypatch.setattr(
        window_diagnostics,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str((tmp_path / ".env.runtime").resolve()),
    )
    monkeypatch.setattr(
        window_diagnostics,
        "_run_window_job",
        lambda **kwargs: captured.update(kwargs) or {"name": "window_1", "ok": True},
    )

    window_diagnostics.run_window_diagnostic_task(project_root=tmp_path, task_config_path="task.yaml")

    assert captured["base_config_path"] == str((tmp_path / "configs" / "runtime.yaml").resolve())
    assert captured["env_path"] == str((tmp_path / ".env.runtime").resolve())


def test_latest_signal_monitor_resolves_env_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    monkeypatch.setattr(
        latest_signal_monitor,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str((tmp_path / ".env.runtime").resolve()),
    )
    monkeypatch.setattr(
        latest_signal_monitor,
        "_load_base_config_cached",
        lambda base_config_path, env_path: captured.update({"base_config_path": base_config_path, "env_path": env_path}) or type(
            "Cfg", (), {"timeframe_main": "1h", "backtest": type("B", (), {"initial_equity_usdt": 100.0})(), "execution": type("E", (), {"collect_ml_training_data": False})()}
        )(),
    )
    monkeypatch.setattr(latest_signal_monitor, "build_baseline_config", lambda base_cfg, project_root, research_symbols: base_cfg)
    monkeypatch.setattr(latest_signal_monitor, "_apply_overrides", lambda cfg, overrides: None)
    monkeypatch.setattr(latest_signal_monitor, "load_cached_market_data", lambda cache_dir, symbols, timeframe, limit: {"BTC/USDT": type("S", (), {"ts": [1, 2], "close": [1.0, 2.0]})()})
    monkeypatch.setattr(latest_signal_monitor, "seed_sandbox_read_only_artifacts", lambda *args, **kwargs: None)
    monkeypatch.setattr(latest_signal_monitor, "_sandbox_reports_dir", lambda output_dir: __import__("contextlib").nullcontext(output_dir))
    monkeypatch.setattr(latest_signal_monitor, "sandbox_working_directory", lambda output_dir: __import__("contextlib").nullcontext(output_dir))
    monkeypatch.setattr(latest_signal_monitor, "V5Pipeline", lambda cfg, clock=None, data_provider=None: type("P", (), {"run": lambda self, *a, **k: type("O", (), {"regime": type("R", (), {"state": "SIDEWAYS", "multiplier": 1.0, "atr_pct": 0.0, "ma20": 0.0, "ma60": 0.0})(), "portfolio": type("Port", (), {"selected": [], "entry_candidates": [], "target_weights": {}})(), "orders": []})()})())
    monkeypatch.setattr(latest_signal_monitor, "DecisionAudit", lambda **kwargs: type("A", (), {"top_scores": [], "counts": {}, "rejects": {}, "router_decisions": [], "notes": []})())
    monkeypatch.setattr(latest_signal_monitor, "RunLogger", lambda run_dir: object())

    latest_signal_monitor.run_latest_signal_variant(
        variant={"name": "baseline", "symbols": ["BTC/USDT"], "overrides": {}},
        base_config_path=str(tmp_path / "configs" / "runtime.yaml"),
        env_path=".env.runtime",
        cache_dir=str(tmp_path / "data" / "cache"),
        project_root=tmp_path,
        output_dir=tmp_path / "out",
        ohlcv_limit=10,
        initial_equity_usdt=100.0,
    )

    assert captured["env_path"] == str((tmp_path / ".env.runtime").resolve())


def test_latest_signal_monitor_uses_min_max_series_timestamps_when_ts_is_unsorted(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    monkeypatch.setattr(
        latest_signal_monitor,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str((tmp_path / ".env.runtime").resolve()),
    )
    monkeypatch.setattr(
        latest_signal_monitor,
        "_load_base_config_cached",
        lambda base_config_path, env_path: type(
            "Cfg", (), {"timeframe_main": "1h", "backtest": type("B", (), {"initial_equity_usdt": 100.0})(), "execution": type("E", (), {"collect_ml_training_data": False})()}
        )(),
    )
    monkeypatch.setattr(latest_signal_monitor, "build_baseline_config", lambda base_cfg, project_root, research_symbols: base_cfg)
    monkeypatch.setattr(latest_signal_monitor, "_apply_overrides", lambda cfg, overrides: None)
    monkeypatch.setattr(
        latest_signal_monitor,
        "load_cached_market_data",
        lambda cache_dir, symbols, timeframe, limit: {
            "BTC/USDT": type("S", (), {"ts": [2_000, 1_000], "close": [1.0, 2.0]})()
        },
    )
    monkeypatch.setattr(latest_signal_monitor, "seed_sandbox_read_only_artifacts", lambda *args, **kwargs: None)
    monkeypatch.setattr(latest_signal_monitor, "_sandbox_reports_dir", lambda output_dir: __import__("contextlib").nullcontext(output_dir))
    monkeypatch.setattr(latest_signal_monitor, "sandbox_working_directory", lambda output_dir: __import__("contextlib").nullcontext(output_dir))
    monkeypatch.setattr(latest_signal_monitor, "V5Pipeline", lambda cfg, clock=None, data_provider=None: type("P", (), {"run": lambda self, *a, **k: type("O", (), {"regime": type("R", (), {"state": "SIDEWAYS", "multiplier": 1.0, "atr_pct": 0.0, "ma20": 0.0, "ma60": 0.0})(), "portfolio": type("Port", (), {"selected": [], "entry_candidates": [], "target_weights": {}})(), "orders": []})()})())
    monkeypatch.setattr(
        latest_signal_monitor,
        "DecisionAudit",
        lambda **kwargs: captured.update(kwargs) or type("A", (), {"top_scores": [], "counts": {}, "rejects": {}, "router_decisions": [], "notes": []})(),
    )
    monkeypatch.setattr(latest_signal_monitor, "RunLogger", lambda run_dir: object())

    latest_signal_monitor.run_latest_signal_variant(
        variant={"name": "baseline", "symbols": ["BTC/USDT"], "overrides": {}},
        base_config_path=str(tmp_path / "configs" / "runtime.yaml"),
        env_path=".env.runtime",
        cache_dir=str(tmp_path / "data" / "cache"),
        project_root=tmp_path,
        output_dir=tmp_path / "out",
        ohlcv_limit=10,
        initial_equity_usdt=100.0,
    )

    assert captured["window_start_ts"] == 1_000
    assert captured["window_end_ts"] == 2_000


def test_walk_forward_optimizer_resolves_env_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    monkeypatch.setattr(
        walk_forward_optimizer,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )
    monkeypatch.setattr(
        walk_forward_optimizer,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str((tmp_path / ".env.runtime").resolve()),
    )
    monkeypatch.setattr(
        walk_forward_optimizer,
        "load_config",
        lambda config_path, env_path=None: captured.update({"config_path": config_path, "env_path": env_path}) or AppConfig(symbols=["BTC/USDT"]),
    )
    monkeypatch.setattr(walk_forward_optimizer, "MockProvider", lambda seed=7: type("P", (), {"fetch_ohlcv": lambda self, symbols, timeframe, limit: {}})())

    walk_forward_optimizer._load_market_data_for_task(
        project_root=tmp_path,
        task_config={"walk_forward": {"env_path": ".env.runtime", "provider": "mock"}},
    )

    assert captured["config_path"] == str((tmp_path / "configs" / "runtime.yaml").resolve())
    assert captured["env_path"] == str((tmp_path / ".env.runtime").resolve())


def test_task_runner_walk_forward_resolves_env_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    import configs.loader as loader_mod
    import src.backtest.walk_forward as wf_mod
    import src.data.mock_provider as mock_provider_mod
    import src.research.cache_loader as cache_loader_mod

    captured = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)
            self.run_id = "r1"

        def write_json(self, *args, **kwargs):
            return self.run_dir / "x.json"

    class FakeRecorder:
        def __init__(self, *args, **kwargs):
            pass

        def start_run(self, **kwargs):
            return FakeRun()

        def finalize_run(self, run, status, summary):
            return run.run_dir / "meta.json"

    monkeypatch.setattr(
        task_runner,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )
    monkeypatch.setattr(
        task_runner,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str((tmp_path / ".env.runtime").resolve()),
    )
    monkeypatch.setattr(
        loader_mod,
        "load_config",
        lambda config_path, env_path=None: captured.update({"config_path": config_path, "env_path": env_path}) or AppConfig(symbols=["BTC/USDT"]),
    )
    monkeypatch.setattr(mock_provider_mod, "MockProvider", lambda seed=7: type("P", (), {"fetch_ohlcv": lambda self, symbols, timeframe, limit: {}})())
    monkeypatch.setattr(task_runner, "ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(wf_mod, "run_walk_forward", lambda *args, **kwargs: {})
    monkeypatch.setattr(wf_mod, "build_walk_forward_report", lambda *args, **kwargs: {"summary": args[0] if args else {}})
    monkeypatch.setattr(wf_mod, "build_portfolio_analysis_record", lambda *args, **kwargs: {"portfolio": args[0] if args else {}})
    monkeypatch.setattr(cache_loader_mod, "summarize_market_data", lambda market_data, source=None, source_path=None: {"source": source})

    task_runner.run_walk_forward_task(
        project_root=tmp_path,
        task_config={"task": {"name": "walk"}, "paths": {}, "walk_forward": {"env_path": ".env.runtime", "provider": "mock"}},
    )

    assert captured["config_path"] == str((tmp_path / "configs" / "runtime.yaml").resolve())
    assert captured["env_path"] == str((tmp_path / ".env.runtime").resolve())
