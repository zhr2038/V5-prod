from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import scripts.equity_anomaly_detector as detector


def test_build_paths_uses_runtime_order_store(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("execution:\n  order_store_path: reports/shadow_runtime/orders.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(
        detector,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(
        detector,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = detector.build_paths(tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_runtime" / "orders.sqlite").resolve()
    assert paths.reports_dir == (tmp_path / "reports" / "shadow_runtime").resolve()
    assert paths.runs_dir == (tmp_path / "reports" / "shadow_runtime" / "runs").resolve()


def test_build_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    config_path = (tmp_path / "configs" / "live_prod.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(detector, "load_runtime_config", lambda project_root=None: {})

    with pytest.raises(ValueError, match="live_prod.yaml"):
        detector.build_paths(tmp_path)


def test_build_paths_fails_fast_when_runtime_config_is_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        detector.build_paths(tmp_path)


def test_load_equity_data_limits_recent_equity_file_reads_before_parsing(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    paths = detector.DetectorPaths(
        workspace=tmp_path,
        reports_dir=reports_dir,
        runs_dir=runs_dir,
        orders_db=reports_dir / "orders.sqlite",
    )
    monkeypatch.setattr(detector, "build_paths", lambda workspace=None: paths)

    now = datetime.now()
    recent_hours = {19, 18, 17, 16}
    for hour in range(20):
        day_offset = 0 if hour in recent_hours else 10
        run_dt = now - timedelta(days=day_offset, hours=19 - hour)
        run_name = run_dt.strftime("%Y%m%d_%H")
        run_dir = runs_dir / run_name
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "equity.jsonl").write_text(
            json.dumps(
                {
                    "ts": (run_dt + timedelta(minutes=30)).isoformat(),
                    "equity": 100.0 + hour,
                    "cash": 10.0,
                    "positions_value": 90.0 + hour,
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )

    original_read_text = detector.Path.read_text
    reads = {"equity": 0}

    def counting_read_text(self, *args, **kwargs):
        if self.name == "equity.jsonl":
            reads["equity"] += 1
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(detector.Path, "read_text", counting_read_text)

    eq_detector = detector.EquityAnomalyDetector(workspace=tmp_path)
    points = eq_detector.load_equity_data(days=1)

    assert len(points) == 4
    assert reads["equity"] <= 4
