from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from src.research.recorder import (
    _run_id_timestamp,
    find_latest_task_run,
    load_latest_task_record,
)


def test_load_latest_task_record_prefers_run_id_timestamp_when_dir_mtime_is_misleading(tmp_path) -> None:
    base_dir = tmp_path / "reports" / "runs"
    older_run = base_dir / "research_ml_training_20260421_210000_000001"
    newer_run = base_dir / "research_ml_training_20260421_220000_000001"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    older_meta = {"run_id": older_run.name, "task_name": "ml_training", "status": "running"}
    newer_meta = {"run_id": newer_run.name, "task_name": "ml_training", "status": "running"}
    (older_run / "meta.json").write_text(json.dumps(older_meta, ensure_ascii=False), encoding="utf-8")
    (newer_run / "meta.json").write_text(json.dumps(newer_meta, ensure_ascii=False), encoding="utf-8")
    (older_run / "metrics.json").write_text(json.dumps({"run_id": older_run.name, "valid_ic": 0.01}), encoding="utf-8")
    (newer_run / "metrics.json").write_text(json.dumps({"run_id": newer_run.name, "valid_ic": 0.12}), encoding="utf-8")

    os.utime(older_run, (2_000_000_000, 2_000_000_000))
    os.utime(newer_run, (1_000_000_000, 1_000_000_000))

    latest = load_latest_task_record("ml_training", "metrics.json", base_dir=base_dir)

    assert latest is not None
    assert latest["run_id"] == newer_run.name
    assert latest["valid_ic"] == 0.12


def test_find_latest_task_run_uses_utc_run_id_timestamp_when_meta_has_no_times(tmp_path) -> None:
    base_dir = tmp_path / "reports" / "runs"
    run_dir = base_dir / "research_ml_training_20260421_220000_000001"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "meta.json").write_text(
        json.dumps({"run_id": run_dir.name, "task_name": "ml_training"}),
        encoding="utf-8",
    )

    latest = find_latest_task_run("ml_training", base_dir=base_dir)

    assert latest == run_dir


def test_run_id_timestamp_uses_utc() -> None:
    assert _run_id_timestamp("research_ml_training_20260421_220000_000001") == datetime(
        2026,
        4,
        21,
        22,
        0,
        0,
        1,
        tzinfo=timezone.utc,
    ).timestamp()
