from __future__ import annotations

import json
import sys
from pathlib import Path

import scripts.backfill_ml_cache_snapshots as backfill_ml_cache_snapshots


def test_backfill_ml_cache_snapshots_uses_runtime_defaults_from_active_config(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured = {}
    fake_root = tmp_path / "repo"
    runtime_reports = fake_root / "reports" / "shadow_tuned_xgboost"
    runtime_reports.mkdir(parents=True, exist_ok=True)
    (fake_root / "configs").mkdir(parents=True, exist_ok=True)
    (fake_root / "configs" / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_tuned_xgboost/orders.sqlite",
                "  ml_research_universe_path: reports/shadow_tuned_xgboost/universe_cache.json",
            ]
        ),
        encoding="utf-8",
    )
    (runtime_reports / "universe_cache.json").write_text(
        json.dumps({"symbols": ["BTC/USDT", "ETH/USDT"]}),
        encoding="utf-8",
    )

    class DummyCollector:
        def __init__(self, *, db_path: str) -> None:
            captured["db_path"] = Path(db_path).resolve()

        def backfill_feature_snapshots_from_cache(self, **kwargs):
            captured["backfill_kwargs"] = kwargs
            return {"inserted": 2, "updated": 0}

        def fill_all_labels(self, end_ms: int, max_batches: int):
            captured["fill_args"] = {"end_ms": end_ms, "max_batches": max_batches}
            return {"filled": 2, "batches": 1}

        def export_training_data(self, csv_path: str, min_samples: int):
            captured["csv_path"] = Path(csv_path).resolve()
            captured["min_samples"] = min_samples
            return True

        def get_statistics(self):
            return {"rows": 2}

    monkeypatch.setattr(backfill_ml_cache_snapshots, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(backfill_ml_cache_snapshots, "MLDataCollector", DummyCollector)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_ml_cache_snapshots.py",
            "--start",
            "2026-04-01T00:00:00Z",
            "--end",
            "2026-04-01T02:00:00Z",
        ],
    )

    backfill_ml_cache_snapshots.main()

    assert captured["db_path"] == (runtime_reports / "ml_training_data.db").resolve()
    assert captured["csv_path"] == (runtime_reports / "ml_training_data.csv").resolve()
    assert captured["backfill_kwargs"]["symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert not (tmp_path / "reports" / "ml_training_data.db").exists()


def test_backfill_ml_cache_snapshots_explicit_paths_override_runtime_defaults(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured = {}
    fake_root = tmp_path / "repo"
    runtime_reports = fake_root / "reports" / "shadow_tuned_xgboost"
    runtime_reports.mkdir(parents=True, exist_ok=True)
    (fake_root / "configs").mkdir(parents=True, exist_ok=True)
    (fake_root / "configs" / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_tuned_xgboost/orders.sqlite",
                "  ml_research_universe_path: reports/shadow_tuned_xgboost/universe_cache.json",
            ]
        ),
        encoding="utf-8",
    )

    explicit_db = tmp_path / "custom" / "training.sqlite"
    explicit_csv = tmp_path / "custom" / "training.csv"
    explicit_universe = tmp_path / "custom" / "universe.json"
    explicit_universe.parent.mkdir(parents=True, exist_ok=True)
    explicit_universe.write_text(json.dumps({"symbols": ["SOL/USDT"]}), encoding="utf-8")

    class DummyCollector:
        def __init__(self, *, db_path: str) -> None:
            captured["db_path"] = Path(db_path).resolve()

        def backfill_feature_snapshots_from_cache(self, **kwargs):
            captured["backfill_kwargs"] = kwargs
            return {"inserted": 1, "updated": 0}

        def fill_all_labels(self, end_ms: int, max_batches: int):
            return {"filled": 1, "batches": 1}

        def export_training_data(self, csv_path: str, min_samples: int):
            captured["csv_path"] = Path(csv_path).resolve()
            return True

        def get_statistics(self):
            return {"rows": 1}

    monkeypatch.setattr(backfill_ml_cache_snapshots, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(backfill_ml_cache_snapshots, "MLDataCollector", DummyCollector)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_ml_cache_snapshots.py",
            "--db-path",
            str(explicit_db),
            "--csv-path",
            str(explicit_csv),
            "--universe-path",
            str(explicit_universe),
            "--start",
            "2026-04-01T00:00:00Z",
            "--end",
            "2026-04-01T02:00:00Z",
        ],
    )

    backfill_ml_cache_snapshots.main()

    assert captured["db_path"] == explicit_db.resolve()
    assert captured["csv_path"] == explicit_csv.resolve()
    assert captured["backfill_kwargs"]["symbols"] == ["SOL/USDT"]
