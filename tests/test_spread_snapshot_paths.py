from __future__ import annotations

import json
import sys
from pathlib import Path

import scripts.rollup_spreads as rollup_spreads
from src.reporting import spread_snapshot_store, spread_snapshots


def test_append_spread_snapshot_defaults_to_repo_reports_dir(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    monkeypatch.setattr(spread_snapshots, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)

    path = spread_snapshots.append_spread_snapshot(
        {
            "window_end_ts": 1700000000,
            "symbols": [{"symbol": "BTC/USDT", "bid": 99.0, "ask": 101.0, "mid": 100.0, "spread_bps": 200.0}],
        }
    )

    assert path == (fake_root / "reports" / "spread_snapshots" / "20231114.jsonl").resolve()
    assert path.exists()
    assert not (tmp_path / "reports" / "spread_snapshots" / "20231114.jsonl").exists()


def test_spread_snapshot_store_defaults_to_repo_reports_dir(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    snapshots_dir = fake_root / "reports" / "spread_snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    (snapshots_dir / "20231114.jsonl").write_text(
        json.dumps(
            {
                "window_end_ts": 1700000000,
                "symbols": [{"symbol": "BTC/USDT", "bid": 99.0, "ask": 101.0, "mid": 100.0, "spread_bps": 200.0}],
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(spread_snapshot_store, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)

    store = spread_snapshot_store.SpreadSnapshotStore()
    snap = store.get_latest_before(symbol="BTC/USDT", ts_ms=1700000000 * 1000)

    assert snap is not None
    assert snap.mid == 100.0
    assert store.base_dir == (fake_root / "reports" / "spread_snapshots").resolve()
    assert not (tmp_path / "reports" / "spread_snapshots" / "20231114.jsonl").exists()


def test_rollup_spreads_main_defaults_to_repo_root(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    snapshots_dir = fake_root / "reports" / "spread_snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    day = "20231114"
    (snapshots_dir / f"{day}.jsonl").write_text(
        json.dumps(
            {
                "window_end_ts": 1700000000,
                "symbols": [{"symbol": "BTC/USDT", "bid": 99.0, "ask": 101.0, "mid": 100.0, "spread_bps": 200.0}],
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(rollup_spreads, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["rollup_spreads.py", "--day", day])

    rollup_spreads.main()

    out_path = fake_root / "reports" / "spread_stats" / f"daily_spread_stats_{day}.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["symbols"]["BTC/USDT"]["count"] == 1
    assert not (tmp_path / "reports" / "spread_stats" / f"daily_spread_stats_{day}.json").exists()


def test_rollup_spreads_main_uses_runtime_dirs_from_active_config(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    config_dir = fake_root / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "live_prod.yaml").write_text(
        "execution:\n  order_store_path: reports/shadow_orders.sqlite\n",
        encoding="utf-8",
    )

    snapshots_dir = fake_root / "reports" / "shadow_spread_snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    day = "20231114"
    (snapshots_dir / f"{day}.jsonl").write_text(
        json.dumps(
            {
                "window_end_ts": 1700000000,
                "symbols": [{"symbol": "BTC/USDT", "bid": 99.0, "ask": 101.0, "mid": 100.0, "spread_bps": 200.0}],
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(rollup_spreads, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["rollup_spreads.py", "--day", day, "--config", "configs/live_prod.yaml"],
    )

    rollup_spreads.main()

    out_path = fake_root / "reports" / "shadow_spread_stats" / f"daily_spread_stats_{day}.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["symbols"]["BTC/USDT"]["count"] == 1
    assert not (fake_root / "reports" / "spread_stats" / f"daily_spread_stats_{day}.json").exists()
