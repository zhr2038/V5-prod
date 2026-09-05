"""Bounded real equity history, cache freshness, and unavailable-data contracts."""
import importlib.util
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest


@pytest.fixture
def dashboard():
    path = Path(__file__).resolve().parents[1] / "scripts/web_dashboard.py"
    spec = importlib.util.spec_from_file_location("equity_dashboard_" + uuid.uuid4().hex, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_recent_file_bound_with_real_cst_run_labels(dashboard, tmp_path, monkeypatch):
    runs = tmp_path / "runs"
    start = datetime(2026, 9, 5, 1, tzinfo=timezone.utc)
    china = timezone(timedelta(hours=8))
    for age in range(30):
        run = start - timedelta(hours=age)
        write_rows(runs / run.astimezone(china).strftime("%Y%m%d_%H") / "equity.jsonl", [
            {"ts": (run + timedelta(seconds=i)).isoformat(), "equity": 100 - age + i / 10}
            for i in range(3)
        ])
    original_open = Path.open
    opened = []

    def traced_open(path, *args, **kwargs):
        if path.name == "equity.jsonl":
            opened.append(path)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", traced_open)
    points = dashboard._load_equity_points(limit=4, runtime_paths=SimpleNamespace(runs_dir=runs))
    assert len(opened) == 4
    assert [value for _, value in points] == [99.2, 100.0, 100.1, 100.2]


def test_cache_reuses_unchanged_files_and_refreshes_append_and_new_run(dashboard, tmp_path, monkeypatch):
    runs = tmp_path / "runs"
    path = runs / "20260905_09" / "equity.jsonl"
    write_rows(path, [{"ts": "2026-09-05T01:00:00Z", "equity": 100}])
    runtime = SimpleNamespace(runs_dir=runs)
    original_open = Path.open
    opened = []

    def traced_open(target, *args, **kwargs):
        if target.name == "equity.jsonl" and "r" in (args[0] if args else "r"):
            opened.append(target)
        return original_open(target, *args, **kwargs)

    monkeypatch.setattr(Path, "open", traced_open)
    first = dashboard._load_equity_points(runtime_paths=runtime)
    first.append(("mutated_by_client", 999))
    assert dashboard._load_equity_points(runtime_paths=runtime) == [("2026-09-05T01:00:00Z", 100)]
    assert len(opened) == 1
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"ts": "2026-09-05T01:01:00Z", "equity": 101}) + "\n")
    assert dashboard._load_equity_points(runtime_paths=runtime)[-1][1] == 101
    newest = runs / "20260905_10" / "equity.jsonl"
    write_rows(newest, [{"ts": "2026-09-05T02:00:00Z", "equity": 102}])
    assert dashboard._load_equity_points(runtime_paths=runtime)[-1][1] == 102
    old_stat = newest.stat()
    write_rows(newest, [{"ts": "2026-09-05T02:00:00Z", "equity": 103}])
    os.utime(newest, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000))
    assert newest.stat().st_size == old_stat.st_size
    assert dashboard._load_equity_points(runtime_paths=runtime)[-1][1] == 103


def test_duplicate_instants_prefer_newer_source_and_sort_by_epoch(dashboard, tmp_path):
    runs = tmp_path / "runs"
    stale = runs / "stale" / "equity.jsonl"
    fresh = runs / "fresh" / "equity.jsonl"
    write_rows(stale, [{"ts": "2026-09-05T01:00:00Z", "equity": 111}])
    write_rows(fresh, [
        {"ts": "2026-09-05T09:00:00+08:00", "equity": 222},
        {"ts": "2026-09-05T02:00:00Z", "equity": 223},
    ])
    os.utime(stale, (100, 100))
    os.utime(fresh, (200, 200))
    assert dashboard._load_equity_points(runtime_paths=SimpleNamespace(runs_dir=runs)) == [
        ("2026-09-05T09:00:00+08:00", 222), ("2026-09-05T02:00:00Z", 223),
    ]


def test_bounded_tail_discards_partial_record(dashboard, tmp_path, monkeypatch):
    path = tmp_path / "runs/20260905_09/equity.jsonl"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"x" * 10000 + b"\n" + json.dumps({"ts": "2026-09-05T01:00:00Z", "equity": 123}).encode() + b"\n")
    monkeypatch.setattr(dashboard, "_EQUITY_HISTORY_MAX_FILE_BYTES", 128)
    original_open = Path.open
    requests = []

    class Reader:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def seek(self, offset):
            return self.stream.seek(offset)

        def read(self, size):
            requests.append(size)
            return self.stream.read(size)

    def bounded_open(target, *args, **kwargs):
        stream = original_open(target, *args, **kwargs)
        return Reader(stream) if target == path else stream

    monkeypatch.setattr(Path, "open", bounded_open)
    assert dashboard._load_equity_points(runtime_paths=SimpleNamespace(runs_dir=tmp_path / "runs")) == [("2026-09-05T01:00:00Z", 123)]
    assert requests == [128]


def test_invalid_rows_are_unavailable_but_real_zero_is_preserved(dashboard, tmp_path):
    path = tmp_path / "runs/20260905_09/equity.jsonl"
    write_rows(path, [
        {"ts": "2026-09-05T01:00:00Z", "equity": None},
        {"ts": "2026-09-05T01:00:01Z", "equity": float("nan")},
        {"ts": "2026-09-05T01:00:02Z", "equity": float("inf")},
        {"ts": "2026-09-05T01:00:03Z", "equity": -1},
        {"ts": "not-a-date", "equity": 999},
        {"ts": "2026-09-05T01:00:04Z", "equity": 0},
    ])
    assert dashboard._load_equity_points(runtime_paths=SimpleNamespace(runs_dir=tmp_path / "runs")) == [("2026-09-05T01:00:04Z", 0)]


def test_missing_history_returns_empty_without_creating_files(dashboard, tmp_path):
    missing = tmp_path / "missing"
    assert dashboard._load_equity_points(runtime_paths=SimpleNamespace(runs_dir=missing)) == []
    assert not missing.exists()
