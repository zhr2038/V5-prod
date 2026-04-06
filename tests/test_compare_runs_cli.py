from __future__ import annotations

import json
import sys
from pathlib import Path

import scripts.compare_runs as compare_runs


def test_compare_runs_defaults_anchor_outputs_to_repo_root(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    summary_dir = fake_root / "reports" / "rollups" / "last24h"
    summary_dir.mkdir(parents=True, exist_ok=True)
    (summary_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "v5",
                "window_start_ts": 1700000000,
                "window_end_ts": 1700003600,
                "num_trades": 2,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def fake_check_call(cmd):
        captured["cmd"] = cmd
        out_dir = fake_root / "v4_export"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "summary.json").write_text(
            json.dumps(
                {
                    "run_id": "v4",
                    "window_start_ts": 1700000000,
                    "window_end_ts": 1700003600,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    monkeypatch.setattr(compare_runs, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(compare_runs.subprocess, "check_call", fake_check_call)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_runs.py",
            "--v5_summary",
            "reports/rollups/last24h/summary.json",
            "--v4_reports_dir",
            "legacy_v4_reports",
        ],
    )

    compare_runs.main()

    out_path = fake_root / "reports" / "compare" / "v4_vs_v5.md"
    assert out_path.exists()
    assert "num_trades" in out_path.read_text(encoding="utf-8")
    cmd = captured["cmd"]
    assert cmd[3] == str((fake_root / "legacy_v4_reports").resolve())
    assert cmd[5] == str((fake_root / "v4_export").resolve())
    assert not (tmp_path / "reports" / "compare" / "v4_vs_v5.md").exists()
