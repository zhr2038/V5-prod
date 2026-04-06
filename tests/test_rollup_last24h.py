from __future__ import annotations

import json
import sys
from pathlib import Path

import scripts.rollup_last24h as rollup_last24h


def test_rollup_last24h_defaults_anchor_paths_to_repo_root(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    captured: dict[str, object] = {}

    def fake_rollup(end_hour_exclusive, runs_dir: Path, out_dir: Path, hours: int = 24) -> Path:
        captured["runs_dir"] = runs_dir
        captured["out_dir"] = out_dir
        captured["hours"] = hours
        out_dir.mkdir(parents=True, exist_ok=True)
        summary_path = out_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "run_id": "v5",
                    "window_start_ts": 1700000000,
                    "window_end_ts": 1700003600,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return summary_path

    def fake_check_call(cmd):
        captured["cmd"] = cmd

    monkeypatch.setattr(rollup_last24h, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(rollup_last24h, "rollup", fake_rollup)
    monkeypatch.setattr(rollup_last24h.subprocess, "check_call", fake_check_call)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["rollup_last24h.py"])

    rollup_last24h.main()

    assert captured["runs_dir"] == (fake_root / "reports" / "runs").resolve()
    assert str(captured["out_dir"]).startswith(str((fake_root / "reports" / "rollups").resolve()))
    cmd = captured["cmd"]
    assert cmd[0] == sys.executable
    assert cmd[2] == "--v4_reports_dir"
    assert cmd[3] == str((fake_root / "v4_export").resolve())
    assert "--out" in cmd
    compare_out = Path(cmd[cmd.index("--out") + 1])
    assert str(compare_out).startswith(str((fake_root / "reports" / "compare" / "daily").resolve()))
    assert not (tmp_path / "reports" / "rollups").exists()
