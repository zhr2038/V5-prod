from __future__ import annotations

import sqlite3

import scripts.check_regime_monitor as check_regime_monitor


def _prepare_regime_db(db_path, *, alerts_json: str = "[]") -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE regime_history (
                ts_ms INTEGER,
                final_state TEXT,
                hmm_state TEXT,
                hmm_sideways_prob REAL,
                alerts_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO regime_history(ts_ms, final_state, hmm_state, hmm_sideways_prob, alerts_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (1_710_000_000_000, "TRENDING", "bull", 0.25, alerts_json),
        )
        conn.commit()
    finally:
        conn.close()


def test_check_regime_monitor_build_paths_anchor_to_workspace(tmp_path) -> None:
    paths = check_regime_monitor.build_paths(workspace=tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.db_path == (tmp_path / "reports" / "regime_history.db").resolve()


def test_check_regime_monitor_main_uses_workspace_reports_when_cwd_differs(tmp_path, monkeypatch, capsys) -> None:
    workspace = tmp_path / "workspace"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(parents=True, exist_ok=True)
    db_path = workspace / "reports" / "regime_history.db"
    _prepare_regime_db(db_path)

    monkeypatch.chdir(elsewhere)
    monkeypatch.setattr(check_regime_monitor, "PROJECT_ROOT", workspace.resolve())

    rc = check_regime_monitor.main([])

    assert rc == 0
    output = capsys.readouterr().out
    assert str(db_path.resolve()) in output
    assert "regime monitor healthy" in output
    assert not (elsewhere / "reports" / "regime_history.db").exists()
