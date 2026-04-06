from __future__ import annotations

import sqlite3
from pathlib import Path

import scripts.run_data_quality_check as run_data_quality_check


def _create_alpha_history_db(db_path: Path, *, symbols: list[str]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE market_data_1h (
            symbol TEXT,
            timestamp INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
        """
    )
    rows = []
    for index, symbol in enumerate(symbols):
        base_ts = 1_700_000_000 + index * 10_000
        rows.append((symbol, base_ts, 100.0, 110.0, 90.0, 105.0, 10.0))
        rows.append((symbol, base_ts + 3600, 105.0, 115.0, 95.0, 110.0, 12.0))
    conn.executemany(
        """
        INSERT INTO market_data_1h(symbol, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def test_build_paths_anchors_quality_check_to_repo_root(tmp_path: Path) -> None:
    paths = run_data_quality_check.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports"
    assert paths.alpha_history_db == tmp_path / "reports" / "alpha_history.db"


def test_run_data_quality_checks_uses_workspace_alpha_history_db(monkeypatch, tmp_path: Path, capsys) -> None:
    fake_root = tmp_path / "repo"
    db_path = fake_root / "reports" / "alpha_history.db"
    _create_alpha_history_db(db_path, symbols=["BTC/USDT"])

    monkeypatch.setattr(run_data_quality_check, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)

    stats = run_data_quality_check.run_data_quality_checks()

    assert stats["total_symbols"] == 1
    assert stats["need_improvement"] == 1
    assert not (tmp_path / "reports" / "alpha_history.db").exists()
    assert str(db_path) in capsys.readouterr().out


def test_run_data_quality_checks_counts_all_symbols_not_only_first_page(
    monkeypatch,
    tmp_path: Path,
) -> None:
    fake_root = tmp_path / "repo"
    symbols = [f"SYM{i}/USDT" for i in range(12)]
    _create_alpha_history_db(fake_root / "reports" / "alpha_history.db", symbols=symbols)

    monkeypatch.setattr(run_data_quality_check, "PROJECT_ROOT", fake_root)

    stats = run_data_quality_check.run_data_quality_checks()

    assert stats["total_symbols"] == 12
    assert stats["need_improvement"] == 12
