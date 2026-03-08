from __future__ import annotations

from pathlib import Path

from deploy.prod_release import iter_production_files, render_unit_text


def test_render_unit_text_rewrites_known_roots() -> None:
    source = (
        "WorkingDirectory=/home/admin/clawd/v5-trading-bot\n"
        "ExecStart=/home/admin/clawd/v5-prod/.venv/bin/python main.py\n"
    )
    rendered = render_unit_text(source, "/srv/v5-prod")
    assert "/home/admin/clawd/v5-trading-bot" not in rendered
    assert "/home/admin/clawd/v5-prod" not in rendered
    assert rendered.count("/srv/v5-prod") == 2


def test_iter_production_files_excludes_runtime_state(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("print('ok')", encoding="utf-8")
    (tmp_path / "reports").mkdir()
    (tmp_path / "reports" / "state.json").write_text("{}", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "run.py").write_text("print('run')", encoding="utf-8")
    (tmp_path / "scripts" / "archive").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts" / "archive" / "old.py").write_text("print('old')", encoding="utf-8")

    files = sorted(
        path.relative_to(tmp_path).as_posix()
        for path in iter_production_files(tmp_path, items=("main.py", "reports", "scripts"))
    )

    assert files == ["main.py", "scripts/run.py"]
