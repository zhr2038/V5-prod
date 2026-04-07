from __future__ import annotations

import tarfile

import scripts.backup_manager as backup_manager


def test_backup_manager_build_paths_anchor_to_workspace(tmp_path) -> None:
    paths = backup_manager.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.backup_dir == (tmp_path / "backups").resolve()


def test_backup_manager_creates_backup_under_workspace(tmp_path) -> None:
    orders_db = tmp_path / "reports" / "orders.sqlite"
    orders_db.parent.mkdir(parents=True, exist_ok=True)
    orders_db.write_text("stub", encoding="utf-8")

    manager = backup_manager.BackupManager(workspace=tmp_path)
    backup_path = manager.create_backup(name="unit_backup")

    assert backup_path == (tmp_path / "backups" / "unit_backup.tar.gz")
    assert backup_path.exists()

    with tarfile.open(backup_path, "r:gz") as archive:
        assert "reports/orders.sqlite" in archive.getnames()
