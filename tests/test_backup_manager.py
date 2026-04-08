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


def test_backup_manager_includes_active_config_runtime_state_files(tmp_path) -> None:
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
                "  kill_switch_path: reports/shadow_runtime/kill_switch_shadow.json",
                "  reconcile_status_path: reports/shadow_runtime/reconcile_shadow.json",
                "",
            ]
        ),
        encoding="utf-8",
    )

    shadow_dir = tmp_path / "reports" / "shadow_runtime"
    shadow_dir.mkdir(parents=True, exist_ok=True)
    (shadow_dir / "orders.sqlite").write_text("shadow-orders", encoding="utf-8")
    (shadow_dir / "fills.sqlite").write_text("shadow-fills", encoding="utf-8")
    (shadow_dir / "positions.sqlite").write_text("shadow-positions", encoding="utf-8")
    (shadow_dir / "kill_switch_shadow.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "reconcile_shadow.json").write_text("{}", encoding="utf-8")
    root_positions = tmp_path / "reports" / "positions.sqlite"
    root_positions.parent.mkdir(parents=True, exist_ok=True)
    root_positions.write_text("root-positions", encoding="utf-8")

    manager = backup_manager.BackupManager(workspace=tmp_path)
    backup_path = manager.create_backup(name="runtime_backup")

    with tarfile.open(backup_path, "r:gz") as archive:
        names = archive.getnames()

    assert "reports/shadow_runtime/orders.sqlite" in names
    assert "reports/shadow_runtime/fills.sqlite" in names
    assert "reports/shadow_runtime/positions.sqlite" in names
    assert "reports/shadow_runtime/kill_switch_shadow.json" in names
    assert "reports/shadow_runtime/reconcile_shadow.json" in names
    assert "reports/positions.sqlite" not in names
