from __future__ import annotations

import io
import tarfile
import pytest

import scripts.backup_manager as backup_manager


@pytest.fixture(autouse=True)
def _runtime_config(monkeypatch, tmp_path):
    config_path = tmp_path / "configs" / "live_prod.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/orders.sqlite\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        backup_manager,
        "resolve_runtime_config_path",
        lambda project_root=None: str(config_path),
    )
    return config_path


def test_backup_manager_build_paths_anchor_to_workspace(tmp_path) -> None:
    paths = backup_manager.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.backup_dir == (tmp_path / "backups").resolve()


def test_backup_manager_load_active_config_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(
        backup_manager,
        "resolve_runtime_config_path",
        lambda project_root=None: str(missing),
    )

    manager = backup_manager.BackupManager(workspace=tmp_path)

    with pytest.raises(FileNotFoundError, match=str(missing)):
        manager._load_active_config()


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
    (shadow_dir / "bills.sqlite").write_text("shadow-bills", encoding="utf-8")
    (shadow_dir / "ledger_state.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "ledger_status.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "stop_loss_state.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "fixed_stop_loss_state.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "profit_taking_state.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "highest_px_state.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "rank_exit_cooldown_state.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "take_profit_cooldown_state.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "order_state_machine.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "negative_expectancy_cooldown.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "kill_switch_shadow.json").write_text("{}", encoding="utf-8")
    (shadow_dir / "reconcile_shadow.json").write_text("{}", encoding="utf-8")
    root_positions = tmp_path / "reports" / "positions.sqlite"
    root_positions.parent.mkdir(parents=True, exist_ok=True)
    root_positions.write_text("root-positions", encoding="utf-8")
    root_bills = tmp_path / "reports" / "bills.sqlite"
    root_bills.write_text("root-bills", encoding="utf-8")

    manager = backup_manager.BackupManager(workspace=tmp_path)
    backup_path = manager.create_backup(name="runtime_backup")

    with tarfile.open(backup_path, "r:gz") as archive:
        names = archive.getnames()

    assert "reports/shadow_runtime/orders.sqlite" in names
    assert "reports/shadow_runtime/fills.sqlite" in names
    assert "reports/shadow_runtime/positions.sqlite" in names
    assert "reports/shadow_runtime/bills.sqlite" in names
    assert "reports/shadow_runtime/ledger_state.json" in names
    assert "reports/shadow_runtime/ledger_status.json" in names
    assert "reports/shadow_runtime/stop_loss_state.json" in names
    assert "reports/shadow_runtime/fixed_stop_loss_state.json" in names
    assert "reports/shadow_runtime/profit_taking_state.json" in names
    assert "reports/shadow_runtime/highest_px_state.json" in names
    assert "reports/shadow_runtime/rank_exit_cooldown_state.json" in names
    assert "reports/shadow_runtime/take_profit_cooldown_state.json" in names
    assert "reports/shadow_runtime/order_state_machine.json" in names
    assert "reports/shadow_runtime/negative_expectancy_cooldown.json" in names
    assert "reports/shadow_runtime/kill_switch_shadow.json" in names
    assert "reports/shadow_runtime/reconcile_shadow.json" in names
    assert "reports/positions.sqlite" not in names
    assert "reports/bills.sqlite" not in names


def test_backup_manager_derives_runtime_state_files_when_config_uses_legacy_defaults(tmp_path) -> None:
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_orders.sqlite",
                "",
            ]
        ),
        encoding="utf-8",
    )

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "orders.sqlite").write_text("root-orders", encoding="utf-8")
    (reports_dir / "kill_switch.json").write_text("root-kill", encoding="utf-8")
    (reports_dir / "reconcile_status.json").write_text("root-reconcile", encoding="utf-8")

    (reports_dir / "shadow_orders.sqlite").write_text("shadow-orders", encoding="utf-8")
    (reports_dir / "shadow_fills.sqlite").write_text("shadow-fills", encoding="utf-8")
    (reports_dir / "shadow_positions.sqlite").write_text("shadow-positions", encoding="utf-8")
    (reports_dir / "shadow_bills.sqlite").write_text("shadow-bills", encoding="utf-8")
    (reports_dir / "shadow_ledger_state.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_ledger_status.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_stop_loss_state.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_fixed_stop_loss_state.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_profit_taking_state.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_highest_px_state.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_rank_exit_cooldown_state.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_take_profit_cooldown_state.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_order_state_machine.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_negative_expectancy_cooldown.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_kill_switch.json").write_text("{}", encoding="utf-8")
    (reports_dir / "shadow_reconcile_status.json").write_text("{}", encoding="utf-8")

    manager = backup_manager.BackupManager(workspace=tmp_path)
    backup_path = manager.create_backup(name="runtime_legacy_default_backup")

    with tarfile.open(backup_path, "r:gz") as archive:
        names = archive.getnames()

    assert "reports/shadow_orders.sqlite" in names
    assert "reports/shadow_fills.sqlite" in names
    assert "reports/shadow_positions.sqlite" in names
    assert "reports/shadow_bills.sqlite" in names
    assert "reports/shadow_ledger_state.json" in names
    assert "reports/shadow_ledger_status.json" in names
    assert "reports/shadow_stop_loss_state.json" in names
    assert "reports/shadow_fixed_stop_loss_state.json" in names
    assert "reports/shadow_profit_taking_state.json" in names
    assert "reports/shadow_highest_px_state.json" in names
    assert "reports/shadow_rank_exit_cooldown_state.json" in names
    assert "reports/shadow_take_profit_cooldown_state.json" in names
    assert "reports/shadow_order_state_machine.json" in names
    assert "reports/shadow_negative_expectancy_cooldown.json" in names
    assert "reports/shadow_kill_switch.json" in names
    assert "reports/shadow_reconcile_status.json" in names
    assert "reports/kill_switch.json" not in names
    assert "reports/reconcile_status.json" not in names


def test_backup_manager_restore_rejects_path_traversal(tmp_path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    archive_path = backup_dir / "evil.tar.gz"

    payload = b"owned"
    with tarfile.open(archive_path, "w:gz") as archive:
        info = tarfile.TarInfo(name="../escape.txt")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))

    manager = backup_manager.BackupManager(workspace=tmp_path)

    with pytest.raises(RuntimeError, match="unsafe backup member"):
        manager.restore_backup("evil.tar.gz")

    assert not (tmp_path / "escape.txt").exists()
