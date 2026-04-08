#!/usr/bin/env python3
"""
V5 automated backup helper.
"""

from __future__ import annotations

import tarfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_path
from src.execution.fill_store import derive_fill_store_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class BackupPaths:
    workspace: Path
    backup_dir: Path


def build_paths(workspace: Path | None = None) -> BackupPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    return BackupPaths(
        workspace=root,
        backup_dir=root / "backups",
    )


STATIC_BACKUP_ITEMS = [
    "reports/positions.sqlite",
    "reports/bills.sqlite",
    "configs/",
    "memory/",
    "MEMORY.md",
    "SOUL.md",
    "IDENTITY.md",
    "USER.md",
]

KEEP_BACKUPS = 7


class BackupManager:
    """Create and retain workspace backups."""

    def __init__(self, workspace: Path | None = None):
        self.paths = build_paths(workspace)
        self.stats = {"backed_up": 0, "errors": 0, "size_mb": 0.0}

    def _load_active_config(self):
        config_path = Path(resolve_runtime_config_path(project_root=self.paths.workspace))
        try:
            import yaml

            if config_path.exists():
                return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except Exception:
            pass
        return {}

    def _runtime_backup_paths(self) -> list[Path]:
        cfg = self._load_active_config()
        execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
        orders_db = Path(
            resolve_runtime_path(
                execution_cfg.get("order_store_path"),
                default="reports/orders.sqlite",
                project_root=self.paths.workspace,
            )
        ).resolve()
        fills_db = derive_fill_store_path(orders_db).resolve()
        kill_switch = Path(
            resolve_runtime_path(
                execution_cfg.get("kill_switch_path"),
                default="reports/kill_switch.json",
                project_root=self.paths.workspace,
            )
        ).resolve()
        reconcile_status = Path(
            resolve_runtime_path(
                execution_cfg.get("reconcile_status_path"),
                default="reports/reconcile_status.json",
                project_root=self.paths.workspace,
            )
        ).resolve()
        return [orders_db, fills_db, kill_switch, reconcile_status]

    def _iter_backup_items(self):
        seen: set[Path] = set()

        for item in STATIC_BACKUP_ITEMS:
            path = (self.paths.workspace / item).resolve()
            if path in seen:
                continue
            seen.add(path)
            yield path, item

        for path in self._runtime_backup_paths():
            if path in seen:
                continue
            seen.add(path)
            try:
                arcname = str(path.relative_to(self.paths.workspace)).replace("\\", "/")
            except ValueError:
                arcname = f"external_runtime/{path.name}"
            yield path, arcname

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def create_backup(self, name=None):
        self.paths.backup_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = name or f"v5_backup_{timestamp}"
        backup_path = self.paths.backup_dir / f"{backup_name}.tar.gz"

        self.log("=" * 60)
        self.log(f"Creating backup: {backup_name}")
        self.log("=" * 60)

        with tarfile.open(backup_path, "w:gz") as tar:
            for src_path, arcname in self._iter_backup_items():
                if src_path.exists():
                    try:
                        tar.add(src_path, arcname=arcname)
                        kind = "directory" if src_path.is_dir() else "file"
                        self.log(f"backed up {kind}: {arcname}")
                        self.stats["backed_up"] += 1
                    except Exception as exc:
                        self.log(f"backup failed {arcname}: {exc}")
                        self.stats["errors"] += 1
                else:
                    self.log(f"skip missing: {arcname}")

        size_mb = backup_path.stat().st_size / (1024 * 1024)
        self.stats["size_mb"] = size_mb

        self.log(f"backup complete: {backup_path}")
        self.log(f"size: {size_mb:.1f} MB")
        return backup_path

    def cleanup_old_backups(self):
        if not self.paths.backup_dir.exists():
            return

        backups = sorted(self.paths.backup_dir.glob("*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True)

        if len(backups) > KEEP_BACKUPS:
            to_delete = backups[KEEP_BACKUPS:]
            self.log(f"cleaning {len(to_delete)} old backups...")
            for backup in to_delete:
                try:
                    backup.unlink()
                    self.log(f"deleted: {backup.name}")
                except Exception as exc:
                    self.log(f"delete failed: {backup.name} - {exc}")

    def list_backups(self):
        if not self.paths.backup_dir.exists():
            print("no backups")
            return

        backups = sorted(self.paths.backup_dir.glob("*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True)

        print("\nbackup list:")
        print("-" * 60)
        for i, backup in enumerate(backups, 1):
            size_mb = backup.stat().st_size / (1024 * 1024)
            mtime = datetime.fromtimestamp(backup.stat().st_mtime)
            print(f"{i}. {backup.name}")
            print(f"   size: {size_mb:.1f} MB  time: {mtime.strftime('%Y-%m-%d %H:%M')}")
        print("-" * 60)

    def restore_backup(self, backup_name):
        backup_path = self.paths.backup_dir / backup_name
        if not backup_path.exists():
            self.log(f"backup missing: {backup_name}")
            return False

        self.log(f"restoring backup: {backup_name}")

        restore_dir = self.paths.workspace / f"restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        restore_dir.mkdir(parents=True, exist_ok=True)

        with tarfile.open(backup_path, "r:gz") as tar:
            tar.extractall(path=restore_dir)

        self.log(f"backup extracted to: {restore_dir}")
        self.log("please review restored files before replacing runtime data")
        return True

    def run(self):
        self.log("starting backup flow")
        self.create_backup()
        self.cleanup_old_backups()
        self.log("\n" + "=" * 60)
        self.log("backup stats")
        self.log("=" * 60)
        self.log(f"backed up: {self.stats['backed_up']}")
        self.log(f"errors: {self.stats['errors']}")
        self.log(f"backup size: {self.stats['size_mb']:.1f} MB")
        self.log("=" * 60)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="V5 automated backup helper")
    parser.add_argument("action", choices=["backup", "list", "restore"], default="backup", nargs="?")
    parser.add_argument("--name", help="Backup name")
    parser.add_argument("--restore-file", help="Backup archive to restore")
    args = parser.parse_args()

    manager = BackupManager()

    if args.action == "backup":
        manager.run()
    elif args.action == "list":
        manager.list_backups()
    elif args.action == "restore":
        if args.restore_file:
            manager.restore_backup(args.restore_file)
        else:
            print("missing --restore-file")


if __name__ == "__main__":
    main()
