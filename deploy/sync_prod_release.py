#!/usr/bin/env python3
from __future__ import annotations

import argparse
import posixpath
import shlex
import stat
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


try:
    import paramiko
except ImportError as exc:  # pragma: no cover - exercised operationally
    raise SystemExit("missing dependency: paramiko") from exc

from deploy.prod_release import (
    iter_production_files,
    production_snapshot,
    production_sync_relative_paths,
    production_sync_roots,
)


def _remote_join(root: str, rel_path: Path) -> str:
    parts = [part for part in rel_path.as_posix().split("/") if part]
    return "/".join([root.rstrip("/"), *parts])


def _ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    parts = []
    if remote_dir.startswith("/"):
        prefix = "/"
        segments = [segment for segment in remote_dir.split("/") if segment]
    else:
        prefix = ""
        segments = [segment for segment in remote_dir.split("/") if segment]
    for segment in segments:
        parts.append(segment)
        candidate = prefix + "/".join(parts)
        try:
            sftp.stat(candidate)
        except FileNotFoundError:
            sftp.mkdir(candidate)


def _file_mode(path: Path) -> int:
    if path.suffix in {".sh", ".py"}:
        return 0o755
    return 0o644


def _should_upload(sftp: paramiko.SFTPClient, local_path: Path, remote_path: str) -> bool:
    local_stat = local_path.stat()
    try:
        remote_stat = sftp.stat(remote_path)
    except FileNotFoundError:
        return True
    return not (
        int(remote_stat.st_size) == int(local_stat.st_size)
        and int(getattr(remote_stat, "st_mtime", -1)) == int(local_stat.st_mtime)
    )


def _upload_files(sftp: paramiko.SFTPClient, workspace_root: Path, remote_root: str) -> tuple[int, int, list[str]]:
    uploaded = 0
    skipped = 0
    rel_paths: list[str] = []
    for local_path in iter_production_files(workspace_root):
        rel_path = local_path.relative_to(workspace_root)
        remote_path = _remote_join(remote_root, rel_path)
        parent = remote_path.rsplit("/", 1)[0]
        _ensure_remote_dir(sftp, parent)
        if not _should_upload(sftp, local_path, remote_path):
            skipped += 1
            continue
        sftp.put(str(local_path), remote_path)
        sftp.chmod(remote_path, _file_mode(local_path))
        local_mtime = int(local_path.stat().st_mtime)
        try:
            sftp.utime(remote_path, (local_mtime, local_mtime))
        except OSError:
            pass
        uploaded += 1
        rel_paths.append(rel_path.as_posix())
    return uploaded, skipped, rel_paths


def _iter_remote_files(sftp: paramiko.SFTPClient, remote_dir: str) -> list[str]:
    try:
        attrs = sftp.listdir_attr(remote_dir)
    except FileNotFoundError:
        return []

    files: list[str] = []
    for attr in attrs:
        child = posixpath.join(remote_dir.rstrip("/"), attr.filename)
        if stat.S_ISDIR(int(attr.st_mode)):
            files.extend(_iter_remote_files(sftp, child))
        else:
            files.append(child)
    return files


def _collect_remote_sync_files(sftp: paramiko.SFTPClient, remote_root: str) -> set[str]:
    remote_files: set[str] = set()
    normalized_root = remote_root.rstrip("/")
    for root in production_sync_roots():
        remote_path = _remote_join(normalized_root, Path(root))
        try:
            attr = sftp.stat(remote_path)
        except FileNotFoundError:
            continue
        if stat.S_ISDIR(int(attr.st_mode)):
            for child in _iter_remote_files(sftp, remote_path):
                remote_files.add(posixpath.relpath(child, normalized_root))
        else:
            remote_files.add(root)
    return remote_files


def _prune_remote_files(sftp: paramiko.SFTPClient, workspace_root: Path, remote_root: str) -> list[str]:
    expected = production_sync_relative_paths(workspace_root)
    remote_files = _collect_remote_sync_files(sftp, remote_root)
    stale = sorted(remote_files - expected)
    for rel_path in stale:
        sftp.remove(_remote_join(remote_root, Path(rel_path)))
    return stale


def _run(client: paramiko.SSHClient, command: str) -> tuple[int, str, str]:
    stdin, stdout, stderr = client.exec_command(command)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    return stdout.channel.recv_exit_status(), out, err


def _user_bus_wrapped_command(service_user: str, inner: str) -> str:
    escaped_inner = shlex.quote(inner)
    escaped_user = shlex.quote(service_user)
    return (
        "uid=$(id -u {user}) && "
        "sudo -u {user} env "
        "XDG_RUNTIME_DIR=/run/user/$uid "
        "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus "
        "bash -lc {inner}"
    ).format(user=escaped_user, inner=escaped_inner)


def _resolve_remote_root(raw_remote_root: str, ssh_user: str) -> str:
    value = str(raw_remote_root or "").strip()
    if value:
        return value
    return f"/home/{ssh_user}/clawd/v5-prod"


def _resolve_service_user(raw_service_user: str, ssh_user: str) -> str:
    value = str(raw_service_user or "").strip()
    if value:
        return value
    return ssh_user


def _install_units(
    client: paramiko.SSHClient,
    remote_root: str,
    service_user: str,
    enable_prod_timer: bool,
    enable_event_driven_timer: bool,
) -> None:
    cmd = [
        "bash",
        "deploy/install_systemd.sh",
        "--user",
        "--production-only",
        "--root",
        remote_root,
    ]
    if enable_prod_timer:
        cmd.append("--enable-prod-timer")
    if enable_event_driven_timer:
        cmd.append("--enable-event-driven-timer")

    inner = f"cd {shlex.quote(remote_root)} && {' '.join(shlex.quote(part) for part in cmd)}"
    wrapped = _user_bus_wrapped_command(service_user, inner)
    code, out, err = _run(client, wrapped)
    if code != 0:
        raise RuntimeError(f"install_systemd failed\nSTDOUT:\n{out}\nSTDERR:\n{err}")


def _validate_units(client: paramiko.SSHClient, service_user: str) -> str:
    inner = (
        "systemctl --user is-enabled v5-web-dashboard.service "
        "&& systemctl --user is-enabled v5-trade-monitor.timer "
        "&& systemctl --user is-enabled v5-daily-ml-training.timer "
        "&& systemctl --user is-enabled v5-model-promotion-gate.timer "
        "&& systemctl --user is-enabled v5-sentiment-collect.timer "
        "&& systemctl --user is-enabled v5-auto-risk-eval.timer "
        "&& systemctl --user is-enabled v5-reconcile.timer "
        "&& systemctl --user is-enabled v5-ledger.timer "
        "&& systemctl --user is-enabled v5-cost-rollup-real.user.timer "
        "&& systemctl --user is-enabled v5-spread-rollup.timer "
        "&& test \"$(systemctl --user is-active v5-web-dashboard.service)\" = active "
        "&& test \"$(systemctl --user is-active v5-trade-monitor.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-daily-ml-training.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-model-promotion-gate.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-sentiment-collect.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-auto-risk-eval.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-reconcile.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-ledger.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-cost-rollup-real.user.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-spread-rollup.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-prod.user.timer)\" = active "
        "&& test \"$(systemctl --user is-active v5-event-driven.timer)\" = active "
        "&& systemctl --user show v5-web-dashboard.service --property=UnitFileState,ActiveState "
        "&& systemctl --user show v5-trade-monitor.timer --property=UnitFileState,ActiveState "
        "&& systemctl --user show v5-daily-ml-training.timer --property=UnitFileState "
        "&& systemctl --user show v5-model-promotion-gate.timer --property=UnitFileState "
        "&& systemctl --user show v5-sentiment-collect.timer --property=UnitFileState "
        "&& systemctl --user show v5-auto-risk-eval.timer --property=UnitFileState "
        "&& systemctl --user show v5-spread-rollup.timer --property=UnitFileState "
        "&& systemctl --user show v5-prod.user.timer --property=UnitFileState "
        "&& systemctl --user show v5-event-driven.timer --property=UnitFileState"
    )
    cmd = _user_bus_wrapped_command(service_user, inner)
    code, out, err = _run(client, cmd)
    if code != 0:
        raise RuntimeError(f"systemd validation failed\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out.strip()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--port", type=int, default=22)
    ap.add_argument("--key-file", default="")
    ap.add_argument("--remote-root", default="")
    ap.add_argument("--service-user", default="")
    ap.add_argument("--skip-install", action="store_true")
    ap.add_argument("--no-prune", action="store_true")
    ap.add_argument("--enable-prod-timer", action="store_true")
    ap.add_argument("--enable-event-driven-timer", action="store_true")
    args = ap.parse_args()
    remote_root = _resolve_remote_root(args.remote_root, args.user)
    service_user = _resolve_service_user(args.service_user, args.user)

    workspace_root = Path(__file__).resolve().parents[1]

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connect_kwargs = {
        "hostname": args.host,
        "port": args.port,
        "username": args.user,
        "timeout": 20,
        "banner_timeout": 30,
        "auth_timeout": 30,
    }
    if args.key_file:
        connect_kwargs["key_filename"] = args.key_file
    else:
        connect_kwargs["password"] = args.password

    client.connect(**connect_kwargs)
    try:
        sftp = client.open_sftp()
        _ensure_remote_dir(sftp, remote_root)
        with production_snapshot(workspace_root) as snapshot_root:
            uploaded, skipped, rel_paths = _upload_files(sftp, snapshot_root, remote_root)
            pruned = [] if args.no_prune else _prune_remote_files(sftp, snapshot_root, remote_root)
        sftp.close()

        print(f"uploaded_files={uploaded}")
        print(f"skipped_files={skipped}")
        print(f"pruned_files={len(pruned)}")
        if rel_paths:
            print(f"first_file={rel_paths[0]}")
            print(f"last_file={rel_paths[-1]}")

        if not args.skip_install:
            _install_units(
                client,
                remote_root=remote_root,
                service_user=service_user,
                enable_prod_timer=args.enable_prod_timer,
                enable_event_driven_timer=args.enable_event_driven_timer,
            )
            print(_validate_units(client, service_user))
    finally:
        client.close()


if __name__ == "__main__":
    main()
