#!/usr/bin/env python3
"""Publish a committed production snapshot through an atomic release pointer."""
from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import shlex
import stat
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import paramiko

from deploy.prod_release import (
    PRODUCTION_SYNC_ITEMS,
    PRODUCTION_USER_UNIT_MAPPINGS,
    iter_production_files,
    production_snapshot,
)
from deploy.sync_prod_release import (
    _ensure_remote_dir,
    _file_mode,
    _install_units,
    _resolve_remote_root,
    _resolve_service_user,
    _resolve_shadow_root,
    _run,
    _upload_files,
    _validate_units,
)
from scripts.verify_release_manifest import verify


RUNTIME_LINK_NAMES = (".env", ".venv", "reports", "state", "data", "models", "logs")
RELEASE_SYNC_ITEMS = tuple(
    item for item in PRODUCTION_SYNC_ITEMS if Path(item).parts[0] not in RUNTIME_LINK_NAMES
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_value(workspace_root: Path, value: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(workspace_root), "rev-parse", value],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git rev-parse {value} failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def _read_remote_bytes(sftp: paramiko.SFTPClient, remote_path: str) -> bytes:
    with sftp.open(remote_path, "rb") as handle:
        return handle.read()


def _remote_lstat(sftp: paramiko.SFTPClient, remote_path: str):
    try:
        return sftp.lstat(remote_path)
    except FileNotFoundError:
        return None


def _active_release_target(
    sftp: paramiko.SFTPClient,
    remote_root: str,
    release_root: str,
) -> str:
    active_stat = _remote_lstat(sftp, remote_root)
    if active_stat is None or not stat.S_ISLNK(int(active_stat.st_mode)):
        raise RuntimeError(f"active production root must be a symbolic link: {remote_root}")
    target = posixpath.normpath(sftp.normalize(remote_root))
    normalized_release_root = posixpath.normpath(release_root)
    if posixpath.commonpath((normalized_release_root, target)) != normalized_release_root:
        raise RuntimeError(f"active release is outside configured release root: {target}")
    return target


def _runtime_link_sources(
    sftp: paramiko.SFTPClient,
    remote_root: str,
) -> dict[str, str]:
    sources: dict[str, str] = {}
    for name in RUNTIME_LINK_NAMES:
        active_path = posixpath.join(remote_root, name)
        try:
            resolved = posixpath.normpath(sftp.normalize(active_path))
            sftp.stat(resolved)
        except (FileNotFoundError, OSError) as exc:
            raise RuntimeError(f"required runtime entry is unavailable: {active_path}") from exc
        sources[name] = resolved
    return sources


def _build_release_manifest(
    snapshot_root: Path,
    *,
    revision: str,
    source_tree: str,
    previous_manifest: dict,
    previous_manifest_bytes: bytes,
    previous_target: str,
) -> dict:
    files: dict[str, str] = {}
    file_modes: dict[str, int] = {}
    for path in iter_production_files(snapshot_root, items=RELEASE_SYNC_ITEMS):
        relative = path.relative_to(snapshot_root).as_posix()
        files[relative] = _sha256_file(path)
        file_modes[relative] = _file_mode(path)

    manifest = dict(previous_manifest)
    manifest.update(
        {
            "schema_version": "review.release.v1",
            "code_revision": revision,
            "source_tree": source_tree,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "release_kind": "immutable_production_sync_from_committed_git_archive",
            "files": dict(sorted(files.items())),
            "file_modes": dict(sorted(file_modes.items())),
            "previous_code_revision": previous_manifest.get("code_revision"),
            "previous_release_target": previous_target,
            "previous_release_manifest_sha256": _sha256_bytes(previous_manifest_bytes),
            "release_integrity_repair": {
                "atomic_pointer_switch": True,
                "prevents_symlink_write_through": True,
                "verifies_file_modes": True,
            },
        }
    )
    dependency_lock = snapshot_root / "deploy/locks/v5-production.txt"
    if dependency_lock.is_file():
        manifest["dependency_lock_sha256"] = _sha256_file(dependency_lock)
    runtime_config = snapshot_root / "configs/live_prod.yaml"
    if runtime_config.is_file():
        manifest["runtime_config_sha256"] = _sha256_file(runtime_config)

    strategy_hashes: dict[str, str] = {}
    for relative in sorted(previous_manifest.get("strategy_hashes", {})):
        path = snapshot_root / relative
        if path.is_file():
            strategy_hashes[relative] = _sha256_file(path)
    manifest["strategy_hashes"] = strategy_hashes

    verify(snapshot_root, manifest)
    return manifest


def _write_remote_file(
    sftp: paramiko.SFTPClient,
    remote_path: str,
    payload: bytes,
    *,
    mode: int,
) -> None:
    parent = posixpath.dirname(remote_path)
    _ensure_remote_dir(sftp, parent)
    temporary = f"{remote_path}.tmp-{uuid.uuid4().hex[:12]}"
    with sftp.open(temporary, "wb") as handle:
        handle.write(payload)
    sftp.chmod(temporary, mode)
    try:
        sftp.posix_rename(temporary, remote_path)
    except OSError:
        if _remote_lstat(sftp, remote_path) is not None:
            raise
        sftp.rename(temporary, remote_path)


def _link_runtime_entries(
    sftp: paramiko.SFTPClient,
    release_target: str,
    runtime_sources: dict[str, str],
) -> None:
    for name, source in runtime_sources.items():
        destination = posixpath.join(release_target, name)
        if _remote_lstat(sftp, destination) is not None:
            raise RuntimeError(f"runtime link destination already exists: {destination}")
        sftp.symlink(source, destination)


def _verify_remote_release(
    client: paramiko.SSHClient,
    release_target: str,
) -> str:
    python = posixpath.join(release_target, ".venv/bin/python")
    verifier = posixpath.join(release_target, "scripts/verify_release_manifest.py")
    hourly = posixpath.join(release_target, "scripts/run_hourly_live_window.sh")
    command = " && ".join(
        (
            f"test -x {shlex.quote(hourly)}",
            shlex.join(
                [
                    python,
                    "-B",
                    verifier,
                    "--root",
                    release_target,
                    "--dependencies",
                    "--runtime",
                ]
            ),
        )
    )
    code, out, err = _run(client, command)
    if code != 0:
        raise RuntimeError(f"staged release verification failed\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out.strip()


def _switch_active_pointer(
    client: paramiko.SSHClient,
    remote_root: str,
    release_target: str,
) -> None:
    temporary = f"{remote_root}.next-{uuid.uuid4().hex[:12]}"
    command = " && ".join(
        (
            f"test -L {shlex.quote(remote_root)}",
            f"test -d {shlex.quote(release_target)}",
            f"ln -s {shlex.quote(release_target)} {shlex.quote(temporary)}",
            f"mv -Tf {shlex.quote(temporary)} {shlex.quote(remote_root)}",
            f"test \"$(readlink -f {shlex.quote(remote_root)})\" = {shlex.quote(release_target)}",
        )
    )
    code, out, err = _run(client, command)
    if code != 0:
        raise RuntimeError(f"atomic release pointer switch failed\nSTDOUT:\n{out}\nSTDERR:\n{err}")


def _manifest_service_names() -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                installed
                for _source, installed in PRODUCTION_USER_UNIT_MAPPINGS
                if installed.endswith(".service")
            }
        )
    )


def _updated_manifest_dropin(existing: bytes | None, exec_start_pre: str) -> bytes:
    if existing is None:
        return f"[Service]\n{exec_start_pre}\n".encode()
    lines = existing.decode().splitlines()
    if "[Service]" not in lines:
        raise RuntimeError("release-manifest drop-in is missing [Service]")
    replaced = False
    output: list[str] = []
    for line in lines:
        if line.startswith("ExecStartPre=") and "verify_release_manifest.py" in line:
            if not replaced:
                output.append(exec_start_pre)
                replaced = True
            continue
        output.append(line)
    if not replaced:
        service_index = output.index("[Service]")
        output.insert(service_index + 1, exec_start_pre)
    return ("\n".join(output).rstrip() + "\n").encode()


def _install_manifest_dropins(
    sftp: paramiko.SFTPClient,
    *,
    service_user: str,
    remote_root: str,
    verifier_release: str,
) -> tuple[str, ...]:
    unit_names = _manifest_service_names()
    python = posixpath.join(remote_root, ".venv/bin/python")
    verifier = posixpath.join(verifier_release, "scripts/verify_release_manifest.py")
    exec_start_pre = f"ExecStartPre={python} -B {verifier} --root {remote_root} --runtime"
    unit_root = f"/home/{service_user}/.config/systemd/user"
    for unit_name in unit_names:
        remote_path = posixpath.join(unit_root, f"{unit_name}.d", "90-release-manifest.conf")
        existing = None
        if _remote_lstat(sftp, remote_path) is not None:
            existing = _read_remote_bytes(sftp, remote_path)
        payload = _updated_manifest_dropin(existing, exec_start_pre)
        _write_remote_file(sftp, remote_path, payload, mode=0o644)
    return unit_names


def _connect(args) -> paramiko.SSHClient:
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
    return client


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", default="")
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument("--key-file", default="")
    parser.add_argument("--remote-root", default="")
    parser.add_argument("--release-root", default="")
    parser.add_argument("--shadow-root", default="")
    parser.add_argument("--service-user", default="")
    parser.add_argument("--enable-prod-timer", action="store_true")
    parser.add_argument("--enable-event-driven-timer", action="store_true")
    parser.add_argument("--allow-active-manifest-target-mismatch", action="store_true")
    args = parser.parse_args()

    remote_root = _resolve_remote_root(args.remote_root, args.user)
    release_root = args.release_root.strip() or posixpath.join(
        posixpath.dirname(remote_root.rstrip("/")), "v5-releases"
    )
    shadow_root = _resolve_shadow_root(args.shadow_root, remote_root)
    service_user = _resolve_service_user(args.service_user, args.user)
    if service_user != args.user:
        raise RuntimeError("immutable publisher requires the SSH user to match the user-service owner")

    workspace_root = Path(__file__).resolve().parents[1]
    revision = _git_value(workspace_root, "HEAD")
    source_tree = _git_value(workspace_root, "HEAD^{tree}")
    if len(revision) != 40:
        raise RuntimeError(f"unexpected Git revision: {revision}")
    release_target = posixpath.join(release_root, revision)

    client = _connect(args)
    sftp = client.open_sftp()
    pointer_switched = False
    previous_target = ""
    try:
        release_root_stat = _remote_lstat(sftp, release_root)
        if release_root_stat is None or not stat.S_ISDIR(int(release_root_stat.st_mode)):
            raise RuntimeError(f"release root is missing or is not a directory: {release_root}")
        previous_target = _active_release_target(sftp, remote_root, release_root)
        if _remote_lstat(sftp, release_target) is not None:
            raise RuntimeError(f"release target already exists; inspect before retry: {release_target}")

        previous_manifest_path = posixpath.join(remote_root, "release-manifest.json")
        previous_manifest_bytes = _read_remote_bytes(sftp, previous_manifest_path)
        previous_manifest = json.loads(previous_manifest_bytes)
        previous_revision = str(previous_manifest.get("code_revision", ""))
        target_name = posixpath.basename(previous_target)
        if target_name != previous_revision and not args.allow_active_manifest_target_mismatch:
            raise RuntimeError(
                "active release directory and manifest revision disagree: "
                f"directory={target_name} manifest={previous_revision}"
            )

        runtime_sources = _runtime_link_sources(sftp, remote_root)
        with production_snapshot(workspace_root, rev=revision, items=RELEASE_SYNC_ITEMS) as snapshot_root:
            manifest = _build_release_manifest(
                snapshot_root,
                revision=revision,
                source_tree=source_tree,
                previous_manifest=previous_manifest,
                previous_manifest_bytes=previous_manifest_bytes,
                previous_target=previous_target,
            )
            _ensure_remote_dir(sftp, release_target)
            uploaded, skipped, changed_paths = _upload_files(
                sftp,
                snapshot_root,
                release_target,
                items=RELEASE_SYNC_ITEMS,
            )
            if skipped or uploaded != len(manifest["files"]):
                raise RuntimeError(
                    "new release upload was not complete: "
                    f"uploaded={uploaded} skipped={skipped} expected={len(manifest['files'])}"
                )
            _link_runtime_entries(sftp, release_target, runtime_sources)
            manifest_payload = (
                json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
            ).encode()
            _write_remote_file(
                sftp,
                posixpath.join(release_target, "release-manifest.json"),
                manifest_payload,
                mode=0o644,
            )

        staged_verification = _verify_remote_release(client, release_target)
        _switch_active_pointer(client, remote_root, release_target)
        pointer_switched = True
        dropins = _install_manifest_dropins(
            sftp,
            service_user=service_user,
            remote_root=remote_root,
            verifier_release=release_target,
        )
        _install_units(
            client,
            remote_root=remote_root,
            shadow_root=shadow_root,
            service_user=service_user,
            ssh_user=args.user,
            enable_prod_timer=args.enable_prod_timer,
            enable_event_driven_timer=args.enable_event_driven_timer,
            restart_web_dashboard=True,
        )
        unit_validation = _validate_units(
            client,
            service_user,
            args.user,
            remote_root=remote_root,
            enable_prod_timer=args.enable_prod_timer,
            enable_event_driven_timer=args.enable_event_driven_timer,
        )
        print(
            json.dumps(
                {
                    "status": "PUBLISHED_VERIFIED",
                    "revision": revision,
                    "source_tree": source_tree,
                    "previous_target": previous_target,
                    "release_target": release_target,
                    "uploaded_files": uploaded,
                    "first_file": changed_paths[0] if changed_paths else None,
                    "last_file": changed_paths[-1] if changed_paths else None,
                    "manifest_sha256": _sha256_bytes(manifest_payload),
                    "manifest_dropins": len(dropins),
                    "staged_verification": staged_verification,
                    "unit_validation": unit_validation,
                },
                indent=2,
                sort_keys=True,
            )
        )
    except Exception as exc:
        if pointer_switched and previous_target:
            rollback_errors: list[str] = []
            try:
                _switch_active_pointer(client, remote_root, previous_target)
                _install_manifest_dropins(
                    sftp,
                    service_user=service_user,
                    remote_root=remote_root,
                    verifier_release=previous_target,
                )
                _install_units(
                    client,
                    remote_root=remote_root,
                    shadow_root=shadow_root,
                    service_user=service_user,
                    ssh_user=args.user,
                    enable_prod_timer=args.enable_prod_timer,
                    enable_event_driven_timer=args.enable_event_driven_timer,
                    restart_web_dashboard=True,
                )
            except Exception as rollback_exc:  # pragma: no cover - operational safeguard
                rollback_errors.append(str(rollback_exc))
            if rollback_errors:
                raise RuntimeError(
                    f"publish failed: {exc}; rollback also failed: {'; '.join(rollback_errors)}"
                ) from exc
        raise
    finally:
        sftp.close()
        client.close()


if __name__ == "__main__":
    main()
