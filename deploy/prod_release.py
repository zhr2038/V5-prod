from __future__ import annotations

import io
import subprocess
import tarfile
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator

KNOWN_DEPLOY_ROOTS = (
    "/home/admin/clawd/v5-trading-bot",
    "/home/admin/clawd/v5-prod",
    "/home/admin/clawd/v5-shadow-tuned-xgboost",
    "/home/ubuntu/clawd/v5-prod",
    "/home/ubuntu/clawd/v5-shadow-tuned-xgboost",
)

PRODUCTION_SYNC_ITEMS = (
    "main.py",
    "event_driven_check.py",
    "requirements.txt",
    "pyproject.toml",
    "models",
    "configs",
    "deploy",
    "scripts",
    "src",
    "web",
    "docs/CURRENT_PRODUCTION_FLOW.md",
    "docs/PRODUCTION_MINIMAL_FILES.md",
    "docs/PRODUCTION_ONLY_DEPLOYMENT.md",
)

PRODUCTION_SYNC_EXCLUDES = (
    ".git",
    ".venv",
    "node_modules",
    "logs",
    "reports",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "scripts/archive",
)

GIT_COMMAND_TIMEOUT = 30

PRODUCTION_USER_UNIT_MAPPINGS = (
    ("v5-prod.user.service", "v5-prod.user.service"),
    ("v5-prod.user.timer", "v5-prod.user.timer"),
    ("v5-event-driven.service", "v5-event-driven.service"),
    ("v5-event-driven.timer", "v5-event-driven.timer"),
    ("v5-web-dashboard.service", "v5-web-dashboard.service"),
    ("v5-sentiment-collect.service", "v5-sentiment-collect.service"),
    ("v5-sentiment-collect.timer", "v5-sentiment-collect.timer"),
    ("v5-auto-risk-eval.service", "v5-auto-risk-eval.service"),
    ("v5-auto-risk-eval.timer", "v5-auto-risk-eval.timer"),
    ("v5-daily-ml-training.service", "v5-daily-ml-training.service"),
    ("v5-daily-ml-training.timer", "v5-daily-ml-training.timer"),
    ("v5-model-promotion-gate.service", "v5-model-promotion-gate.service"),
    ("v5-model-promotion-gate.timer", "v5-model-promotion-gate.timer"),
    ("v5-reconcile.user.service", "v5-reconcile.service"),
    ("v5-reconcile.timer", "v5-reconcile.timer"),
    ("v5-ledger.user.service", "v5-ledger.service"),
    ("v5-ledger.timer", "v5-ledger.timer"),
    ("v5-cost-rollup-real.user.service", "v5-cost-rollup-real.user.service"),
    ("v5-cost-rollup-real.user.timer", "v5-cost-rollup-real.user.timer"),
    ("v5-spread-rollup.user.service", "v5-spread-rollup.service"),
    ("v5-spread-rollup.timer", "v5-spread-rollup.timer"),
)


def normalize_root(root: str) -> str:
    return root.replace("\\", "/").rstrip("/")


def _is_excluded(rel_path: Path) -> bool:
    rel_text = rel_path.as_posix()
    for prefix in PRODUCTION_SYNC_EXCLUDES:
        if rel_text == prefix or rel_text.startswith(prefix + "/"):
            return True
    return any(
        part in {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "node_modules"}
        for part in rel_path.parts
    )


def iter_production_files(workspace_root: Path, items: Iterable[str] = PRODUCTION_SYNC_ITEMS) -> Iterator[Path]:
    root = Path(workspace_root).resolve()
    seen: set[Path] = set()

    for item in items:
        base = (root / item).resolve()
        if not base.exists():
            continue
        if base.is_file():
            rel = base.relative_to(root)
            if not _is_excluded(rel) and base not in seen:
                seen.add(base)
                yield base
            continue

        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(root)
            if _is_excluded(rel):
                continue
            if path.suffix in {".pyc", ".pyo"}:
                continue
            if path not in seen:
                seen.add(path)
                yield path


def production_sync_relative_paths(
    workspace_root: Path,
    items: Iterable[str] = PRODUCTION_SYNC_ITEMS,
) -> set[str]:
    return {
        path.relative_to(Path(workspace_root).resolve()).as_posix()
        for path in iter_production_files(workspace_root, items=items)
    }


def production_sync_roots(items: Iterable[str] = PRODUCTION_SYNC_ITEMS) -> tuple[str, ...]:
    roots: list[str] = []
    seen: set[str] = set()
    for item in items:
        root = Path(item).parts[0]
        if root not in seen:
            seen.add(root)
            roots.append(root)
    return tuple(roots)


def render_unit_text(text: str, root: str, *, drop_user_directive: bool = False) -> str:
    rendered = text
    normalized_root = normalize_root(root)
    for known in KNOWN_DEPLOY_ROOTS:
        rendered = rendered.replace(known, normalized_root)
    rendered = rendered.replace("\r\n", "\n")
    if drop_user_directive:
        rendered = "\n".join(
            line for line in rendered.splitlines() if not line.strip().startswith("User=")
        )
    if not rendered.endswith("\n"):
        rendered += "\n"
    return rendered


def _extract_git_archive(blob: bytes, destination: Path) -> None:
    destination = destination.resolve()
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:") as archive:
        members = archive.getmembers()
        for member in members:
            member_path = destination / member.name
            resolved = member_path.resolve()
            if destination not in resolved.parents and resolved != destination:
                raise RuntimeError(f"unsafe archive member: {member.name}")
            if member.issym() or member.islnk():
                raise RuntimeError(f"unsupported archive link member: {member.name}")
        try:
            archive.extractall(destination, filter="data")
        except TypeError:
            archive.extractall(destination, members=members)


def _git_existing_items(root: Path, rev: str, items: Iterable[str]) -> tuple[str, ...]:
    existing: list[str] = []
    for item in items:
        proc = subprocess.run(
            ["git", "-C", str(root), "ls-tree", "--name-only", rev, "--", item],
            capture_output=True,
            timeout=GIT_COMMAND_TIMEOUT,
            check=False,
        )
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(f"git ls-tree failed for {rev}: {stderr.strip()}")
        if proc.stdout.strip():
            existing.append(item)
    return tuple(existing)


@contextmanager
def production_snapshot(
    workspace_root: Path,
    *,
    rev: str = "HEAD",
    items: Iterable[str] = PRODUCTION_SYNC_ITEMS,
) -> Iterator[Path]:
    root = Path(workspace_root).resolve()
    existing_items = _git_existing_items(root, rev, tuple(items))
    with tempfile.TemporaryDirectory(prefix="prod-release-") as tmp_dir:
        snapshot_root = Path(tmp_dir)
        if not existing_items:
            yield snapshot_root
            return

        archive_cmd = [
            "git",
            "-C",
            str(root),
            "archive",
            "--format=tar",
            rev,
            "--",
            *existing_items,
        ]
        proc = subprocess.run(
            archive_cmd,
            capture_output=True,
            timeout=GIT_COMMAND_TIMEOUT,
            check=False,
        )
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(f"git archive failed for {rev}: {stderr.strip()}")
        _extract_git_archive(proc.stdout, snapshot_root)
        yield snapshot_root
