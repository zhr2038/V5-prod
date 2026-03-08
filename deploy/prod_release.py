from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator

KNOWN_DEPLOY_ROOTS = (
    "/home/admin/clawd/v5-trading-bot",
    "/home/admin/clawd/v5-prod",
)

PRODUCTION_SYNC_ITEMS = (
    "main.py",
    "event_driven_check.py",
    "requirements.txt",
    "pyproject.toml",
    "configs",
    "deploy",
    "scripts",
    "src",
    "docs/CURRENT_PRODUCTION_FLOW.md",
    "docs/PRODUCTION_MINIMAL_FILES.md",
    "docs/PRODUCTION_ONLY_DEPLOYMENT.md",
)

PRODUCTION_SYNC_EXCLUDES = (
    ".git",
    ".venv",
    "logs",
    "reports",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "scripts/archive",
)

PRODUCTION_USER_UNIT_MAPPINGS = (
    ("v5-prod.user.service", "v5-prod.user.service"),
    ("v5-prod.user.timer", "v5-prod.user.timer"),
    ("v5-event-driven.service", "v5-event-driven.service"),
    ("v5-event-driven.timer", "v5-event-driven.timer"),
    ("v5-sentiment-collect.service", "v5-sentiment-collect.service"),
    ("v5-sentiment-collect.timer", "v5-sentiment-collect.timer"),
    ("v5-reconcile.user.service", "v5-reconcile.service"),
    ("v5-reconcile.timer", "v5-reconcile.timer"),
    ("v5-ledger.user.service", "v5-ledger.service"),
    ("v5-ledger.timer", "v5-ledger.timer"),
    ("v5-cost-rollup-real.user.service", "v5-cost-rollup-real.user.service"),
    ("v5-cost-rollup-real.user.timer", "v5-cost-rollup-real.user.timer"),
)


def normalize_root(root: str) -> str:
    return root.replace("\\", "/").rstrip("/")


def _is_excluded(rel_path: Path) -> bool:
    rel_text = rel_path.as_posix()
    for prefix in PRODUCTION_SYNC_EXCLUDES:
        if rel_text == prefix or rel_text.startswith(prefix + "/"):
            return True
    return any(part in {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"} for part in rel_path.parts)


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


def render_unit_text(text: str, root: str) -> str:
    rendered = text
    normalized_root = normalize_root(root)
    for known in KNOWN_DEPLOY_ROOTS:
        rendered = rendered.replace(known, normalized_root)
    rendered = rendered.replace("\r\n", "\n")
    if not rendered.endswith("\n"):
        rendered += "\n"
    return rendered
