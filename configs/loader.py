from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - exercised in deployment/runtime validation
    def load_dotenv(*_args, **_kwargs):
        return False

from src.utils.auto_blacklist import resolve_auto_blacklist_path

from .schema import AppConfig


PROJECT_ROOT = Path(__file__).resolve().parents[1]
logger = logging.getLogger(__name__)


def load_config(path: str = "configs/config.yaml", env_path: Optional[str] = ".env") -> AppConfig:
    """Load YAML config with ${ENV} substitution.

    - Loads .env (if present)
    - Parses YAML
    - Resolves ${VAR} occurrences from environment
    - Validates with Pydantic
    """
    if env_path:
        try:
            load_dotenv(env_path, override=True)
        except Exception:
            pass

    p = Path(path)
    raw: Dict[str, Any] = {}
    if p.exists():
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}

    def _resolve(x: Any) -> Any:
        if isinstance(x, str):
            import re

            def repl(m):
                k = m.group(1)
                return os.getenv(k, m.group(0))

            return re.sub(r"\$\{([^}]+)\}", repl, x)
        if isinstance(x, dict):
            return {k: _resolve(v) for k, v in x.items()}
        if isinstance(x, list):
            return [_resolve(v) for v in x]
        return x

    raw = _resolve(raw)
    try:
        return AppConfig.model_validate(raw)
    except Exception:
        logger.exception("Config validation failed: %s", p)
        raise


def load_blacklist(path: str) -> Dict[str, Any]:
    """Load blacklist from configs + optional dynamic auto blacklist.

    Auto blacklist lives at the active runtime's auto_blacklist.json and is merged if present.
    Format:
      {"symbols": ["PEPE/USDT", ...]}
    Auto format:
      {"symbols": [...], "entries": [...]}  (we only consume symbols)
    """

    out = {"symbols": []}

    # static
    try:
        p = Path(path)
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        if p.exists():
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, dict) and isinstance(obj.get("symbols"), list):
                out["symbols"].extend([str(s) for s in obj.get("symbols") or []])
    except Exception:
        pass

    # dynamic
    try:
        ap = resolve_auto_blacklist_path(project_root=PROJECT_ROOT)
        if ap.exists():
            obj = json.loads(ap.read_text(encoding="utf-8"))
            if isinstance(obj, dict) and isinstance(obj.get("symbols"), list):
                out["symbols"].extend([str(s) for s in obj.get("symbols") or []])
    except Exception:
        pass

    # de-dupe
    seen = set()
    merged = []
    for s in out["symbols"]:
        su = str(s).upper()
        if su in seen:
            continue
        seen.add(su)
        merged.append(str(s))
    out["symbols"] = merged

    return out
