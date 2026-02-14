from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

from .schema import AppConfig


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
    return AppConfig.model_validate(raw)


def load_blacklist(path: str) -> Dict[str, Any]:
    try:
        p = Path(path)
        if not p.exists():
            return {"symbols": []}
        obj = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {"symbols": []}
