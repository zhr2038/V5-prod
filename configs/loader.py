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


def _warn_backtest_cost_below_live(cfg: AppConfig, *, path: Path) -> None:
    try:
        bt_fee = float(getattr(cfg.backtest, "fee_bps", 0.0) or 0.0)
        live_fee = float(getattr(cfg.execution, "fee_bps", 0.0) or 0.0)
        if bt_fee < live_fee:
            logger.warning(
                "Backtest fee_bps below live execution fee_bps in %s: backtest.fee_bps=%s < execution.fee_bps=%s",
                path,
                bt_fee,
                live_fee,
            )
    except Exception:
        logger.warning("Unable to compare backtest fee_bps with live execution fee_bps in %s", path)

    try:
        bt_slippage = float(getattr(cfg.backtest, "slippage_bps", 0.0) or 0.0)
        live_slippage = float(getattr(cfg.execution, "slippage_bps", 0.0) or 0.0)
        if bt_slippage < live_slippage:
            logger.warning(
                "Backtest slippage_bps below live execution slippage_bps in %s: "
                "backtest.slippage_bps=%s < execution.slippage_bps=%s",
                path,
                bt_slippage,
                live_slippage,
            )
    except Exception:
        logger.warning("Unable to compare backtest slippage_bps with live execution slippage_bps in %s", path)


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
        cfg = AppConfig.model_validate(raw)
        _warn_backtest_cost_below_live(cfg, path=p)
        return cfg
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
