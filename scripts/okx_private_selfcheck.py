from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def resolve_runtime_config_path(raw_config_path=None, project_root=None):
    from configs.runtime_config import resolve_runtime_config_path as _resolve_runtime_config_path

    return _resolve_runtime_config_path(raw_config_path, project_root=project_root)


def resolve_runtime_env_path(raw_env_path=None, project_root=None):
    from configs.runtime_config import resolve_runtime_env_path as _resolve_runtime_env_path

    return _resolve_runtime_env_path(raw_env_path, project_root=project_root)


def load_runtime_config(raw_config_path=None, project_root=None):
    from configs.runtime_config import load_runtime_config as _load_runtime_config

    return _load_runtime_config(raw_config_path, project_root=project_root)


def load_config(path, env_path=None):
    from configs.loader import load_config as _load_config

    return _load_config(path, env_path=env_path)


def OKXPrivateClient(*args, **kwargs):
    from src.execution.okx_private_client import OKXPrivateClient as _OKXPrivateClient

    return _OKXPrivateClient(*args, **kwargs)


def _resolve_runtime_entry_paths(
    config_path: str | None = None,
    env_path: str | None = None,
) -> tuple[str, str]:
    resolved_config_path = Path(resolve_runtime_config_path(config_path, project_root=PROJECT_ROOT)).resolve()
    if not resolved_config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {resolved_config_path}")
    cfg = load_runtime_config(config_path, project_root=PROJECT_ROOT)
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {resolved_config_path}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {resolved_config_path}")
    return (
        str(resolved_config_path),
        resolve_runtime_env_path(env_path, project_root=PROJECT_ROOT),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run a private OKX credential self-check.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--env", default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    resolved_config_path, resolved_env_path = _resolve_runtime_entry_paths(args.config, args.env)
    cfg = load_config(
        resolved_config_path,
        env_path=resolved_env_path,
    )

    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        r = client.get_balance(ccy="USDT")
        logging.info(f"HTTP={r.http_status} code={r.okx_code} msg={r.okx_msg}")
        logging.info(r.data)
    finally:
        client.close()


if __name__ == "__main__":
    main()
