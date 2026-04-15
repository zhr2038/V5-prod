from __future__ import annotations

import argparse
import logging
from pathlib import Path

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path
from src.execution.okx_private_client import OKXPrivateClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _resolve_runtime_entry_paths(
    config_path: str | None = None,
    env_path: str | None = None,
) -> tuple[str, str]:
    return (
        resolve_runtime_config_path(config_path, project_root=PROJECT_ROOT),
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
