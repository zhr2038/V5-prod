from __future__ import annotations

import logging

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path
from src.execution.okx_private_client import OKXPrivateClient


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    cfg = load_config(
        resolve_runtime_config_path(),
        env_path=resolve_runtime_env_path(".env"),
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
