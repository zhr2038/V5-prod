"""Read and record the new middle-layer reference in an independent process."""
import argparse
import json
from pathlib import Path

from configs.loader import load_config
from src.execution.fill_store import derive_fill_store_path, derive_runtime_runs_dir
from src.quant_lab_client.reference_consumer import (
    fetch_reference, read_v5_contexts, record_cycle, utc_timestamp,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/live_prod.yaml")
    args = parser.parse_args()
    cfg = load_config(args.config)
    reference = cfg.decision_reference
    if not reference.enabled:
        print(json.dumps({"status": "DISABLED", "live_order_effect": "none"}))
        return
    payload, error = None, None
    try:
        payload = fetch_reference(reference.endpoint)
    except Exception as exc:
        error = "REFERENCE_UNAVAILABLE:" + type(exc).__name__
    orders = Path(cfg.execution.order_store_path)
    contexts = read_v5_contexts(derive_runtime_runs_dir(orders), orders, derive_fill_store_path(orders))
    result = record_cycle(payload=payload, contexts=contexts, now=utc_timestamp(),
                          output=Path(reference.output_path), error=error)
    print(json.dumps({key: result[key] for key in ("status", "mode", "counts", "context_runs", "live_order_effect")}))


if __name__ == "__main__":
    main()
