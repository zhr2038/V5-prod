from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config  # noqa: E402
from scripts.cost_probe_live_once import (  # noqa: E402
    AUTHORIZATION_HMAC_SECRET_ENV,
    AUTHORIZATION_MAX_TTL_SEC,
    AUTHORIZATION_OPERATOR_ALLOWLIST_ENV,
    AUTHORIZATION_SCOPE,
    REQUIRED_ACKS,
    _authorization_hmac_signature,
    _authorization_signature_sha256,
    build_live_probe_preflight,
)


def build_authorization_payload(
    *,
    preflight: dict[str, Any],
    signed_by: str,
    secret: str,
    ttl_sec: int = AUTHORIZATION_MAX_TTL_SEC,
    authorization_id: str | None = None,
    nonce: str | None = None,
    now: datetime | None = None,
    max_notional_usdt: str | float | None = None,
) -> dict[str, Any]:
    required = preflight.get("required_authorization")
    if not isinstance(required, dict):
        raise ValueError("required_authorization_missing")
    ttl = max(1, min(int(ttl_sec), AUTHORIZATION_MAX_TTL_SEC))
    issued = (now or datetime.now(UTC)).astimezone(UTC)
    expires = issued + timedelta(seconds=ttl)
    required_notional = Decimal(str(required.get("max_notional_usdt") or "0"))
    requested_notional = Decimal(
        str(max_notional_usdt if max_notional_usdt is not None else required_notional)
    )
    if requested_notional <= 0:
        raise ValueError("max_notional_usdt_must_be_positive")
    if required_notional > 0 and requested_notional > required_notional:
        raise ValueError("max_notional_usdt_exceeds_preflight")
    payload = {
        "scope": AUTHORIZATION_SCOPE,
        "authorization_id": authorization_id or f"cost-probe-{uuid.uuid4()}",
        "nonce": nonce or secrets.token_urlsafe(18),
        "code_sha": str(required.get("code_sha") or ""),
        "config_sha256": str(required.get("config_sha256") or ""),
        "signed_by": signed_by,
        "signature": "",
        "approved_live_order_execution": True,
        "symbol": str(required.get("symbol") or ""),
        "max_notional_usdt": str(requested_notional),
        "issued_at": issued.isoformat().replace("+00:00", "Z"),
        "expires_at": expires.isoformat().replace("+00:00", "Z"),
        "acknowledged_risks": sorted(REQUIRED_ACKS),
    }
    payload["signature"] = _authorization_hmac_signature(payload, secret)
    return payload


def _write_pending_authorization(path: Path, payload: dict[str, Any]) -> None:
    if not path.name.endswith(".pending.json"):
        raise ValueError("authorization_output_must_end_with_pending_json")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    except Exception:
        try:
            path.unlink(missing_ok=True)
        finally:
            raise
    try:
        path.chmod(0o600)
    except Exception:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create one signed V5 cost-probe authorization pending file. This does not submit orders."
    )
    parser.add_argument("--config", default="configs/live_prod.yaml")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--out", required=True, help="Destination path ending in .pending.json")
    parser.add_argument("--signed-by", required=True)
    parser.add_argument("--ttl-sec", type=int, default=AUTHORIZATION_MAX_TTL_SEC)
    parser.add_argument("--max-notional-usdt", default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    out = Path(args.out)
    if out.exists():
        if not args.force:
            raise SystemExit(f"authorization file already exists: {out}")
        out.unlink()
    secret = os.environ.get(AUTHORIZATION_HMAC_SECRET_ENV)
    if not secret:
        raise SystemExit(f"{AUTHORIZATION_HMAC_SECRET_ENV} is required")
    allowlist = {
        item.strip()
        for item in str(os.environ.get(AUTHORIZATION_OPERATOR_ALLOWLIST_ENV) or "").split(",")
        if item.strip()
    }
    if not allowlist:
        raise SystemExit(f"{AUTHORIZATION_OPERATOR_ALLOWLIST_ENV} is required")
    if args.signed_by not in allowlist:
        raise SystemExit("signed_by is not in V5_COST_PROBE_AUTH_OPERATORS")

    from src.execution.okx_private_client import OKXPrivateClient

    cfg = load_config(args.config)
    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        preflight = build_live_probe_preflight(
            cfg,
            reports_dir=args.reports_dir,
            auth_path=out,
            okx=client,
        )
    finally:
        client.close()
    p3 = preflight.get("p3_preflight") if isinstance(preflight.get("p3_preflight"), dict) else {}
    if p3.get("state") != "READY_FOR_MANUAL_AUTHORIZATION":
        raise SystemExit(
            "cost probe P3 preflight is not ready: "
            + ",".join(str(item) for item in (p3.get("blockers") or []))
        )
    unexpected = [
        blocker
        for blocker in list(preflight.get("blockers") or [])
        if blocker != "manual_authorization_file_missing_or_invalid"
    ]
    if unexpected:
        raise SystemExit("live preflight has unexpected blockers: " + ",".join(unexpected))
    payload = build_authorization_payload(
        preflight=preflight,
        signed_by=args.signed_by,
        secret=secret,
        ttl_sec=args.ttl_sec,
        max_notional_usdt=args.max_notional_usdt,
    )
    _write_pending_authorization(out, payload)
    print(
        json.dumps(
            {
                "state": "AUTHORIZATION_PENDING_FILE_CREATED",
                "path": str(out),
                "scope": payload["scope"],
                "authorization_id": payload["authorization_id"],
                "signed_by": payload["signed_by"],
                "symbol": payload["symbol"],
                "max_notional_usdt": payload["max_notional_usdt"],
                "issued_at": payload["issued_at"],
                "expires_at": payload["expires_at"],
                "signature_sha256": _authorization_signature_sha256(payload),
                "next_action": "run_cost_probe_live_once_without_execute_flags",
                "no_order_submitted": True,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
