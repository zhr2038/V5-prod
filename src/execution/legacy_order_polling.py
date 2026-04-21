from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from configs.runtime_config import load_runtime_config, resolve_runtime_path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TERMINAL_ORDER_STATES = {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "MMP_CANCELED"}


def _normalize_symbol(symbol: str | None) -> str:
    raw = str(symbol or "").strip().upper().replace("-", "/")
    if raw.endswith("/USDT"):
        return raw
    if raw.endswith("-USDT"):
        return raw.replace("-", "/")
    return raw


def _parse_ts_ms(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric <= 0:
            return 0
        return int(numeric * 1000) if numeric < 1_000_000_000_000 else int(numeric)

    raw = str(value).strip()
    if not raw:
        return 0
    try:
        numeric = float(raw)
    except Exception:
        numeric = None
    if numeric is not None:
        if numeric <= 0:
            return 0
        return int(numeric * 1000) if numeric < 1_000_000_000_000 else int(numeric)

    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def first_submit_ts_ms(record: Mapping[str, Any]) -> int:
    for key in ("submitted_ts", "created_ts", "created_at"):
        ts = _parse_ts_ms(record.get(key))
        if ts > 0:
            return ts
    return 0


def status_refresh_ts_ms(record: Mapping[str, Any]) -> int:
    for key in ("updated_ts", "last_checked_ts", "updated_at", "last_poll_ts"):
        ts = _parse_ts_ms(record.get(key))
        if ts > 0:
            return ts
    return 0


def split_whitelist_breach_records(
    records: Sequence[Mapping[str, Any]],
    *,
    whitelist_symbols: Iterable[str],
    release_start_ts: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    whitelist = {_normalize_symbol(symbol) for symbol in (whitelist_symbols or []) if str(symbol or "").strip()}
    current: list[dict[str, Any]] = []
    legacy: list[dict[str, Any]] = []
    for record in records or []:
        symbol = _normalize_symbol(
            str(record.get("symbol") or record.get("inst_id") or "")
        )
        if not symbol or symbol in whitelist:
            continue
        row = dict(record)
        row.setdefault("symbol", symbol)
        submit_ts = first_submit_ts_ms(row)
        row["first_submit_ts"] = submit_ts
        if release_start_ts > 0 and submit_ts > 0 and submit_ts < int(release_start_ts):
            legacy.append(row)
        else:
            current.append(row)
    return current, legacy


@dataclass(frozen=True)
class LegacyOrderPollPolicy:
    allow_legacy_order_backfill: bool = False
    legacy_order_poll_max_age_hours: int = 72
    skip_non_whitelist_legacy_orders: bool = True
    whitelist_symbols: set[str] = field(default_factory=set)


def load_legacy_order_poll_policy(*, project_root: Path | None = None) -> LegacyOrderPollPolicy:
    root = (project_root or PROJECT_ROOT).resolve()
    cfg = load_runtime_config(project_root=root)
    ml_labeler_cfg = cfg.get("ml_labeler") if isinstance(cfg, dict) else None
    if not isinstance(ml_labeler_cfg, dict):
        ml_labeler_cfg = {}
    raw_symbols = cfg.get("symbols") if isinstance(cfg, dict) else None
    symbols = {
        _normalize_symbol(symbol)
        for symbol in (raw_symbols or [])
        if str(symbol or "").strip()
    }
    return LegacyOrderPollPolicy(
        allow_legacy_order_backfill=bool(ml_labeler_cfg.get("allow_legacy_order_backfill", False)),
        legacy_order_poll_max_age_hours=int(ml_labeler_cfg.get("legacy_order_poll_max_age_hours", 72) or 72),
        skip_non_whitelist_legacy_orders=bool(ml_labeler_cfg.get("skip_non_whitelist_legacy_orders", True)),
        whitelist_symbols=symbols,
    )


def legacy_order_poll_skip_reason(
    record: Mapping[str, Any],
    *,
    policy: LegacyOrderPollPolicy,
    now_ms: int,
) -> str | None:
    state = str(record.get("state") or "").upper()
    if state in TERMINAL_ORDER_STATES:
        return "fully_labeled_terminal_order"

    submit_ts = first_submit_ts_ms(record)
    if submit_ts <= 0:
        return None

    age_ms = max(0, int(now_ms) - int(submit_ts))
    max_age_ms = int(policy.legacy_order_poll_max_age_hours) * 3600 * 1000
    if age_ms <= max_age_ms:
        return None

    symbol = _normalize_symbol(str(record.get("symbol") or record.get("inst_id") or ""))
    if (
        policy.skip_non_whitelist_legacy_orders
        and policy.whitelist_symbols
        and symbol
        and symbol not in policy.whitelist_symbols
    ):
        return "non_whitelist_legacy_order"

    if not policy.allow_legacy_order_backfill:
        return "legacy_order_max_age"

    return None
