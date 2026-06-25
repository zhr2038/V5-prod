from __future__ import annotations

import re
from datetime import datetime, timezone


_TIMEFRAME_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.IGNORECASE)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def utc_now_timestamp() -> int:
    return int(utc_now().timestamp())


def timeframe_seconds(timeframe: str) -> int:
    """Return the wall-clock seconds represented by one completed bar."""
    text = str(timeframe or "").strip().lower()
    match = _TIMEFRAME_RE.match(text)
    if not match:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    amount = int(match.group(1))
    if amount <= 0:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    unit = match.group(2).lower()
    multipliers = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 7 * 86400,
    }
    return amount * multipliers[unit]
