from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_PATH = "reports/auto_blacklist.json"


@dataclass
class AutoBlacklistEntry:
    symbol: str
    reason: str
    ts_ms: int
    expires_ts_ms: Optional[int] = None
    meta: Optional[Dict[str, Any]] = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"schema_version": 1, "symbols": [], "entries": []}
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {"schema_version": 1, "symbols": [], "entries": []}
    except Exception:
        return {"schema_version": 1, "symbols": [], "entries": []}


def _write(path: str, obj: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def prune(obj: Dict[str, Any], *, now_ms: Optional[int] = None, max_entries: int = 500) -> Dict[str, Any]:
    now_ms = int(now_ms or _now_ms())
    entries = obj.get("entries")
    if not isinstance(entries, list):
        entries = []

    kept: List[Dict[str, Any]] = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        exp = e.get("expires_ts_ms")
        if exp is not None:
            try:
                if int(exp) <= now_ms:
                    continue
            except Exception:
                pass
        kept.append(e)

    # keep newest
    kept.sort(key=lambda x: int(x.get("ts_ms") or 0), reverse=True)
    kept = kept[: int(max_entries)]

    syms = []
    seen = set()
    for e in kept:
        s = str(e.get("symbol") or "").strip()
        if not s:
            continue
        su = s.upper()
        if su in seen:
            continue
        seen.add(su)
        syms.append(s)

    out = dict(obj)
    out["schema_version"] = int(out.get("schema_version") or 1)
    out["updated_ts_ms"] = now_ms
    out["entries"] = list(reversed(kept))  # store chronological
    out["symbols"] = syms
    return out


def add_symbol(
    symbol: str,
    *,
    reason: str,
    path: str = DEFAULT_PATH,
    ttl_sec: Optional[int] = 7 * 24 * 3600,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    s = str(symbol).strip()
    if not s:
        return

    now = _now_ms()
    exp = (now + int(ttl_sec) * 1000) if ttl_sec is not None else None

    obj = _read(path)
    entries = obj.get("entries")
    if not isinstance(entries, list):
        entries = []

    # de-dupe: if exists, just refresh expiry
    su = s.upper()
    found = False
    for e in entries:
        if isinstance(e, dict) and str(e.get("symbol") or "").upper() == su:
            e["ts_ms"] = now
            e["reason"] = str(reason)
            e["expires_ts_ms"] = exp
            if meta is not None:
                e["meta"] = meta
            found = True
            break

    if not found:
        entries.append(
            {
                "symbol": s,
                "reason": str(reason),
                "ts_ms": now,
                "expires_ts_ms": exp,
                "meta": meta or None,
            }
        )

    obj["entries"] = entries
    obj = prune(obj, now_ms=now)
    _write(path, obj)


def read_symbols(path: str = DEFAULT_PATH) -> List[str]:
    obj = prune(_read(path))
    syms = obj.get("symbols")
    if isinstance(syms, list):
        return [str(x) for x in syms]
    return []
