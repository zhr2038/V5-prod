from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Any, Dict


_ALNUM_RE = re.compile(r"^[A-Za-z0-9]+$")


def _b32_no_pad(b: bytes) -> str:
    return base64.b32encode(b).decode("ascii").rstrip("=")


def _short_tag(text: str, n: int) -> str:
    h = hashlib.sha256(text.encode("utf-8")).digest()
    return _b32_no_pad(h)[: int(n)]


def make_decision_hash(payload: Dict[str, Any]) -> str:
    """Stable decision hash for idempotency.

    Caller should pass a dict built from semantically relevant fields only.
    This function canonicalizes JSON (sorted keys, compact separators).
    """
    s = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def make_cl_ord_id(
    run_id: str,
    inst_id: str,
    intent: str,
    decision_hash: str,
    side: str,
    ord_type: str,
    td_mode: str,
) -> str:
    """Make OKX clOrdId (<=32 alphanumeric chars) bound to *intent-level* semantics.

    Inputs are part of a stable hash; same intent => same clOrdId.

    Constraints enforced:
    - only [A-Za-z0-9]
    - length <= 32
    """

    run_tag = re.sub(r"[^A-Za-z0-9]", "", str(run_id))[-6:] or "RUN"
    inst_tag = _short_tag(str(inst_id), 4)
    intent_tag = _short_tag(str(intent), 2)

    stable = {
        "run_id": str(run_id),
        "inst_id": str(inst_id),
        "intent": str(intent),
        "decision_hash": str(decision_hash),
        "side": str(side),
        "ord_type": str(ord_type),
        "td_mode": str(td_mode),
    }
    s = json.dumps(stable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    h = hashlib.sha256(s.encode("utf-8")).digest()
    h16 = _b32_no_pad(h)[:16]

    clid = f"V5{run_tag}{inst_tag}{intent_tag}{h16}"
    clid = re.sub(r"[^A-Za-z0-9]", "", clid)
    if len(clid) > 32:
        clid = clid[:32]

    if not clid or len(clid) > 32 or not _ALNUM_RE.match(clid):
        raise ValueError(f"invalid clOrdId generated: {clid!r}")
    return clid
