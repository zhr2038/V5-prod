from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# allow running as a script from repo root
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except Exception:
            continue


def _quantiles(xs: List[float], ps: List[float]) -> Dict[str, Optional[float]]:
    if not xs:
        out: Dict[str, Optional[float]] = {f"p{int(p*100)}": None for p in ps}
        out.update({"mean": None, "max": None, "count": 0})
        return out
    xs2 = sorted(xs)
    n = len(xs2)
    out: Dict[str, Optional[float]] = {"count": n, "mean": sum(xs2) / n, "max": xs2[-1]}
    for p in ps:
        # nearest-rank
        k = max(0, min(n - 1, int(round(p * (n - 1)))))
        out[f"p{int(p*100)}"] = xs2[k]
    return out


def notional_bucket(x: float) -> str:
    x = float(x)
    if x < 25:
        return "lt25"
    if x < 50:
        return "25_50"
    if x < 100:
        return "50_100"
    if x < 250:
        return "100_250"
    return "ge250"


def rollup_day(day_yyyymmdd: str, base_dir: str = "reports/cost_events", out_dir: str = "reports/cost_stats") -> Path:
    src = Path(base_dir) / f"{day_yyyymmdd}.jsonl"
    dst = Path(out_dir)
    dst.mkdir(parents=True, exist_ok=True)

    fills: List[Dict[str, Any]] = [e for e in _iter_jsonl(src) if e.get("event_type") == "fill"]

    # group by dims
    groups: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = {}
    missing_bidask = 0
    for e in fills:
        sym = str(e.get("symbol") or "")
        regime = str(e.get("regime") or "Unknown")
        action = str(e.get("router_action") or "fill")
        nb = notional_bucket(float(e.get("notional_usdt") or 0.0))
        key = (sym, regime, action, nb)
        groups.setdefault(key, []).append(e)
        if e.get("bid") is None or e.get("ask") is None:
            missing_bidask += 1

    stats: Dict[str, Any] = {
        "schema_version": 1,
        "day": day_yyyymmdd,
        "coverage": {
            "events_total": len(list(_iter_jsonl(src))),
            "fills": len(fills),
            "missing_bidask": missing_bidask,
        },
        "buckets": {},
    }

    for (sym, regime, action, nb), rows in groups.items():
        def _get_f(name: str) -> List[float]:
            out = []
            for r in rows:
                v = r.get(name)
                if v is None:
                    continue
                try:
                    out.append(float(v))
                except Exception:
                    pass
            return out

        stats["buckets"][f"{sym}|{regime}|{action}|{nb}"] = {
            "count": len(rows),
            "spread_bps": _quantiles(_get_f("spread_bps"), [0.5, 0.75, 0.9, 0.95]),
            "slippage_bps": _quantiles(_get_f("slippage_bps"), [0.5, 0.75, 0.9, 0.95]),
            "fee_bps": _quantiles(_get_f("fee_bps"), [0.5, 0.75, 0.9, 0.95]),
            "cost_bps_total": _quantiles(_get_f("cost_bps_total"), [0.5, 0.75, 0.9, 0.95]),
        }

    out_path = dst / f"daily_cost_stats_{day_yyyymmdd}.json"
    tmp_path = dst / f".daily_cost_stats_{day_yyyymmdd}.json.tmp"
    tmp_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(out_path)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=None, help="UTC day YYYYMMDD; default today(UTC)")
    ap.add_argument("--base_dir", default="reports/cost_events")
    ap.add_argument("--out_dir", default="reports/cost_stats")
    args = ap.parse_args()

    day = args.day
    if not day:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")

    out = rollup_day(day, base_dir=args.base_dir, out_dir=args.out_dir)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
