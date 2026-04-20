from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# allow running as a script from repo root
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_path
from src.execution.fill_store import derive_runtime_cost_events_dir, derive_runtime_named_artifact_path


def _resolve_repo_path(value: str | Path | None, *, default: Path) -> Path:
    path = Path(value) if value is not None else default
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


@dataclass(frozen=True)
class RuntimeCostPaths:
    events_dir: Path
    stats_dir: Path


def _resolve_runtime_cost_paths(
    base_dir: str | Path | None = None,
    out_dir: str | Path | None = None,
    *,
    project_root: Path = PROJECT_ROOT,
) -> RuntimeCostPaths:
    root = project_root.resolve()
    if base_dir is not None and out_dir is not None:
        return RuntimeCostPaths(
            events_dir=_resolve_repo_path(base_dir, default=root / "reports" / "cost_events"),
            stats_dir=_resolve_repo_path(out_dir, default=root / "reports" / "cost_stats"),
        )

    cfg = load_runtime_config(project_root=root)
    config_path = Path(resolve_runtime_config_path(project_root=root)).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {config_path}")
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {config_path}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {config_path}")

    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=root,
        )
    ).resolve()
    default_events_dir = derive_runtime_cost_events_dir(orders_db).resolve()
    default_stats_dir = derive_runtime_named_artifact_path(orders_db, "cost_stats", "").resolve()
    return RuntimeCostPaths(
        events_dir=_resolve_repo_path(base_dir, default=default_events_dir),
        stats_dir=_resolve_repo_path(out_dir, default=default_stats_dir),
    )


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


def _is_invalid_cost_event(evt: Dict[str, Any]) -> bool:
    """Drop obviously-bad placeholder events.

    We saw polluted okx_fill events like: ts=0, run_id='r', window_start_ts=1.
    Those break F2 calibration rollups.
    """
    try:
        src = evt.get("source")
        ts = int(evt.get("ts") or 0)
        run_id = str(evt.get("run_id") or "")
        w0 = int(evt.get("window_start_ts") or 0)
    except Exception:
        return True

    if src == "okx_fill":
        if ts <= 0:
            return True
        if run_id in {"r", "", "none", "null"}:
            return True
        # windows are epoch seconds; anything tiny is placeholder
        if w0 > 0 and w0 < 1_600_000_000:
            return True
    return False


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


def _median(xs: List[float]) -> Optional[float]:
    xs2 = [float(x) for x in xs if x is not None]
    if not xs2:
        return None
    xs2.sort()
    n = len(xs2)
    mid = n // 2
    if n % 2 == 1:
        return xs2[mid]
    return 0.5 * (xs2[mid - 1] + xs2[mid])


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


def rollup_day(
    day_yyyymmdd: str,
    base_dir: str | Path | None = None,
    out_dir: str | Path | None = None,
    source: Optional[str] = None,
) -> Path:
    runtime_paths = _resolve_runtime_cost_paths(base_dir, out_dir)
    src = runtime_paths.events_dir / f"{day_yyyymmdd}.jsonl"
    dst = runtime_paths.stats_dir
    dst.mkdir(parents=True, exist_ok=True)

    all_events0 = list(_iter_jsonl(src))
    dropped_invalid = sum(1 for e in all_events0 if _is_invalid_cost_event(e))
    all_events = [e for e in all_events0 if not _is_invalid_cost_event(e)]

    if source:
        src_norm = str(source).strip().lower()
        all_events = [e for e in all_events if str(e.get("source") or "").strip().lower() == src_norm]

    fills: List[Dict[str, Any]] = [e for e in all_events if e.get("event_type") == "fill"]

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
            "events_total": len(all_events),
            "fills": len(fills),
            "missing_bidask": missing_bidask,
            "dropped_invalid": int(dropped_invalid),
            "source": (str(source) if source else None),
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


def _parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def _overall_p50_cost_bps(stats: Dict[str, Any]) -> Optional[float]:
    ps: List[float] = []
    for b in (stats.get("buckets") or {}).values():
        q = (b or {}).get("cost_bps_total") or {}
        p50 = q.get("p50")
        if p50 is None:
            continue
        try:
            ps.append(float(p50))
        except Exception:
            continue
    return _median(ps)


def check_anomaly(
    day_yyyymmdd: str,
    out_dir: str | Path | None = None,
    lookback_days: int = 7,
    multiplier: float = 2.0,
    abs_bps: float = 30.0,
) -> Dict[str, Any]:
    """Detect cost anomalies by comparing today's overall p50(total_cost_bps) vs lookback median.

    - overall p50 is computed as the median of per-bucket p50 values.
    - anomaly if today's p50 >= max(lookback_median * multiplier, abs_bps)
    """

    resolved_out_dir = _resolve_runtime_cost_paths(out_dir=out_dir).stats_dir
    out_path = resolved_out_dir / f"daily_cost_stats_{day_yyyymmdd}.json"
    today_stats = {}
    try:
        today_stats = json.loads(out_path.read_text(encoding="utf-8"))
    except Exception:
        pass

    today_p50 = _overall_p50_cost_bps(today_stats)

    d0 = _parse_yyyymmdd(day_yyyymmdd)
    prev_p50s: List[float] = []
    prev_days: List[str] = []

    for i in range(1, int(lookback_days) + 1):
        di = d0 - timedelta(days=i)
        ds = di.strftime("%Y%m%d")
        p = resolved_out_dir / f"daily_cost_stats_{ds}.json"
        if not p.exists():
            continue
        try:
            s = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        p50 = _overall_p50_cost_bps(s)
        if p50 is None:
            continue
        prev_p50s.append(float(p50))
        prev_days.append(ds)

    baseline = _median(prev_p50s)
    threshold = None
    if baseline is not None:
        threshold = max(float(abs_bps), float(baseline) * float(multiplier))
    else:
        threshold = float(abs_bps)

    is_anomaly = (today_p50 is not None) and (float(today_p50) >= float(threshold))

    report: Dict[str, Any] = {
        "schema_version": 1,
        "day": day_yyyymmdd,
        "today_overall_p50_cost_bps": today_p50,
        "lookback_days": int(lookback_days),
        "lookback_used_days": prev_days,
        "lookback_overall_p50_cost_bps": prev_p50s,
        "baseline_median_p50_cost_bps": baseline,
        "threshold": {
            "multiplier": float(multiplier),
            "abs_bps": float(abs_bps),
            "computed_bps": float(threshold) if threshold is not None else None,
        },
        "is_anomaly": bool(is_anomaly),
    }

    # write sidecar report for ops
    rpt_path = resolved_out_dir / f"daily_cost_anomaly_{day_yyyymmdd}.json"
    tmp = resolved_out_dir / f".daily_cost_anomaly_{day_yyyymmdd}.json.tmp"
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(rpt_path)

    return report


def main() -> None:
    import time

    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=None, help="UTC day YYYYMMDD; default today(UTC)")
    ap.add_argument("--base_dir", default=None)
    ap.add_argument("--out_dir", default=None)
    ap.add_argument("--source", default=None, help="Filter cost_events by event.source (e.g. okx_fill|dry_run)")

    ap.add_argument("--check_anomaly", action="store_true", help="Enable basic cost anomaly detection")
    ap.add_argument("--lookback_days", type=int, default=7)
    ap.add_argument("--anomaly_multiplier", type=float, default=2.0)
    ap.add_argument("--anomaly_abs_bps", type=float, default=30.0)
    args = ap.parse_args()

    day = args.day
    if not day:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")

    t0 = time.time()
    out = rollup_day(day, base_dir=args.base_dir, out_dir=args.out_dir, source=args.source)
    duration_ms = int((time.time() - t0) * 1000)

    # load stats to print an ops-friendly one-line summary
    try:
        d = json.loads(Path(out).read_text(encoding="utf-8"))
    except Exception:
        d = {}

    cov = d.get("coverage") or {}
    bucket_count = len(d.get("buckets") or {})

    print(
        "COST_ROLLUP "
        f"day={day} "
        f"out={out} "
        f"events_total={cov.get('events_total')} "
        f"fills={cov.get('fills')} "
        f"missing_bidask={cov.get('missing_bidask')} "
        f"buckets={bucket_count} "
        f"duration_ms={duration_ms}",
        flush=True,
    )

    if args.check_anomaly:
        rpt = check_anomaly(
            day,
            out_dir=args.out_dir,
            lookback_days=args.lookback_days,
            multiplier=args.anomaly_multiplier,
            abs_bps=args.anomaly_abs_bps,
        )
        print(
            "COST_ANOMALY "
            f"day={day} "
            f"today_overall_p50_cost_bps={rpt.get('today_overall_p50_cost_bps')} "
            f"baseline_median_p50_cost_bps={rpt.get('baseline_median_p50_cost_bps')} "
            f"threshold_bps={(rpt.get('threshold') or {}).get('computed_bps')} "
            f"is_anomaly={rpt.get('is_anomaly')}",
            flush=True,
        )
        if rpt.get("is_anomaly") is True:
            raise SystemExit(2)

    print(f"wrote {out}")


if __name__ == "__main__":
    main()
