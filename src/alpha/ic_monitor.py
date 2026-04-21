
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd


@dataclass
class AlphaICMonitorConfig:
    history_path: str = "reports/alpha_ic_history.jsonl"
    timeseries_path: str = "reports/alpha_ic_timeseries.jsonl"
    summary_path: str = "reports/alpha_ic_monitor.json"
    max_history_points: int = 500
    roll_points_short: int = 24
    roll_points_long: int = 96
    min_cross_section: int = 6


class AlphaICMonitor:
    """轻量 IC/RankIC 追踪器。

    机制：
    - 每轮保存当下 score / z_factors / close
    - 使用“上一轮特征 vs 本轮收益”构造一步前瞻 IC
    - 产出滚动统计用于因子失效检测与动态权重参考
    """

    def __init__(self, cfg: Optional[AlphaICMonitorConfig] = None):
        self.cfg = cfg or AlphaICMonitorConfig()

    @staticmethod
    def _corr(a: pd.Series, b: pd.Series, method: str = "pearson") -> float:
        try:
            aligned = pd.concat([a, b], axis=1)
            aligned = aligned.replace([np.inf, -np.inf], np.nan).dropna()
            if len(aligned) < 2:
                return 0.0
            lhs = aligned.iloc[:, 0]
            rhs = aligned.iloc[:, 1]
            if lhs.nunique(dropna=True) <= 1 or rhs.nunique(dropna=True) <= 1:
                return 0.0
            x = float(lhs.corr(rhs, method=method))
            if np.isfinite(x):
                return x
        except Exception:
            pass
        return 0.0

    def _read_jsonl(self, path: str) -> List[Dict[str, Any]]:
        p = Path(path)
        if not p.exists():
            return []
        out = []
        try:
            for line in p.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s:
                    continue
                try:
                    out.append(json.loads(s))
                except Exception:
                    continue
        except Exception:
            return []
        return out

    def _append_jsonl(self, path: str, obj: Dict[str, Any]) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def _trim_jsonl(self, path: str, keep_last: int) -> None:
        if keep_last <= 0:
            return
        rows = self._read_jsonl(path)
        if len(rows) <= keep_last:
            return
        p = Path(path)
        p.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in rows[-keep_last:]) + "\n", encoding="utf-8")

    def _build_snapshot(self, *, now_ts_ms: int, alpha_snapshot: Any, closes: Dict[str, float]) -> Dict[str, Any]:
        raw_scores = dict(getattr(alpha_snapshot, "scores", {}) or {})
        telemetry_scores = dict(getattr(alpha_snapshot, "telemetry_scores", {}) or {})
        score_source = telemetry_scores or raw_scores
        return {
            "ts_ms": int(now_ts_ms),
            "ts_iso": datetime.fromtimestamp(int(now_ts_ms) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "scores": score_source,
            "routing_scores": raw_scores,
            "score_source": "telemetry_scores" if telemetry_scores else "scores",
            "z_factors": dict(getattr(alpha_snapshot, "z_factors", {}) or {}),
            "closes": closes,
        }

    def _compute_step_ic(self, prev: Dict[str, Any], cur: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        prev_scores = prev.get("scores") or {}
        prev_factors = prev.get("z_factors") or {}
        prev_closes = prev.get("closes") or {}
        cur_closes = cur.get("closes") or {}

        common = sorted(set(prev_closes.keys()) & set(cur_closes.keys()) & set(prev_scores.keys()))
        if len(common) < int(self.cfg.min_cross_section):
            return None

        # 一步前瞻收益：cur/prev - 1
        rets = pd.Series({s: float(cur_closes[s]) / float(prev_closes[s]) - 1.0 for s in common})

        score_s = pd.Series({s: float(prev_scores[s]) for s in common})
        score_ic = self._corr(score_s, rets, method="pearson")
        score_rank_ic = self._corr(score_s, rets, method="spearman")

        # 因子 IC
        factor_ic: Dict[str, Dict[str, float]] = {}
        # union factor names from prev snapshot
        factor_names = set()
        for s in common:
            z = prev_factors.get(s)
            if isinstance(z, dict):
                factor_names.update(z.keys())

        for fn in sorted(factor_names):
            fv = {}
            for s in common:
                z = prev_factors.get(s)
                if isinstance(z, dict) and fn in z:
                    try:
                        fv[s] = float(z[fn])
                    except Exception:
                        pass
            if len(fv) < int(self.cfg.min_cross_section):
                continue
            fs = pd.Series(fv)
            rr = rets.reindex(fs.index)
            factor_ic[fn] = {
                "ic": self._corr(fs, rr, method="pearson"),
                "rank_ic": self._corr(fs, rr, method="spearman"),
                "count": int(len(fs)),
            }

        return {
            "from_ts_ms": int(prev.get("ts_ms") or 0),
            "to_ts_ms": int(cur.get("ts_ms") or 0),
            "from_ts_iso": prev.get("ts_iso"),
            "to_ts_iso": cur.get("ts_iso"),
            "count": int(len(common)),
            "score_ic": score_ic,
            "score_rank_ic": score_rank_ic,
            "factor_ic": factor_ic,
        }

    @staticmethod
    def _agg(vals: List[float]) -> Dict[str, float]:
        if not vals:
            return {"mean": 0.0, "std": 0.0, "ir": 0.0, "positive_ratio": 0.0, "count": 0}
        arr = np.asarray(vals, dtype=float)
        mean = float(np.mean(arr))
        std = float(np.std(arr))
        ir = float(mean / std) if std > 1e-12 else 0.0
        pos = float(np.mean(arr > 0))
        return {"mean": mean, "std": std, "ir": ir, "positive_ratio": pos, "count": int(len(arr))}

    def _build_summary(self, ts_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        short_n = max(1, int(self.cfg.roll_points_short))
        long_n = max(short_n, int(self.cfg.roll_points_long))

        rows = ts_rows[-long_n:]
        rows_short = rows[-short_n:]

        score_ic_short = [float(r.get("score_ic") or 0.0) for r in rows_short]
        score_rank_short = [float(r.get("score_rank_ic") or 0.0) for r in rows_short]

        score_ic_long = [float(r.get("score_ic") or 0.0) for r in rows]
        score_rank_long = [float(r.get("score_rank_ic") or 0.0) for r in rows]

        # 因子聚合
        factor_names = set()
        for r in rows:
            fi = r.get("factor_ic") or {}
            if isinstance(fi, dict):
                factor_names.update(fi.keys())

        factor_summary = {}
        for fn in sorted(factor_names):
            ic_short = []
            ric_short = []
            ic_long = []
            ric_long = []
            for r in rows_short:
                f = (r.get("factor_ic") or {}).get(fn)
                if isinstance(f, dict):
                    ic_short.append(float(f.get("ic") or 0.0))
                    ric_short.append(float(f.get("rank_ic") or 0.0))
            for r in rows:
                f = (r.get("factor_ic") or {}).get(fn)
                if isinstance(f, dict):
                    ic_long.append(float(f.get("ic") or 0.0))
                    ric_long.append(float(f.get("rank_ic") or 0.0))

            factor_summary[fn] = {
                "ic_short": self._agg(ic_short),
                "rank_ic_short": self._agg(ric_short),
                "ic_long": self._agg(ic_long),
                "rank_ic_long": self._agg(ric_long),
            }

        short_mean = float(np.mean(score_ic_short)) if score_ic_short else 0.0
        long_mean = float(np.mean(score_ic_long)) if score_ic_long else 0.0
        decay_ratio = float(short_mean / long_mean) if abs(long_mean) > 1e-12 else 0.0

        return {
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "score_source": (rows[-1].get("score_source") if rows else "scores"),
            "points_short": len(rows_short),
            "points_long": len(rows),
            "score_ic_short": self._agg(score_ic_short),
            "score_rank_ic_short": self._agg(score_rank_short),
            "score_ic_long": self._agg(score_ic_long),
            "score_rank_ic_long": self._agg(score_rank_long),
            "factor_ic": factor_summary,
            "decay": {
                "short_mean_ic": short_mean,
                "long_mean_ic": long_mean,
                "decay_ratio": decay_ratio,
            },
        }

    def update(self, *, now_ts_ms: int, alpha_snapshot: Any, closes: Dict[str, float]) -> Optional[Dict[str, Any]]:
        snap = self._build_snapshot(now_ts_ms=now_ts_ms, alpha_snapshot=alpha_snapshot, closes=closes)
        history = self._read_jsonl(self.cfg.history_path)

        prev = None
        if history:
            prev = max(
                (item for item in history if isinstance(item, dict)),
                key=lambda item: int(item.get("ts_ms") or 0),
                default=None,
            )
        self._append_jsonl(self.cfg.history_path, snap)
        self._trim_jsonl(self.cfg.history_path, keep_last=max(50, int(self.cfg.max_history_points)))

        if not prev:
            return None

        ts = self._compute_step_ic(prev, snap)
        if ts is None:
            return None
        ts["score_source"] = snap.get("score_source", prev.get("score_source", "scores"))

        self._append_jsonl(self.cfg.timeseries_path, ts)
        self._trim_jsonl(self.cfg.timeseries_path, keep_last=max(50, int(self.cfg.max_history_points)))

        rows = self._read_jsonl(self.cfg.timeseries_path)
        summary = self._build_summary(rows)
        p = Path(self.cfg.summary_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary
