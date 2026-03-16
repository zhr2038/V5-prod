from __future__ import annotations

from pathlib import Path
from typing import Any


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _window_metric(window: dict[str, Any], metric_name: str) -> float | None:
    summary = window.get("summary") or {}
    metrics = summary.get("metrics") or {}
    return _safe_float(metrics.get(metric_name))


def _variant_by_name(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item.get("name")): item for item in (report.get("results") or [])}


def _windows_by_name(windows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(item.get("name")): item for item in (windows or [])}


def _diff_metrics(
    champion_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    metric_names: list[str],
) -> dict[str, float | None]:
    diffs: dict[str, float | None] = {}
    for metric_name in metric_names:
        champion_value = _safe_float(champion_metrics.get(metric_name))
        baseline_value = _safe_float(baseline_metrics.get(metric_name))
        key = f"delta_{metric_name}"
        if champion_value is None or baseline_value is None:
            diffs[key] = None
        else:
            diffs[key] = champion_value - baseline_value
    return diffs


def summarize_hotpath_report(
    report: dict[str, Any],
    *,
    champion_name: str,
    baseline_name: str,
) -> dict[str, Any]:
    variants = _variant_by_name(report)
    champion = variants[str(champion_name)]
    baseline = variants[str(baseline_name)]

    champion_agg = dict(champion.get("aggregate") or {})
    baseline_agg = dict(baseline.get("aggregate") or {})
    metric_names = [
        "mean_total_return",
        "mean_sharpe",
        "max_max_dd",
        "mean_turnover",
        "positive_windows",
        "negative_windows",
        "flat_windows",
    ]
    aggregate_diff = _diff_metrics(champion_agg, baseline_agg, metric_names)

    champion_windows = _windows_by_name(champion.get("windows") or [])
    baseline_windows = _windows_by_name(baseline.get("windows") or [])
    evaluation_names = [str(item.get("name")) for item in (report.get("evaluations") or [])]

    per_window: list[dict[str, Any]] = []
    champion_win_count = 0
    baseline_win_count = 0
    tie_count = 0

    for evaluation_name in evaluation_names:
        champion_window = champion_windows.get(evaluation_name) or {}
        baseline_window = baseline_windows.get(evaluation_name) or {}
        champion_return = _window_metric(champion_window, "total_return")
        baseline_return = _window_metric(baseline_window, "total_return")
        champion_sharpe = _window_metric(champion_window, "sharpe")
        baseline_sharpe = _window_metric(baseline_window, "sharpe")
        champion_dd = _window_metric(champion_window, "max_dd")
        baseline_dd = _window_metric(baseline_window, "max_dd")
        champion_turnover = _window_metric(champion_window, "turnover")
        baseline_turnover = _window_metric(baseline_window, "turnover")

        if champion_return is not None and baseline_return is not None:
            if champion_return > baseline_return:
                champion_win_count += 1
            elif champion_return < baseline_return:
                baseline_win_count += 1
            else:
                tie_count += 1

        per_window.append(
            {
                "name": evaluation_name,
                "champion_total_return": champion_return,
                "baseline_total_return": baseline_return,
                "delta_total_return": (
                    None
                    if champion_return is None or baseline_return is None
                    else champion_return - baseline_return
                ),
                "champion_sharpe": champion_sharpe,
                "baseline_sharpe": baseline_sharpe,
                "champion_max_dd": champion_dd,
                "baseline_max_dd": baseline_dd,
                "champion_turnover": champion_turnover,
                "baseline_turnover": baseline_turnover,
            }
        )

    return {
        "generated_at": report.get("generated_at"),
        "workers": report.get("workers"),
        "parallel_granularity": report.get("parallel_granularity"),
        "baseline_name": str(baseline_name),
        "champion_name": str(champion_name),
        "baseline_aggregate": baseline_agg,
        "champion_aggregate": champion_agg,
        "aggregate_diff": aggregate_diff,
        "per_window": per_window,
        "champion_window_wins": int(champion_win_count),
        "baseline_window_wins": int(baseline_win_count),
        "tie_windows": int(tie_count),
    }


def summarize_shadow_report(report: dict[str, Any]) -> dict[str, Any]:
    windows = report.get("windows") or []
    metric_names = [
        "total_return",
        "sharpe",
        "max_dd",
        "turnover",
        "profit_factor",
    ]
    window_summaries: list[dict[str, Any]] = []
    positive_windows = 0
    negative_windows = 0
    flat_windows = 0

    for window in windows:
        metrics = {
            metric_name: _window_metric(window, metric_name)
            for metric_name in metric_names
        }
        total_return = metrics.get("total_return")
        if total_return is None:
            pass
        elif total_return > 0:
            positive_windows += 1
        elif total_return < 0:
            negative_windows += 1
        else:
            flat_windows += 1

        window_summaries.append(
            {
                "name": str(window.get("name")),
                "ohlcv_limit": window.get("ohlcv_limit"),
                "window_shift_bars": window.get("window_shift_bars"),
                "metrics": metrics,
                "window_dir": window.get("window_dir"),
            }
        )

    return {
        "generated_at": report.get("generated_at"),
        "workers": report.get("workers"),
        "symbols": list(report.get("symbols") or []),
        "overrides": dict(report.get("overrides") or {}),
        "positive_windows": int(positive_windows),
        "negative_windows": int(negative_windows),
        "flat_windows": int(flat_windows),
        "windows": window_summaries,
    }


def build_shadow_cycle_summary(
    *,
    hotpath_report: dict[str, Any],
    shadow_report: dict[str, Any],
    champion_name: str,
    baseline_name: str,
) -> dict[str, Any]:
    hotpath = summarize_hotpath_report(
        hotpath_report,
        champion_name=champion_name,
        baseline_name=baseline_name,
    )
    shadow = summarize_shadow_report(shadow_report)

    champion_agg = hotpath.get("champion_aggregate") or {}
    baseline_agg = hotpath.get("baseline_aggregate") or {}
    champion_mean_return = _safe_float(champion_agg.get("mean_total_return")) or 0.0
    baseline_mean_return = _safe_float(baseline_agg.get("mean_total_return")) or 0.0
    champion_mean_sharpe = _safe_float(champion_agg.get("mean_sharpe")) or 0.0
    baseline_mean_sharpe = _safe_float(baseline_agg.get("mean_sharpe")) or 0.0
    champion_max_dd = _safe_float(champion_agg.get("max_max_dd")) or 0.0
    baseline_max_dd = _safe_float(baseline_agg.get("max_max_dd")) or 0.0

    full_window = next((item for item in shadow["windows"] if item["name"] == "full_cached_latest"), None)
    recent_window = next((item for item in shadow["windows"] if item["name"] == "recent_1440"), None)
    full_return = _safe_float(((full_window or {}).get("metrics") or {}).get("total_return")) or 0.0
    recent_return = _safe_float(((recent_window or {}).get("metrics") or {}).get("total_return")) or 0.0

    recommend_shadow = (
        champion_mean_return > baseline_mean_return
        and champion_mean_sharpe > baseline_mean_sharpe
        and champion_max_dd <= max(baseline_max_dd * 1.05, baseline_max_dd + 1e-9)
        and int(hotpath.get("champion_window_wins") or 0) >= int(hotpath.get("baseline_window_wins") or 0)
        and full_return > 0.0
        and recent_return > 0.0
    )

    return {
        "hotpath": hotpath,
        "shadow": shadow,
        "decision": {
            "champion_name": champion_name,
            "baseline_name": baseline_name,
            "recommend_shadow": bool(recommend_shadow),
            "reason": (
                "champion_beats_baseline_and_shadow_is_positive"
                if recommend_shadow
                else "keep_monitoring"
            ),
        },
    }


def build_shadow_cycle_markdown(
    *,
    summary: dict[str, Any],
    hotpath_report_path: Path,
    shadow_report_path: Path,
) -> str:
    hotpath = summary["hotpath"]
    shadow = summary["shadow"]
    decision = summary["decision"]
    champion_agg = hotpath["champion_aggregate"]
    baseline_agg = hotpath["baseline_aggregate"]

    lines = [
        "# Core6 AVAX Shadow Cycle",
        "",
        f"- decision: `{decision['reason']}`",
        f"- recommend_shadow: `{str(bool(decision['recommend_shadow'])).lower()}`",
        f"- champion: `{decision['champion_name']}`",
        f"- baseline: `{decision['baseline_name']}`",
        f"- hotpath_report: `{hotpath_report_path}`",
        f"- shadow_report: `{shadow_report_path}`",
        "",
        "## Hotpath AB",
        "",
        f"- champion mean_total_return: `{champion_agg.get('mean_total_return')}`",
        f"- baseline mean_total_return: `{baseline_agg.get('mean_total_return')}`",
        f"- champion mean_sharpe: `{champion_agg.get('mean_sharpe')}`",
        f"- baseline mean_sharpe: `{baseline_agg.get('mean_sharpe')}`",
        f"- champion max_max_dd: `{champion_agg.get('max_max_dd')}`",
        f"- baseline max_max_dd: `{baseline_agg.get('max_max_dd')}`",
        f"- champion window wins: `{hotpath.get('champion_window_wins')}`",
        f"- baseline window wins: `{hotpath.get('baseline_window_wins')}`",
        f"- tie windows: `{hotpath.get('tie_windows')}`",
        "",
        "## Champion Shadow",
        "",
        f"- positive_windows: `{shadow.get('positive_windows')}`",
        f"- negative_windows: `{shadow.get('negative_windows')}`",
        f"- flat_windows: `{shadow.get('flat_windows')}`",
        "",
        "| window | total_return | sharpe | max_dd | turnover |",
        "|---|---:|---:|---:|---:|",
    ]

    for window in shadow["windows"]:
        metrics = window["metrics"]
        lines.append(
            "| {name} | {ret:.10f} | {sharpe:.4f} | {max_dd:.4f} | {turnover:.6f} |".format(
                name=window["name"],
                ret=float(metrics.get("total_return") or 0.0),
                sharpe=float(metrics.get("sharpe") or 0.0),
                max_dd=float(metrics.get("max_dd") or 0.0),
                turnover=float(metrics.get("turnover") or 0.0),
            )
        )

    return "\n".join(lines) + "\n"
