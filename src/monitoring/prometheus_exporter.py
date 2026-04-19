from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_path
from src.execution.fill_store import (
    derive_fill_store_path,
    derive_runtime_named_artifact_path,
    derive_runtime_runs_dir,
)
from src.reporting.metrics import read_trades_csv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LATENCY_BUCKETS_SECONDS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


@dataclass(frozen=True)
class PrometheusRuntimePaths:
    orders_db: Path
    fills_db: Path
    runs_dir: Path
    telemetry_db: Path


class PrometheusTextBuilder:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.defined: set[str] = set()

    def define(self, name: str, metric_type: str, help_text: str) -> None:
        if name in self.defined:
            return
        self.defined.add(name)
        self.lines.append(f"# HELP {name} {help_text}")
        self.lines.append(f"# TYPE {name} {metric_type}")

    def sample(self, name: str, value: float | int, labels: Optional[Dict[str, Any]] = None) -> None:
        if labels:
            label_text = ",".join(
                f'{key}="{self._escape(value)}"'
                for key, value in sorted(labels.items())
            )
            target = f"{name}{{{label_text}}}"
        else:
            target = name
        self.lines.append(f"{target} {self._format_number(value)}")

    @staticmethod
    def _escape(value: Any) -> str:
        text = str(value)
        return text.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    @staticmethod
    def _format_number(value: float | int) -> str:
        try:
            numeric = float(value)
        except Exception:
            return "NaN"
        if math.isnan(numeric):
            return "NaN"
        if math.isinf(numeric):
            return "+Inf" if numeric > 0 else "-Inf"
        if numeric.is_integer():
            return str(int(numeric))
        return f"{numeric:.12g}"

    def render(self) -> str:
        return "\n".join(self.lines) + "\n"


def resolve_prometheus_runtime_paths(
    *,
    workspace: Path | None = None,
    config: Optional[Dict[str, Any]] = None,
) -> PrometheusRuntimePaths:
    root = (workspace or PROJECT_ROOT).resolve()
    config_path = Path(resolve_runtime_config_path(project_root=root)).resolve()
    cfg = config if isinstance(config, dict) else load_runtime_config(project_root=root)
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
    return PrometheusRuntimePaths(
        orders_db=orders_db,
        fills_db=derive_fill_store_path(orders_db).resolve(),
        runs_dir=derive_runtime_runs_dir(orders_db).resolve(),
        telemetry_db=derive_runtime_named_artifact_path(orders_db, "api_telemetry", ".sqlite").resolve(),
    )


def _append_source_metrics(builder: PrometheusTextBuilder, paths: PrometheusRuntimePaths) -> None:
    builder.define(
        "v5_metrics_source_up",
        "gauge",
        "Whether the runtime metrics source is available.",
    )
    builder.sample("v5_metrics_source_up", 1 if paths.telemetry_db.exists() else 0, {"source": "telemetry_db"})
    builder.sample("v5_metrics_source_up", 1 if paths.orders_db.exists() else 0, {"source": "orders_db"})
    builder.sample("v5_metrics_source_up", 1 if paths.fills_db.exists() else 0, {"source": "fills_db"})
    builder.sample("v5_metrics_source_up", 1 if paths.runs_dir.exists() else 0, {"source": "runs_dir"})


def _query_rows(path: Path, sql: str) -> list[sqlite3.Row]:
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        return list(conn.execute(sql).fetchall())
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def _append_api_metrics(builder: PrometheusTextBuilder, telemetry_db: Path) -> None:
    builder.define(
        "v5_api_requests_total",
        "counter",
        "Total exchange API requests grouped by method, endpoint, and status class.",
    )
    builder.define(
        "v5_api_rate_limit_total",
        "counter",
        "Total exchange API requests classified as rate limited.",
    )
    builder.define(
        "v5_api_latency_seconds",
        "histogram",
        "Observed exchange API latency by method and endpoint.",
    )
    if not telemetry_db.exists():
        return

    request_rows = _query_rows(
        telemetry_db,
        """
        SELECT method, endpoint, status_class, COUNT(*) AS total
        FROM api_request_log
        GROUP BY method, endpoint, status_class
        ORDER BY method, endpoint, status_class
        """,
    )
    for row in request_rows:
        builder.sample(
            "v5_api_requests_total",
            row["total"] or 0,
            {
                "exchange": "okx",
                "method": row["method"] or "UNKNOWN",
                "endpoint": row["endpoint"] or "unknown",
                "status_class": row["status_class"] or "unknown",
            },
        )

    rate_limit_rows = _query_rows(
        telemetry_db,
        """
        SELECT
          method,
          endpoint,
          CASE
            WHEN NULLIF(okx_code, '') IS NOT NULL THEN okx_code
            WHEN http_status IS NOT NULL THEN CAST(http_status AS TEXT)
            ELSE 'unknown'
          END AS reason,
          COUNT(*) AS total
        FROM api_request_log
        WHERE rate_limited = 1
        GROUP BY method, endpoint, reason
        ORDER BY method, endpoint, reason
        """,
    )
    for row in rate_limit_rows:
        builder.sample(
            "v5_api_rate_limit_total",
            row["total"] or 0,
            {
                "exchange": "okx",
                "method": row["method"] or "UNKNOWN",
                "endpoint": row["endpoint"] or "unknown",
                "reason": row["reason"] or "unknown",
            },
        )

    bucket_expr = ", ".join(
        [
            (
                f"SUM(CASE WHEN duration_ms <= {bucket * 1000.0:.6f} "
                f"THEN 1 ELSE 0 END) AS bucket_{index}"
            )
            for index, bucket in enumerate(LATENCY_BUCKETS_SECONDS)
        ]
    )
    latency_rows = _query_rows(
        telemetry_db,
        f"""
        SELECT
          method,
          endpoint,
          {bucket_expr},
          COUNT(*) AS total_count,
          COALESCE(SUM(duration_ms), 0.0) AS total_duration_ms
        FROM api_request_log
        GROUP BY method, endpoint
        ORDER BY method, endpoint
        """,
    )
    for row in latency_rows:
        labels = {
            "exchange": "okx",
            "method": row["method"] or "UNKNOWN",
            "endpoint": row["endpoint"] or "unknown",
        }
        for index, bucket in enumerate(LATENCY_BUCKETS_SECONDS):
            builder.sample(
                "v5_api_latency_seconds_bucket",
                row[f"bucket_{index}"] or 0,
                {**labels, "le": str(bucket)},
            )
        builder.sample(
            "v5_api_latency_seconds_bucket",
            row["total_count"] or 0,
            {**labels, "le": "+Inf"},
        )
        builder.sample("v5_api_latency_seconds_count", row["total_count"] or 0, labels)
        builder.sample(
            "v5_api_latency_seconds_sum",
            float(row["total_duration_ms"] or 0.0) / 1000.0,
            labels,
        )


def _append_order_metrics(builder: PrometheusTextBuilder, orders_db: Path) -> None:
    builder.define(
        "v5_orders_total",
        "gauge",
        "Current order rows grouped by side, intent, and state.",
    )
    builder.define(
        "v5_order_fill_rate",
        "gauge",
        "Filled orders divided by submitted orders for each side and intent.",
    )
    if not orders_db.exists():
        return

    order_rows = _query_rows(
        orders_db,
        """
        SELECT side, intent, state, COUNT(*) AS total
        FROM orders
        GROUP BY side, intent, state
        ORDER BY side, intent, state
        """,
    )
    for row in order_rows:
        builder.sample(
            "v5_orders_total",
            row["total"] or 0,
            {
                "side": row["side"] or "unknown",
                "intent": row["intent"] or "unknown",
                "state": row["state"] or "unknown",
            },
        )

    fill_rate_rows = _query_rows(
        orders_db,
        """
        SELECT
          side,
          intent,
          COUNT(*) AS total_orders,
          SUM(CASE WHEN state = 'FILLED' THEN 1 ELSE 0 END) AS filled_orders
        FROM orders
        GROUP BY side, intent
        ORDER BY side, intent
        """,
    )
    for row in fill_rate_rows:
        total_orders = int(row["total_orders"] or 0)
        filled_orders = int(row["filled_orders"] or 0)
        fill_rate = (float(filled_orders) / float(total_orders)) if total_orders > 0 else float("nan")
        builder.sample(
            "v5_order_fill_rate",
            fill_rate,
            {
                "side": row["side"] or "unknown",
                "intent": row["intent"] or "unknown",
            },
        )


def _append_fill_metrics(builder: PrometheusTextBuilder, fills_db: Path) -> None:
    builder.define(
        "v5_fills_total",
        "gauge",
        "Current fill rows grouped by side.",
    )
    if not fills_db.exists():
        return

    fill_rows = _query_rows(
        fills_db,
        """
        SELECT COALESCE(side, 'unknown') AS side, COUNT(*) AS total
        FROM fills
        GROUP BY COALESCE(side, 'unknown')
        ORDER BY side
        """,
    )
    for row in fill_rows:
        builder.sample(
            "v5_fills_total",
            row["total"] or 0,
            {"side": row["side"] or "unknown"},
        )


def _find_latest_run_dir(runs_dir: Path) -> Optional[Path]:
    if not runs_dir.exists():
        return None
    candidates = [path for path in runs_dir.iterdir() if path.is_dir() and (path / "trades.csv").exists()]
    if not candidates:
        return None
    candidates.sort(key=lambda path: (path / "trades.csv").stat().st_mtime, reverse=True)
    return candidates[0]


def _append_latest_run_metrics(builder: PrometheusTextBuilder, runs_dir: Path) -> None:
    builder.define(
        "v5_latest_run_trade_count",
        "gauge",
        "Trade count for the latest runtime run directory.",
    )
    builder.define(
        "v5_latest_run_realized_pnl_usdt",
        "gauge",
        "Realized PnL in USDT for the latest runtime run directory.",
    )
    builder.define(
        "v5_latest_run_slippage_usdt_total",
        "gauge",
        "Total slippage in USDT for the latest runtime run directory.",
    )
    builder.define(
        "v5_latest_run_fee_usdt_total",
        "gauge",
        "Total fees in USDT for the latest runtime run directory.",
    )
    latest_run_dir = _find_latest_run_dir(runs_dir)
    if latest_run_dir is None:
        return

    trades = read_trades_csv(str(latest_run_dir / "trades.csv"))
    realized_pnl_total = 0.0
    slippage_total = 0.0
    fee_total = 0.0
    for row in trades:
        try:
            fee_total += float(row.get("fee_usdt") or 0.0)
        except Exception:
            pass
        try:
            slippage_total += float(row.get("slippage_usdt") or 0.0)
        except Exception:
            pass
        try:
            raw_pnl = str(row.get("realized_pnl_usdt") or "").strip()
            if raw_pnl:
                realized_pnl_total += float(raw_pnl)
        except Exception:
            pass

    labels = {"run_id": latest_run_dir.name}
    builder.sample("v5_latest_run_trade_count", len(trades), labels)
    builder.sample("v5_latest_run_realized_pnl_usdt", realized_pnl_total, labels)
    builder.sample("v5_latest_run_slippage_usdt_total", slippage_total, labels)
    builder.sample("v5_latest_run_fee_usdt_total", fee_total, labels)


def _append_last_trade_metric(builder: PrometheusTextBuilder, paths: PrometheusRuntimePaths) -> None:
    builder.define(
        "v5_last_trade_timestamp_seconds",
        "gauge",
        "Unix timestamp of the most recent filled order or fill.",
    )
    candidates: list[int] = []

    order_rows = _query_rows(
        paths.orders_db,
        """
        SELECT MAX(
          CASE
            WHEN state = 'FILLED' THEN COALESCE(NULLIF(updated_ts, 0), created_ts)
            ELSE NULL
          END
        ) AS last_trade_ts_ms
        FROM orders
        """,
    )
    if order_rows:
        ts_ms = int(order_rows[0]["last_trade_ts_ms"] or 0)
        if ts_ms > 0:
            candidates.append(ts_ms)

    fill_rows = _query_rows(
        paths.fills_db,
        "SELECT MAX(ts_ms) AS last_fill_ts_ms FROM fills",
    )
    if fill_rows:
        ts_ms = int(fill_rows[0]["last_fill_ts_ms"] or 0)
        if ts_ms > 0:
            candidates.append(ts_ms)

    if candidates:
        builder.sample("v5_last_trade_timestamp_seconds", float(max(candidates)) / 1000.0)


def render_prometheus_metrics(
    *,
    workspace: Path | None = None,
    config: Optional[Dict[str, Any]] = None,
    runtime_paths: PrometheusRuntimePaths | None = None,
) -> str:
    paths = runtime_paths or resolve_prometheus_runtime_paths(workspace=workspace, config=config)
    builder = PrometheusTextBuilder()
    builder.define("v5_metrics_exporter_up", "gauge", "Whether the V5 Prometheus exporter rendered successfully.")
    builder.sample("v5_metrics_exporter_up", 1)
    _append_source_metrics(builder, paths)
    _append_api_metrics(builder, paths.telemetry_db)
    _append_order_metrics(builder, paths.orders_db)
    _append_fill_metrics(builder, paths.fills_db)
    _append_latest_run_metrics(builder, paths.runs_dir)
    _append_last_trade_metric(builder, paths)
    return builder.render()
