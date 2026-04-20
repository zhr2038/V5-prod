from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.execution.fill_store import FillRow, FillStore, derive_fill_store_path, derive_runtime_runs_dir
from src.execution.order_store import OrderStore
import src.monitoring.api_telemetry as api_telemetry
import src.monitoring.prometheus_exporter as prometheus_exporter
from src.monitoring.api_telemetry import APITelemetryRecord, APITelemetryStore, classify_api_status
from src.monitoring.prometheus_exporter import PrometheusRuntimePaths, render_prometheus_metrics


def test_classify_api_status_handles_rate_limit_codes() -> None:
    assert classify_api_status(http_status=200, okx_code="50011") == "429"
    assert classify_api_status(http_status=200, okx_code="50061") == "429"
    assert classify_api_status(http_status=200, okx_code="51008") == "okx_error"
    assert classify_api_status(http_status=502, okx_code=None) == "5xx"


def test_render_prometheus_metrics_exports_runtime_metrics(tmp_path: Path) -> None:
    orders_db = (tmp_path / "reports" / "orders.sqlite").resolve()
    fills_db = derive_fill_store_path(orders_db).resolve()
    runs_dir = derive_runtime_runs_dir(orders_db).resolve()
    telemetry_db = orders_db.with_name("api_telemetry.sqlite")

    order_store = OrderStore(str(orders_db))
    order_store.upsert_new(
        cl_ord_id="clid-1",
        run_id="run-1",
        inst_id="BTC-USDT",
        side="buy",
        intent="OPEN_LONG",
        decision_hash="hash-1",
        td_mode="cash",
        ord_type="market",
        notional_usdt=100.0,
        req={"demo": True},
    )
    order_store.update_state("clid-1", new_state="FILLED", event_type="TEST")

    fill_store = FillStore(str(fills_db))
    fill_store.upsert_many(
        [
            FillRow(
                inst_id="BTC-USDT",
                trade_id="trade-1",
                ts_ms=1710000000000,
                ord_id="ord-1",
                cl_ord_id="clid-1",
                side="buy",
                exec_type="T",
                fill_px="50000",
                fill_sz="0.002",
                fill_notional="100",
                fee="-0.1",
                fee_ccy="USDT",
                source="fills",
                raw_json="{}",
            )
        ]
    )

    telemetry_store = APITelemetryStore(telemetry_db)
    telemetry_store.record(
        APITelemetryRecord(
            ts_ms=1710000000000,
            exchange="okx",
            method="GET",
            endpoint="/api/v5/trade/order",
            status_class="2xx",
            http_status=200,
            okx_code="0",
            okx_msg="",
            duration_ms=123.0,
            rate_limited=False,
            attempt=1,
            error_type=None,
        )
    )
    telemetry_store.record(
        APITelemetryRecord(
            ts_ms=1710000001000,
            exchange="okx",
            method="GET",
            endpoint="/api/v5/trade/order",
            status_class="429",
            http_status=200,
            okx_code="50011",
            okx_msg="rate limit",
            duration_ms=250.0,
            rate_limited=True,
            attempt=2,
            error_type=None,
        )
    )

    latest_run = runs_dir / "run-1"
    latest_run.mkdir(parents=True, exist_ok=True)
    (latest_run / "trades.csv").write_text(
        "\n".join(
            [
                "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct",
                "2026-04-16T00:00:00Z,run-1,BTC/USDT,OPEN_LONG,buy,0.002,50000,100,0.1,0.2,,",
                "2026-04-16T01:00:00Z,run-1,BTC/USDT,CLOSE_LONG,sell,0.002,50500,101,0.1,0.1,0.8,0.008",
            ]
        ),
        encoding="utf-8",
    )

    body = render_prometheus_metrics(
        runtime_paths=PrometheusRuntimePaths(
            orders_db=orders_db,
            fills_db=fills_db,
            runs_dir=runs_dir,
            telemetry_db=telemetry_db,
        )
    )

    assert "v5_metrics_exporter_up 1" in body
    assert 'v5_api_requests_total{endpoint="/api/v5/trade/order",exchange="okx",method="GET",status_class="2xx"} 1' in body
    assert 'v5_api_rate_limit_total{endpoint="/api/v5/trade/order",exchange="okx",method="GET",reason="50011"} 1' in body
    assert 'v5_orders_total{intent="OPEN_LONG",side="buy",state="FILLED"} 1' in body
    assert 'v5_fills_total{side="buy"} 1' in body
    assert 'v5_latest_run_realized_pnl_usdt{run_id="run-1"} 0.8' in body


def test_resolve_api_telemetry_path_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_telemetry, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        api_telemetry,
        "resolve_runtime_config_path",
        lambda project_root=None: str((tmp_path / "configs" / "live_prod.yaml").resolve()),
    )
    monkeypatch.setattr(api_telemetry, "load_runtime_config", lambda project_root=None: {})

    with pytest.raises(ValueError, match="live_prod.yaml"):
        api_telemetry.resolve_api_telemetry_path(project_root=tmp_path)


def test_resolve_prometheus_runtime_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(prometheus_exporter, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        prometheus_exporter,
        "resolve_runtime_config_path",
        lambda project_root=None: str((tmp_path / "configs" / "live_prod.yaml").resolve()),
    )
    monkeypatch.setattr(prometheus_exporter, "load_runtime_config", lambda project_root=None: {})

    with pytest.raises(ValueError, match="live_prod.yaml"):
        prometheus_exporter.resolve_prometheus_runtime_paths(workspace=tmp_path)


def test_render_prometheus_metrics_prefers_latest_run_by_run_id_epoch_when_trade_file_mtime_is_misleading(tmp_path: Path) -> None:
    orders_db = (tmp_path / "reports" / "orders.sqlite").resolve()
    fills_db = derive_fill_store_path(orders_db).resolve()
    runs_dir = derive_runtime_runs_dir(orders_db).resolve()
    telemetry_db = orders_db.with_name("api_telemetry.sqlite")

    older_run = runs_dir / "20260416_00"
    newer_run = runs_dir / "20260416_01"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    older_trades = older_run / "trades.csv"
    newer_trades = newer_run / "trades.csv"
    older_trades.write_text(
        "\n".join(
            [
                "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct",
                "2026-04-16T00:00:00Z,20260416_00,BTC/USDT,OPEN_LONG,buy,0.002,50000,100,0.1,0.2,,",
                "2026-04-16T01:00:00Z,20260416_00,BTC/USDT,CLOSE_LONG,sell,0.002,50500,101,0.1,0.1,0.5,0.005",
            ]
        ),
        encoding="utf-8",
    )
    newer_trades.write_text(
        "\n".join(
            [
                "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct",
                "2026-04-16T02:00:00Z,20260416_01,BTC/USDT,OPEN_LONG,buy,0.002,50000,100,0.1,0.2,,",
                "2026-04-16T03:00:00Z,20260416_01,BTC/USDT,CLOSE_LONG,sell,0.002,50500,101,0.1,0.1,1.1,0.011",
            ]
        ),
        encoding="utf-8",
    )
    os.utime(older_trades, (200, 200))
    os.utime(newer_trades, (100, 100))

    body = render_prometheus_metrics(
        runtime_paths=PrometheusRuntimePaths(
            orders_db=orders_db,
            fills_db=fills_db,
            runs_dir=runs_dir,
            telemetry_db=telemetry_db,
        )
    )

    assert 'v5_latest_run_realized_pnl_usdt{run_id="20260416_01"} 1.1' in body
