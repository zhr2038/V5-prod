import sqlite3
import uuid
import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "web_dashboard.py"


def load_web_dashboard_module():
    name = f"web_dashboard_cost_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def build_fills_db(path: Path, rows):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE fills (
            inst_id TEXT,
            trade_id TEXT,
            ts_ms INTEGER,
            ord_id TEXT,
            cl_ord_id TEXT,
            side TEXT,
            exec_type TEXT,
            fill_px TEXT,
            fill_sz TEXT,
            fill_notional TEXT,
            fee TEXT,
            fee_ccy TEXT,
            source TEXT,
            raw_json TEXT,
            created_ts_ms INTEGER
        )
        """
    )
    for idx, row in enumerate(rows, start=1):
        cur.execute(
            """
            INSERT INTO fills (
                inst_id, trade_id, ts_ms, ord_id, cl_ord_id, side, exec_type,
                fill_px, fill_sz, fill_notional, fee, fee_ccy, source, raw_json, created_ts_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["inst_id"],
                str(idx),
                row.get("ts_ms", idx),
                "",
                "",
                row["side"],
                "T",
                str(row["fill_px"]),
                str(row["fill_sz"]),
                str(row.get("fill_notional", "")),
                str(row.get("fee", 0)),
                row.get("fee_ccy", ""),
                "test",
                "{}",
                row.get("ts_ms", idx),
            ),
        )
    conn.commit()
    conn.close()


def test_load_avg_cost_from_fills_handles_base_fee_and_trim(tmp_path):
    module = load_web_dashboard_module()
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    build_fills_db(
        reports_dir / "fills.sqlite",
        [
            {"inst_id": "OKB-USDT", "side": "buy", "fill_px": 97.4, "fill_sz": 0.706776, "fee": -0.000706776, "fee_ccy": "OKB", "ts_ms": 1},
            {"inst_id": "OKB-USDT", "side": "sell", "fill_px": 98.05, "fill_sz": 0.38446, "fee": -0.037696303, "fee_ccy": "USDT", "ts_ms": 2},
            {"inst_id": "OKB-USDT", "side": "sell", "fill_px": 98.4, "fill_sz": 0.322299, "fee": -0.0317142216, "fee_ccy": "USDT", "ts_ms": 3},
            {"inst_id": "OKB-USDT", "side": "sell", "fill_px": 98.41, "fill_sz": 0.000016, "fee": -0.00000157456, "fee_ccy": "USDT", "ts_ms": 4},
            {"inst_id": "OKB-USDT", "side": "buy", "fill_px": 99.01, "fill_sz": 0.01, "fee": -0.000008, "fee_ccy": "OKB", "ts_ms": 5},
            {"inst_id": "OKB-USDT", "side": "buy", "fill_px": 98.67, "fill_sz": 0.557616, "fee": -0.000557616, "fee_ccy": "OKB", "ts_ms": 6},
        ],
    )

    avg_cost = module._load_avg_cost_from_fills("OKB", 0.55705838, reports_dir=reports_dir)

    assert avg_cost == pytest.approx(98.67 * 0.557616 / 0.557058384, rel=1e-6)


def test_load_avg_cost_from_fills_matches_exact_inst_id(tmp_path):
    module = load_web_dashboard_module()
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    build_fills_db(
        reports_dir / "fills.sqlite",
        [
            {"inst_id": "ETHFI-USDT", "side": "buy", "fill_px": 1.5, "fill_sz": 10, "fee": -0.01, "fee_ccy": "ETHFI", "ts_ms": 1},
            {"inst_id": "ETH-USDT", "side": "buy", "fill_px": 1989, "fill_sz": 0.020583, "fee": -0.000020583, "fee_ccy": "ETH", "ts_ms": 2},
        ],
    )

    avg_cost = module._load_avg_cost_from_fills("ETH", 0.020562417, reports_dir=reports_dir)

    assert avg_cost == pytest.approx(1989 * 0.020583 / 0.020562417, rel=1e-6)
