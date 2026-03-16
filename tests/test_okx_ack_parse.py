from __future__ import annotations

from src.execution.live_execution_engine import _parse_okx_order_ack


def test_parse_okx_ack_success() -> None:
    ack = {"code": "0", "msg": "", "data": [{"sCode": "0", "sMsg": "", "ordId": "123"}]}
    ok, ord_id, err_code, err_msg = _parse_okx_order_ack(ack)
    assert ok is True
    assert ord_id == "123"
    assert err_code is None
    assert err_msg is None


def test_parse_okx_ack_reject_by_scode() -> None:
    ack = {"code": "1", "msg": "All operations failed", "data": [{"sCode": "51000", "sMsg": "Parameter sz error"}]}
    ok, ord_id, err_code, err_msg = _parse_okx_order_ack(ack)
    assert ok is False
    assert ord_id is None
    assert err_code == "51000"
    assert "Parameter sz" in (err_msg or "")
