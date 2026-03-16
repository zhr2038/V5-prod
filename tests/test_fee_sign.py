from __future__ import annotations

from decimal import Decimal

from src.reporting.fill_trade_exporter import fee_cost_usdt


def test_fee_sign_cost_normalization_base_ccy() -> None:
    # fee is negative (cost) in base currency; convert to positive USDT cost
    cost = fee_cost_usdt(fee="-0.001", fee_ccy="BTC", inst_id="BTC-USDT", fill_px="50000")
    assert cost == Decimal("50")
