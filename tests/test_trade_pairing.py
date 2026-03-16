from src.reporting.trade_log import Fill
from src.reporting.metrics import compute_trade_metrics


def test_profit_factor_win_rate_from_realized():
    trades = [
        {"notional_usdt": "100", "fee_usdt": "0", "slippage_usdt": "0", "realized_pnl_usdt": "10"},
        {"notional_usdt": "100", "fee_usdt": "0", "slippage_usdt": "0", "realized_pnl_usdt": "-5"},
    ]
    m = compute_trade_metrics(trades, avg_equity=1000)
    assert abs(m["win_rate"] - 0.5) < 1e-9
    assert abs(m["profit_factor"] - 2.0) < 1e-6
