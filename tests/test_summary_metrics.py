from src.reporting.metrics import compute_equity_metrics


def test_max_dd_and_return():
    eq_rows = [
        {"equity": 100.0},
        {"equity": 120.0},
        {"equity": 110.0},
    ]
    m = compute_equity_metrics(eq_rows)
    assert abs(m["total_return_pct"] - 0.1) < 1e-9
    assert abs(m["max_drawdown_pct"] - (10.0 / 120.0)) < 1e-9
