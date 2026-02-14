from src.portfolio.portfolio_state import PortfolioState


def test_drawdown_updates_peak():
    st = PortfolioState(cash_usdt=100.0, equity_usdt=100.0, peak_equity_usdt=100.0)
    st.update_equity(120.0)
    assert st.peak_equity_usdt == 120.0
    st.update_equity(110.0)
    assert abs(st.drawdown_pct - (10.0 / 120.0)) < 1e-9
