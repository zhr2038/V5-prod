# Crypto Quant Profitability Notes (2026-03-12)

## Bottom line

Crypto quant does not make money because it is "AI" or because it trades often.
It makes money only when all three conditions hold at the same time:

1. There is a real, repeatable edge.
2. The edge survives fees, slippage, and latency.
3. Risk sizing prevents one bad regime from wiping out many small gains.

For this repo, the current architecture is best aligned with:

- liquid-coin spot cross-sectional trend / rotation
- regime-filtered participation
- strong execution and turnover control

It is not naturally aligned with:

- true HFT market making
- latency arbitrage
- basis / funding arbitrage without derivatives support
- deep microstructure alpha built on full depth feeds

## What edges still look plausible

### 1. Large, liquid coin momentum / trend

This is the most natural fit for the current V5 design.

Why:

- the repo is already OKX spot, long-only, multi-asset, hourly
- academic evidence still supports trend-style signals in crypto
- large and liquid coins behave differently from small illiquid coins

Implication:

- focus on BTC / ETH / SOL / BNB / top liquid alts
- prefer cross-sectional ranking among liquid names
- avoid chasing microcaps where spread, impact, and manipulation dominate

### 2. Regime-gated participation

In crypto, not trading is often an edge.

Why:

- many signals decay sharply in choppy periods
- funding, sentiment, and volatility can help decide when to reduce exposure
- a bad regime filter destroys a good alpha less often than a bad alpha survives a bad regime

Implication:

- treat regime as exposure control first, alpha enhancer second
- use risk-off to reduce gross exposure and turnover, not just to relabel the market

### 3. Execution edge and cost control

For spot systems, this is often the difference between slightly positive and negative PnL.

Why:

- if gross alpha is thin, every rebalance hurts
- order type, spread filters, and turnover caps matter more than adding a weak new factor
- OKX increasingly rewards maker-style liquidity and exposes richer order-book feeds through WebSocket

Implication:

- prefer fewer, better trades
- test maker / passive entry logic where feasible
- measure realized cost by symbol, spread bucket, and order type

### 4. Funding / carry / basis style signals

These can be good inputs, but mostly as regime or overlay signals unless the system supports derivatives and borrowing cleanly.

Implication:

- in the current repo, funding is more useful as a participation filter than as the main standalone strategy
- a true carry stack would require swap / futures execution, margin logic, and basis accounting

## What usually does not work for small crypto bots

- trading too many small coins
- using sentiment or ML before proving a basic cost-adjusted trend edge
- overfitting backtests on 24/7 data
- frequent rebalancing with weak score differences
- treating paper alpha as live alpha without slippage calibration
- trying to market-make with retail-grade infra and taker fees

## What the local repo is saying right now

Current repo facts:

- strategy shape: OKX spot, long-only, cross-sectional rotation, hourly bars
- alpha stack: momentum / vol-adjusted return / volume / RSI plus overlays
- regime stack: HMM + funding + RSS
- execution: split orders, spread-aware routing, strong safety checks

Local artifacts suggest the following:

- `reports/backtest_100u_report.json` shows single strategies mostly negative, while the fused multi-strategy variant is only slightly positive
- `reports/alpha_ic_monitor.json` shows weak short-horizon score IC and only modest long-horizon IC
- `reports/walk_forward_real.json` currently does not demonstrate convincing out-of-sample edge

Interpretation:

- the system is already structurally reasonable
- the problem is not "missing enough features"
- the problem is that the live edge appears thin after cost

## Best direction for V5

The highest-probability route is:

1. Narrow universe to the most liquid names.
2. Simplify entry to only high-conviction trend / trend-quality setups.
3. Reduce turnover hard.
4. Upgrade execution quality measurement before adding more ML.
5. Use regime mainly to scale exposure and pause bad environments.

In practice this means:

- keep spot long-only for now
- bias toward top-cap, high-volume coins
- rank by trend quality, not just raw momentum
- only enter when score exceeds a cost-aware threshold
- avoid frequent rank churn around close scores

## Concrete next experiments

### Priority A: prove a clean base edge

- Compare 10-15 liquid symbols versus 30-50 symbols.
- Compare 4h rebalance versus 1h rebalance.
- Compare top-2 / top-3 holdings versus wider portfolios.
- Test a pure trend-quality model without sentiment / ML overlays.

Success condition:

- net return stays positive after calibrated fees and slippage
- turnover drops materially
- performance is stable across walk-forward folds

### Priority B: improve trend-quality ranking

Instead of only simple momentum, test a richer trend score:

- multi-horizon return slope
- distance to recent high
- volume-confirmed breakout quality
- volatility-normalized trend persistence
- drawdown from local high as a penalty

Goal:

- prefer clean, persistent leaders
- avoid noisy pumps and weak mean-reverting names

### Priority C: execution upgrade

- Record fill quality versus mid, spread, and post-trade drift.
- Separate market, aggressive-limit, passive-limit outcomes.
- Add symbol-level "do not chase" rules when spread and volatility widen together.
- Evaluate WebSocket book feeds before trying any faster logic.

### Priority D: ML last, not first

Use ML only after the simpler trend system is already net positive.

Good ML role:

- ranking refinement
- regime classification
- trade/no-trade gating

Bad ML role:

- replacing a nonexistent base edge

## Recommended implementation order

1. Shrink the universe to liquid majors and near-majors only.
2. Build a pure trend-quality benchmark strategy.
3. Re-run cost-aware walk-forward with turnover diagnostics.
4. Tighten entry / exit deadbands and minimum hold windows.
5. Only then test whether ML or sentiment improves the benchmark.

## Key external references

- OKX API guide: WebSocket is recommended for market data and order book depth, and order placement has explicit rate limits.
  - https://my.okx.com/docs-v5/en/
- OKX ELP: maker-style ELP liquidity and optional taker access with speed bump were updated in late 2025 to early 2026.
  - https://www.okx.com/en-us/help/okx-enhanced-liquidity-program
  - https://www.okx.com/help/okx-to-adjust-elp-taker-eligibility
- OKX fees vary by tier and region, so cost modeling should not assume one static fee forever.
  - https://www.okx.com/help/fee-details
- Cambridge JFQA, "A Trend Factor for the Cross-Section of Cryptocurrency Returns":
  trend-style signals remain meaningful in crypto and can survive trading costs, especially when built from richer technical information than simple momentum.
  - https://www.cambridge.org/core/services/aop-cambridge-core/content/view/4C1509ACBA33D5DCAF0AC24379148178/S0022109024000747a.pdf/trend_factor_for_the_cross_section_of_cryptocurrency_returns.pdf
- SSRN, "Impact of Size and Volume on Cryptocurrency Momentum and Reversal":
  large, liquid coins show momentum while small, illiquid coins show more reversal and noise.
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4378429
- NBER working paper evidence using perpetual funding rates:
  funding rates are informative about speculative demand / expected returns and are better used as market state inputs than blindly traded in a spot-only system.
  - https://www.nber.org/system/files/working_papers/w30796/w30796.pdf
