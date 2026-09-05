# Exchange-style market chart delivery

The chart remains in the existing market detail area under “行情与机会”. The homepage, navigation and account overview keep their current layout. Clicking a candidate opens the chart in the same place.

Code commit: `62de9c0969de7c301494ddb8256f9be95b70f78f` on `feat/web-command-center-20260905`.

## Behavior

- BTC, ETH, SOL and BNB spot candles with 1m, 5m, 15m, 30m, 1h, 4h and 1d periods; the latest 300 bars include the exchange's unclosed candle.
- Candles and a separate volume pane, MA7/25/99, historical OHLC on crosshair, drag, wheel/buttons for zoom, reset to recent candles, full screen and Escape to return.
- Refresh preserves a user's historical viewport. Changing pair or period aborts pending requests and replaces the chart session, so the previous instrument cannot appear under the next label.
- Independent public OKX price, actual rolling 24-hour change, high/low and quote-currency volume. The decision reference price stays separate from current exchange quotes.
- Prices stay unknown when unavailable. Refresh failures retain the last observed chart with an explicit warning; delayed/future exchange timestamps are labelled. Red/green volume bars indicate candle direction, not taker buy/sell volume.
- All displayed times use Beijing time, while data points retain their original UTC timestamps. Daily bars use OKX `1Dutc`, opening at 08:00 Beijing time.
- A holding cost line is drawn only for a matching, positive, currently observed account position.

## Modified files

| Files | Purpose |
|---|---|
| `scripts/web_dashboard.py` | Add GET-only `/api/market_chart`; preserve the legacy kline endpoint. |
| `src/reporting/dashboard_market_chart.py` | Fixed public market GET endpoints, four-pair/seven-period allowlist, bounded 300-bar parsing, eight-second cache with at most 28 keys, explicit upstream errors. No account client or trading/configuration loading. |
| `web/dashboard/src/components/CommandCenter.tsx` | Replace the old chart within the existing detail section and bind cost/reference data to the selected pair. |
| `web/dashboard/src/components/ExchangeChart.tsx` | Chart lifecycle, crosshair, panes, controls, polling, failure states and responsive full screen. |
| `web/dashboard/src/components/exchange-chart.css` | Chart-only visual styling. |
| `web/dashboard/src/lib/marketChart.ts` | Validate response identity/data, assess freshness, format Beijing time and calculate moving averages. |
| `web/dashboard/package.json`, `package-lock.json` | Pin `lightweight-charts` 5.2.1 and its dependency. |
| `tests/test_dashboard_market_chart.py`, `web/dashboard/tests/market-chart.test.mjs` | Data-integrity, read-only route, cache, timestamp, missing-data and identity checks. |
| `web/dashboard/public/legal/*`, `web/dist/*` | License notices and built release assets. Old hashed assets are retained on production for open tabs and rollback. |

## Validation

- `python -m pytest tests/test_dashboard_market_chart.py tests/test_dashboard_command_center.py -q`: 46 passed.
- `node --test tests/market-chart.test.mjs tests/command-center-render.test.mjs`: 16 passed.
- ESLint on the three changed TypeScript files: passed with zero warnings.
- `npm run build`: passed; `git diff --cached --check`: passed before committing.
- Six local deployment-helper tests passed, including rollback after a served-asset mismatch, preserving executable mode, and rejecting pre-existing production drift.
- Actual browser verification at 1280×720 and 390×844: all seven periods, live OHLC/volume, crosshair, drag, zoom without losing the historical viewport on refresh, rapid pair changes, MA toggle, full screen/Escape, no mobile horizontal or vertical overflow in full screen.
- A local-only simulated upstream failure verified visible retry/failure states, retaining an old chart with a warning, clearing old prices after switching pairs, and automatic recovery. No production failure was injected. Physical touch gestures were not tested on a phone.
- Read-only preflight on qyun retrieved 300 current BTC 1m, BTC 1d and BNB 1h candles and valid public tickers. The five modified pre-existing deployment files matched the expected base hashes.

## Operational limits

This is a ten-second polling chart, not a tick stream. It exposes only the most recent 300 bars per timeframe. Public exchange/network outages may interrupt quotes; the chart makes those states explicit. Daily UTC boundaries differ from midnight Beijing bars. No change in this release activates strategies, changes trading parameters or submits orders.

The chart library is locally bundled. Upstream references: [Lightweight Charts](https://github.com/tradingview/lightweight-charts), [OKX candles and ticker API](https://app.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks).

## Production verification and rollback

Deployed on qyun on 2026-09-05 at 11:11:05 Beijing time. The helper returned `DEPLOYED_VERIFIED` for all 22 published files. Loopback HTTP verified command center, equity history, the new chart API, HTML and all seven current JS/CSS assets. External HTTP also verified the chart bundle, styles, HTML and three license files against the committed hashes; a fresh BNB 1h response contained 300 bars and a valid ticker.

The actual production page rendered BNB candles/volume, live quotes and all controls in the existing detail section, with zero browser errors or warnings. Exit from full screen returned to that section. The 13 trading source/configuration hashes were identical before and after publication. The hourly service start and trading/reconciliation/ledger timer states were unchanged; only `v5-web-dashboard.service` was restarted.

Rollback restores the touched source/static files and the previous HTML, while retaining immutable chart assets for existing tabs. The previous deployed Web code was `b6c22cf7a2b8cba6e6461fe2b7334308f4bd6c28`. The rollback helper rejects later conflicting changes instead of overwriting them. This release does not reset or pull the dirty production Git checkout.

Backup: `/home/ubuntu/clawd/v5-prod/.deploy-backups/web-command-center-62de9c0969de-20260905T030858Z`.

On qyun as the deployment user:

```sh
/home/ubuntu/clawd/v5-prod/.venv/bin/python -B /home/ubuntu/clawd/v5-prod/.deploy-releases/web-command-center-62de9c0969de-20260905T030858Z/web_deploy_remote.py --manifest /home/ubuntu/clawd/v5-prod/.deploy-releases/web-command-center-62de9c0969de-20260905T030858Z/manifest.json --rollback
```

Exact local release and verification records are under `E:\v5-prod\output\exchange-chart-20260905`.
