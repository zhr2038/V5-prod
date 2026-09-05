# Forward-paper quote execution

The hourly paper loop created an entry intention, then waited until the next
hourly observation to execute it. On 2026-09-05, BNB intentions at 15:00 and
16:00 Beijing time waited approximately 3,570 and 3,610 seconds. By then the
observed ask exceeded the existing 0.6% price-premium limit, cancelling both
intentions. These are recorded cancellations, not missed fills that can be
inserted retrospectively.

## Behavior

- The existing hourly main run continues to compute completed-bar features,
  choose candidates and create entry intentions.
- `v5-participation-quotes.service` subscribes to public OKX BTC/ETH/SOL/BNB
  spot tickers. An independent public REST fallback refreshes stale streams.
- Every 2 seconds, the worker checks an existing paper intention or position.
  Entry and exit fills require a genuinely later, fresh quote. Quote checks
  cannot generate a new entry signal. Intrahour price, drawdown and time stops
  can create an exit intention, which also requires a subsequent quote.
- The policy kernel, ranking, 80 bps four-hour momentum floor, price-premium
  limit, costs, sizing and other risk limits remain unchanged. Current kill,
  reconciliation, ledger and negative-expectancy state are checked again.
- SQLite serializes the hourly producer and quote worker. State and events
  commit together; duplicate hours, repeated ticks and restarts cannot refill
  an intention. Report publication reads the latest committed event under the
  same lock, so an older hourly report cannot overwrite a later fill.
- Fills, cancellations, exit intentions, valuation validity changes, equity
  peaks and halt transitions persist immediately. Other valuation checkpoints
  are written at most once per minute. A risk peak cannot disappear between
  checkpoints or across a restart.
- The paper panel shows worker freshness, pending intentions, original signal
  time, cancellation reasons and measured signal-to-fill latency. The curve
  keeps at most one observation per hour over 168 hours. Chart placement is
  unchanged. Visible pages refresh command status every 10 seconds, within the
  worker's 30-second freshness boundary; hidden pages skip these requests.

## Cohort and operational boundary

The new cohort uses `reports/participation/forward-quotes-20260905.sqlite` and
`reports/participation/forward-quotes-20260905.latest.json`. The old
`forward.sqlite` and `latest.json` remain intact. A legacy schema or mismatched
code/policy identity fails explicitly rather than migrating or resetting data.
The quote worker waits for the next natural hourly signal to initialize this
cohort; it does not replay a historical signal or invoke the live main loop.

The worker reads public prices and writes only its isolated paper evidence.
It does not import an authenticated exchange client, read an exchange API key,
submit orders, promote a strategy or alter the live portfolio. Faster paper
execution is not evidence of positive future returns or live readiness.

## Validation

- 110 targeted Python tests pass across participation policy, runtime, store,
  quote execution, exit contracts and the command-center API.
- 32 Node tests pass, including actual TSX rendering of pending intentions,
  cancellations, measured fill latency and an expired worker heartbeat.
- Dashboard TypeScript/Vite build and ESLint pass; targeted Python Ruff passes.
- Regression quotes used in tests are synthetic and explicitly labelled. They
  do not constitute historical or production fills.
- The production public WebSocket was read-only tested for all four symbols
  before deployment. Production release, hash, service and rendered-page
  evidence is saved separately with the deployment record.

## Risks and rollback

Hourly signal generation can still miss a move that begins and ends inside an
hour. Public-feed outages, invalid quotes or an existing policy rejection can
still prevent a fill. Checking every 2 seconds is the configured cadence, not
a guaranteed latency under network or host failure. Paper quotes do not model
real exchange fills, depth, queue position or actual slippage. More timely
participation can still lose money.

Deployment uses exact committed bytes, verifies the previous hashes, retains
old immutable Web assets and backs up touched source/configuration plus the
new user service unit. The existing hourly/event/risk timers are briefly paused
only while their services are inactive, then restored to their prior state.
Only the Web service is restarted; the new paper worker is enabled separately.

The release's `remote_apply.py --manifest <release>/manifest.json --rollback`
verifies that no later release has changed the target files, stops/disables the
new paper worker, restores the backed-up files and resumes the original timers.
Both paper cohorts and their evidence are retained. Never reset the production
Git checkout or overwrite a later release to perform a rollback.
