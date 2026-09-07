# Account inventory attribution and exit audit repair — 2026-09-07

Strategy-only lifecycle FIFO could match a recent exit to an old buy after the
account had already disposed of that inventory externally. Base-denominated buy
fees also left phantom inventory. The repair corroborates fills with account
bills before using strategy P&L; it does not edit historical orders or balances.

## Changed contracts

- `src/risk/account_inventory.py`: read-only Decimal reconstruction of account
  inventory. Match trade bills by instrument/trade/currency and check order,
  timestamp, base quantity and quote cash. External sells, conversions and
  transfers consume inventory without being attributed as strategy profit.
  Fees/rebates change the correct currency. Residual quantity/cost and unknown
  initial cost remain explicit. Reconciliation boundaries preserve displaced lot
  evidence. Partial fills of one entry/exit order pair count as one pair.
- `src/risk/negative_expectancy_cooldown.py`: corroborated inventory takes
  precedence over legacy CSV attribution. When an account ledger exists but is
  incomplete/corrupt, publish uncertainty rather than recover a favorable old
  CSV result. Incomplete evidence cannot create or erase a cooldown. Offline
  legacy lifecycle results retain compatibility but are explicitly unverified;
  comparing a result to itself is no longer presented as reconciliation.
- `src/core/pipeline.py`: minimum holding checks use the shared decision time
  and position entry, overriding stale producer `hold_hours`. ATR audit F5 comes
  from the current Alpha snapshot, with entry F5 and provenance separate;
  unavailable current F5 stays unavailable.
- `src/execution/live_execution_engine.py`: an allowed exit keeps its real
  reason/action; it is not labeled as execution of `swing_min_hold_exit_block`.
- `configs/schema.py`: warn when retired ATR exception settings are supplied.
  The tested 24-hour soft ATR rule remains unchanged. Existing hard stop, risk,
  operator exit priority and live risk parameters remain unchanged. Loss/F5
  exception settings were not silently reactivated. The existing post-hold
  net-or-score replacement rule was not converted into a new holding model.
- CI includes the new inventory regressions and the pipeline/live exit tests
  in its focused release job, in addition to the complete suite.

## Validation and limits

Before implementation, formal repository tests reproduced 10 failures: stale
inventory profit, base-fee accounting, external strategy attribution, favorable
CSV fallback, residual visibility, both sides of the 24-hour boundary, two F5
provenance cases, and the executed exit reason. The prior lifecycle test now
asserts explicit uncertainty instead of requiring circular self-verification;
its monetary and order-matching assertions are retained.

Additional tests cover partial fills, fee rebates, very small residuals, missing
initial inventory, excluded probe inventory and malformed/mismatched bills.
Full CI and local evidence results are recorded in the deployment closeout.
Production account replay uses a private, read-only snapshot outside git; raw
account records and credentials are not publication artifacts.

The corroboration contract currently supports spot USDT pairs and fees in base
or quote currency. Unsupported fee currency and incomplete legs are unverified,
not zero-cost estimates. Inventory-attributed realized P&L allocates acquisition
cost proportionally; retained residual cost/value explains the difference from
whole-trade cash flow or marked economic P&L. Historical CSV reports are retained.
Fee signs/currency and the distinction between execution `fillTime` and record
generation `ts` follow the [OKX API contract](https://www.okx.com/docs-v5/).
These repairs improve evidence and timing consistency; they do not establish
improved profitability or validate a replacement holding strategy.

## Release and rollback

Deploy an immutable production package with manifest and dependency/config
checks. Preserve shared fills, bills, orders, account peaks, positions and all
paper databases. Pin the existing A/B/C/D worker to its original `79c4f2ff8cff`
release and output directory: its A arm must not silently consume changed live
source. Keep its identity and event stream; do not migrate or replay it under
this change. Existing participation remains a distinct paper account.

Before activation, back up unit overrides, derived expectancy reports and live
database snapshots. Switch the production symlink while execution timers are
paused and maintenance locks held; do not invoke the trading entrypoint for
verification. Restore timers and observe natural execution/research cycles.
Rollback switches the symlink and startup verifier to the preserved prior
release. Never restore stale account databases over new fills; research stays
pinned to its original source. Dependency, live config and migration versions
must be checked from the actual staged release, not inferred from branch HEAD.
