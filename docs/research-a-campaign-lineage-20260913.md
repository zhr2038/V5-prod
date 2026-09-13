# A-group campaign state and trade-count repair, 2026-09-13

The v2 A research account could retain a small, non-executable quantity after an
exit. When a later buy made the symbol active again, the account replaced the
entry time but retained `highest_px`. The original-V5 adapter then supplied that
old campaign peak to the trailing-exit pipeline. BNB and SOL production research
evidence showed new A entries being evaluated against peaks from earlier trades.
This is a research-account defect; it is not evidence of the same defect in the
live position store.

## Lifecycle contract

- Every new position and every executable re-entry over `residual_after_exit`
  starts a new `campaign_id` derived from that entry intent.
- A new campaign initializes `highest_px` from its effective entry basis and
  current fill. The adapter accepts a stored peak only when
  `highest_px_campaign_id` matches the current campaign.
- Adding to an active campaign retains its existing peak.
- Old residual quantity and cost remain in the portfolio. Re-entry records the
  predecessor campaign, carried quantity and carried cost. Exits allocate cost
  in proportion to consumed base quantity; no residual is deleted or valued as
  zero merely because it is not immediately executable.

## Count contract

Research reports expose three different quantities:

- `actual_simulated_fills`: executed simulated fills;
- `exit_allocation_segment_count`: proportional cost-allocation rows produced by
  exits;
- `independent_closed_trade_count`: completed unique research `campaign_id`
  values. Partial exits of one campaign do not add independent trades.

The verified live-account attribution now applies the same distinction at order
level. FIFO inventory matching remains the monetary allocation method, and every
entry-lot allocation remains auditable. `closed_cycles` and
`independent_closed_trade_count` count unique strategy exit orders. When one exit
consumes both old residual inventory and a new entry, it is one independent exit
trade and multiple `inventory_allocation_segment_count` rows. The largest
allocated-cost entry supplies holding-time diagnostics, with quantity and newest
entry used only as deterministic tie-breakers. This prevents a tiny old residual
from being counted as another sample while retaining its PnL and cost.

This count correction can reduce negative-expectancy sample counts relative to
the former entry-lot/exit-order pair count. Net PnL, total allocated cost and bps
expectancy remain sums over all allocation segments. Hard stops, reconciliation,
quote validity and exit authority are unchanged.

## Evidence transition

`v5-review-20260906-v2` remains the immutable predecessor. Deployment stops its
timer, waits for the worker lock, records the ledger identity, event boundary and
SHA-256 evidence, and writes `FROZEN.json`. The worker refuses any output
directory containing that marker.

`v5-review-20260913-v3` starts from a new empty SQLite ledger and independent
100 USDT simulated accounts at the first natural post-activation observation.
No v2 checkpoint, position, peak, fill, PnL or event is copied, replayed or
recalculated. Reports keep the predecessor and transition contract explicit.
V2 and v3 returns must remain separately addressable and must never be joined as
one forward curve.

V3 remains research-only: `live_execution_eligible=false` and
`automatic_live_scaling=false`. This repair improves comparison validity; it is
not profitability evidence and does not authorize a production strategy change.

Rollback freezes v3 before selecting any older research directory. It may switch
the code and report pointer, but it must not restart writes into v2 or overwrite
live orders, fills, balances, account peaks, or either research ledger.
