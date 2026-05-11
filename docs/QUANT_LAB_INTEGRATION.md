# V5 quant-lab Integration

V5 remains the only component that can execute real trading: OKX private API, order placement, cancellation, fill sync, reconcile, ledger, kill-switch, and live preflight stay inside V5. quant-lab is a read-only research and gate service. V5 only calls quant-lab HTTP GET endpoints and never writes the quant-lab lake.

## Boundary

- V5 calls quant-lab for read-only health, live permission, cost estimate, and alpha gate decision.
- quant-lab does not place, cancel, amend, or repair orders.
- V5 does not POST/PUT/PATCH/DELETE to quant-lab and does not write quant-lab lake tables.
- V5 writes local sanitized telemetry under `reports/` and exports a local bundle. quant-lab pulls that bundle from `qyun.hrhome.top`.

## Endpoints

- `GET /v1/health`
- `GET /v1/risk/live-permission?strategy=v5&version=5.0.0`
- `GET /v1/costs/estimate?symbol=BTC-USDT&regime=normal&notional_usdt=200&quantile=p75`
- `GET /v1/gates/decision/{alpha_id}`

## Permissions

`ALLOW`, `SELL_ONLY`, and `ABORT` are combined conservatively:

- Any `ABORT` makes the final permission `ABORT`.
- Otherwise any `SELL_ONLY` makes the final permission `SELL_ONLY`.
- Only `ALLOW + ALLOW` stays `ALLOW`.

Current quant-lab bootstrap gate is `QUARANTINE`, so live permission currently returns `SELL_ONLY`. In that state V5 must not submit new buy/open/rebalance risk. Sell, close, and reduce-only orders remain allowed.

## Failure Policy

- `abort`: quant-lab unavailable blocks orders.
- `sell_only`: quant-lab unavailable allows only sell/close/reduce-only.
- `allow_local_fallback`: quant-lab unavailable falls back to local V5 controls and records fallback telemetry.

## Cost Gate

V5 calls quant-lab cost estimates before order submission. The effective all-in cost is:

```text
effective_total_cost_bps = max(
  quant_lab.total_cost_bps,
  quant_lab.min_cost_bps_floor,
  execution.fee_bps + execution.slippage_bps
)
```

This floor is required because current quant-lab cost buckets can be `public_spread_proxy`, which is useful telemetry but can understate real all-in trading cost. If `order.meta.expected_edge_bps` exists, V5 requires it to exceed `effective_total_cost_bps * cost_min_edge_multiplier`; missing expected edge is audited but not filtered.

## Telemetry

V5 records quant-lab usage in:

- `decision_audit.json`: top-level `quant_lab` summary, permission, cost model, fallback and filter counts.
- `summary.json`: top-level `quant_lab` summary for quick reporting.
- `reports/quant_lab_usage.jsonl`: permission, cost, fallback, final permission, and filter events.
- `reports/quant_lab_requests.jsonl`: GET request summaries only, no auth headers or tokens.

Fallback events include policy and action taken so quant-lab can later audit why V5 ran in `SELL_ONLY`, `ABORT`, or local fallback mode.

## Operations

```bash
python scripts/quant_lab_selfcheck.py --config configs/config.yaml
tail -f reports/quant_lab_usage.jsonl
python scripts/export_v5_bundle.py --reports-dir reports --out-dir /var/lib/v5/exports/bundles
```
