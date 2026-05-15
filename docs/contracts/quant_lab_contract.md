# V5 Quant Lab Contract

Version: `v5.quant_lab.telemetry.v2`

Schema version: `1.0.0`

This contract defines the read-only integration between V5 and Quant Lab. V5 remains the trading executor. Quant Lab is a read-only research and gate service. V5 calls Quant Lab over HTTP GET and exports sanitized telemetry for downstream audit and idempotent import.

## Required Event Fields

Every Quant Lab telemetry event emitted by V5 must include:

- `schema_version`: telemetry schema version, currently `1.0.0`
- `contract_version`: integration contract version, currently `v5.quant_lab.telemetry.v2`
- `event_id`: stable idempotency key for the event
- `request_id`: stable request/correlation id for the related API call or local decision
- `run_id`: V5 run id
- `ts_utc`: UTC event timestamp

Events may also keep legacy `ts` for bundle compatibility, but `ts_utc` is authoritative.

## Cost Estimate Request

Endpoint: `GET /v1/costs/estimate`

Required request fields:

- `symbol`: original V5 order symbol, e.g. `BNB/USDT`
- `normalized_symbol`: same normalized symbol used for matching cost buckets
- `venue`: `OKX`
- `instrument_type`: `spot`
- `side`: `buy` or `sell`
- `regime`: requested cost regime
- `notional_usdt`: positive numeric notional
- `quantile`: `p50`, `p75`, or `p90`
- `requested_regime`: original requested regime
- `requested_quantile`: original requested quantile
- `strategy_id`: strategy id, normally `v5`
- `expected_edge_bps`: optional expected edge
- `request_id`: stable request id
- `event_id`: stable event id
- `run_id`: V5 run id
- `ts_utc`: request timestamp
- `contract_version`: `v5.quant_lab.telemetry.v2`

Symbol normalization rules:

- `BNB/USDT` -> `BNB-USDT`
- `BNB-USDT` -> `BNB-USDT`
- `BNBUSDT` -> `BNB-USDT`

## Cost Estimate Response

Required response fields:

- `symbol`
- `regime`
- `notional_usdt`
- `quantile`
- `fee_bps`
- `slippage_bps`
- `spread_bps`
- `total_cost_bps`
- `cost_bps`
- `source`
- `fallback_level`
- `sample_count`
- `cost_model_version`
- optional `total_cost_bps_p50`, `total_cost_bps_p75`, `total_cost_bps_p90`
- optional `required_edge_bps`
- optional `fallback_reason`

V5 must treat `source=global_default` or `fallback_level=GLOBAL_DEFAULT` as a degraded cost model:

- `degraded_cost_model=true`
- `fallback_used_for_cost_model=true`
- `diagnosis=global_default_cost`

HTTP success alone does not mean the cost model is non-degraded.

## Risk Permission Response

Endpoint: `GET /v1/risk/live-permission`

Required response fields:

- `strategy`
- `version`
- `permission`: `ALLOW`, `SELL_ONLY`, or `ABORT`
- `allowed_modes`
- `reasons`
- `cost_model_version`
- `gate_version`
- `created_at`
- `expires_at`
- `status`
- `contract_version`

V5 records the remote decision as `raw_permission_decision`. In `shadow` mode, `raw_permission_decision=ABORT` or `SELL_ONLY` is not an actual block unless the local effective decision also blocks.

## Quant Lab Request Event

Event type: `quant_lab_request`

Required fields:

- all required event fields
- `method=GET`
- `endpoint_path`
- `query_keys`
- `status_code`
- `latency_ms`
- `success`
- `error_type`
- `cached`
- `response_summary`

Do not log `Authorization`, bearer tokens, OKX API keys, secrets, passphrases, private keys, or raw Quant Lab tokens.

## Quant Lab Fallback Event

Event type: `fallback`

Required fields:

- all required event fields
- `fallback_used=true`
- `fallback_reason`
- `fallback_policy`
- `fallback_scope`
- `action_taken`
- `error_type`

HTTP `200` with `success=true` must not be converted into a fallback event. Timeout, connection error, unavailable API, invalid response, or explicit `fallback_used=true` are fallback conditions.

## Quant Lab Cost Usage Event

Event type: `cost_estimate`

Required fields:

- all required event fields
- `request_symbol`
- `normalized_symbol`
- `response_symbol`
- `requested_regime`
- `matched_regime`
- `cost_source`
- `fallback_level`
- `fallback_reason`
- `cost_model_version`
- `sample_count`
- `selected_total_cost_bps`
- `total_cost_bps_p50`
- `total_cost_bps_p75`
- `total_cost_bps_p90`
- `expected_edge_bps`
- `required_edge_bps`
- `would_block_by_cost`
- `cost_gate_enforced`
- `fallback_used`
- `fallback_used_for_cost_model`
- `degraded_cost_model`
- `diagnosis`

If `expected_edge_bps` is missing, V5 must record `filter_reason=expected_edge_missing_no_filter` in shadow or block configured new-risk orders in `cost_only`/`enforce`.

## Local Effective Permission Event

Event type: `final_permission`

Required fields:

- all required event fields
- `raw_permission_decision`
- `local_mode`
- `local_preflight_permission`
- `effective_permission_decision`
- `would_block_if_enforced`
- `fallback_used`
- `fallback_reason`
- `remote_permission_as_of_ts`
- `remote_permission_expires_at`
- `remote_permission_status`

In `shadow`, raw remote `ABORT` plus local `ALLOW` must be recorded as `effective_permission_decision=ALLOW` and `would_block_if_enforced=true`, not as an actual block.

## Trade And Fill Export Schema

`trades.csv` must be parseable by V5 summary and Quant Lab import. Required columns:

- `run_id`
- `ts_utc`
- `symbol`
- `normalized_symbol`
- `side`
- `action`
- `qty`
- `price`
- `notional_usdt`
- `fee`
- `fee_ccy`
- `fee_usdt`
- `slippage_usdt`
- `order_id`
- `trade_id`
- `strategy_id`
- `position_id`

Compatibility columns may also be present:

- `ts`
- `intent`
- `realized_pnl_usdt`
- `realized_pnl_pct`
- `trade_export_schema_version`

If `trades.csv` has non-empty rows with positive `notional_usdt`, V5 summary must report `num_trades > 0`.
`summary.json` must derive `num_trades`, `turnover_usdt`, `fees_usdt_total`, `slippage_usdt_total`,
`cost_usdt_total`, and `fills_count_today` from `trades.csv`; if fields are unavailable, V5 records
`null` in the export row where possible and surfaces `trade_metrics_warning` instead of silently
treating the value as verified zero.

Follow-up bundles must include:

- `summaries/trade_metrics.csv`
- `summaries/fill_metrics.csv`
- `reports/summary_trade_count_mismatch.csv`

Current schema markers:

- `trade_export_schema_version = v5.trade_export.v1`
- `summary_metrics_version = v5.summary_metrics.v1`

## Candidate Snapshot Export Schema

V5 emits candidate-level research snapshots for Quant Lab alpha search. Each run writes
`candidate_snapshot.csv`; follow-up bundles include per-run copies under
`raw/recent_runs/<run_id>/candidate_snapshot.csv` and an aggregate summary at
`summaries/candidate_snapshot.csv`.

Current marker:

- `candidate_snapshot_schema_version = v5.candidate_snapshot.v1`

Required columns:

- `candidate_id`: stable hash of `run_id + symbol + strategy_candidate`
- `run_id`
- `ts_utc`
- `symbol`
- `regime_state`
- `risk_level`
- `current_position`
- `current_weight`
- `target_weight_raw`
- `target_weight_after_risk`
- `final_score`
- `rank`
- `f1_mom_5d`
- `f2_mom_20d`
- `f3_vol_adj_ret`
- `f4_volume_expansion`
- `f5_rsi_trend_confirm`
- `alpha6_score`
- `alpha6_side`
- `ml_score`
- `mean_reversion_score`
- `expected_edge_bps`
- `required_edge_bps`
- `cost_bps`
- `selected_total_cost_bps`
- `cost_source`
- `cost_model_version`
- `cost_gate_verified`
- `would_block_by_cost`
- `cost_reason`
- `eligible_before_filters`
- `final_decision`
- `block_reason`
- `strategy_candidate`

Quant Lab imports this file into `silver/v5_candidate_event`. Labels are derived from
`market_bar` into `gold/v5_candidate_label` at 4h, 8h, 12h, 24h, 48h, 72h, and 120h.
Label outputs should include gross bps, net bps after cost, MFE bps, MAE bps, win, and
label status. Data quality checks should report rows by run, feature completeness, label
completeness, and cost source coverage. Candidate cost fields are populated for actual
orders, blocked candidates, and no-order candidates: Quant Lab cost estimates are used
when present, otherwise V5 writes a degraded `local_estimate` based on configured
roundtrip cost assumptions.
