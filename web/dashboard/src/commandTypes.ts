export interface ObservedCount {
  value: number | null;
  unit: string;
  status: string;
}

export interface CommandHealth {
  status: string;
  observed_at?: string | null;
  age_seconds?: number | null;
  level?: string | null;
  enabled?: boolean | null;
  ok?: boolean | null;
  dd_pct?: number | null;
  reason?: string | null;
  config?: Record<string, unknown>;
}

export interface CommandCandidate {
  symbol: string;
  alpha_score: number | null;
  target_weight: number | null;
  router_reasons: string[];
  selection_reasons?: string[];
  reference_price?: number | null;
  price_as_of?: string | null;
  direction?: string | null;
  momentum_4h_bps?: number | null;
  volume_confirm?: number | null;
  rsi_confirm?: number | null;
}

export interface ParticipationAction {
  action?: string;
  reason?: string;
  symbol?: string;
  side?: string;
  decision_ts?: number;
  latency_seconds?: number;
  candidate_reasons?: Record<string, string>;
}

export interface ParticipationSummary {
  enabled: boolean | null;
  mode: string;
  status: string;
  observed_at?: string | null;
  age_seconds?: number | null;
  live_order_effect: string;
  live_promotion_allowed: boolean;
  entry_count: number | null;
  closed_trade_count: number | null;
  net_realized_pnl_usdt: number | null;
  equity_usdt: number | null;
  valuation_status?: string | null;
  curve: { observed_ts: number | string; equity_usdt: number | null; net_realized_pnl_usdt: number | null; valuation_status?: string }[];
  latest_decision?: ParticipationAction | null;
  latest_execution?: ParticipationAction | null;
  signal_decision?: ParticipationAction | null;
  signal_observed_at?: string | null;
  pending?: ParticipationAction | null;
  quote_execution_enabled?: boolean;
  quote_worker?: { status: string; observed_at?: string | null; interval_seconds?: number; websocket_connected?: boolean; last_error?: string | null; last_check_status?: string };
  events?: { observed_ts: string; decision?: ParticipationAction | null; execution?: ParticipationAction | null }[];
}

export interface CommandCenterData {
  schema_version: string;
  generated_at: string;
  read_only: boolean;
  status: string;
  latest_decision: {
    status: string;
    run_id: string | null;
    decision_ts: string | null;
    window_start_ts: string | null;
    window_end_ts: string | null;
    age_seconds: number | null;
    regime: string | null;
  };
  candidates: CommandCandidate[];
  window_72h: {
    start_ts: string;
    end_ts: string;
    hours: number;
    observed_runs: number;
    expected_runs: number;
    coverage_status: string;
    selected_candidates: ObservedCount;
    generated_orders: ObservedCount;
    actual_fill_events: ObservedCount;
    actual_filled_orders: ObservedCount;
    attributed_filled_candidates: ObservedCount;
  };
  blockers: { reason: string; count: number; unit: string; symbols: string[] }[];
  health: { risk: CommandHealth; kill_switch: CommandHealth; reconcile: CommandHealth; ledger: CommandHealth };
  participation: ParticipationSummary;
  quant_lab?: { mode?: string | null; permission?: string | null; permission_gate_enforced?: boolean | null };
  warnings: string[];
}
