type Numeric = string | number;
export interface ReviewPortfolio {
  equity_usdt: Numeric;
  asset_equity_usdt?: Numeric;
  immediately_executable_equity_usdt?: Numeric;
  restricted_residual_value_usdt?: Numeric;
  net_equity_increment_usdt: Numeric;
  maximum_drawdown_fraction: number;
  actual_simulated_fills: number;
  trial_halted?: boolean;
  trial_stop_reason?: { reason: string; observed_at: number } | null;
  restricted_positions?: Record<string, { quantity: Numeric; cost_basis_usdt: Numeric; estimated_net_value_usdt: Numeric; reason: string }>;
}
export interface ReviewView {
  status: string;
  reason?: string;
  ledger?: string;
  worker?: { ok: boolean; detail?: string; observed_at?: number };
  frozen_predecessors?: { directory: string; identity: string; frozen_at: string }[];
  report: null | {
    experiment_id: string; identity: string; status: string;
    latest_observed_at: number; ledger_start_ts: number;
    reference_contract: Record<string, string | number>;
    latest_decision_clock: { status: string; cutoff_ts: number; deadline_ts: number; actual_decision_ts: number | null };
    latest_reference_observation: Record<string, { status: string }>;
    reference_read?: { status: string; reason?: string };
    scenarios: Record<string, Record<string, ReviewPortfolio>>;
    reference_funnel: Record<string, { candidates: number; valid_reference_coverage: number; matched_candidates: number;
      defer_eligible: number; decisions_changed: number; late_arrivals: number; coverage_rate: number | null; statuses: Record<string, number> }>;
    acceptance: { comparisons: Record<string, { status: string; requirements: { criterion: string; actual: unknown; required: unknown; result: string; reason: string | null }[] }> };
  };
}
