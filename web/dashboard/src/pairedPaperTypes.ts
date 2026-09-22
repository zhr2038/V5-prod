type Numeric = number | string;

export interface PairedPaperAccount {
  equity_usdt: Numeric;
  cash_usdt: Numeric;
  net_equity_increment_usdt: Numeric;
  realized_pnl_usdt: Numeric;
  unrealized_pnl_usdt: Numeric;
  maximum_drawdown_fraction: Numeric;
  actual_simulated_fills: number;
  independent_closed_campaign_count: number;
  risk_snapshot?: null | {
    current_level: string;
    metrics?: { recovery_evidence_ok?: boolean };
  };
}

interface PairedScenario {
  accounts: Record<string, PairedPaperAccount>;
  comparison: {
    net_equity_delta_usdt: number;
    avoided_net_loss_usdt: number;
    missed_net_profit_usdt: number;
    completed_matched_veto_campaigns: number;
    paired_daily_block_bootstrap_95pct_delta_usdt?: [number, number] | null;
  };
}

export interface PairedPaperView {
  status: string;
  reason?: string;
  worker?: { ok: boolean; detail?: string };
  report?: null | PairedScenario & {
    identity: string;
    status: string;
    ledger_start_ts: number;
    latest_observed_at: number;
    calendar_days: number;
    cost_model: {
      fee_bps_per_side: number;
      slippage_bps_per_side: number;
      explicit_roundtrip_cost_bps: number;
      calibrated_to_real_fills: boolean;
      fixed_operating_cost_included: boolean;
    };
    coverage: {
      observed_decision_hours: number;
      expected_decision_hours: number;
      missed_decision_hours: number;
      decision_coverage_rate: number | null;
      quote_coverage_rate?: number | null;
      observation_count: number;
    };
    references: {
      candidates: number;
      valid: number;
      deferred: number;
      matched_candidates: number;
      matched_vetoes: number;
      valid_coverage_rate: number | null;
    };
    scenarios: Record<string, PairedScenario>;
    acceptance: {
      status: string;
      requirements: { criterion: string; actual: unknown; required: unknown; result: string }[];
    };
  };
}
