type Numeric = string | number;

export interface BenchmarkAccount {
  equity_usdt: number;
  net_equity_increment_usdt: number;
  net_return_fraction: number;
  realized_pnl_usdt: number;
  unrealized_pnl_usdt: number;
  observed_maximum_drawdown_fraction: number;
  independent_closed_campaign_count: number;
}

export interface DailyTrendBenchmark {
  status: string;
  reason?: string;
  observation_count?: number;
  strategy?: BenchmarkAccount;
  passive?: BenchmarkAccount;
  cash?: BenchmarkAccount;
  excess_vs_passive_usdt?: number;
  excess_vs_cash_usdt?: number;
  limitations?: string[];
  curve?: {
    observed_at: number;
    strategy_equity_usdt: number;
    passive_equity_usdt: number;
    cash_equity_usdt: number;
    strategy_net_return_fraction: number;
    passive_net_return_fraction: number;
    excess_vs_passive_usdt: number;
    strategy_drawdown_fraction: number;
    passive_drawdown_fraction: number;
  }[];
}

export interface DailyTrendSleeve {
  signal: {
    long: boolean;
    close: number;
    sma: number;
    momentum_return: number;
    sma_days: number;
    momentum_days: number;
  };
  portfolio: {
    equity_usdt: Numeric;
    net_equity_increment_usdt: Numeric;
    gross_exposure_usdt: Numeric;
    cash_usdt: Numeric;
    drawdown_fraction: number;
  };
  observed_maximum_drawdown_fraction: number;
  actual_simulated_fill_count: number;
  independent_closed_campaign_count: number;
}

export interface DailyTrendView {
  status: string;
  reason?: string;
  ledger?: string;
  benchmark?: DailyTrendBenchmark;
  worker?: { ok: boolean; detail?: string; observed_at?: number };
  report: null | {
    schema_version: string;
    experiment_id: string;
    identity: string;
    status: string;
    finalized: boolean;
    observed_at: number;
    completed_bar_ts: number;
    decision_ts: number;
    initial_capital_usdt: number;
    total_equity_usdt: number;
    net_equity_increment_usdt: number;
    modeled_fee_usdt: number;
    explicit_roundtrip_cost_bps: number;
    observed_maximum_drawdown_fraction: number;
    observation_gap_days: number;
    cumulative_missed_decision_days: number;
    observation_count: number;
    evidence_status: string;
    start_utc: string;
    review_utc: string;
    paper_only: true;
    live_order_effect: 'none';
    live_execution_eligible: false;
    automatic_live_scaling: false;
    actions: { symbol: string; action: string; reason: string }[];
    sleeves: Record<string, DailyTrendSleeve>;
  };
}
