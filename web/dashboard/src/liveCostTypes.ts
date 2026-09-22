export interface LiveCostView {
  status: string;
  reason?: string;
  report?: null | {
    status: 'OBSERVED' | 'NO_FILLS' | 'UNAVAILABLE';
    reason?: string;
    generated_at_utc: string;
    window_days?: number;
    fill_count?: number;
    qualified_fill_count?: number;
    qualified_order_count?: number;
    fee_known_fill_count?: number;
    missing_reason_counts?: Record<string, number>;
    groups?: {
      symbol: string; side: string; origin: string; qualified_order_count: number;
      mean_fee_bps: number | null; mean_signed_slippage_bps: number | null; mean_one_way_cost_bps: number | null;
    }[];
    recent_fills?: {
      symbol: string; side: string; fill_ts_ms: number | null; fee_bps: number | null;
      signed_slippage_bps: number | null; expected_one_way_cost_bps: number | null;
      observed_one_way_cost_bps: number | null; cost_error_bps: number | null; missing_reasons: string[];
    }[];
  };
}
