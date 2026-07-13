// TypeScript types aligned with backend core/models.py and api/schemas.py

export interface ApiResponse<T> {
  ok: boolean
  data: T
  error?: string
}

export interface ApiProblem {
  status: number
  code: string
  detail: string
  request_id: string
  retryable: boolean
  context: Record<string, unknown>
}

export type DecimalString = string & { readonly __brand: "DecimalString" }

export interface SystemStatus {
  environment: "dev" | "testnet" | "mainnet"
  trading_enabled: boolean
  kill_switch_active: boolean
  kill_switch_reason: string | null
  safety_mode: "starting" | "reconciling" | "normal" | "reduce_only" | "cancel_only" | "halting" | "halted" | "recovering" | "stopping"
  safety_reason: string | null
  shutting_down: boolean
  meta_loaded: boolean
  features: Record<string, boolean>
}

export interface InstrumentMeta {
  symbol: string
  price_decimals: number
  size_decimals: number
  tick_size: DecimalString
  lot_size: DecimalString
  min_order_size: DecimalString
  max_leverage: number
}

// --- Account ---

export interface AccountData {
  equity: DecimalString
  available_balance: DecimalString
  total_margin_used: DecimalString
  total_unrealized_pnl: DecimalString
  peak_equity: DecimalString
  drawdown_pct: DecimalString
  leverage: DecimalString
  total_fees: DecimalString
  total_funding: DecimalString
  fill_count: number
  position_count: number
  last_update: string | null
  trading_enabled: boolean
}

export interface EquityPoint {
  timestamp: number
  equity: DecimalString
}

// --- Positions ---

export interface Position {
  symbol: string
  size: DecimalString
  entry_price: DecimalString | null
  mark_price: DecimalString | null
  unrealized_pnl: DecimalString
  leverage: number
  side: "long" | "short" | "flat"
}

// --- Orders ---

export type OrderStatus =
  | "pending"
  | "submitted"
  | "submit_unknown"
  | "acknowledged"
  | "partial_fill"
  | "cancel_pending"
  | "cancel_unknown"
  | "filled"
  | "cancelled"
  | "rejected"
  | "expired"

export interface Order {
  cloid: string
  symbol: string
  side: "buy" | "sell"
  size: DecimalString
  price: DecimalString | null
  order_type: string
  status: OrderStatus
  filled_size: DecimalString
  avg_fill_price: DecimalString | null
  strategy_id: string | null
  error_message: string | null
  created_at: string | null
}

export interface OrderSubmitRequest {
  symbol: string
  side: "buy" | "sell"
  size: DecimalString
  price?: DecimalString
  order_type?: string
  reduce_only?: boolean
  strategy_id?: string
}

// --- Strategies ---

export interface StrategyData {
  strategy_id: string
  status: string
  symbol: string
  position_size: DecimalString
  entry_price: DecimalString | null
  stop_price: DecimalString | null
  params: Record<string, DecimalString | number | string>
}

// --- Strategy control plane ---

export type StrategyLifecycleState =
  | "stopped"
  | "warming"
  | "shadow"
  | "running"
  | "paused"
  | "draining"
  | "faulted"

export type StrategyDesiredState = "stopped" | "shadow" | "running" | "paused"

export interface StrategyInstanceBase {
  strategy_id: string
  strategy_type: "trend_follow" | "market_maker" | "legacy"
  symbol: string
  sub_account: string | null
  desired_state: StrategyDesiredState
  actual_state: StrategyLifecycleState
  desired_config_version_id: number | null
  effective_config_version_id: number | null
  revision: number
  archived_at: string | null
  created_at: string
  updated_at: string
  metadata?: Record<string, string>
}

export interface TrendFollowStrategyInstance extends StrategyInstanceBase {
  strategy_type: "trend_follow"
  parameters: {
    fast_ema_period: number
    slow_ema_period: number
    atr_period: number
    atr_stop_multiplier: DecimalString
    max_position_pct: DecimalString
  }
}

export interface MarketMakerStrategyInstance extends StrategyInstanceBase {
  strategy_type: "market_maker"
  session_mode: "shadow" | "testnet" | "mainnet" | null
}

export interface LegacyStrategyInstance extends StrategyInstanceBase {
  strategy_type: "legacy"
  legacy_kind: string
}

export type StrategyInstance =
  | TrendFollowStrategyInstance
  | MarketMakerStrategyInstance
  | LegacyStrategyInstance

// --- Market-making control plane ---

export type FreshnessStatus = "fresh" | "stale" | "unknown" | "unhealthy" | "missing"
export type QuoteSlotState =
  | "empty"
  | "live"
  | "inflight"
  | "unknown"
  | "orphaned_live"
  | "recovery_required"
export type BudgetMode = "normal" | "conserve" | "critical" | "cancel_only" | "exhausted"

export interface FreshnessDimension {
  status: FreshnessStatus
  observed_at: string | null
  age_ms: number | null
  threshold_ms: number
  reason: string | null
}

export interface MarketMakingAlert {
  id: string
  severity: "info" | "warning" | "critical"
  code: string
  message: string
  created_at: string
  acknowledged_at: string | null
}

export interface MarketMakingStateSnapshot {
  strategy_id: string
  strategy_type: "market_maker"
  symbol: string
  sub_account: string
  environment: SystemStatus["environment"]
  desired_state: StrategyDesiredState
  actual_state: StrategyLifecycleState
  runtime_revision: number
  market_revision: number
  config_version: number
  session_id: string | null
  session_mode: "shadow" | "testnet" | "mainnet" | null
  quote_uptime_pct: DecimalString | null
  kill_switch_active: boolean
  safety_mode: SystemStatus["safety_mode"]
  freshness: {
    market: FreshnessDimension
    inventory: FreshnessDimension
    clearinghouse: FreshnessDimension
    user_stream: FreshnessDimension
    reconciliation: FreshnessDimension
    action_budget: FreshnessDimension
    postgres: FreshnessDimension
  }
  alerts: MarketMakingAlert[]
  observed_at: string
  stale: boolean
}

export interface QuoteSlotSnapshot {
  side: "buy" | "sell"
  level: number
  state: QuoteSlotState
  desired_price: DecimalString | null
  desired_size: DecimalString | null
  live_price: DecimalString | null
  live_remaining_size: DecimalString | null
  cloid: string | null
  quote_revision: number
  quote_age_ms: number | null
  gross_edge_bps: DecimalString | null
  no_quote_reason: string | null
}

export type ExternalReferenceQuality = "healthy" | "degraded" | "stale" | "disabled"

export interface ExternalReferenceSnapshot {
  source?: string
  symbol?: string
  raw_price?: DecimalString | null
  adjusted_price?: DecimalString | null
  basis_bps?: DecimalString | null
  divergence_bps?: DecimalString | null
  configured_weight?: DecimalString | null
  effective_weight?: DecimalString | null
  confidence?: DecimalString | null
  age_ms?: number | null
  quality?: ExternalReferenceQuality
  observed_at?: string
}

export interface MarketMakingQuotesSnapshot {
  strategy_id: string
  symbol: string
  runtime_revision: number
  market_revision: number
  fair_price: DecimalString | null
  reservation_price: DecimalString | null
  best_bid: DecimalString | null
  best_ask: DecimalString | null
  external_reference?: ExternalReferenceSnapshot | null
  slots: QuoteSlotSnapshot[]
  observed_at: string
  stale: boolean
}

export interface MarketMakingInventorySnapshot {
  strategy_id: string
  symbol: string
  runtime_revision: number
  market_revision: number
  position_size: DecimalString
  inventory_notional: DecimalString
  soft_limit_notional: DecimalString
  hard_limit_notional: DecimalString
  emergency_limit_notional: DecimalString
  inventory_utilization: DecimalString
  inventory_shift_bps: DecimalString | null
  margin_used: DecimalString | null
  available_margin: DecimalString | null
  liquidation_distance_pct: DecimalString | null
  funding_carry: DecimalString
  reduction_mode: "none" | "soft" | "hard" | "emergency"
  observed_at: string
  stale: boolean
}

export interface AccountingPnl {
  realized_trading_pnl: DecimalString
  unrealized_inventory_change: DecimalString
  net_fees_and_rebates: DecimalString
  funding_pnl: DecimalString
  paid_action_cost: DecimalString
  accounting_net_pnl: DecimalString
  ledger_reconciled: boolean
}

export interface ExecutionQuality {
  quoted_spread_bps: DecimalString
  captured_spread_bps: DecimalString
  maker_ratio: DecimalString
  markout_1s: DecimalString
  markout_5s: DecimalString
  markout_30s: DecimalString
  fill_count: number
  unknown_count: number
  reject_count: number
  actions_per_fill: DecimalString
}

export interface InventoryEpisodePoint {
  observed_at: string
  accounting_net_pnl: DecimalString
  inventory_notional: DecimalString
}

export interface MarketMakingPerformanceSnapshot {
  strategy_id: string
  accounting: AccountingPnl | null
  execution_quality: ExecutionQuality | null
  inventory_episodes: InventoryEpisodePoint[]
  source: "postgres" | "clickhouse" | "mixed"
  as_of: string
  stale: boolean
}

export interface MarketMakingActionBudgetSnapshot {
  strategy_id: string
  mode: BudgetMode
  remote_cap: DecimalString
  remote_used: DecimalString
  remote_remaining: DecimalString
  shadow_remaining: DecimalString
  emergency_reserve: DecimalString
  cancel_headroom: DecimalString
  ip_weight_remaining: DecimalString
  burn_rate_1h: DecimalString
  burn_rate_6h: DecimalString
  burn_rate_24h: DecimalString
  earned_rate_24h: DecimalString
  usdc_per_action: DecimalString | null
  actions_per_fill: DecimalString | null
  runway_hours: DecimalString | null
  revision: number
  observed_at: string
  stale: boolean
}

export interface MarketMakerConfig {
  soft_inventory_notional: DecimalString
  hard_inventory_notional: DecimalString
  emergency_inventory_notional: DecimalString
  quote_size: DecimalString
  max_depth_participation: DecimalString
  inventory_skew_bps: DecimalString
  max_inventory_shift_bps: DecimalString
  min_half_spread_bps: DecimalString
  toxicity_spread_bps: DecimalString
  min_expected_pnl_usdc: DecimalString
  external_reference_weight: DecimalString
  external_max_age_seconds: DecimalString
  external_outlier_bps: DecimalString
  max_external_shift_ticks: DecimalString
  max_total_fair_shift_ticks: DecimalString
  latency_risk_multiplier: DecimalString
  conservative_latency_seconds: DecimalString
  conservative_markout_bps: DecimalString
  min_markout_samples: number
  min_quote_lifetime_ms: number
  refresh_cooldown_ms: number
  max_quote_age_ms: number
  market_stale_after_ms: number
  account_stale_after_ms: number
}

/** Payload for POST /api/v1/strategies (market_maker only). */
export interface StrategyCreateRequest {
  strategy_id: string
  strategy_type: "market_maker"
  sub_account: string
  symbol: string
  initial_config: MarketMakerConfig
  metadata?: Record<string, string>
}

export interface MarketMakerConfigVersion {
  id: number
  strategy_id: string
  version: number
  config_hash: string
  config: MarketMakerConfig
  created_by: string | null
  created_at: string | null
  approved_by: string | null
  approved_at: string | null
  shadow_preview: {
    expected_quote_uptime_pct: DecimalString
    expected_actions_per_hour: DecimalString
    pessimistic_edge_usdc: DecimalString
    warnings: string[]
  } | null
}

export interface MarketMakingEvent {
  id: string
  category: "lifecycle" | "risk" | "reconciliation" | "execution" | "operator" | "budget"
  severity: "info" | "warning" | "critical"
  event_type: string
  message: string
  actor: string | null
  correlation_id: string | null
  created_at: string
  metadata: Record<string, string | number | boolean | null>
}

export type MarketMakingRealtimeMessage =
  | {
      type: "fair_value"
      strategy_id: string
      runtime_revision: number
      market_revision: number
      observed_at: string
      fair_price: DecimalString
      reservation_price: DecimalString
      best_bid: DecimalString | null
      best_ask: DecimalString | null
      external_reference?: ExternalReferenceSnapshot | null
    }
  | {
      type: "quotes"
      strategy_id: string
      runtime_revision: number
      market_revision: number
      observed_at: string
      slots: QuoteSlotSnapshot[]
    }
  | {
      type: "inventory"
      strategy_id: string
      runtime_revision: number
      market_revision: number
      observed_at: string
      position_size: DecimalString
      inventory_notional: DecimalString
      inventory_utilization: DecimalString
      inventory_shift_bps: DecimalString
    }

// --- Risk ---

export interface RiskLimit {
  name: string
  current: DecimalString
  limit: DecimalString
  unit: string
  pct_used: DecimalString
}

export interface RiskStatusData {
  kill_switch_active: boolean
  kill_switch_reason: string | null
  safety_mode: SystemStatus["safety_mode"]
  safety_reason: string | null
  limits: RiskLimit[]
  check_stats: Record<string, number>
  strategy_pnl: Record<string, DecimalString>
  action_credits_remaining: number
}

// --- Market ---

export interface FundingRateData {
  symbol: string
  funding_rate: DecimalString
  premium: DecimalString
  mark_price: DecimalString
  open_interest: DecimalString
  timestamp: number
}

export interface MarketBookData {
  symbol: string
  bids: [DecimalString, DecimalString][]
  asks: [DecimalString, DecimalString][]
  timestamp: number
  source: "websocket" | "rest"
}

export interface CandleData {
  symbol: string
  interval: string
  open: DecimalString
  high: DecimalString
  low: DecimalString
  close: DecimalString
  volume: DecimalString
  timestamp: number
}

export interface InstrumentMetaData {
  symbol: string
  price_decimals: number
  size_decimals: number
  tick_size: DecimalString
  lot_size: DecimalString
  min_order_size: DecimalString
  max_leverage: number
}

// --- Kill Switch ---

export interface KillSwitchRequest {
  action: "trigger" | "reset"
  reason?: string
}

export interface SSEEvent {
  schema_version?: number
  sequence?: number
  event_type: string
  payload: Record<string, unknown>
  timestamp: string
  correlation_id: string | null
}
