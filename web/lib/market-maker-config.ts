import Decimal from "decimal.js"
import type { DecimalString, InstrumentMeta, MarketMakerConfig } from "@/lib/types"
import { asDecimalString } from "@/lib/utils"

export type MarketMakerConfigFieldKey = keyof MarketMakerConfig

export const MM_DECIMAL_FIELDS: Array<[MarketMakerConfigFieldKey, string]> = [
  ["soft_inventory_notional", "Soft inventory (USDC)"],
  ["hard_inventory_notional", "Hard inventory (USDC)"],
  ["emergency_inventory_notional", "Emergency inventory (USDC)"],
  ["quote_size", "Quote size"],
  ["max_depth_participation", "Max depth participation"],
  ["inventory_skew_bps", "Inventory skew (bps)"],
  ["max_inventory_shift_bps", "Max inventory shift (bps)"],
  ["min_half_spread_bps", "Minimum half spread (bps)"],
  ["toxicity_spread_bps", "Toxicity spread (bps)"],
  ["min_expected_pnl_usdc", "Min expected PnL (USDC)"],
  ["external_reference_weight", "External reference weight"],
  ["external_max_age_seconds", "External max age (seconds)"],
  ["external_outlier_bps", "External outlier threshold (bps)"],
  ["max_external_shift_ticks", "Max external shift (ticks)"],
  ["max_total_fair_shift_ticks", "Max total fair shift (ticks)"],
  ["latency_risk_multiplier", "Latency risk multiplier"],
  ["conservative_latency_seconds", "Conservative latency (seconds)"],
  ["conservative_markout_bps", "Conservative markout (bps)"],
]

export const MM_INTEGER_FIELDS: Array<[MarketMakerConfigFieldKey, string]> = [
  ["min_quote_lifetime_ms", "Min quote lifetime (ms)"],
  ["refresh_cooldown_ms", "Refresh cooldown (ms)"],
  ["max_quote_age_ms", "Max quote age (ms)"],
  ["market_stale_after_ms", "Max market age (ms)"],
  ["account_stale_after_ms", "Max account age (ms)"],
  ["min_markout_samples", "Minimum mature markout samples"],
]

/** Required fields shown on create wizard Step 2 (non-advanced). */
export const MM_CREATE_CORE_DECIMAL_KEYS: MarketMakerConfigFieldKey[] = [
  "soft_inventory_notional",
  "hard_inventory_notional",
  "emergency_inventory_notional",
  "quote_size",
  "max_depth_participation",
  "min_half_spread_bps",
]

export const MM_CREATE_CORE_INTEGER_KEYS: MarketMakerConfigFieldKey[] = [
  "min_quote_lifetime_ms",
  "refresh_cooldown_ms",
  "max_quote_age_ms",
  "market_stale_after_ms",
  "account_stale_after_ms",
]

export const STRATEGY_ID_PATTERN = /^[A-Za-z0-9_.:-]+$/
export const SYMBOL_PATTERN = /^[A-Z0-9][A-Z0-9_.-]*$/

/** Safe defaults aligned with backend schema + repository tests. */
export const DEFAULT_MM_CONFIG: MarketMakerConfig = {
  soft_inventory_notional: "100" as DecimalString,
  hard_inventory_notional: "150" as DecimalString,
  emergency_inventory_notional: "200" as DecimalString,
  quote_size: "0.001" as DecimalString,
  max_depth_participation: "0.1" as DecimalString,
  inventory_skew_bps: "5" as DecimalString,
  max_inventory_shift_bps: "20" as DecimalString,
  min_half_spread_bps: "1" as DecimalString,
  toxicity_spread_bps: "10" as DecimalString,
  min_expected_pnl_usdc: "0.01" as DecimalString,
  external_reference_weight: "0.25" as DecimalString,
  external_max_age_seconds: "0.5" as DecimalString,
  external_outlier_bps: "75" as DecimalString,
  max_external_shift_ticks: "2" as DecimalString,
  max_total_fair_shift_ticks: "3" as DecimalString,
  latency_risk_multiplier: "1" as DecimalString,
  conservative_latency_seconds: "0.1" as DecimalString,
  conservative_markout_bps: "1" as DecimalString,
  min_markout_samples: 20,
  min_quote_lifetime_ms: 500,
  refresh_cooldown_ms: 250,
  max_quote_age_ms: 10_000,
  market_stale_after_ms: 1_000,
  account_stale_after_ms: 5_000,
}

export function cloneDefaultMmConfig(): MarketMakerConfig {
  return { ...DEFAULT_MM_CONFIG }
}

/** Suggest inventory bands from account equity (5% / 10% / 15%). */
export function inventoryBandsFromEquity(equity: Decimal.Value): Pick<
  MarketMakerConfig,
  "soft_inventory_notional" | "hard_inventory_notional" | "emergency_inventory_notional"
> {
  const base = new Decimal(equity)
  if (!base.isFinite() || base.lte(0)) {
    return {
      soft_inventory_notional: DEFAULT_MM_CONFIG.soft_inventory_notional,
      hard_inventory_notional: DEFAULT_MM_CONFIG.hard_inventory_notional,
      emergency_inventory_notional: DEFAULT_MM_CONFIG.emergency_inventory_notional,
    }
  }
  const soft = Decimal.max(base.mul(0.05), 10).toDecimalPlaces(2)
  const hard = Decimal.max(base.mul(0.1), soft.mul(1.5)).toDecimalPlaces(2)
  const emergency = Decimal.max(base.mul(0.15), hard.mul(1.25)).toDecimalPlaces(2)
  return {
    soft_inventory_notional: asDecimalString(soft),
    hard_inventory_notional: asDecimalString(hard),
    emergency_inventory_notional: asDecimalString(emergency),
  }
}

export function suggestQuoteSize(meta: InstrumentMeta | undefined): DecimalString {
  if (!meta) return DEFAULT_MM_CONFIG.quote_size
  const lot = new Decimal(meta.lot_size)
  const minOrder = new Decimal(meta.min_order_size)
  const size = Decimal.max(lot, minOrder)
  if (!size.isFinite() || size.lte(0)) return DEFAULT_MM_CONFIG.quote_size
  return asDecimalString(size.toDecimalPlaces(meta.size_decimals))
}

export function validateMmConfig(config: MarketMakerConfig): string | null {
  const soft = new Decimal(config.soft_inventory_notional)
  const hard = new Decimal(config.hard_inventory_notional)
  const emergency = new Decimal(config.emergency_inventory_notional)
  if (!(soft.gt(0) && hard.gt(0) && emergency.gt(0))) {
    return "库存带必须大于 0"
  }
  if (!(soft.lt(hard) && hard.lt(emergency))) {
    return "库存带须满足 soft < hard < emergency"
  }
  if (config.min_quote_lifetime_ms > config.max_quote_age_ms) {
    return "最小挂单寿命不能超过最大挂单年龄"
  }
  const quoteSize = new Decimal(config.quote_size)
  const participation = new Decimal(config.max_depth_participation)
  if (!(quoteSize.gt(0) && participation.gt(0) && participation.lte(1))) {
    return "quote_size > 0 且 depth participation 须在 (0, 1]"
  }
  return null
}

export function validateStrategyIdentity(input: {
  strategy_id: string
  sub_account: string
  symbol: string
}): string | null {
  if (!input.strategy_id || input.strategy_id.length > 64 || !STRATEGY_ID_PATTERN.test(input.strategy_id)) {
    return "strategy_id 须为 1–64 位，仅含字母数字和 _ . : -"
  }
  if (!input.sub_account.trim() || input.sub_account.length > 128) {
    return "sub_account 不能为空（最长 128）"
  }
  const symbol = input.symbol.trim().toUpperCase()
  if (!symbol || symbol.length > 20 || !SYMBOL_PATTERN.test(symbol)) {
    return "symbol 须为大写字母/数字开头的交易对"
  }
  return null
}
