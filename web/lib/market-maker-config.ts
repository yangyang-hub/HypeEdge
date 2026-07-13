import Decimal from "decimal.js"
import type { DecimalString, InstrumentMeta, MarketMakerConfig } from "@/lib/types"
import { asDecimalString } from "@/lib/utils"

export type MarketMakerConfigFieldKey = keyof MarketMakerConfig

export interface MarketMakerFieldMeta {
  key: MarketMakerConfigFieldKey
  label: string
  description: string
}

export const MM_DECIMAL_FIELDS: MarketMakerFieldMeta[] = [
  {
    key: "soft_inventory_notional",
    label: "软库存上限（USDC）",
    description: "库存名义超过该值后停止向同方向加仓，但仍可挂减仓侧报价。",
  },
  {
    key: "hard_inventory_notional",
    label: "硬库存上限（USDC）",
    description: "超过后仅允许降低库存的操作，用于强制收敛风险。须大于软库存上限。",
  },
  {
    key: "emergency_inventory_notional",
    label: "紧急库存上限（USDC）",
    description: "最高库存红线；触发后进入紧急减仓/撤单路径。须大于硬库存上限。",
  },
  {
    key: "quote_size",
    label: "单档报价数量",
    description: "每侧单档挂单的基础数量，需满足合约最小下单与 lot 精度。",
  },
  {
    key: "max_depth_participation",
    label: "最大盘口参与度",
    description: "报价量相对盘口深度的上限，取值 (0, 1]，用于避免吃穿过深。",
  },
  {
    key: "inventory_skew_bps",
    label: "库存倾斜（bps）",
    description: "按库存偏离对 reservation price 做倾斜的基准强度（基点）。",
  },
  {
    key: "max_inventory_shift_bps",
    label: "最大库存偏移（bps）",
    description: "库存倾斜导致的价格偏移上限，防止过度追价。",
  },
  {
    key: "min_half_spread_bps",
    label: "最小半价差（bps）",
    description: "相对公平价的最小半边点差；点差过窄时宁可 NO_QUOTE。",
  },
  {
    key: "toxicity_spread_bps",
    label: "毒性加点（bps）",
    description: "行情毒性升高时额外扩大的点差，用于对抗逆向选择。",
  },
  {
    key: "min_expected_pnl_usdc",
    label: "最小预期收益（USDC）",
    description: "一次报价生命周期的净预期收益门槛；不足则不下单。",
  },
  {
    key: "external_reference_weight",
    label: "外部参考权重",
    description: "外部市场对公平价的最大贡献权重，取值 [0, 1]；失稳时自动降为 0。",
  },
  {
    key: "external_max_age_seconds",
    label: "外部参考最大龄期（秒）",
    description: "外部行情超过该龄期视为过期，权重清零。",
  },
  {
    key: "external_outlier_bps",
    label: "外部异常阈值（bps）",
    description: "本地与外部价差超过该阈值时视为异常，降低或关闭外部权重。",
  },
  {
    key: "max_external_shift_ticks",
    label: "外部最大偏移（tick）",
    description: "外部参考对公平价的硬封顶偏移（按 tick）。",
  },
  {
    key: "max_total_fair_shift_ticks",
    label: "公平价总偏移上限（tick）",
    description: "所有模型修正合计后，公平价相对 mid 的最大偏移。",
  },
  {
    key: "latency_risk_multiplier",
    label: "延迟风险乘数",
    description: "放大延迟相关成本；>1 更保守，减少在高延迟下的报价激进程度。",
  },
  {
    key: "conservative_latency_seconds",
    label: "保守延迟假设（秒）",
    description: "决策到成交路径的保守延迟估计，用于计算尾部成本。",
  },
  {
    key: "conservative_markout_bps",
    label: "保守 Markout（bps）",
    description: "样本不足时使用的默认不利 markout 假设。",
  },
]

export const MM_INTEGER_FIELDS: MarketMakerFieldMeta[] = [
  {
    key: "min_quote_lifetime_ms",
    label: "最小挂单寿命（毫秒）",
    description: "挂单存活短于该值一般不主动撤换，避免无意义刷单。",
  },
  {
    key: "refresh_cooldown_ms",
    label: "刷新冷却（毫秒）",
    description: "两次报价刷新之间的最短间隔。",
  },
  {
    key: "max_quote_age_ms",
    label: "最大挂单年龄（毫秒）",
    description: "挂单超过该年龄必须刷新或撤单；须不小于最小挂单寿命。",
  },
  {
    key: "market_stale_after_ms",
    label: "行情过期阈值（毫秒）",
    description: "本地盘口/行情超过该龄期视为过期，进入只撤不下。",
  },
  {
    key: "account_stale_after_ms",
    label: "账户过期阈值（毫秒）",
    description: "账户/持仓快照超过该龄期视为过期，禁止加风险。",
  },
  {
    key: "min_markout_samples",
    label: "最少 Markout 样本数",
    description: "成熟 markout 样本少于此数时，改用保守默认假设。",
  },
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
    return "库存带须满足：软库存 < 硬库存 < 紧急库存"
  }
  if (config.min_quote_lifetime_ms > config.max_quote_age_ms) {
    return "最小挂单寿命不能超过最大挂单年龄"
  }
  const quoteSize = new Decimal(config.quote_size)
  const participation = new Decimal(config.max_depth_participation)
  if (!(quoteSize.gt(0) && participation.gt(0) && participation.lte(1))) {
    return "单档报价数量须大于 0，且盘口参与度须在 (0, 1]"
  }
  return null
}

export function validateStrategyIdentity(input: {
  strategy_id: string
  sub_account: string
  symbol: string
}): string | null {
  if (!input.strategy_id || input.strategy_id.length > 64 || !STRATEGY_ID_PATTERN.test(input.strategy_id)) {
    return "策略 ID 须为 1–64 位，仅含字母、数字和 _ . : -"
  }
  if (!input.sub_account.trim() || input.sub_account.length > 128) {
    return "子账户不能为空（最长 128 字符）"
  }
  const symbol = input.symbol.trim().toUpperCase()
  if (!symbol || symbol.length > 20 || !SYMBOL_PATTERN.test(symbol)) {
    return "交易品种须为大写字母/数字开头的合约代码"
  }
  return null
}
