import Decimal from "decimal.js"
import type { DecimalString, FundingArbConfig } from "@/lib/types"
import { asDecimalString } from "@/lib/utils"
import { STRATEGY_ID_PATTERN, SYMBOL_PATTERN } from "@/lib/market-maker-config"

export type FundingArbConfigFieldKey = keyof FundingArbConfig

export interface FundingArbFieldMeta {
  key: FundingArbConfigFieldKey
  label: string
  description: string
}

export const FA_INTEGER_FIELDS: FundingArbFieldMeta[] = [
  {
    key: "rebalance_threshold_bps",
    label: "再平衡阈值 (bps)",
    description: "永续与现货腿 delta 偏离超过该阈值时触发再平衡（骨架参数，运行时尚未实盘生效）。",
  },
]

export const FA_DECIMAL_FIELDS: FundingArbFieldMeta[] = [
  {
    key: "entry_funding_rate",
    label: "入场资金费阈值",
    description: "永续资金费（绝对小时率）高于该值时开仓：空永续 + 多现货对冲以收取 funding。",
  },
  {
    key: "exit_funding_rate",
    label: "平仓资金费阈值",
    description: "资金费回落到该值以下时平仓了结套利腿。",
  },
  {
    key: "max_notional_usd",
    label: "最大名义敞口 (USDC)",
    description: "单策略允许的最大对冲名义敞口（USDC）。",
  },
  {
    key: "hedge_ratio",
    label: "对冲比例",
    description: "现货腿相对永续腿的对冲比例，取值 (0, 1]；1.0 为完全 delta 对冲。",
  },
  {
    key: "leverage",
    label: "永续腿杠杆",
    description: "永续腿采用的杠杆倍数，须大于 0。",
  },
]

export const FA_STRING_FIELDS: FundingArbFieldMeta[] = [
  {
    key: "spot_coin",
    label: "现货标的",
    description: "对冲现货腿的 Hyperliquid 现货 coin（HIP-1/HIP-2，如 PURR）；永续腿使用实例 symbol。",
  },
]

/** Safe defaults aligned with FundingArbParams / backend default_funding_arb_config. */
export const DEFAULT_FA_CONFIG: FundingArbConfig = {
  spot_coin: "PURR",
  entry_funding_rate: "0.0001" as DecimalString,
  exit_funding_rate: "0" as DecimalString,
  max_notional_usd: "1000" as DecimalString,
  hedge_ratio: "1" as DecimalString,
  rebalance_threshold_bps: 50,
  leverage: "1" as DecimalString,
}

export function cloneDefaultFaConfig(): FundingArbConfig {
  return { ...DEFAULT_FA_CONFIG }
}

export function validateFaConfig(config: FundingArbConfig): string | null {
  if (!config.spot_coin.trim()) {
    return "现货标的不能为空"
  }
  if (new Decimal(config.entry_funding_rate).lt(0)) {
    return "入场资金费阈值不能为负"
  }
  if (new Decimal(config.exit_funding_rate).lt(0)) {
    return "平仓资金费阈值不能为负"
  }
  if (!new Decimal(config.max_notional_usd).gt(0)) {
    return "最大名义敞口必须大于 0"
  }
  if (!(new Decimal(config.hedge_ratio).gt(0) && new Decimal(config.hedge_ratio).lte(1))) {
    return "对冲比例必须在 (0, 1]"
  }
  if (!(config.rebalance_threshold_bps > 0)) {
    return "再平衡阈值必须大于 0"
  }
  if (!new Decimal(config.leverage).gt(0)) {
    return "永续腿杠杆必须大于 0"
  }
  return null
}

export { STRATEGY_ID_PATTERN, SYMBOL_PATTERN, asDecimalString }
