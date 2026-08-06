import Decimal from "decimal.js"
import type { DecimalString, FundingArbConfig } from "@/lib/types"
import { asDecimalString } from "@/lib/utils"
import { STRATEGY_ID_PATTERN, SYMBOL_PATTERN } from "@/lib/market-maker-config"

export type FundingArbConfigFieldKey = keyof FundingArbConfig
const DECIMAL_STRING_PATTERN = /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/

export interface FundingArbFieldMeta {
  key: FundingArbConfigFieldKey
  label: string
  description: string
}

export const FA_INTEGER_FIELDS: FundingArbFieldMeta[] = [
  {
    key: "rebalance_threshold_bps",
    label: "再平衡阈值 (bps)",
    description: "两腿 delta 偏离超过该阈值时，只缩减较大一腿。",
  },
  {
    key: "max_slippage_bps",
    label: "最大滑点 (bps)",
    description: "每条 IOC 市价保护单允许的最大滑点，范围 1–500。",
  },
  {
    key: "max_basis_bps",
    label: "最大基差 (bps)",
    description: "现货与永续中间价偏离超过该值时不入场。",
  },
  {
    key: "expected_hold_hours",
    label: "预期持有小时",
    description: "入场净 edge 估算使用的持有期，范围 1–168 小时。",
  },
  {
    key: "max_unhedged_seconds",
    label: "未对冲超时 (秒)",
    description: "第一腿成交后允许等待认证成交/补偿的最长时间，范围 1–60 秒。",
  },
]

export const FA_DECIMAL_FIELDS: FundingArbFieldMeta[] = [
  {
    key: "entry_funding_rate",
    label: "入场资金费阈值",
    description: "小时资金费达到该正值后才评估入场。",
  },
  {
    key: "exit_funding_rate",
    label: "平仓资金费阈值",
    description: "资金费回落至该值或以下时安全退出，必须低于入场阈值。",
  },
  {
    key: "max_notional_usd",
    label: "最大名义敞口 (USDC)",
    description: "单 cycle 永续目标名义；仍受部署级 testnet 25 USDC 硬上限约束。",
  },
  {
    key: "hedge_ratio",
    label: "对冲比例",
    description: "现货腿相对永续基础资产数量的对冲比例，取值 (0, 1]。",
  },
  {
    key: "leverage",
    label: "永续腿杠杆",
    description: "永续入场前设置的整数 isolated leverage。",
  },
  {
    key: "min_expected_edge_bps",
    label: "最低预期净 edge (bps)",
    description: "预期 funding 扣除盘口与往返成本后必须达到的最低值。",
  },
  {
    key: "round_trip_fee_bps",
    label: "往返成本缓冲 (bps)",
    description: "两腿完整开平手续费与额外保守成本缓冲。",
  },
]

/** Safe defaults aligned with FundingArbParams / backend default_funding_arb_config. */
export const DEFAULT_FA_CONFIG: FundingArbConfig = {
  entry_funding_rate: "0.0001" as DecimalString,
  exit_funding_rate: "0" as DecimalString,
  max_notional_usd: "1000" as DecimalString,
  hedge_ratio: "1" as DecimalString,
  rebalance_threshold_bps: 50,
  leverage: "1" as DecimalString,
  max_slippage_bps: 50,
  max_basis_bps: 500,
  min_expected_edge_bps: "5" as DecimalString,
  expected_hold_hours: 8,
  round_trip_fee_bps: "20" as DecimalString,
  max_unhedged_seconds: 15,
}

export function cloneDefaultFaConfig(): FundingArbConfig {
  return { ...DEFAULT_FA_CONFIG }
}

function parseDecimalString(value: DecimalString): Decimal | null {
  if (!DECIMAL_STRING_PATTERN.test(value)) return null
  try {
    const parsed = new Decimal(value)
    return parsed.isFinite() ? parsed : null
  } catch {
    return null
  }
}

export function validateFaConfig(config: FundingArbConfig): string | null {
  const entryFunding = parseDecimalString(config.entry_funding_rate)
  const exitFunding = parseDecimalString(config.exit_funding_rate)
  const maxNotional = parseDecimalString(config.max_notional_usd)
  const hedgeRatio = parseDecimalString(config.hedge_ratio)
  const leverage = parseDecimalString(config.leverage)
  const minExpectedEdge = parseDecimalString(config.min_expected_edge_bps)
  const roundTripFee = parseDecimalString(config.round_trip_fee_bps)
  if (
    !entryFunding ||
    !exitFunding ||
    !maxNotional ||
    !hedgeRatio ||
    !leverage ||
    !minExpectedEdge ||
    !roundTripFee
  ) {
    return "数值参数必须是有效的十进制字符串"
  }
  if (!entryFunding.gt(0)) {
    return "入场资金费阈值必须大于 0"
  }
  if (exitFunding.lt(0)) {
    return "平仓资金费阈值不能为负"
  }
  if (exitFunding.gte(entryFunding)) {
    return "平仓资金费阈值必须低于入场阈值"
  }
  if (!maxNotional.gt(0)) {
    return "最大名义敞口必须大于 0"
  }
  if (!(hedgeRatio.gt(0) && hedgeRatio.lte(1))) {
    return "对冲比例必须在 (0, 1]"
  }
  if (
    !Number.isInteger(config.rebalance_threshold_bps) ||
    config.rebalance_threshold_bps <= 0 ||
    config.rebalance_threshold_bps > 1_000_000
  ) {
    return "再平衡阈值必须是 1 到 1000000 之间的整数"
  }
  if (!leverage.gt(0) || !leverage.isInteger()) {
    return "永续腿杠杆必须是正整数"
  }
  if (!Number.isInteger(config.max_slippage_bps) || config.max_slippage_bps < 1 || config.max_slippage_bps > 500) {
    return "最大滑点必须是 1 到 500 之间的整数"
  }
  if (!Number.isInteger(config.max_basis_bps) || config.max_basis_bps <= 0 || config.max_basis_bps > 100_000) {
    return "最大基差必须是 1 到 100000 之间的整数"
  }
  if (!minExpectedEdge.gte(0)) {
    return "最低预期净 edge 不能为负"
  }
  if (!Number.isInteger(config.expected_hold_hours) || config.expected_hold_hours < 1 || config.expected_hold_hours > 168) {
    return "预期持有小时必须是 1 到 168 之间的整数"
  }
  if (!roundTripFee.gte(0)) {
    return "往返成本缓冲不能为负"
  }
  if (
    !Number.isInteger(config.max_unhedged_seconds) ||
    config.max_unhedged_seconds < 1 ||
    config.max_unhedged_seconds > 60
  ) {
    return "未对冲超时必须是 1 到 60 之间的整数"
  }
  return null
}

export { STRATEGY_ID_PATTERN, SYMBOL_PATTERN, asDecimalString }
