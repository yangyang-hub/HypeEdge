import Decimal from "decimal.js"
import type { DecimalString, TrendFollowConfig } from "@/lib/types"
import { asDecimalString } from "@/lib/utils"
import { STRATEGY_ID_PATTERN, SYMBOL_PATTERN } from "@/lib/market-maker-config"

export type TrendFollowConfigFieldKey = keyof TrendFollowConfig

export interface TrendFollowFieldMeta {
  key: TrendFollowConfigFieldKey
  label: string
  description: string
}

export const TF_INTEGER_FIELDS: TrendFollowFieldMeta[] = [
  {
    key: "fast_ema_period",
    label: "快线 EMA 周期",
    description: "MACD 快线均线窗口；须小于慢线周期。",
  },
  {
    key: "slow_ema_period",
    label: "慢线 EMA 周期",
    description: "MACD 慢线均线窗口；常用 26。",
  },
  {
    key: "signal_ema_period",
    label: "信号线 EMA 周期",
    description: "对 MACD 线再平滑得到信号线的窗口；常用 9。",
  },
  {
    key: "momentum_period",
    label: "动量周期",
    description: "价格变化率（动量）回看周期，用于确认趋势方向。",
  },
  {
    key: "atr_period",
    label: "ATR 周期",
    description: "平均真实波幅窗口，用于仓位与止损计算。",
  },
]

export const TF_DECIMAL_FIELDS: TrendFollowFieldMeta[] = [
  {
    key: "momentum_threshold",
    label: "动量阈值",
    description: "开仓需 |动量| 大于该阈值；0 表示只要方向一致即可。",
  },
  {
    key: "atr_position_multiplier",
    label: "ATR 仓位乘数",
    description: "仓位 ≈ (权益 × 单笔风险%) / (ATR × 该乘数)；越大仓位越小。",
  },
  {
    key: "atr_stop_multiplier",
    label: "ATR 止损乘数",
    description: "止损距离 = ATR × 该乘数；越大止损越宽。",
  },
  {
    key: "max_position_pct",
    label: "最大仓位占比",
    description: "相对账户权益的最大持仓名义占比，取值 (0, 1]。",
  },
  {
    key: "risk_per_trade_pct",
    label: "单笔风险占比",
    description: "每笔交易相对权益愿意承担的风险比例，取值 (0, 1]。",
  },
  {
    key: "macd_cross_threshold",
    label: "MACD 交叉阈值",
    description: "MACD 与信号线交叉需超过该缓冲才触发；0 表示零缓冲。",
  },
]

/** Safe defaults aligned with TrendParams / backend default_trend_follow_config. */
export const DEFAULT_TF_CONFIG: TrendFollowConfig = {
  fast_ema_period: 12,
  slow_ema_period: 26,
  signal_ema_period: 9,
  momentum_period: 10,
  momentum_threshold: "0" as DecimalString,
  atr_period: 14,
  atr_position_multiplier: "0.5" as DecimalString,
  atr_stop_multiplier: "2" as DecimalString,
  max_position_pct: "0.15" as DecimalString,
  risk_per_trade_pct: "0.01" as DecimalString,
  macd_cross_threshold: "0" as DecimalString,
}

export function cloneDefaultTfConfig(): TrendFollowConfig {
  return { ...DEFAULT_TF_CONFIG }
}

export function validateTfConfig(config: TrendFollowConfig): string | null {
  if (!(config.fast_ema_period < config.slow_ema_period)) {
    return "快线 EMA 周期必须小于慢线 EMA 周期"
  }
  if (!(new Decimal(config.max_position_pct).gt(0) && new Decimal(config.max_position_pct).lte(1))) {
    return "最大仓位占比必须在 (0, 1]"
  }
  if (!(new Decimal(config.risk_per_trade_pct).gt(0) && new Decimal(config.risk_per_trade_pct).lte(1))) {
    return "单笔风险占比必须在 (0, 1]"
  }
  if (!new Decimal(config.atr_position_multiplier).gt(0)) {
    return "ATR 仓位乘数必须大于 0"
  }
  if (!new Decimal(config.atr_stop_multiplier).gt(0)) {
    return "ATR 止损乘数必须大于 0"
  }
  return null
}

export { STRATEGY_ID_PATTERN, SYMBOL_PATTERN, asDecimalString }
