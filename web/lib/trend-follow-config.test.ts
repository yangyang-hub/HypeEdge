import { describe, expect, it } from "vitest"
import {
  DEFAULT_TF_CONFIG,
  cloneDefaultTfConfig,
  validateTfConfig,
} from "@/lib/trend-follow-config"
import { asDecimalString } from "@/lib/utils"
import { normalizeStrategy } from "@/hooks/use-strategies"

describe("trend-follow-config helpers", () => {
  it("clones defaults", () => {
    const cloned = cloneDefaultTfConfig()
    expect(cloned.fast_ema_period).toBe(DEFAULT_TF_CONFIG.fast_ema_period)
    cloned.fast_ema_period = 1
    expect(DEFAULT_TF_CONFIG.fast_ema_period).toBe(12)
  })

  it("rejects inverted ema periods", () => {
    expect(
      validateTfConfig({
        ...DEFAULT_TF_CONFIG,
        fast_ema_period: 30,
        slow_ema_period: 12,
      }),
    ).toMatch(/快线 EMA/)
  })

  it("rejects invalid position pct", () => {
    expect(
      validateTfConfig({
        ...DEFAULT_TF_CONFIG,
        max_position_pct: asDecimalString(1.5),
      }),
    ).toMatch(/最大仓位占比/)
  })

  it("maps trend_follow create payload fields via normalizeStrategy", () => {
    const strategy = normalizeStrategy({
      strategy_id: "trend-btc-1",
      strategy_type: "trend_follow",
      symbol: "BTC",
      sub_account: "trend_btc",
      desired_state: "stopped",
      actual_state: "stopped",
      revision: 0,
      created_at: "2026-07-13T00:00:00Z",
      updated_at: "2026-07-13T00:00:00Z",
      parameters: {
        fast_ema_period: 12,
        slow_ema_period: 26,
        atr_period: 14,
        atr_stop_multiplier: "2" as never,
        max_position_pct: "0.15" as never,
      },
    })
    expect(strategy.strategy_type).toBe("trend_follow")
    if (strategy.strategy_type === "trend_follow") {
      expect(strategy.parameters.fast_ema_period).toBe(12)
    }
  })
})
