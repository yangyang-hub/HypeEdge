import { describe, expect, it } from "vitest"
import {
  DEFAULT_FA_CONFIG,
  cloneDefaultFaConfig,
  validateFaConfig,
} from "@/lib/funding-arb-config"
import { asDecimalString } from "@/lib/utils"

describe("funding-arb-config helpers", () => {
  it("validates automatic-market defaults without pair fields", () => {
    expect(DEFAULT_FA_CONFIG).not.toHaveProperty("spot_coin")
    expect(validateFaConfig(cloneDefaultFaConfig())).toBeNull()
  })

  it("rejects missing funding-rate hysteresis", () => {
    expect(
      validateFaConfig({
        ...DEFAULT_FA_CONFIG,
        exit_funding_rate: DEFAULT_FA_CONFIG.entry_funding_rate,
      }),
    ).toMatch(/平仓资金费阈值必须低于入场阈值/)
  })

  it("rejects non-positive entry funding", () => {
    expect(
      validateFaConfig({
        ...DEFAULT_FA_CONFIG,
        entry_funding_rate: asDecimalString(0),
      }),
    ).toMatch(/入场资金费阈值/)
  })

  it("returns a validation error for malformed decimals instead of throwing", () => {
    const validate = () =>
      validateFaConfig({
        ...DEFAULT_FA_CONFIG,
        entry_funding_rate: "" as ReturnType<typeof asDecimalString>,
      })
    expect(validate).not.toThrow()
    expect(validate()).toMatch(/十进制字符串/)
  })

  it("enforces the backend rebalance threshold bound", () => {
    expect(
      validateFaConfig({
        ...DEFAULT_FA_CONFIG,
        rebalance_threshold_bps: 1_000_001,
      }),
    ).toMatch(/1 到 1000000/)
  })
})
