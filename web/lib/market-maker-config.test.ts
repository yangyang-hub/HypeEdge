import { describe, expect, it } from "vitest"
import {
  DEFAULT_MM_CONFIG,
  inventoryBandsFromEquity,
  suggestQuoteSize,
  validateMmConfig,
  validateStrategyIdentity,
} from "@/lib/market-maker-config"
import type { InstrumentMeta } from "@/lib/types"
import { asDecimalString } from "@/lib/utils"
import { normalizeStrategy } from "@/hooks/use-strategies"

describe("market-maker-config helpers", () => {
  it("suggests inventory bands from equity", () => {
    const bands = inventoryBandsFromEquity(1000)
    expect(bands.soft_inventory_notional).toBe("50")
    expect(bands.hard_inventory_notional).toBe("100")
    expect(bands.emergency_inventory_notional).toBe("150")
  })

  it("rejects invalid inventory ordering", () => {
    expect(
      validateMmConfig({
        ...DEFAULT_MM_CONFIG,
        soft_inventory_notional: asDecimalString(200),
        hard_inventory_notional: asDecimalString(100),
        emergency_inventory_notional: asDecimalString(300),
      }),
    ).toMatch(/软库存 < 硬库存 < 紧急库存/)
  })

  it("validates strategy identity", () => {
    expect(validateStrategyIdentity({ strategy_id: "bad id", sub_account: "a", symbol: "BTC" })).not.toBeNull()
    expect(validateStrategyIdentity({ strategy_id: "mm-btc-1", sub_account: "mm-btc-1", symbol: "BTC" })).toBeNull()
  })

  it("suggests quote size from instrument meta", () => {
    const meta: InstrumentMeta = {
      symbol: "BTC",
      price_decimals: 1,
      size_decimals: 4,
      tick_size: asDecimalString("0.1"),
      lot_size: asDecimalString("0.001"),
      min_order_size: asDecimalString("0.001"),
      max_leverage: 40,
    }
    expect(suggestQuoteSize(meta)).toBe("0.001")
  })
})

describe("normalizeStrategy", () => {
  it("maps market_maker create payload fields", () => {
    const strategy = normalizeStrategy({
      strategy_id: "mm-btc-1",
      strategy_type: "market_maker",
      symbol: "BTC",
      sub_account: "mm-btc-1",
      desired_state: "stopped",
      actual_state: "stopped",
      desired_config_version: 1,
      revision: 0,
      metadata: { note: "lab" },
      created_at: "2026-07-12T00:00:00Z",
      updated_at: "2026-07-12T00:00:00Z",
    })
    expect(strategy.strategy_type).toBe("market_maker")
    expect(strategy.desired_config_version_id).toBe(1)
    expect(strategy.metadata?.note).toBe("lab")
  })

  it("falls back status to actual_state for legacy trend", () => {
    const strategy = normalizeStrategy({
      strategy_id: "trend_v1",
      strategy_type: "trend_follow",
      status: "running",
      symbol: "BTC",
      revision: 0,
    })
    expect(strategy.actual_state).toBe("running")
  })
})
