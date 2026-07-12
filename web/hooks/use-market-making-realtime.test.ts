import { describe, expect, it } from "vitest"
import { mergeRealtimeMessage, shouldAcceptRealtimeMessage } from "@/hooks/use-market-making-realtime"
import type { DecimalString, MarketMakingRealtimeMessage } from "@/lib/types"

const d = (value: string) => value as DecimalString

function fairMessage(marketRevision: number, runtimeRevision: number = 4): MarketMakingRealtimeMessage {
  return {
    type: "fair_value",
    strategy_id: "mm-btc",
    runtime_revision: runtimeRevision,
    market_revision: marketRevision,
    observed_at: "2026-07-11T10:00:00Z",
    fair_price: d("60000.125"),
    reservation_price: d("59999.875"),
    best_bid: d("59999.5"),
    best_ask: d("60000.5"),
    external_reference: {
      source: "binance_perp",
      symbol: "BTCUSDT",
      raw_price: d("60010"),
      adjusted_price: d("60001"),
      basis_bps: d("-1.5"),
      divergence_bps: d("0.15"),
      configured_weight: d("0.25"),
      effective_weight: d("0.2"),
      confidence: d("0.8"),
      age_ms: 25,
      quality: "healthy",
      observed_at: "2026-07-11T10:00:00Z",
    },
  }
}

describe("market-making realtime display overlay", () => {
  it("accepts only the next revision for the authoritative runtime", () => {
    expect(shouldAcceptRealtimeMessage(fairMessage(11), "mm-btc", 4, 10)).toBe("accept")
    expect(shouldAcceptRealtimeMessage(fairMessage(10), "mm-btc", 4, 10)).toBe("ignore")
    expect(shouldAcceptRealtimeMessage(fairMessage(13), "mm-btc", 4, 10)).toBe("resync")
    expect(shouldAcceptRealtimeMessage(fairMessage(11, 5), "mm-btc", 4, 10)).toBe("resync")
  })

  it("keeps decimal strings intact when merging display-only data", () => {
    const overlay = mergeRealtimeMessage(null, fairMessage(11))
    expect(overlay.fair_price).toBe("60000.125")
    expect(overlay.reservation_price).toBe("59999.875")
    expect(overlay.external_reference?.adjusted_price).toBe("60001")
    expect(overlay.market_revision).toBe(11)
  })
})
