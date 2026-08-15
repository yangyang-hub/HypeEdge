import { describe, expect, it } from "vitest"
import { BACKEND_EVENT_TYPES, EVENT_KEYS, STREAM_RESYNC_EVENT, revalidationTargets } from "@/lib/event-contract"
import { RELIABLE_MARKET_MAKING_EVENTS, shouldResyncMarketMaking } from "@/hooks/use-market-making"
import type { SSEEvent } from "@/lib/types"

/** 后端可下发的全部事件名 = 29 个后端事件 + 前端专用 StreamResyncRequired。 */
const ALL_KNOWN_EVENTS = new Set<string>([...BACKEND_EVENT_TYPES, STREAM_RESYNC_EVENT])

describe("SSE 事件契约（P4-2）", () => {
  it("后端事件表固定为 29 个且无重复", () => {
    expect(BACKEND_EVENT_TYPES.length).toBe(29)
    expect(new Set(BACKEND_EVENT_TYPES).size).toBe(29)
  })

  it("后端全部事件名都能在前端 EVENT_KEYS 中命中重验证映射", () => {
    for (const eventType of BACKEND_EVENT_TYPES) {
      expect(EVENT_KEYS, `EVENT_KEYS 缺少后端事件 ${eventType}`).toHaveProperty(eventType)
    }
  })

  it("EVENT_KEYS 中所有 PascalCase 键都是真实后端事件名", () => {
    for (const key of Object.keys(EVENT_KEYS)) {
      if (!/^[A-Z]/.test(key)) continue // 历史别名（snake/dot）不做反向断言
      expect(ALL_KNOWN_EVENTS, `未知的 PascalCase 事件键 ${key}`).toContain(key)
    }
  })

  it("PascalCase 可靠事件触发对应端点的重验证", () => {
    // ReconciliationComplete → risk/status 等五组端点。
    expect(revalidationTargets("ReconciliationComplete")).toContain("/api/v1/risk/status")
    expect(revalidationTargets("ReconciliationComplete")).toContain("/api/v1/orders")
    // RiskCheckPassed / ActionCreditsLow → risk/status。
    expect(revalidationTargets("RiskCheckPassed")).toContain("/api/v1/risk/status")
    expect(revalidationTargets("RiskCheckFailed")).toContain("/api/v1/risk/status")
    expect(revalidationTargets("ActionCreditsLow")).toContain("/api/v1/risk/status")
    // SignalGenerated → strategies。
    expect(revalidationTargets("SignalGenerated")).toContain("/api/v1/strategies")
    // 未知事件不触发任何重验证。
    expect(revalidationTargets("NoSuchEvent")).toEqual([])
  })

  it("RELIABLE_MARKET_MAKING_EVENTS 只含真实后端事件或 StreamResyncRequired", () => {
    for (const eventType of RELIABLE_MARKET_MAKING_EVENTS) {
      expect(ALL_KNOWN_EVENTS, `非真实后端事件 ${eventType}`).toContain(eventType)
    }
    // 后端根本不存在的 dot-case 事件必须已删除。
    expect(RELIABLE_MARKET_MAKING_EVENTS.has("strategy.lifecycle.changed")).toBe(false)
    expect(RELIABLE_MARKET_MAKING_EVENTS.has("market_making.reconciliation.completed")).toBe(false)
    // 做市相关真实事件必须存在。
    expect(RELIABLE_MARKET_MAKING_EVENTS.has("MarketMakerQuoteDecision")).toBe(true)
    expect(RELIABLE_MARKET_MAKING_EVENTS.has("MarketMakerActionCreditSample")).toBe(true)
    expect(RELIABLE_MARKET_MAKING_EVENTS.has("MarketMakerFillMarkout")).toBe(true)
    expect(RELIABLE_MARKET_MAKING_EVENTS.has(STREAM_RESYNC_EVENT)).toBe(true)
  })

  it("真实后端事件（PascalCase）能触发 MM resync，并按 strategy_id 过滤", () => {
    const event = (eventType: string, strategyId?: unknown): SSEEvent => ({
      event_type: eventType,
      payload: strategyId === undefined ? {} : { strategy_id: strategyId },
      timestamp: "2026-08-15T00:00:00Z",
      correlation_id: null,
    })

    // 真实事件触发。
    expect(shouldResyncMarketMaking(event("MarketMakerQuoteDecision", "mm-btc"), "mm-btc")).toBe(true)
    expect(shouldResyncMarketMaking(event("ReconciliationComplete"), "mm-btc")).toBe(true)
    expect(shouldResyncMarketMaking(event(STREAM_RESYNC_EVENT), "mm-btc")).toBe(true)
    // 其它策略的事件被过滤。
    expect(shouldResyncMarketMaking(event("MarketMakerQuoteDecision", "mm-eth"), "mm-btc")).toBe(false)
    // 非可靠事件不触发。
    expect(shouldResyncMarketMaking(event("L2BookUpdate"), "mm-btc")).toBe(false)
    // 后端不存在的事件名不触发。
    expect(shouldResyncMarketMaking(event("strategy.lifecycle.changed"), "mm-btc")).toBe(false)
  })
})
