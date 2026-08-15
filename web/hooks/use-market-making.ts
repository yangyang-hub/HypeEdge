"use client"

import { useCallback, useEffect } from "react"
import useSWR from "swr"
import { fetcher, poster } from "@/lib/api"
import { STREAM_RESYNC_EVENT } from "@/lib/event-contract"
import { SWR_REFRESH_INTERVAL, SWR_SLOW_INTERVAL } from "@/lib/constants"
import { useSSE } from "@/hooks/use-sse"
import type {
  MarketMakerConfig,
  MarketMakerConfigVersion,
  MarketMakingActionBudgetSnapshot,
  MarketMakingEvent,
  MarketMakingInventorySnapshot,
  MarketMakingPerformanceSnapshot,
  MarketMakingQuotesSnapshot,
  MarketMakingStateSnapshot,
  SSEEvent,
  StrategyDesiredState,
} from "@/lib/types"

/**
 * 触发 MM 工作台全量 resync 的事件（真实后端事件名，见
 * crates/domain/src/events.rs `ALL_EVENT_TYPES`）。旧实现里的
 * "strategy.lifecycle.changed" 等 dot-case 事件后端并不存在，已删除；
 * `StreamResyncRequired` 是 SSE 重放缺口时前端专用事件。
 */
export const RELIABLE_MARKET_MAKING_EVENTS = new Set<string>([
  // Market-making analytics（lossy；经 durable outbox 流式下发时携带 strategy_id）。
  "MarketMakerFeatureSample",
  "MarketMakerQuoteDecision",
  "MarketMakerInventorySample",
  "MarketMakerActionCreditSample",
  "MarketMakerFillMarkout",
  // Execution —— 成交/撤单直接改变库存与报价。
  "OrderSubmitted",
  "OrderAcknowledged",
  "OrderFilled",
  "OrderPartialFill",
  "OrderCancelled",
  "OrderRejected",
  "OrderExpired",
  // Account。
  "PositionChanged",
  "BalanceChanged",
  "AccountStateUpdate",
  // Risk / system。
  "RiskCheckPassed",
  "RiskCheckFailed",
  "KillSwitchTriggered",
  "ReconciliationComplete",
  "ActionCreditsLow",
  // 前端专用。
  STREAM_RESYNC_EVENT,
])

/** 判断某个 SSE 事件是否需要触发该策略的 resync（事件类型 + strategy_id 过滤）。 */
export function shouldResyncMarketMaking(event: SSEEvent, strategyId: string): boolean {
  if (!RELIABLE_MARKET_MAKING_EVENTS.has(event.event_type)) return false
  const eventStrategyId = event.payload.strategy_id
  if (typeof eventStrategyId === "string" && eventStrategyId !== strategyId) return false
  return true
}

export function useMarketMaking(strategyId: string) {
  const base = `/api/v1/market-making/${encodeURIComponent(strategyId)}`
  const state = useSWR<MarketMakingStateSnapshot>(`${base}/state`, fetcher, {
    refreshInterval: SWR_REFRESH_INTERVAL,
    keepPreviousData: true,
  })
  const quotes = useSWR<MarketMakingQuotesSnapshot>(`${base}/quotes`, fetcher, {
    refreshInterval: SWR_REFRESH_INTERVAL,
    keepPreviousData: true,
  })
  const inventory = useSWR<MarketMakingInventorySnapshot>(`${base}/inventory`, fetcher, {
    refreshInterval: SWR_REFRESH_INTERVAL,
    keepPreviousData: true,
  })
  const performance = useSWR<MarketMakingPerformanceSnapshot>(`${base}/performance`, fetcher, {
    refreshInterval: SWR_SLOW_INTERVAL,
    keepPreviousData: true,
  })
  const budget = useSWR<MarketMakingActionBudgetSnapshot>(`${base}/action-budget`, fetcher, {
    refreshInterval: SWR_REFRESH_INTERVAL,
    keepPreviousData: true,
  })
  const configs = useSWR<MarketMakerConfigVersion[]>(
    `/api/v1/strategies/${encodeURIComponent(strategyId)}/config-versions`,
    fetcher,
    { refreshInterval: SWR_SLOW_INTERVAL, keepPreviousData: true },
  )
  const events = useSWR<MarketMakingEvent[]>(`${base}/events?limit=200`, fetcher, {
    refreshInterval: SWR_SLOW_INTERVAL,
    keepPreviousData: true,
  })
  const { lastEvent, connected: reliableConnected } = useSSE()
  const mutateState = state.mutate
  const mutateQuotes = quotes.mutate
  const mutateInventory = inventory.mutate
  const mutatePerformance = performance.mutate
  const mutateBudget = budget.mutate
  const mutateConfigs = configs.mutate
  const mutateEvents = events.mutate

  const resync = useCallback(async () => {
    await Promise.all([
      mutateState(),
      mutateQuotes(),
      mutateInventory(),
      mutatePerformance(),
      mutateBudget(),
      mutateConfigs(),
      mutateEvents(),
    ])
  }, [mutateBudget, mutateConfigs, mutateEvents, mutateInventory, mutatePerformance, mutateQuotes, mutateState])

  useEffect(() => {
    if (!lastEvent || !shouldResyncMarketMaking(lastEvent, strategyId)) return
    void resync()
  }, [lastEvent, resync, strategyId])

  return {
    state: state.data,
    quotes: quotes.data,
    inventory: inventory.data,
    performance: performance.data,
    budget: budget.data,
    configs: configs.data ?? [],
    events: events.data ?? [],
    reliableConnected,
    isLoading:
      state.isLoading || quotes.isLoading || inventory.isLoading || performance.isLoading || budget.isLoading,
    error: state.error ?? quotes.error ?? inventory.error ?? performance.error ?? budget.error,
    resync,
  }
}

export async function runStrategyAction(
  strategyId: string,
  action: "start" | "pause" | "resume" | "drain" | "stop",
  revision: number,
  options: { target_state?: StrategyDesiredState; confirmation?: string } = {},
) {
  return poster(`/api/v1/strategies/${encodeURIComponent(strategyId)}/actions/${action}`, options, {
    ifMatch: revision,
  })
}

export async function createMarketMakerConfig(strategyId: string, config: MarketMakerConfig, revision: number) {
  return poster<MarketMakerConfigVersion>(
    `/api/v1/strategies/${encodeURIComponent(strategyId)}/config-versions`,
    { strategy_type: "market_maker", config },
    { ifMatch: revision },
  )
}

export async function activateMarketMakerConfig(
  strategyId: string,
  version: number,
  revision: number,
  confirmation: string,
) {
  return poster<MarketMakerConfigVersion>(
    `/api/v1/strategies/${encodeURIComponent(strategyId)}/config-versions/${version}/activate`,
    { confirmation },
    { ifMatch: revision },
  )
}

export async function rollbackMarketMakerConfig(
  strategyId: string,
  version: number,
  revision: number,
  confirmation: string,
) {
  return poster<MarketMakerConfigVersion>(
    `/api/v1/strategies/${encodeURIComponent(strategyId)}/config-versions/${version}/rollback`,
    { confirmation },
    { ifMatch: revision },
  )
}
