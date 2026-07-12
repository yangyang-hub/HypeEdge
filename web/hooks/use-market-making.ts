"use client"

import { useCallback, useEffect } from "react"
import useSWR from "swr"
import { fetcher, poster } from "@/lib/api"
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
  StrategyDesiredState,
} from "@/lib/types"

const RELIABLE_MARKET_MAKING_EVENTS = new Set([
  "strategy.lifecycle.changed",
  "strategy.config.activated",
  "strategy.config.rolled_back",
  "market_making.risk.changed",
  "market_making.execution.unknown",
  "market_making.budget.changed",
  "market_making.reconciliation.completed",
  "market_making.operator.audit",
  "StreamResyncRequired",
])

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
    if (!lastEvent || !RELIABLE_MARKET_MAKING_EVENTS.has(lastEvent.event_type)) return
    const eventStrategyId = lastEvent.payload.strategy_id
    if (typeof eventStrategyId === "string" && eventStrategyId !== strategyId) return
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
