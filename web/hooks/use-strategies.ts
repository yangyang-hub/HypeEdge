"use client"

import useSWR from "swr"
import { fetcher, poster } from "@/lib/api"
import type { StrategyDesiredState, StrategyInstance, StrategyLifecycleState } from "@/lib/types"
import { SWR_REFRESH_INTERVAL } from "@/lib/constants"

type StrategyListItem = Partial<StrategyInstance> & {
  strategy_id: string
  status?: string
}

function normalizeLifecycle(value: unknown, fallback: StrategyLifecycleState = "stopped"): StrategyLifecycleState {
  if (typeof value !== "string" || !value) return fallback
  return value.toLowerCase() as StrategyLifecycleState
}

function normalizeStrategy(raw: StrategyListItem): StrategyInstance {
  const actual_state = normalizeLifecycle(raw.actual_state ?? raw.status)
  const desired_state = normalizeLifecycle(
    raw.desired_state,
    actual_state === "running" || actual_state === "shadow" || actual_state === "paused"
      ? (actual_state as StrategyDesiredState)
      : "stopped",
  ) as StrategyDesiredState

  return {
    strategy_id: raw.strategy_id,
    strategy_type: (raw.strategy_type ?? "legacy") as StrategyInstance["strategy_type"],
    symbol: raw.symbol ?? "—",
    sub_account: raw.sub_account ?? null,
    desired_state,
    actual_state,
    desired_config_version_id: raw.desired_config_version_id ?? null,
    effective_config_version_id: raw.effective_config_version_id ?? null,
    revision: raw.revision ?? 0,
    archived_at: raw.archived_at ?? null,
    created_at: raw.created_at ?? new Date(0).toISOString(),
    updated_at: raw.updated_at ?? new Date(0).toISOString(),
    ...(raw.strategy_type === "trend_follow" && "parameters" in raw ? { parameters: raw.parameters } : {}),
    ...(raw.strategy_type === "legacy" && "legacy_kind" in raw ? { legacy_kind: raw.legacy_kind } : {}),
  } as StrategyInstance
}

async function strategiesFetcher(url: string): Promise<StrategyInstance[]> {
  const data = await fetcher<StrategyListItem[]>(url)
  return (data ?? []).map(normalizeStrategy)
}

export function useStrategies() {
  const { data, error, isLoading, mutate } = useSWR<StrategyInstance[]>(
    "/api/v1/strategies",
    strategiesFetcher,
    { refreshInterval: SWR_REFRESH_INTERVAL },
  )
  return { strategies: data ?? [], error, isLoading, refresh: mutate }
}

export async function startStrategy(id: string) {
  return poster(`/api/v1/strategies/${id}/actions/start`, {})
}

export async function stopStrategy(id: string) {
  return poster(`/api/v1/strategies/${id}/actions/stop`, {})
}
