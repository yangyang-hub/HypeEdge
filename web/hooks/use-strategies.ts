"use client"

import useSWR from "swr"
import { fetcher, poster } from "@/lib/api"
import type { StrategyInstance } from "@/lib/types"
import { SWR_REFRESH_INTERVAL } from "@/lib/constants"

export function useStrategies() {
  const { data, error, isLoading, mutate } = useSWR<StrategyInstance[]>(
    "/api/v1/strategies",
    fetcher,
    { refreshInterval: SWR_REFRESH_INTERVAL }
  )
  return { strategies: data ?? [], error, isLoading, refresh: mutate }
}

export async function startStrategy(id: string) {
  return poster(`/api/v1/strategies/${id}/actions/start`, {})
}

export async function stopStrategy(id: string) {
  return poster(`/api/v1/strategies/${id}/actions/stop`, {})
}
