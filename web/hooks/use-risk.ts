"use client"

import useSWR from "swr"
import { fetcher, poster } from "@/lib/api"
import type { RiskStatusData } from "@/lib/types"
import { SWR_REFRESH_INTERVAL } from "@/lib/constants"

export function useRiskStatus() {
  const { data, error, isLoading, mutate } = useSWR<RiskStatusData>(
    "/api/v1/risk/status",
    fetcher,
    { refreshInterval: SWR_REFRESH_INTERVAL }
  )
  return { risk: data, error, isLoading, refresh: mutate }
}

export async function triggerKillSwitch(reason: string) {
  return poster("/api/v1/kill-switch", { action: "trigger", reason })
}

export async function resetKillSwitch() {
  return poster("/api/v1/kill-switch", { action: "reset" })
}
