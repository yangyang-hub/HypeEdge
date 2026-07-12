"use client"

import useSWR from "swr"
import { fetcher } from "@/lib/api"
import type { InstrumentMeta, SystemStatus } from "@/lib/types"
import { SWR_REFRESH_INTERVAL, SWR_SLOW_INTERVAL } from "@/lib/constants"

export function useSystemStatus() {
  const result = useSWR<SystemStatus>("/api/v1/system/status", fetcher, {
    refreshInterval: SWR_REFRESH_INTERVAL,
    keepPreviousData: true,
  })
  return { status: result.data, ...result }
}

export function useInstrumentMeta(symbol?: string) {
  const result = useSWR<InstrumentMeta>(
    symbol ? `/api/v1/market/${encodeURIComponent(symbol)}/meta` : null,
    fetcher,
    { refreshInterval: SWR_SLOW_INTERVAL, keepPreviousData: true },
  )
  return { meta: result.data, ...result }
}
