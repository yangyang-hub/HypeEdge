"use client"

import useSWR from "swr"
import { fetcher, poster } from "@/lib/api"
import type { Position } from "@/lib/types"
import { asDecimalString } from "@/lib/utils"
import { SWR_REFRESH_INTERVAL } from "@/lib/constants"

export function usePositions() {
  const { data, error, isLoading, mutate } = useSWR<Position[]>(
    "/api/v1/positions",
    fetcher,
    { refreshInterval: SWR_REFRESH_INTERVAL }
  )
  // B12: the backend emits is_long/is_short; the frontend contract keys on
  // `side`. Map defensively so a long never renders as a red "卖".
  const positions = (data ?? []).map((p) => ({
    ...p,
    side: p.side ?? (p.is_long ? "long" : p.is_short ? "short" : "flat"),
  }))
  return { positions, error, isLoading, refresh: mutate }
}

export async function closePosition(symbol: string, closeFraction: string = "1", idempotencyKey?: string) {
  return poster(`/api/v1/positions/${encodeURIComponent(symbol)}/close`, {
    close_fraction: asDecimalString(closeFraction),
    max_slippage_bps: 30,
  }, { idempotencyKey })
}
