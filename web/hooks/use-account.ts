"use client"

import useSWR from "swr"
import { fetcher } from "@/lib/api"
import type { AccountData, EquityPoint } from "@/lib/types"
import { SWR_REFRESH_INTERVAL, SWR_SLOW_INTERVAL } from "@/lib/constants"

export function useAccount() {
  const { data, error, isLoading } = useSWR<AccountData>(
    "/api/v1/account",
    fetcher,
    { refreshInterval: SWR_REFRESH_INTERVAL }
  )
  return { account: data, error, isLoading }
}

export function useEquityCurve(days: number = 30) {
  const { data, error, isLoading } = useSWR<EquityPoint[]>(
    `/api/v1/account/equity-curve?days=${days}`,
    fetcher,
    { refreshInterval: SWR_SLOW_INTERVAL }
  )
  return { curve: data ?? [], error, isLoading }
}
