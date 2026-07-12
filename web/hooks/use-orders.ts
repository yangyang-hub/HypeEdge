"use client"

import useSWR from "swr"
import { fetcher, poster } from "@/lib/api"
import type { Order, OrderSubmitRequest } from "@/lib/types"
import { SWR_REFRESH_INTERVAL, SWR_SLOW_INTERVAL } from "@/lib/constants"

export function useOrders(status: string = "active") {
  const { data, error, isLoading, mutate } = useSWR<Order[]>(
    `/api/v1/orders?status=${status}`,
    fetcher,
    { refreshInterval: status === "active" ? SWR_REFRESH_INTERVAL : SWR_SLOW_INTERVAL }
  )
  return { orders: data ?? [], error, isLoading, refresh: mutate }
}

export async function submitOrder(req: OrderSubmitRequest, idempotencyKey?: string) {
  return poster("/api/v1/orders", req, { idempotencyKey })
}

export async function cancelOrder(cloid: string, idempotencyKey?: string) {
  return poster(`/api/v1/orders/${encodeURIComponent(cloid)}/cancel`, {}, { idempotencyKey })
}
