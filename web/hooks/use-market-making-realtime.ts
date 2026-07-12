"use client"

import { useEffect, useRef, useState } from "react"
import type {
  DecimalString,
  ExternalReferenceSnapshot,
  MarketMakingRealtimeMessage,
  QuoteSlotSnapshot,
} from "@/lib/types"

export type RealtimeConnectionState = "disabled" | "connecting" | "connected" | "disconnected" | "stale"

export interface MarketMakingDisplayOverlay {
  runtime_revision: number
  market_revision: number
  observed_at: string
  fair_price?: DecimalString
  reservation_price?: DecimalString
  best_bid?: DecimalString | null
  best_ask?: DecimalString | null
  external_reference?: ExternalReferenceSnapshot | null
  slots?: QuoteSlotSnapshot[]
  position_size?: DecimalString
  inventory_notional?: DecimalString
  inventory_utilization?: DecimalString
  inventory_shift_bps?: DecimalString
}

export function shouldAcceptRealtimeMessage(
  message: MarketMakingRealtimeMessage,
  strategyId: string,
  runtimeRevision: number,
  lastMarketRevision: number,
): "accept" | "ignore" | "resync" {
  if (message.strategy_id !== strategyId) return "ignore"
  if (message.runtime_revision !== runtimeRevision) return "resync"
  if (message.market_revision <= lastMarketRevision) return "ignore"
  if (lastMarketRevision > 0 && message.market_revision > lastMarketRevision + 1) return "resync"
  return "accept"
}

export function mergeRealtimeMessage(
  current: MarketMakingDisplayOverlay | null,
  message: MarketMakingRealtimeMessage,
): MarketMakingDisplayOverlay {
  const base = {
    ...current,
    runtime_revision: message.runtime_revision,
    market_revision: message.market_revision,
    observed_at: message.observed_at,
  }
  if (message.type === "fair_value") {
    return {
      ...base,
      fair_price: message.fair_price,
      reservation_price: message.reservation_price,
      best_bid: message.best_bid,
      best_ask: message.best_ask,
      external_reference: message.external_reference,
    }
  }
  if (message.type === "quotes") return { ...base, slots: message.slots }
  return {
    ...base,
    position_size: message.position_size,
    inventory_notional: message.inventory_notional,
    inventory_utilization: message.inventory_utilization,
    inventory_shift_bps: message.inventory_shift_bps,
  }
}

export function useMarketMakingRealtime(
  strategyId: string,
  runtimeRevision: number,
  authoritativeMarketRevision: number,
  onResync: () => void,
) {
  const [overlay, setOverlay] = useState<MarketMakingDisplayOverlay | null>(null)
  const [connectionState, setConnectionState] = useState<RealtimeConnectionState>("disabled")
  const pendingRef = useRef<MarketMakingRealtimeMessage | null>(null)
  const lastMarketRevisionRef = useRef(authoritativeMarketRevision)
  const onResyncRef = useRef(onResync)

  useEffect(() => {
    onResyncRef.current = onResync
  }, [onResync])

  useEffect(() => {
    lastMarketRevisionRef.current = authoritativeMarketRevision
    setOverlay(null)
  }, [authoritativeMarketRevision, runtimeRevision])

  useEffect(() => {
    const configuredBase = process.env.NEXT_PUBLIC_HYPEEDGE_MM_WS_URL?.replace(/\/$/, "")
    if (!configuredBase || runtimeRevision <= 0) {
      setConnectionState("disabled")
      return
    }

    let disposed = false
    let socket: WebSocket | null = null
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined
    let staleTimer: ReturnType<typeof setTimeout> | undefined

    function markFresh() {
      if (staleTimer) clearTimeout(staleTimer)
      staleTimer = setTimeout(() => setConnectionState("stale"), 5000)
    }

    function connect() {
      if (disposed) return
      setConnectionState("connecting")
      const url = new URL(`${configuredBase}/ws/v1/market-making`)
      url.searchParams.set("strategy_id", strategyId)
      socket = new WebSocket(url)
      socket.onopen = () => {
        setConnectionState("connected")
        markFresh()
      }
      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(String(event.data)) as MarketMakingRealtimeMessage
          const decision = shouldAcceptRealtimeMessage(
            message,
            strategyId,
            runtimeRevision,
            lastMarketRevisionRef.current,
          )
          if (decision === "resync") {
            pendingRef.current = null
            onResyncRef.current()
            return
          }
          if (decision === "ignore") return
          lastMarketRevisionRef.current = message.market_revision
          pendingRef.current = message
          markFresh()
        } catch {
          onResyncRef.current()
        }
      }
      socket.onclose = () => {
        if (disposed) return
        setConnectionState("disconnected")
        reconnectTimer = setTimeout(connect, 3000)
      }
      socket.onerror = () => socket?.close()
    }

    const flushTimer = setInterval(() => {
      const pending = pendingRef.current
      if (!pending) return
      pendingRef.current = null
      setOverlay((current) => mergeRealtimeMessage(current, pending))
    }, 200)
    connect()

    return () => {
      disposed = true
      socket?.close()
      if (reconnectTimer) clearTimeout(reconnectTimer)
      if (staleTimer) clearTimeout(staleTimer)
      clearInterval(flushTimer)
      pendingRef.current = null
    }
  }, [runtimeRevision, strategyId])

  return { overlay, connectionState }
}
