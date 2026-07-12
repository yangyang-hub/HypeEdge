"use client"

import { createContext, useContext, useEffect, useMemo, useState } from "react"
import { useSWRConfig } from "swr"
import type { SSEEvent } from "@/lib/types"

interface SSEState {
  connected: boolean
  lastEvent: SSEEvent | null
}

const SSEContext = createContext<SSEState>({ connected: false, lastEvent: null })

const EVENT_KEYS: Record<string, string[]> = {
  OrderSubmitted: ["/api/v1/orders"],
  OrderAcknowledged: ["/api/v1/orders"],
  OrderFilled: ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  OrderPartialFill: ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  OrderCancelled: ["/api/v1/orders"],
  PositionChanged: ["/api/v1/positions", "/api/v1/account"],
  BalanceChanged: ["/api/v1/account"],
  AccountStateUpdate: ["/api/v1/account", "/api/v1/risk/status"],
  KillSwitchTriggered: ["/api/v1/system/status", "/api/v1/risk/status"],
  "order.submitted": ["/api/v1/orders"],
  "order.acknowledged": ["/api/v1/orders"],
  "order.filled": ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  "order.cancelled": ["/api/v1/orders"],
  "order.rejected": ["/api/v1/orders"],
  "exchange.fill.ingested": ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  "exchange.order.updated": ["/api/v1/orders"],
  "system.safety.transitioned": ["/api/v1/system/status", "/api/v1/risk/status"],
  "reconciliation.completed": [
    "/api/v1/system/status",
    "/api/v1/account",
    "/api/v1/positions",
    "/api/v1/orders",
    "/api/v1/risk/status",
  ],
}

const RESYNC_KEYS = [
  "/api/v1/system/status",
  "/api/v1/account",
  "/api/v1/positions",
  "/api/v1/orders",
  "/api/v1/strategies",
  "/api/v1/risk/status",
]

export function SSEProvider({ children }: { children: React.ReactNode }) {
  const { mutate } = useSWRConfig()
  const [state, setState] = useState<SSEState>({ connected: false, lastEvent: null })

  useEffect(() => {
    const controller = new AbortController()
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined
    let lastEventId = sessionStorage.getItem("hypeedge:last-event-id") ?? ""

    async function connect() {
      try {
        const response = await fetch("/api/v1/events", {
          headers: { Accept: "text/event-stream", "Last-Event-ID": lastEventId },
          cache: "no-store",
          signal: controller.signal,
        })
        if (!response.ok || !response.body) throw new Error(`SSE failed (${response.status})`)
        setState((previous) => ({ ...previous, connected: true }))
        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ""
        while (!controller.signal.aborted) {
          const { value, done } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })
          const frames = buffer.split("\n\n")
          buffer = frames.pop() ?? ""
          for (const frame of frames) {
            const lines = frame.split("\n")
            const id = lines.find((line) => line.startsWith("id: "))?.slice(4)
            const data = lines.find((line) => line.startsWith("data: "))?.slice(6)
            if (!data) continue
            const event = JSON.parse(data) as SSEEvent
            if (id) {
              const previousSequence = Number(lastEventId)
              const nextSequence = Number(id)
              // Postgres identity sequences may legitimately contain gaps.
              // Only the server can distinguish retention loss from rollback
              // gaps, and reports it explicitly below.
              if (
                event.event_type !== "StreamResyncRequired" &&
                previousSequence > 0 &&
                nextSequence <= previousSequence
              ) continue
              lastEventId = id
              sessionStorage.setItem("hypeedge:last-event-id", id)
            }
            if (event.event_type === "StreamResyncRequired") {
              for (const prefix of RESYNC_KEYS) {
                void mutate((key) => typeof key === "string" && key.startsWith(prefix))
              }
            }
            setState({ connected: true, lastEvent: event })
            for (const prefix of EVENT_KEYS[event.event_type] ?? []) {
              void mutate((key) => typeof key === "string" && key.startsWith(prefix))
            }
          }
        }
      } catch {
        if (!controller.signal.aborted) setState((previous) => ({ ...previous, connected: false }))
      }
      if (!controller.signal.aborted) reconnectTimer = setTimeout(connect, 3000)
    }

    void connect()
    return () => {
      controller.abort()
      if (reconnectTimer) clearTimeout(reconnectTimer)
    }
  }, [mutate])

  const value = useMemo(() => state, [state])
  return <SSEContext.Provider value={value}>{children}</SSEContext.Provider>
}

export function useSSE() {
  return useContext(SSEContext)
}
