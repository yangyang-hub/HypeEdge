"use client"

import { createContext, useContext, useEffect, useMemo, useState } from "react"
import { useSWRConfig } from "swr"
import { revalidationTargets } from "@/lib/event-contract"
import type { SSEEvent } from "@/lib/types"

/**
 * SSE 连接状态。`null` = 尚未建立/尚不确定（首帧到达前），避免首屏误报断线。
 */
interface SSEState {
  connected: boolean | null
  lastEvent: SSEEvent | null
}

const SSEContext = createContext<SSEState>({ connected: null, lastEvent: null })

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
  const [state, setState] = useState<SSEState>({ connected: null, lastEvent: null })

  useEffect(() => {
    const controller = new AbortController()
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined
    // N-FE4: sessionStorage can throw (private mode / disabled storage); the
    // stream must keep working without last-event-id replay.
    let lastEventId = ""
    try {
      lastEventId = sessionStorage.getItem("hypeedge:last-event-id") ?? ""
    } catch {
      lastEventId = ""
    }

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
              try {
                sessionStorage.setItem("hypeedge:last-event-id", id)
              } catch {
                // ignore storage failures (private mode / disabled storage)
              }
            }
            if (event.event_type === "StreamResyncRequired") {
              for (const prefix of RESYNC_KEYS) {
                void mutate((key) => typeof key === "string" && key.startsWith(prefix))
              }
            }
            setState({ connected: true, lastEvent: event })
            for (const prefix of revalidationTargets(event.event_type)) {
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
