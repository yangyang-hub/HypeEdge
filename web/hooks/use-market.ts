"use client"

import { useEffect, useState } from "react"
import useSWR from "swr"
import { useSWRConfig } from "swr"
import { fetcher } from "@/lib/api"
import type { CandleData, FundingRateData, InstrumentMetaData, MarketBookData } from "@/lib/types"

function isLoopbackHost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1"
}

// --- 纯函数辅助（M-FE3 / L-FE1，便于单元测试）---

/**
 * M-FE3: keepPreviousData 会在切币种时把旧币数据挂在新的 SWR key 上。
 * 渲染前用数据自带的 symbol 断言过滤，旧币数据一律不对外暴露。
 */
export function bookForSymbol(book: MarketBookData | undefined, symbol: string): MarketBookData | undefined {
  return book && book.symbol === symbol ? book : undefined
}

export function fundingForSymbol(funding: FundingRateData | undefined, symbol: string): FundingRateData | undefined {
  return funding && funding.symbol === symbol ? funding : undefined
}

export function candlesForSymbol(candles: CandleData[] | undefined, symbol: string): CandleData[] {
  return (candles ?? []).filter((candle) => candle.symbol === symbol)
}

/** L-FE1: WS 帧没有 source 字段，mutate 时强制标注来源为 websocket。 */
export function withWebsocketSource<T extends object>(data: T): T & { source: "websocket" } {
  return { ...data, source: "websocket" }
}

/**
 * Resolve market WS base for the browser.
 * Backend Origin checks use api.cors_origins; market WS is public read-only.
 * LAN pages should set NEXT_PUBLIC_HYPEEDGE_MARKET_WS_URL=ws://<lan-ip>:37001.
 */
function resolveMarketWsBase(configured: string | undefined): string | undefined {
  const trimmed = configured?.replace(/\/$/, "")
  if (!trimmed || typeof window === "undefined") return undefined
  try {
    const wsUrl = new URL(trimmed)
    if (wsUrl.protocol !== "ws:" && wsUrl.protocol !== "wss:") return undefined
    const pageHost = window.location.hostname
    // Local pages prefer loopback WS to avoid hairpin NAT to the LAN IP.
    if (isLoopbackHost(pageHost) && !isLoopbackHost(wsUrl.hostname)) {
      // Prefer loopback WS when browsing locally to avoid hairpin NAT issues.
      wsUrl.hostname = pageHost === "::1" ? "127.0.0.1" : pageHost
      return wsUrl.toString().replace(/\/$/, "")
    }
    return trimmed
  } catch {
    return undefined
  }
}

export function useMarket(symbol: string, interval: string) {
  const { mutate } = useSWRConfig()
  const [streamConnected, setStreamConnected] = useState(false)
  const [streamBase, setStreamBase] = useState<string | undefined>(undefined)
  const encodedSymbol = encodeURIComponent(symbol)
  const bookKey = `/api/v1/market/${encodedSymbol}/book`
  const fundingKey = `/api/v1/market/${encodedSymbol}/funding`
  const candlesKey = `/api/v1/market/${encodedSymbol}/candles?interval=${encodeURIComponent(interval)}&limit=160`
  const configuredWs = process.env.NEXT_PUBLIC_HYPEEDGE_MARKET_WS_URL

  useEffect(() => {
    setStreamBase(resolveMarketWsBase(configuredWs))
  }, [configuredWs])

  // Keep REST polling until the market WS is actually connected.
  const { data: book, error: bookError, isLoading: bookLoading } = useSWR<MarketBookData>(
    bookKey,
    fetcher,
    { refreshInterval: streamConnected ? 0 : 1_000, keepPreviousData: true },
  )
  const { data: funding, error: fundingError } = useSWR<FundingRateData>(
    fundingKey,
    fetcher,
    { refreshInterval: streamConnected ? 0 : 2_000, keepPreviousData: true },
  )
  const { data: candles, error: candlesError, isLoading: candlesLoading } = useSWR<CandleData[]>(
    candlesKey,
    fetcher,
    { refreshInterval: streamConnected ? 0 : 5_000, keepPreviousData: true },
  )
  const { data: meta } = useSWR<InstrumentMetaData>(
    `/api/v1/market/${encodedSymbol}/meta`,
    fetcher,
    { revalidateOnFocus: false },
  )

  useEffect(() => {
    if (!streamBase) return
    let socket: WebSocket | undefined
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined
    let stopped = false
    let lastSequence = 0
    // M-FE4: 无帧看门狗 —— WS 半开（既不报错也不来帧）时 REST 永不恢复。
    // 超过阈值无任何帧视为断线，强制 close 走 onclose → 切 REST + 重连。
    let lastFrameAt = Date.now()
    const HEARTBEAT_TIMEOUT_MS = 10_000
    const HEARTBEAT_CHECK_MS = 5_000

    const revalidate = () => {
      void mutate(bookKey)
      void mutate(fundingKey)
      void mutate(candlesKey)
    }

    const connect = () => {
      const url = new URL(`${streamBase}/ws/v1/market`)
      url.searchParams.set("symbol", symbol)
      url.searchParams.set("interval", interval)
      lastFrameAt = Date.now()
      socket = new WebSocket(url)
      socket.onopen = () => {
        lastFrameAt = Date.now()
        setStreamConnected(true)
      }
      socket.onclose = () => {
        setStreamConnected(false)
        if (!stopped) reconnectTimer = setTimeout(connect, 2_000)
      }
      socket.onerror = () => socket?.close()
      socket.onmessage = (event) => {
        lastFrameAt = Date.now()
        try {
          const message = JSON.parse(String(event.data)) as MarketStreamMessage
          // B14: only apply frames for the selected symbol. The WS served the
          // whole book for one symbol before, which polluted other symbols'
          // caches with BTC data.
          if (message.symbol && message.symbol !== symbol) return
          if (lastSequence > 0 && message.sequence !== lastSequence + 1) revalidate()
          lastSequence = message.sequence
          if (message.type === "snapshot") {
            const snapshot = message.data as MarketSnapshot
            if (snapshot.book) {
              void mutate(bookKey, { symbol, ...withWebsocketSource(snapshot.book) }, false)
            }
            if (snapshot.funding) void mutate(fundingKey, { symbol, ...snapshot.funding }, false)
            // Do not replace REST candles with an empty warm-up snapshot.
            if (snapshot.candles && snapshot.candles.length > 0) {
              void mutate(candlesKey, snapshot.candles.map((candle) => ({ symbol, ...candle })), false)
            }
          } else if (message.type === "book") {
            void mutate(
              bookKey,
              { symbol, ...withWebsocketSource(message.data as Omit<MarketBookData, "symbol">) },
              false,
            )
          } else if (message.type === "funding") {
            void mutate(fundingKey, { symbol, ...(message.data as Omit<FundingRateData, "symbol">) }, false)
          } else if (message.type === "candle") {
            const candle = { symbol, ...(message.data as Omit<CandleData, "symbol">) }
            void mutate<CandleData[]>(candlesKey, (current = []) => {
              const next = current.filter((item) => item.timestamp !== candle.timestamp)
              next.push(candle)
              return next.sort((left, right) => left.timestamp - right.timestamp).slice(-160)
            }, false)
          }
        } catch {
          revalidate()
        }
      }
    }

    const heartbeatTimer = setInterval(() => {
      if (socket && Date.now() - lastFrameAt >= HEARTBEAT_TIMEOUT_MS) {
        // 半开连接：强制关闭以触发 onclose（切 REST + 重连）。
        socket.close()
      }
    }, HEARTBEAT_CHECK_MS)

    connect()
    return () => {
      stopped = true
      if (reconnectTimer) clearTimeout(reconnectTimer)
      clearInterval(heartbeatTimer)
      socket?.close()
      setStreamConnected(false)
    }
  }, [bookKey, candlesKey, fundingKey, interval, mutate, streamBase, symbol])

  return {
    book: bookForSymbol(book, symbol),
    funding: fundingForSymbol(funding, symbol),
    candles: candlesForSymbol(candles, symbol),
    meta,
    errors: { book: bookError, funding: fundingError, candles: candlesError },
    isLoading: bookLoading || candlesLoading,
    streamConnected,
  }
}

interface MarketStreamMessage {
  sequence: number
  type: "snapshot" | "book" | "trade" | "candle" | "funding" | "heartbeat"
  data: unknown
  /** The symbol this frame belongs to (B14). */
  symbol?: string
}

interface MarketSnapshot {
  book: Omit<MarketBookData, "symbol"> | null
  funding: Omit<FundingRateData, "symbol"> | null
  candles: Omit<CandleData, "symbol">[]
}
