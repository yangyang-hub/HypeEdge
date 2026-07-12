"use client"

import { useEffect, useState } from "react"
import useSWR from "swr"
import { useSWRConfig } from "swr"
import { fetcher } from "@/lib/api"
import type { CandleData, FundingRateData, InstrumentMetaData, MarketBookData } from "@/lib/types"

function isLoopbackHost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1"
}

/** Resolve market WS base; skip loopback WS when the page is opened via LAN IP. */
function resolveMarketWsBase(configured: string | undefined): string | undefined {
  const trimmed = configured?.replace(/\/$/, "")
  if (!trimmed || typeof window === "undefined") return trimmed
  try {
    const wsHost = new URL(trimmed).hostname
    const pageHost = window.location.hostname
    if (isLoopbackHost(wsHost) && !isLoopbackHost(pageHost)) {
      // Backend listens on 127.0.0.1 and CORS only allows localhost origins.
      // A page at http://192.168.x.x cannot use ws://127.0.0.1 usefully.
      return undefined
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

    const revalidate = () => {
      void mutate(bookKey)
      void mutate(fundingKey)
      void mutate(candlesKey)
    }

    const connect = () => {
      const url = new URL(`${streamBase}/ws/v1/market`)
      url.searchParams.set("symbol", symbol)
      url.searchParams.set("interval", interval)
      socket = new WebSocket(url)
      socket.onopen = () => setStreamConnected(true)
      socket.onclose = () => {
        setStreamConnected(false)
        if (!stopped) reconnectTimer = setTimeout(connect, 2_000)
      }
      socket.onerror = () => socket?.close()
      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(String(event.data)) as MarketStreamMessage
          if (lastSequence > 0 && message.sequence !== lastSequence + 1) revalidate()
          lastSequence = message.sequence
          if (message.type === "snapshot") {
            const snapshot = message.data as MarketSnapshot
            if (snapshot.book) void mutate(bookKey, { symbol, ...snapshot.book }, false)
            if (snapshot.funding) void mutate(fundingKey, { symbol, ...snapshot.funding }, false)
            // Do not replace REST candles with an empty warm-up snapshot.
            if (snapshot.candles && snapshot.candles.length > 0) {
              void mutate(candlesKey, snapshot.candles.map((candle) => ({ symbol, ...candle })), false)
            }
          } else if (message.type === "book") {
            void mutate(bookKey, { symbol, ...(message.data as Omit<MarketBookData, "symbol">) }, false)
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

    connect()
    return () => {
      stopped = true
      if (reconnectTimer) clearTimeout(reconnectTimer)
      socket?.close()
      setStreamConnected(false)
    }
  }, [bookKey, candlesKey, fundingKey, interval, mutate, streamBase, symbol])

  return {
    book,
    funding,
    candles: candles ?? [],
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
}

interface MarketSnapshot {
  book: Omit<MarketBookData, "symbol"> | null
  funding: Omit<FundingRateData, "symbol"> | null
  candles: Omit<CandleData, "symbol">[]
}
