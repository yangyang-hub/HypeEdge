import { act, renderHook } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import {
  bookForSymbol,
  candlesForSymbol,
  fundingForSymbol,
  useMarket,
  withWebsocketSource,
} from "@/hooks/use-market"
import type { CandleData, DecimalString, MarketBookData } from "@/lib/types"

const d = (value: string) => value as DecimalString

class MockWebSocket {
  static instances: MockWebSocket[] = []
  onopen: ((event: Event) => void) | null = null
  onmessage: ((event: MessageEvent) => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null
  onerror: ((event: Event) => void) | null = null
  close = vi.fn(() => {
    this.onclose?.(new Event("close") as CloseEvent)
  })
  constructor(url: string | URL) {
    this.url = String(url)
    MockWebSocket.instances.push(this)
  }
  url: string
}

beforeEach(() => {
  MockWebSocket.instances = []
  process.env.NEXT_PUBLIC_HYPEEDGE_MARKET_WS_URL = "ws://127.0.0.1:37001"
  // 阻断 SWR fetcher 的真实网络请求（单测环境无后端）。
  vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("no backend in unit test")))
  vi.stubGlobal("WebSocket", MockWebSocket)
})

afterEach(() => {
  vi.useRealTimers()
  delete process.env.NEXT_PUBLIC_HYPEEDGE_MARKET_WS_URL
  vi.unstubAllGlobals()
  vi.clearAllMocks()
})

const btcBook: MarketBookData = { symbol: "BTC", bids: [], asks: [], timestamp: 1, source: "rest" }
const btcCandle: CandleData = {
  symbol: "BTC",
  interval: "1m",
  open: d("1"),
  high: d("1"),
  low: d("1"),
  close: d("1"),
  volume: d("1"),
  timestamp: 1,
}
const ethCandle: CandleData = { ...btcCandle, symbol: "ETH", timestamp: 2 }

describe("M-FE3: 跨币种 symbol 断言（keepPreviousData 旧数据不泄漏）", () => {
  it("切币种后不暴露旧币 book/funding", () => {
    expect(bookForSymbol(btcBook, "ETH")).toBeUndefined()
    expect(bookForSymbol(btcBook, "BTC")).toEqual(btcBook)
    expect(
      fundingForSymbol(
        { symbol: "BTC", funding_rate: d("0"), premium: d("0"), mark_price: d("1"), open_interest: d("1"), timestamp: 1 },
        "ETH",
      ),
    ).toBeUndefined()
    expect(fundingForSymbol(undefined, "ETH")).toBeUndefined()
  })

  it("切币种后 candles 只保留当前币种数据", () => {
    expect(candlesForSymbol([btcCandle, ethCandle], "ETH")).toEqual([ethCandle])
    expect(candlesForSymbol([btcCandle, ethCandle], "BTC")).toEqual([btcCandle])
    expect(candlesForSymbol(undefined, "ETH")).toEqual([])
  })
})

describe("L-FE1: WS 帧来源标签", () => {
  it("book mutation 强制标注 source=websocket（帧本身无 source 字段）", () => {
    const frame = { bids: [] as [string, string][], asks: [] as [string, string][], timestamp: 1 }
    expect(withWebsocketSource(frame).source).toBe("websocket")
    // 即使帧里带了 rest 也以 websocket 为准（数据来自实时流）。
    expect(withWebsocketSource({ ...frame, source: "rest" }).source).toBe("websocket")
  })
})

describe("M-FE4: WS 无帧心跳看门狗", () => {
  it("超过 10s 无任何帧 → 强制 close → 切 REST", async () => {
    vi.useFakeTimers()
    const { result, unmount } = renderHook(() => useMarket("BTC", "1m"))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0)
    })

    const socket = MockWebSocket.instances.at(-1)
    expect(socket).toBeDefined()
    expect(socket!.url).toContain("/ws/v1/market")
    expect(socket!.url).toContain("symbol=BTC")

    await act(async () => {
      socket!.onopen?.(new Event("open"))
    })
    expect(result.current.streamConnected).toBe(true)

    // 静默 11s（无任何帧）：看门狗在第 10s 检查点强制关闭。
    await act(async () => {
      await vi.advanceTimersByTimeAsync(11_000)
    })
    expect(socket!.close).toHaveBeenCalled()
    expect(result.current.streamConnected).toBe(false)

    unmount()
  })

  it("持续收到帧时保持连接，不触发看门狗", async () => {
    vi.useFakeTimers()
    const { result, unmount } = renderHook(() => useMarket("BTC", "1m"))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0)
    })

    const socket = MockWebSocket.instances.at(-1)!
    await act(async () => {
      socket.onopen?.(new Event("open"))
    })
    expect(result.current.streamConnected).toBe(true)

    // 每 4s 收到一帧心跳，持续 20s。
    for (let elapsed = 0; elapsed < 20_000; elapsed += 4_000) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(4_000)
        socket.onmessage?.(new MessageEvent("message", { data: JSON.stringify({ sequence: 1, type: "heartbeat" }) }))
      })
    }
    expect(socket.close).not.toHaveBeenCalled()
    expect(result.current.streamConnected).toBe(true)

    unmount()
  })
})
