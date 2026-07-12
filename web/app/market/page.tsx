"use client"

import { useState } from "react"
import { CandlestickChart } from "@/components/market/candlestick-chart"
import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import { useMarket } from "@/hooks/use-market"
import type { DecimalString } from "@/lib/types"
import { formatPct, formatPrice, formatSize } from "@/lib/utils"

const SYMBOLS = ["BTC", "ETH", "SOL"]
const INTERVALS = ["1m", "5m", "15m", "1h"]

export default function MarketPage() {
  const [symbol, setSymbol] = useState("BTC")
  const [interval, setInterval] = useState("1m")
  const { book, funding, candles, meta, errors, isLoading, streamConnected } = useMarket(symbol, interval)
  const priceDecimals = meta?.price_decimals ?? 2
  const sizeDecimals = meta?.size_decimals ?? 4
  const latest = candles.at(-1)

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <main id="main-content" className="flex-1 space-y-4 overflow-y-auto p-3 md:p-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-2xl font-bold">行情</h2>
              <p className="text-sm text-zinc-500">数据由 HypeEdge 后端标准化，与策略和落库使用同一来源 · {streamConnected ? "实时流已连接" : "REST 降级"}</p>
            </div>
            <div className="flex gap-2">
              <label className="sr-only" htmlFor="market-symbol">交易品种</label>
              <select id="market-symbol" value={symbol} onChange={(event) => setSymbol(event.target.value)} className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-2">
                {SYMBOLS.map((item) => <option key={item}>{item}</option>)}
              </select>
              <label className="sr-only" htmlFor="market-interval">K 线周期</label>
              <select id="market-interval" value={interval} onChange={(event) => setInterval(event.target.value)} className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-2">
                {INTERVALS.map((item) => <option key={item}>{item}</option>)}
              </select>
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Metric label="最新价" value={latest ? formatPrice(latest.close, priceDecimals) : "—"} />
            <Metric label="Mark Price" value={funding ? formatPrice(funding.mark_price, priceDecimals) : "—"} unavailable={Boolean(errors.funding)} />
            <Metric label="每小时 Funding" value={funding ? formatPct(funding.funding_rate, 4) : "—"} unavailable={Boolean(errors.funding)} />
            <Metric label="Open Interest" value={funding ? formatSize(funding.open_interest, 2) : "—"} unavailable={Boolean(errors.funding)} />
          </div>

          <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
            <section className="rounded-xl border border-zinc-800 bg-zinc-900 p-4" aria-labelledby="chart-title">
              <div className="flex items-center justify-between">
                <h3 id="chart-title" className="font-medium">{symbol} · {interval}</h3>
                <span className="text-xs text-zinc-500">{isLoading ? "同步中" : `${candles.length} 根`}</span>
              </div>
              {errors.candles && candles.length === 0 ? (
                <Unavailable message="K 线暂不可用，后端正在重连或回填" />
              ) : <CandlestickChart candles={candles} />}
            </section>

            <section className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900" aria-labelledby="book-title">
              <div className="flex items-center justify-between border-b border-zinc-800 px-4 py-3">
                <h3 id="book-title" className="font-medium">订单簿</h3>
                <span className="text-xs text-zinc-500">{book ? (book.source === "websocket" ? "实时 WS" : "REST 回退") : "未连接"}</span>
              </div>
              {errors.book && !book ? <Unavailable message="订单簿暂不可用" /> : (
                <div className="grid grid-cols-2 gap-px bg-zinc-800 text-xs">
                  <BookSide title="买盘" levels={book?.bids ?? []} className="text-profit" priceDecimals={priceDecimals} sizeDecimals={sizeDecimals} />
                  <BookSide title="卖盘" levels={book?.asks ?? []} className="text-loss" priceDecimals={priceDecimals} sizeDecimals={sizeDecimals} />
                </div>
              )}
            </section>
          </div>
        </main>
        <StatusBar />
      </div>
    </div>
  )
}

function Metric({ label, value, unavailable = false }: { label: string; value: string; unavailable?: boolean }) {
  return <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4"><div className="text-xs text-zinc-500">{label}</div><div className="mt-1 font-mono text-xl font-bold">{value}</div>{unavailable && <div className="mt-1 text-xs text-warning">数据未就绪</div>}</div>
}

function BookSide({ title, levels, className, priceDecimals, sizeDecimals }: { title: string; levels: [DecimalString, DecimalString][]; className: string; priceDecimals: number; sizeDecimals: number }) {
  return <div className="bg-zinc-900 p-3"><div className={`mb-2 ${className}`}>{title}</div><div className="space-y-1 font-mono">{levels.slice(0, 12).map(([price, size]) => <div key={price} className="flex justify-between gap-2"><span className={className}>{formatPrice(price, priceDecimals)}</span><span className="text-zinc-400">{formatSize(size, sizeDecimals)}</span></div>)}{levels.length === 0 && <div className="py-8 text-center text-zinc-600">—</div>}</div></div>
}

function Unavailable({ message }: { message: string }) {
  return <div role="status" className="flex min-h-40 items-center justify-center p-6 text-center text-sm text-warning">{message}</div>
}
