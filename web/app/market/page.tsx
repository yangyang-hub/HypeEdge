"use client"

import { useState } from "react"
import { CandlestickChart } from "@/components/market/candlestick-chart"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { LiveIndicator } from "@/components/ui/live-indicator"
import { EmptyState, Panel, StaleBanner } from "@/components/ui/data-display"
import { SegmentedControl } from "@/components/ui/segmented-control"
import { useMarket } from "@/hooks/use-market"
import type { DecimalString } from "@/lib/types"
import { cn, formatPct, formatPrice, formatSize } from "@/lib/utils"

const SYMBOLS = ["BTC", "ETH", "SOL"]
const INTERVALS = [
  { value: "1m", label: "1m" },
  { value: "5m", label: "5m" },
  { value: "15m", label: "15m" },
  { value: "1h", label: "1h" },
  { value: "4h", label: "4h" },
]

export default function MarketPage() {
  const [symbol, setSymbol] = useState("BTC")
  const [interval, setInterval] = useState("1m")
  const [tradesOpen, setTradesOpen] = useState(false)
  const { book, funding, candles, meta, errors, isLoading, streamConnected } = useMarket(symbol, interval)
  const priceDecimals = meta?.price_decimals ?? 2
  const sizeDecimals = meta?.size_decimals ?? 4
  const latest = candles.at(-1)

  const mid =
    book && book.bids.length > 0 && book.asks.length > 0
      ? ((Number(book.bids[0][0]) + Number(book.asks[0][0])) / 2).toFixed(priceDecimals)
      : null

  return (
    <AppShell>
      <main id="main-content" className="flex min-h-0 flex-1 flex-col overflow-hidden">
        <div className="flex flex-wrap items-center gap-3 border-b border-border-default bg-bg-panel px-3 py-2 md:px-4">
          <select
            id="market-symbol"
            aria-label="交易品种"
            value={symbol}
            onChange={(event) => setSymbol(event.target.value)}
            className="h-7 rounded-sm border border-border-default bg-bg-elevated px-2 text-sm font-semibold text-text-primary"
          >
            {SYMBOLS.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>

          <span className="font-mono text-2xl font-semibold tabular-nums text-text-primary">
            {latest ? formatPrice(latest.close, priceDecimals) : "—"}
          </span>

          <ToolbarStat label="Mark" value={funding ? formatPrice(funding.mark_price, priceDecimals) : "—"} warn={Boolean(errors.funding)} />
          <ToolbarStat
            label="Funding"
            value={funding ? formatPct(funding.funding_rate, 4) : "—"}
            warn={Boolean(errors.funding)}
            tone={funding && Number(funding.funding_rate) >= 0 ? "profit" : "loss"}
          />
          <ToolbarStat label="OI" value={funding ? formatSize(funding.open_interest, 2) : "—"} warn={Boolean(errors.funding)} />

          <div className="ml-auto flex flex-wrap items-center gap-2">
            <SegmentedControl
              ariaLabel="K 线周期"
              options={INTERVALS}
              value={interval}
              onChange={setInterval}
              size="sm"
            />
            <LiveIndicator
              tone={streamConnected ? "live" : "degraded"}
              label={streamConnected ? "LIVE" : "REST"}
              title={streamConnected ? "实时流已连接" : "REST 降级"}
            />
          </div>
        </div>

        <div className="flex-1 space-y-3 overflow-y-auto p-3 md:p-4">
          <PageHeader
            title="行情"
            subtitle="数据由 HypeEdge 后端标准化，与策略和落库使用同一来源"
          />

          {!streamConnected || errors.book || errors.candles || errors.funding ? (
            <StaleBanner
              message={
                !streamConnected
                  ? "行情延迟 · 已降级 REST"
                  : "部分行情通道暂不可用，后端正在重连或回填"
              }
            />
          ) : null}

          <div className="grid min-h-[420px] gap-3 xl:grid-cols-[minmax(0,1fr)_320px]">
            <Panel
              title={`${symbol} · ${interval}`}
              action={<span className="text-2xs text-text-tertiary">{isLoading ? "同步中" : `${candles.length} 根`}</span>}
              className="min-h-[360px]"
            >
              <div className="p-2">
                {errors.candles && candles.length === 0 ? (
                  <EmptyState message="K 线暂不可用，后端正在重连或回填" />
                ) : (
                  <CandlestickChart candles={candles} priceDecimals={priceDecimals} />
                )}
              </div>
            </Panel>

            <Panel
              title="订单簿"
              action={
                <span className="text-2xs text-text-tertiary">
                  {book ? (book.source === "websocket" ? "WS" : "REST") : "—"}
                </span>
              }
            >
              {errors.book && !book ? (
                <EmptyState message="订单簿暂不可用" />
              ) : (
                <div className="text-xs">
                  <div className="grid grid-cols-3 gap-2 border-b border-border-subtle px-3 py-1.5 text-2xs uppercase tracking-wider text-text-tertiary">
                    <span>价格</span>
                    <span className="text-right">数量</span>
                    <span className="text-right">累计</span>
                  </div>
                  <BookColumn
                    levels={[...(book?.asks ?? [])].slice(0, 10).reverse()}
                    side="ask"
                    priceDecimals={priceDecimals}
                    sizeDecimals={sizeDecimals}
                  />
                  <div className="border-y border-border-subtle bg-bg-hover px-3 py-1.5 text-center font-mono text-sm text-text-primary">
                    {mid ?? "—"}
                  </div>
                  <BookColumn
                    levels={(book?.bids ?? []).slice(0, 10)}
                    side="bid"
                    priceDecimals={priceDecimals}
                    sizeDecimals={sizeDecimals}
                  />
                </div>
              )}
            </Panel>
          </div>

          <Panel
            title="最近成交"
            action={
              <button
                type="button"
                className="text-2xs text-text-tertiary hover:text-text-secondary"
                onClick={() => setTradesOpen((open) => !open)}
              >
                {tradesOpen ? "收起" : "展开"}
              </button>
            }
          >
            {tradesOpen ? (
              <EmptyState message="成交明细通道待接入；当前以订单簿与 K 线为主视图。" className="py-8" />
            ) : (
              <div className="px-3 py-2 text-xs text-text-tertiary">默认折叠 · 点击展开</div>
            )}
          </Panel>
        </div>
      </main>
    </AppShell>
  )
}

function ToolbarStat({
  label,
  value,
  warn = false,
  tone,
}: {
  label: string
  value: string
  warn?: boolean
  tone?: "profit" | "loss"
}) {
  return (
    <div className="hidden items-baseline gap-1.5 md:flex">
      <span className="text-2xs uppercase tracking-wider text-text-tertiary">{label}</span>
      <span
        className={cn(
          "font-mono text-xs tabular-nums",
          tone === "profit" && "text-profit",
          tone === "loss" && "text-loss",
          !tone && "text-text-secondary",
          warn && "text-warning",
        )}
      >
        {value}
      </span>
    </div>
  )
}

function BookColumn({
  levels,
  side,
  priceDecimals,
  sizeDecimals,
}: {
  levels: [DecimalString, DecimalString][]
  side: "bid" | "ask"
  priceDecimals: number
  sizeDecimals: number
}) {
  let cumulative = 0
  const rows = levels.map(([price, size]) => {
    cumulative += Number(size)
    return { price, size, cumulative }
  })
  const maxCumulative = rows.reduce((max, row) => Math.max(max, row.cumulative), 0) || 1

  return (
    <div className={side === "ask" ? "bg-loss/[0.03]" : "bg-profit/[0.03]"}>
      {rows.length === 0 ? (
        <div className="px-3 py-6 text-center text-text-tertiary">—</div>
      ) : (
        rows.map((row) => {
          const width = (row.cumulative / maxCumulative) * 100
          return (
            <div key={`${side}-${row.price}`} className="relative grid grid-cols-3 gap-2 px-3 py-0.5 font-mono">
              <div
                className={cn(
                  "absolute inset-y-0 right-0 opacity-20",
                  side === "bid" ? "bg-profit" : "bg-loss",
                )}
                style={{ width: `${width}%` }}
                aria-hidden="true"
              />
              <span className={cn("relative", side === "bid" ? "text-profit" : "text-loss")}>
                {formatPrice(row.price, priceDecimals)}
              </span>
              <span className="relative text-right text-text-secondary">{formatSize(row.size, sizeDecimals)}</span>
              <span className="relative text-right text-text-tertiary">{formatSize(row.cumulative, sizeDecimals)}</span>
            </div>
          )
        })
      )}
    </div>
  )
}
