"use client"

import { useState } from "react"
import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import { closePosition, usePositions } from "@/hooks/use-positions"
import { useInstrumentMeta } from "@/hooks/use-system-status"
import type { Position } from "@/lib/types"
import { addDecimals, formatPrice, formatSize, formatUsd, pnlColor } from "@/lib/utils"

export default function PositionsPage() {
  const { positions, error, isLoading, refresh } = usePositions()
  const [pendingSymbol, setPendingSymbol] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)

  async function handleClose(symbol: string) {
    if (!window.confirm(`确认以 reduce-only 市价单全部平掉 ${symbol} 持仓？`)) return
    setPendingSymbol(symbol)
    setActionError(null)
    try {
      await closePosition(symbol)
      await refresh()
    } catch (caught) {
      setActionError(caught instanceof Error ? caught.message : "平仓请求失败")
    } finally {
      setPendingSymbol(null)
    }
  }

  const totalPnl = addDecimals(positions.map((position) => position.unrealized_pnl))

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <main id="main-content" className="flex-1 overflow-y-auto p-3 space-y-4 md:p-6">
          <div className="flex items-center justify-between">
            <h2 className="text-2xl font-bold">持仓管理</h2>
            <div className="flex items-center gap-4">
              <span className="text-sm text-zinc-400">合计 PnL:</span>
              <span className={`text-lg font-bold font-mono ${pnlColor(totalPnl)}`}>
                {formatUsd(totalPnl)}
              </span>
            </div>
          </div>

          {actionError ? <p role="alert" className="rounded-lg bg-loss/15 p-3 text-sm text-loss">{actionError}</p> : null}
          {error ? <p role="status" className="rounded-lg bg-warning/10 p-3 text-sm text-warning">数据刷新失败，正在显示最后一次成功结果</p> : null}
          <div className="overflow-x-auto bg-zinc-900 border border-zinc-800 rounded-xl">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-zinc-500 text-left border-b border-zinc-800">
                  <th className="px-4 py-3">币种</th>
                  <th className="px-4 py-3">方向</th>
                  <th className="px-4 py-3 text-right">数量</th>
                  <th className="px-4 py-3 text-right">入场价</th>
                  <th className="px-4 py-3 text-right">现价</th>
                  <th className="px-4 py-3 text-right">杠杆</th>
                  <th className="px-4 py-3 text-right">未实现PnL</th>
                  <th className="px-4 py-3 text-right">操作</th>
                </tr>
              </thead>
              <tbody>
                {isLoading && positions.length === 0 ? (
                  <tr><td colSpan={8} className="px-4 py-12 text-center text-zinc-500">正在加载持仓…</td></tr>
                ) : positions.length === 0 ? (
                  <tr><td colSpan={8} className="px-4 py-12 text-center text-zinc-600">无活跃持仓</td></tr>
                ) : (
                  positions.map((position) => (
                    <PositionRow
                      key={position.symbol}
                      position={position}
                      pending={pendingSymbol === position.symbol}
                      onClose={handleClose}
                    />
                  ))
                )}
              </tbody>
            </table>
          </div>
        </main>
        <StatusBar />
      </div>
    </div>
  )
}

function PositionRow({ position: p, pending, onClose }: { position: Position; pending: boolean; onClose: (symbol: string) => Promise<void> }) {
  const { meta } = useInstrumentMeta(p.symbol)
  return (
    <tr className="border-b border-zinc-800/50 hover:bg-zinc-800/30">
                      <td className="px-4 py-3 font-medium">{p.symbol}</td>
                      <td className="px-4 py-3">
                        <span className={p.side === "long" ? "text-profit" : "text-loss"}>
                          {p.side === "long" ? "🟢 多" : "🔴 空"}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right font-mono">{formatSize(p.size, meta?.size_decimals ?? 6)}</td>
                      <td className="px-4 py-3 text-right font-mono">{p.entry_price === null ? "—" : formatPrice(p.entry_price, meta?.price_decimals ?? 6)}</td>
                      <td className="px-4 py-3 text-right font-mono">{p.mark_price === null ? "—" : formatPrice(p.mark_price, meta?.price_decimals ?? 6)}</td>
                      <td className="px-4 py-3 text-right">{p.leverage}x</td>
                      <td className={`px-4 py-3 text-right font-mono ${pnlColor(p.unrealized_pnl)}`}>
                        {formatUsd(p.unrealized_pnl)}
                      </td>
                      <td className="px-4 py-3 text-right">
                        <button
                          type="button"
                          disabled={pending}
                          onClick={() => onClose(p.symbol)}
                          className="min-h-9 px-3 py-1 text-xs bg-zinc-800 hover:bg-loss/20 text-zinc-300 hover:text-loss rounded transition-colors disabled:cursor-wait disabled:opacity-50"
                        >
                          {pending ? "提交中…" : "全部平仓"}
                        </button>
                      </td>
    </tr>
  )
}
