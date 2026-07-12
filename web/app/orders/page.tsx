"use client"

import { useState } from "react"
import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import { useOrders, cancelOrder } from "@/hooks/use-orders"
import { ORDER_STATUS_LABELS, ORDER_STATUS_COLORS } from "@/lib/constants"
import { formatPrice, formatSize, formatTime } from "@/lib/utils"
import { useInstrumentMeta } from "@/hooks/use-system-status"
import type { Order } from "@/lib/types"

export default function OrdersPage() {
  const [tab, setTab] = useState<"active" | "terminal">("active")
  const { orders, refresh, error, isLoading } = useOrders(tab)
  const [actionError, setActionError] = useState<string | null>(null)

  const handleCancel = async (cloid: string) => {
    try {
      setActionError(null)
      await cancelOrder(cloid)
      refresh()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "撤单失败")
    }
  }

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <main id="main-content" className="flex-1 overflow-y-auto p-3 space-y-4 md:p-6">
          <h2 className="text-2xl font-bold">订单</h2>

          {/* Tabs */}
          <div role="tablist" aria-label="订单状态" className="flex gap-1 bg-zinc-900 rounded-lg p-1 w-fit">
            <button role="tab" aria-selected={tab === "active"} onClick={() => setTab("active")} className={`min-h-9 px-4 py-1.5 text-sm rounded ${tab === "active" ? "bg-zinc-800 text-white" : "text-zinc-400"}`}>
              活跃订单
            </button>
            <button role="tab" aria-selected={tab === "terminal"} onClick={() => setTab("terminal")} className={`min-h-9 px-4 py-1.5 text-sm rounded ${tab === "terminal" ? "bg-zinc-800 text-white" : "text-zinc-400"}`}>
              历史订单
            </button>
          </div>

          {actionError ? <p role="alert" className="rounded-lg bg-loss/15 p-3 text-sm text-loss">{actionError}</p> : null}
          {error ? <p role="status" className="rounded-lg bg-warning/10 p-3 text-sm text-warning">数据刷新失败，正在显示最后一次成功结果</p> : null}
          <div className="overflow-x-auto bg-zinc-900 border border-zinc-800 rounded-xl">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-zinc-500 text-left border-b border-zinc-800">
                  <th className="px-4 py-3">时间</th>
                  <th className="px-4 py-3">币种</th>
                  <th className="px-4 py-3">方向</th>
                  <th className="px-4 py-3 text-right">数量</th>
                  <th className="px-4 py-3 text-right">价格</th>
                  <th className="px-4 py-3">状态</th>
                  <th className="px-4 py-3 text-right">已成交</th>
                  <th className="px-4 py-3 text-right">操作</th>
                </tr>
              </thead>
              <tbody>
                {isLoading && orders.length === 0 ? (
                  <tr><td colSpan={8} className="px-4 py-12 text-center text-zinc-500">正在加载订单…</td></tr>
                ) : orders.length === 0 ? (
                  <tr><td colSpan={8} className="px-4 py-12 text-center text-zinc-600">
                    {tab === "active" ? "无活跃订单" : "无历史订单"}
                  </td></tr>
                ) : (
                  orders.map((o) => <OrderRow key={o.cloid} order={o} tab={tab} onCancel={handleCancel} />)
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

function OrderRow({ order: o, tab, onCancel }: { order: Order; tab: "active" | "terminal"; onCancel: (cloid: string) => Promise<void> }) {
  const { meta } = useInstrumentMeta(o.symbol)
  const cancelInFlight = o.status === "cancel_pending" || o.status === "cancel_unknown"
  return (
    <tr className="border-b border-zinc-800/50 hover:bg-zinc-800/30">
                      <td className="px-4 py-3 text-zinc-400 font-mono text-xs">{formatTime(o.created_at)}</td>
                      <td className="px-4 py-3 font-medium">{o.symbol}</td>
                      <td className="px-4 py-3">
                        <span className={o.side === "buy" ? "text-profit" : "text-loss"}>
                          {o.side === "buy" ? "买" : "卖"}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right font-mono">{formatSize(o.size, meta?.size_decimals ?? 6)}</td>
                      <td className="px-4 py-3 text-right font-mono">{o.price === null ? "市价" : formatPrice(o.price, meta?.price_decimals ?? 6)}</td>
                      <td className="px-4 py-3">
                        <span className={ORDER_STATUS_COLORS[o.status] ?? "text-zinc-400"}>
                          {ORDER_STATUS_LABELS[o.status] ?? o.status}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right font-mono">{formatSize(o.filled_size, meta?.size_decimals ?? 6)}</td>
                      <td className="px-4 py-3 text-right">
                        {tab === "active" && (
                          <button type="button" disabled={cancelInFlight} onClick={() => onCancel(o.cloid)} className="min-h-9 px-3 py-1 text-xs bg-zinc-800 hover:bg-loss/20 text-zinc-300 hover:text-loss rounded transition-colors disabled:cursor-wait disabled:opacity-50">
                            {cancelInFlight ? "确认中…" : "撤单"}
                          </button>
                        )}
                      </td>
    </tr>
  )
}
