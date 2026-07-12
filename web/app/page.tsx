"use client"

import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import { useAccount } from "@/hooks/use-account"
import { usePositions } from "@/hooks/use-positions"
import { useRiskStatus } from "@/hooks/use-risk"
import { useInstrumentMeta } from "@/hooks/use-system-status"
import type { Position } from "@/lib/types"
import { decimalToNumber, formatPct, formatPrice, formatSize, formatUsd, pnlColor } from "@/lib/utils"

function MetricCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
      <div className="text-xs text-zinc-500 mb-1">{label}</div>
      <div className="text-xl font-bold font-mono">{value}</div>
      {sub && <div className="text-xs text-zinc-500 mt-1">{sub}</div>}
    </div>
  )
}

export default function DashboardPage() {
  const { account } = useAccount()
  const { positions } = usePositions()
  const { risk } = useRiskStatus()

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <main id="main-content" className="flex-1 overflow-y-auto p-3 space-y-6 md:p-6">
          <h2 className="text-2xl font-bold">账户总览</h2>

          {/* Metric Cards */}
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-5">
            <MetricCard label="总权益" value={account ? formatUsd(account.equity) : "—"} sub={account ? `${formatPct(account.drawdown_pct)} 回撤` : undefined} />
            <MetricCard label="未实现PnL" value={account ? formatUsd(account.total_unrealized_pnl) : "—"} />
            <MetricCard label="杠杆" value={account ? `${formatPrice(account.leverage, 1)}x` : "—"} />
            <MetricCard label="总费用" value={account ? formatUsd(account.total_fees) : "—"} />
            <MetricCard label="成交笔数" value={account ? `${account.fill_count}` : "—"} />
          </div>

          {/* Positions Table */}
          <div className="overflow-x-auto bg-zinc-900 border border-zinc-800 rounded-xl">
            <div className="px-4 py-3 border-b border-zinc-800 font-medium">活跃持仓</div>
            <table className="w-full text-sm">
              <thead>
                <tr className="text-zinc-500 text-left border-b border-zinc-800">
                  <th className="px-4 py-2">币种</th>
                  <th className="px-4 py-2">方向</th>
                  <th className="px-4 py-2 text-right">数量</th>
                  <th className="px-4 py-2 text-right">入场价</th>
                  <th className="px-4 py-2 text-right">现价</th>
                  <th className="px-4 py-2 text-right">未实现PnL</th>
                </tr>
              </thead>
              <tbody>
                {positions.length === 0 ? (
                  <tr><td colSpan={6} className="px-4 py-8 text-center text-zinc-600">无持仓</td></tr>
                ) : (
                  positions.map((position) => <DashboardPositionRow key={position.symbol} position={position} />)
                )}
              </tbody>
            </table>
          </div>

          {/* Risk Summary */}
          {risk && (
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
              <div className="flex items-center justify-between mb-3">
                <span className="font-medium">风控状态</span>
                <span className={`text-sm px-2 py-0.5 rounded ${risk.kill_switch_active ? "bg-loss/20 text-loss" : "bg-profit/20 text-profit"}`}>
                  {risk.kill_switch_active ? "🚨 已触发" : "✅ 正常"}
                </span>
              </div>
              <div className="space-y-2">
                {risk.limits.map((l) => {
                  const used = decimalToNumber(l.pct_used)
                  return <div key={l.name} className="flex items-center gap-3">
                    <span className="text-sm text-zinc-400 w-24">{l.name}</span>
                    <div className="flex-1 h-2 bg-zinc-800 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full ${used > 0.8 ? "bg-loss" : used > 0.6 ? "bg-warning" : "bg-profit"}`}
                        style={{ width: `${Math.min(used * 100, 100)}%` }}
                      />
                    </div>
                    <span className="text-xs text-zinc-500 w-20 text-right">
                      {l.unit === "%" ? formatPct(l.current) : `${formatPrice(l.current, 1)} ${l.unit}`} / {l.unit === "%" ? formatPct(l.limit) : formatPrice(l.limit, 1)}
                    </span>
                  </div>
                })}
              </div>
            </div>
          )}
        </main>
        <StatusBar />
      </div>
    </div>
  )
}

function DashboardPositionRow({ position: p }: { position: Position }) {
  const { meta } = useInstrumentMeta(p.symbol)
  return (
    <tr className="border-b border-zinc-800/50 hover:bg-zinc-800/30">
      <td className="px-4 py-2 font-medium">{p.symbol}</td>
      <td className="px-4 py-2"><span className={p.side === "long" ? "text-profit" : "text-loss"}>{p.side === "long" ? "🟢 多" : "🔴 空"}</span></td>
      <td className="px-4 py-2 text-right font-mono">{formatSize(p.size, meta?.size_decimals ?? 6)}</td>
      <td className="px-4 py-2 text-right font-mono">{p.entry_price === null ? "—" : formatPrice(p.entry_price, meta?.price_decimals ?? 6)}</td>
      <td className="px-4 py-2 text-right font-mono">{p.mark_price === null ? "—" : formatPrice(p.mark_price, meta?.price_decimals ?? 6)}</td>
      <td className={`px-4 py-2 text-right font-mono ${pnlColor(p.unrealized_pnl)}`}>{formatUsd(p.unrealized_pnl)}</td>
    </tr>
  )
}
