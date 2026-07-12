"use client"

import Link from "next/link"
import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import { useStrategies, startStrategy, stopStrategy } from "@/hooks/use-strategies"
import { formatDateTime } from "@/lib/utils"
import type { StrategyInstance } from "@/lib/types"

const STATUS_BADGE: Record<string, { label: string; cls: string }> = {
  running: { label: "🟢 运行中", cls: "bg-profit/20 text-profit" },
  shadow: { label: "🔵 Shadow", cls: "bg-zinc-800 text-zinc-300" },
  stopped: { label: "⚪ 已停止", cls: "bg-zinc-800 text-zinc-400" },
  warming: { label: "🔵 预热中", cls: "bg-zinc-800 text-zinc-300" },
  paused: { label: "🟡 暂停", cls: "bg-warning/20 text-warning" },
  faulted: { label: "🔴 故障", cls: "bg-loss/20 text-loss" },
  draining: { label: "🟠 排空中", cls: "bg-warning/20 text-warning" },
}

export default function StrategyPage() {
  const { strategies, refresh } = useStrategies()

  const handleToggle = async (id: string, status: string) => {
    try {
      if (status === "running" || status === "shadow") {
        await stopStrategy(id)
      } else {
        await startStrategy(id)
      }
      refresh()
    } catch (e) {
      console.error("Strategy toggle failed:", e)
    }
  }

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <main id="main-content" className="flex-1 overflow-y-auto p-3 space-y-4 md:p-6">
          <h2 className="text-2xl font-bold">策略管理</h2>

          {strategies.length === 0 ? (
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-8 text-center text-zinc-500">
              无策略实例
            </div>
          ) : (
            strategies.map((strategy) => <StrategyCard key={strategy.strategy_id} strategy={strategy} onToggle={handleToggle} />)
          )}
        </main>
        <StatusBar />
      </div>
    </div>
  )
}

function StrategyCard({ strategy: s, onToggle }: { strategy: StrategyInstance; onToggle: (id: string, status: string) => Promise<void> }) {
  const badge = STATUS_BADGE[s.actual_state] ?? STATUS_BADGE.stopped
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="text-lg font-bold">{s.strategy_id}</h3>
                      <div className="mt-1 flex flex-wrap gap-2">
                        <span className={`text-xs px-2 py-0.5 rounded ${badge.cls}`}>{badge.label}</span>
                        <span className="rounded bg-zinc-800 px-2 py-0.5 text-xs text-zinc-400">{s.strategy_type}</span>
                      </div>
                    </div>
                    <button
                      onClick={() => onToggle(s.strategy_id, s.actual_state)}
                      disabled={s.actual_state === "draining" || s.actual_state === "warming"}
                      className={`px-4 py-2 text-sm rounded-lg font-medium transition-colors disabled:cursor-wait disabled:opacity-50 ${
                        s.actual_state === "running" || s.actual_state === "shadow"
                          ? "bg-loss/20 hover:bg-loss/30 text-loss"
                          : "bg-profit/20 hover:bg-profit/30 text-profit"
                      }`}
                    >
                      {s.actual_state === "running" || s.actual_state === "shadow" ? "⏹ 停止" : "▶ 启动"}
                    </button>
                  </div>

                  <div className="grid grid-cols-1 gap-4 text-sm sm:grid-cols-3">
                    <div>
                      <span className="text-zinc-500">交易对: </span>
                      <span className="font-mono">{s.symbol}</span>
                    </div>
                    <div>
                      <span className="text-zinc-500">子账户: </span>
                      <span className="font-mono">{s.sub_account ?? "—"}</span>
                    </div>
                    <div>
                      <span className="text-zinc-500">最后更新: </span>
                      <span className="font-mono">{formatDateTime(s.updated_at)}</span>
                    </div>
                  </div>

                  <div className="flex items-center justify-between rounded bg-zinc-800/50 p-3 text-xs text-zinc-500">
                    <span>Desired: {s.desired_state} · Runtime revision: {s.revision}</span>
                    {s.strategy_type === "market_maker" ? (
                      <Link href={`/strategy/${encodeURIComponent(s.strategy_id)}/market-making`} className="rounded-md bg-zinc-700 px-3 py-2 text-zinc-100 hover:bg-zinc-600">
                        打开做市工作台
                      </Link>
                    ) : null}
                  </div>
    </div>
  )
}
