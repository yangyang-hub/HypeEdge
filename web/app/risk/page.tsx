"use client"

import { useState } from "react"
import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import { useRiskStatus, triggerKillSwitch, resetKillSwitch } from "@/hooks/use-risk"
import { decimalToNumber, formatPct, formatPrice } from "@/lib/utils"

export default function RiskPage() {
  const { risk, refresh } = useRiskStatus()
  const [confirmText, setConfirmText] = useState("")

  const handleTrigger = async () => {
    if (confirmText !== "CONFIRM") return
    try {
      await triggerKillSwitch("manual_trigger_ui")
      refresh()
      setConfirmText("")
    } catch (e) {
      console.error("Kill switch trigger failed:", e)
    }
  }

  const handleReset = async () => {
    try {
      await resetKillSwitch()
      refresh()
    } catch (e) {
      console.error("Kill switch reset failed:", e)
    }
  }

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <main id="main-content" className="flex-1 overflow-y-auto p-3 space-y-6 md:p-6">
          <div className="flex items-center justify-between">
            <h2 className="text-2xl font-bold">风控面板</h2>
            <div className="flex gap-2">
              <button
                onClick={handleReset}
                className="px-4 py-2 text-sm bg-profit/20 hover:bg-profit/30 text-profit rounded-lg"
              >
                重置 Kill Switch
              </button>
            </div>
          </div>

          {/* Kill Switch */}
          <div className={`border rounded-xl p-5 ${risk?.kill_switch_active ? "border-loss bg-loss/5" : "border-zinc-800 bg-zinc-900"}`}>
            <div className="flex items-center justify-between mb-3">
              <span className="font-medium">Kill Switch</span>
              <span className={`text-sm px-2 py-0.5 rounded ${risk?.kill_switch_active ? "bg-loss/20 text-loss" : "bg-profit/20 text-profit"}`}>
                {risk?.kill_switch_active ? "🚨 已触发" : "✅ 正常"}
              </span>
            </div>
            {risk?.kill_switch_reason && (
              <p className="text-sm text-zinc-400 mb-3">原因: {risk.kill_switch_reason}</p>
            )}
            <div className="flex gap-2 items-end">
              <input
                type="text"
                value={confirmText}
                onChange={(e) => setConfirmText(e.target.value)}
                placeholder="输入 CONFIRM 触发"
                className="bg-zinc-800 border border-zinc-700 rounded px-3 py-2 text-sm w-48"
              />
              <button
                onClick={handleTrigger}
                disabled={confirmText !== "CONFIRM"}
                className="px-4 py-2 text-sm bg-loss/20 hover:bg-loss/30 text-loss rounded-lg disabled:opacity-30 disabled:cursor-not-allowed"
              >
                🚨 触发 Kill Switch
              </button>
            </div>
          </div>

          {/* Limits */}
          {risk && (
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-3">
              <h3 className="font-medium mb-3">限额使用</h3>
              {risk.limits.map((l) => {
                const used = decimalToNumber(l.pct_used)
                return <div key={l.name} className="flex items-center gap-3">
                  <span className="text-sm text-zinc-400 w-28">{l.name}</span>
                  <div className="flex-1 h-3 bg-zinc-800 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all ${used > 0.8 ? "bg-loss" : used > 0.6 ? "bg-warning" : "bg-profit"}`}
                      style={{ width: `${Math.min(used * 100, 100)}%` }}
                    />
                  </div>
                  <span className="text-xs text-zinc-500 w-24 text-right">
                    {l.unit === "%" ? formatPct(l.current, 1) : `${formatPrice(l.current, 1)} ${l.unit}`} / {l.unit === "%" ? formatPct(l.limit, 1) : formatPrice(l.limit, 1)}
                  </span>
                  <span className={`text-xs w-12 text-right ${used > 0.8 ? "text-loss" : used > 0.6 ? "text-warning" : "text-profit"}`}>
                    {formatPct(l.pct_used, 0)}
                  </span>
                </div>
              })}
            </div>
          )}

          {/* Check Stats */}
          {risk?.check_stats && (
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
              <h3 className="font-medium mb-3">风控检查统计</h3>
              <div className="grid grid-cols-1 gap-4 text-center sm:grid-cols-3">
                <div>
                  <div className="text-2xl font-bold font-mono">{risk.check_stats.check_count ?? 0}</div>
                  <div className="text-xs text-zinc-500">总检查</div>
                </div>
                <div>
                  <div className="text-2xl font-bold font-mono text-profit">{risk.check_stats.pass_count ?? 0}</div>
                  <div className="text-xs text-zinc-500">通过</div>
                </div>
                <div>
                  <div className="text-2xl font-bold font-mono text-loss">{risk.check_stats.reject_count ?? 0}</div>
                  <div className="text-xs text-zinc-500">拒绝</div>
                </div>
              </div>
            </div>
          )}
        </main>
        <StatusBar />
      </div>
    </div>
  )
}
