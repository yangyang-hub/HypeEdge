"use client"

import { AlertTriangle, RadioTower } from "lucide-react"
import { useSSE } from "@/hooks/use-sse"
import { useSystemStatus } from "@/hooks/use-system-status"

export function GlobalAlerts() {
  const { status, error } = useSystemStatus()
  const { connected } = useSSE()

  return (
    <div className="fixed inset-x-0 top-0 z-50 space-y-px" aria-live="assertive">
      {status?.environment === "mainnet" ? (
        <div className="flex min-h-10 items-center justify-center gap-2 bg-amber-500 px-4 py-2 text-sm font-bold text-black">
          <AlertTriangle aria-hidden="true" className="h-4 w-4" /> MAINNET 实盘环境
        </div>
      ) : null}
      {status?.kill_switch_active ? (
        <div className="flex min-h-12 items-center justify-center gap-2 bg-critical px-4 py-2 text-sm font-bold text-white" role="alert">
          <AlertTriangle aria-hidden="true" className="h-5 w-5" /> Kill Switch 已触发
          {status.kill_switch_reason ? `：${status.kill_switch_reason}` : ""}
        </div>
      ) : null}
      {status && status.safety_mode !== "normal" && !status.kill_switch_active ? (
        <div className="flex min-h-8 items-center justify-center gap-2 bg-amber-950 px-4 py-1 text-xs text-amber-200" role="status">
          <AlertTriangle aria-hidden="true" className="h-3.5 w-3.5" />
          交易门禁：{status.safety_mode}
          {status.safety_reason ? `（${status.safety_reason}）` : ""}
        </div>
      ) : null}
      {error || !connected ? (
        <div className="flex min-h-8 items-center justify-center gap-2 bg-zinc-800 px-4 py-1 text-xs text-amber-300" role="status">
          <RadioTower aria-hidden="true" className="h-3.5 w-3.5" />
          实时连接中断，当前数据可能已过期；界面保留最后一次成功数据
        </div>
      ) : null}
    </div>
  )
}
