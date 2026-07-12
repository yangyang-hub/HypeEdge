"use client"

import { useSSE } from "@/hooks/use-sse"
import { useAccount } from "@/hooks/use-account"
import { useSystemStatus } from "@/hooks/use-system-status"
import { formatPrice, formatUsd } from "@/lib/utils"

export function StatusBar() {
  const { connected } = useSSE()
  const { account } = useAccount()
  const { status } = useSystemStatus()

  return (
    <footer className="min-h-8 border-t border-zinc-800 bg-zinc-900/50 flex flex-wrap items-center px-4 py-1 text-xs text-zinc-500 gap-x-6 gap-y-1" aria-live="polite">
      <span className="flex items-center gap-1.5">
        <span aria-hidden="true" className={connected ? "w-2 h-2 rounded-full bg-profit" : "w-2 h-2 rounded-full bg-loss animate-pulse"} />
        {connected ? "已连接" : "断开"}
      </span>
      <span>环境: {status?.environment ?? "—"}</span>
      <span>交易门禁: {status?.safety_mode ?? "—"}</span>
      {account && (
        <>
          <span>权益: {formatUsd(account.equity)}</span>
          <span>杠杆: {formatPrice(account.leverage, 1)}x</span>
        </>
      )}
      <span className="ml-auto">HypeEdge v0.2.0</span>
    </footer>
  )
}
