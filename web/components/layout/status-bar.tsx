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
    <footer
      className="flex min-h-7 flex-wrap items-center gap-x-4 gap-y-1 border-t border-border-default bg-bg-base px-3 py-1 text-2xs text-text-tertiary md:px-4"
      aria-live="polite"
    >
      <span>SSE · {connected ? "ok" : "down"}</span>
      <span>环境 · {status?.environment ?? "—"}</span>
      <span>门禁 · {status?.safety_mode ?? "—"}</span>
      {account ? (
        <>
          <span className="font-mono">权益 · {formatUsd(account.equity)}</span>
          <span className="font-mono">杠杆 · {formatPrice(account.leverage, 1)}x</span>
        </>
      ) : null}
      <span className="ml-auto font-mono">HypeEdge v0.2.0</span>
    </footer>
  )
}
