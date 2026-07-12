"use client"

import Link from "next/link"
import { useState } from "react"
import { ShieldAlert } from "lucide-react"
import { useAccount } from "@/hooks/use-account"
import { useSSE } from "@/hooks/use-sse"
import { useSystemStatus } from "@/hooks/use-system-status"
import { triggerKillSwitch } from "@/hooks/use-risk"
import { Button } from "@/components/ui/button"
import { ConfirmPhraseDialog } from "@/components/ui/confirm-phrase-dialog"
import { EnvBadge } from "@/components/ui/env-badge"
import { LiveIndicator, type LiveTone } from "@/components/ui/live-indicator"
import { PnLText } from "@/components/ui/data-display"
import { formatUsd } from "@/lib/utils"

function liveTone(connected: boolean, killActive: boolean): LiveTone {
  if (killActive) return "degraded"
  return connected ? "live" : "offline"
}

export function Topbar() {
  const { status, mutate } = useSystemStatus()
  const { connected } = useSSE()
  const { account } = useAccount()
  const [killOpen, setKillOpen] = useState(false)
  const [killing, setKilling] = useState(false)

  const tone = liveTone(connected, Boolean(status?.kill_switch_active))

  async function handleKill() {
    setKilling(true)
    try {
      await triggerKillSwitch("manual_trigger_topbar")
      await mutate()
      setKillOpen(false)
    } catch (error) {
      console.error("Kill switch trigger failed:", error)
    } finally {
      setKilling(false)
    }
  }

  return (
    <>
      <header className="flex h-12 shrink-0 items-center gap-3 border-b border-border-default bg-bg-base px-3 md:px-4">
        <Link href="/" className="shrink-0 text-sm font-bold tracking-tight text-text-primary">
          HypeEdge
        </Link>

        <EnvBadge environment={status?.environment} />

        <LiveIndicator
          tone={tone}
          title={connected ? "SSE 已连接" : "SSE 断开"}
        />

        {status?.safety_mode ? (
          <span className="hidden rounded-sm border border-border-subtle bg-bg-panel px-2 py-0.5 text-2xs uppercase tracking-wider text-text-secondary sm:inline-flex">
            {status.safety_mode}
          </span>
        ) : null}

        <div className="ml-auto flex items-center gap-3">
          {account ? (
            <Link
              href="/"
              className="hidden items-center gap-3 text-xs text-text-secondary hover:text-text-primary md:flex"
            >
              <span>
                权益 <span className="font-mono text-text-primary">{formatUsd(account.equity)}</span>
              </span>
              <span className="flex items-center gap-1">
                uPnL <PnLText value={account.total_unrealized_pnl} />
              </span>
            </Link>
          ) : null}

          <Button
            type="button"
            variant="danger-soft"
            size="sm"
            disabled={Boolean(status?.kill_switch_active)}
            title={status?.kill_switch_active ? "Kill Switch 已触发" : "触发 Kill Switch"}
            onClick={() => setKillOpen(true)}
          >
            <ShieldAlert className="h-3.5 w-3.5" aria-hidden="true" />
            <span className="hidden sm:inline">Kill Switch</span>
          </Button>
        </div>
      </header>

      <ConfirmPhraseDialog
        open={killOpen}
        onOpenChange={setKillOpen}
        title="触发 Kill Switch"
        description="Kill Switch 已触发后，所有挂单将撤销，交易将被门禁拦截。输入 CONFIRM 确认。"
        phrase="CONFIRM"
        confirmLabel="触发 Kill Switch"
        loading={killing}
        onConfirm={handleKill}
      />
    </>
  )
}
