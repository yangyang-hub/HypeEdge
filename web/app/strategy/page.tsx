"use client"

import Link from "next/link"
import { useState } from "react"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { CreateStrategyDialog } from "@/components/strategy/create-strategy-dialog"
import { AlertConfirmDialog } from "@/components/ui/alert-confirm-dialog"
import { Button } from "@/components/ui/button"
import { EmptyState, Panel } from "@/components/ui/data-display"
import { StrategyStatusChip } from "@/components/ui/strategy-status-chip"
import { archiveStrategy, startStrategy, stopStrategy, useStrategies } from "@/hooks/use-strategies"
import { ApiError } from "@/lib/api"
import { formatDateTime } from "@/lib/utils"
import type { StrategyInstance } from "@/lib/types"

const RUNTIME_REASON_LABELS: Record<string, string> = {
  action_credits_unavailable: "动作额度不可用",
  action_budget_stale: "动作额度数据已过期",
  authenticated_stream_disconnected: "认证数据流已断开",
  clearinghouse_poll_failed: "账户状态刷新失败",
  exchange_history_recovery_failed: "交易历史补缺失败",
  operator_pause: "操作员已暂停",
  reconciliation_failed: "账户对账失败",
  user_fill_queue_overflow: "成交事件队列溢出",
}

function formatRuntimeReason(reason: string): string {
  const systemPausePrefix = "system_safety_pause:"
  const isSystemPause = reason.startsWith(systemPausePrefix)
  const detail = isSystemPause ? reason.slice(systemPausePrefix.length) : reason
  const code = detail.split(":", 1)[0]
  const label = RUNTIME_REASON_LABELS[code] ?? detail
  return isSystemPause ? `系统安全暂停：${label}` : `运行状态：${label}`
}

export default function StrategyPage() {
  const { strategies, refresh, error, isLoading } = useStrategies()
  const [createOpen, setCreateOpen] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)
  const [archiveTarget, setArchiveTarget] = useState<StrategyInstance | null>(null)
  const [archiving, setArchiving] = useState(false)

  async function handleToggle(strategy: StrategyInstance) {
    setActionError(null)
    try {
      if (strategy.desired_state !== "stopped" || strategy.actual_state !== "stopped") {
        await stopStrategy(strategy)
      } else {
        await startStrategy(strategy, strategy.strategy_type === "market_maker" ? "shadow" : "running")
      }
      await refresh()
    } catch (e) {
      setActionError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : "策略启停失败")
    }
  }

  async function handleArchive() {
    if (!archiveTarget) return
    setActionError(null)
    setArchiving(true)
    try {
      const archivedId = archiveTarget.strategy_id
      await archiveStrategy(archiveTarget)
      setArchiveTarget(null)
      await refresh(
        (current) => current?.filter((strategy) => strategy.strategy_id !== archivedId),
        { revalidate: false },
      )
      void refresh()
    } catch (e) {
      setActionError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : "策略删除失败")
    } finally {
      setArchiving(false)
    }
  }

  const createButton = (
    <Button type="button" variant="primary" size="sm" onClick={() => setCreateOpen(true)}>
      新建策略
    </Button>
  )

  return (
    <AppShell>
      <main id="main-content" className="flex-1 space-y-4 overflow-y-auto p-3 md:p-5">
        <PageHeader
          title="策略管理"
          subtitle="创建多类型实例、启停策略并进入工作台"
          actions={createButton}
        />

        {error ? (
          <p role="status" className="rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-sm text-warning">
            策略列表刷新失败，显示缓存数据
          </p>
        ) : null}
        {actionError ? (
          <p role="alert" className="rounded-md border border-critical/30 bg-critical/10 px-3 py-2 text-sm text-critical">
            {actionError}
          </p>
        ) : null}

        {isLoading && strategies.length === 0 ? (
          <Panel>
            <EmptyState message="正在加载策略…" />
          </Panel>
        ) : strategies.length === 0 ? (
          <Panel>
            <EmptyState message="无策略实例" action={createButton} />
          </Panel>
        ) : (
          <div className="space-y-3">
            {strategies.map((strategy) => (
              <StrategyRow
                key={strategy.strategy_id}
                strategy={strategy}
                onToggle={handleToggle}
                onArchive={setArchiveTarget}
              />
            ))}
          </div>
        )}
      </main>

      <CreateStrategyDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        existing={strategies}
        onCreated={() => void refresh()}
      />
      <AlertConfirmDialog
        open={archiveTarget !== null}
        onOpenChange={(open) => {
          if (!open && !archiving) setArchiveTarget(null)
        }}
        title="删除策略"
        description={`确认删除策略 ${archiveTarget?.strategy_id ?? ""}？该策略会从当前列表移除，但配置、订单、成交与审计历史仍会保留。`}
        confirmLabel="确认删除"
        danger
        loading={archiving}
        onConfirm={handleArchive}
      />
    </AppShell>
  )
}

function StrategyRow({
  strategy: s,
  onToggle,
  onArchive,
}: {
  strategy: StrategyInstance
  onToggle: (strategy: StrategyInstance) => Promise<void>
  onArchive: (strategy: StrategyInstance) => void
}) {
  const shouldStop = s.desired_state !== "stopped" || s.actual_state !== "stopped"
  const busy = s.actual_state === "draining" || s.actual_state === "warming"
  const canArchive = s.desired_state === "stopped" && s.actual_state === "stopped"
  const runtimeReason =
    s.runtime_reason && (s.actual_state === "paused" || s.actual_state === "faulted")
      ? formatRuntimeReason(s.runtime_reason)
      : null

  return (
    <Panel>
      <div className="flex flex-wrap items-start justify-between gap-3 p-4">
        <div className="min-w-0 space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-base font-semibold text-text-primary">{s.strategy_id}</h3>
            <StrategyStatusChip state={s.actual_state} />
            <span className="rounded-sm bg-bg-active px-1.5 py-0.5 text-2xs text-text-tertiary">{s.strategy_type}</span>
          </div>
          <div className="flex flex-wrap gap-x-4 gap-y-1 font-mono text-xs text-text-secondary">
            <span>{s.strategy_type === "funding_arb" ? "market 自动选择" : `symbol ${s.symbol}`}</span>
            <span>sub {s.sub_account ?? "—"}</span>
            <span>updated {formatDateTime(s.updated_at)}</span>
          </div>
          <div className="text-2xs text-text-tertiary">
            Desired {s.desired_state} · Runtime revision {s.revision}
            {s.metadata?.note ? ` · ${s.metadata.note}` : ""}
          </div>
          {runtimeReason ? (
            <p className="text-xs text-warning" title={s.runtime_reason ?? undefined}>
              {runtimeReason}
            </p>
          ) : null}
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {s.strategy_type === "market_maker" ? (
            <Button asChild variant="ghost" size="sm">
              <Link href={`/strategy/${encodeURIComponent(s.strategy_id)}/market-making`}>工作台</Link>
            </Button>
          ) : s.strategy_type === "funding_arb" ? (
            <Button asChild variant="ghost" size="sm">
              <Link href={`/strategy/${encodeURIComponent(s.strategy_id)}/funding-arb`}>工作台</Link>
            </Button>
          ) : null}
          {s.strategy_type !== "legacy" ? (
            <Button
              type="button"
              variant="danger-soft"
              size="sm"
              disabled={!canArchive}
              title={canArchive ? undefined : "请先停止策略再删除"}
              onClick={() => onArchive(s)}
            >
              删除
            </Button>
          ) : null}
          <Button
            type="button"
            variant={shouldStop ? "secondary" : "primary"}
            size="sm"
            disabled={busy}
            title={
              busy
                ? "生命周期切换中"
                : s.strategy_type === "market_maker" && !shouldStop
                  ? "启动为 Shadow"
                  : undefined
            }
            onClick={() => void onToggle(s)}
          >
            {shouldStop ? "停止" : "启动"}
          </Button>
        </div>
      </div>
    </Panel>
  )
}
