"use client"

import Link from "next/link"
import { useState } from "react"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { CreateStrategyDialog } from "@/components/strategy/create-strategy-dialog"
import { Button } from "@/components/ui/button"
import { EmptyState, Panel } from "@/components/ui/data-display"
import { StrategyStatusChip } from "@/components/ui/strategy-status-chip"
import { useStrategies, startStrategy, stopStrategy } from "@/hooks/use-strategies"
import { ApiError } from "@/lib/api"
import { formatDateTime } from "@/lib/utils"
import type { StrategyInstance } from "@/lib/types"

export default function StrategyPage() {
  const { strategies, refresh, error, isLoading } = useStrategies()
  const [createOpen, setCreateOpen] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  async function handleToggle(strategy: StrategyInstance) {
    setActionError(null)
    try {
      if (strategy.actual_state === "running" || strategy.actual_state === "shadow") {
        await stopStrategy(strategy)
      } else {
        await startStrategy(strategy, strategy.strategy_type === "market_maker" ? "shadow" : "running")
      }
      await refresh()
    } catch (e) {
      setActionError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : "策略启停失败")
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
              <StrategyRow key={strategy.strategy_id} strategy={strategy} onToggle={handleToggle} />
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
    </AppShell>
  )
}

function StrategyRow({
  strategy: s,
  onToggle,
}: {
  strategy: StrategyInstance
  onToggle: (strategy: StrategyInstance) => Promise<void>
}) {
  const running = s.actual_state === "running" || s.actual_state === "shadow"
  const busy = s.actual_state === "draining" || s.actual_state === "warming"

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
            <span>symbol {s.symbol}</span>
            <span>sub {s.sub_account ?? "—"}</span>
            <span>updated {formatDateTime(s.updated_at)}</span>
          </div>
          <div className="text-2xs text-text-tertiary">
            Desired {s.desired_state} · Runtime revision {s.revision}
            {s.metadata?.note ? ` · ${s.metadata.note}` : ""}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {s.strategy_type === "market_maker" ? (
            <Button asChild variant="ghost" size="sm">
              <Link href={`/strategy/${encodeURIComponent(s.strategy_id)}/market-making`}>工作台</Link>
            </Button>
          ) : null}
          <Button
            type="button"
            variant={running ? "secondary" : "primary"}
            size="sm"
            disabled={busy}
            title={
              busy
                ? "生命周期切换中"
                : s.strategy_type === "market_maker" && !running
                  ? "启动为 Shadow"
                  : undefined
            }
            onClick={() => void onToggle(s)}
          >
            {running ? "停止" : "启动"}
          </Button>
        </div>
      </div>
    </Panel>
  )
}
