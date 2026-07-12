"use client"

import Link from "next/link"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { Button } from "@/components/ui/button"
import { EmptyState, Panel } from "@/components/ui/data-display"
import { StrategyStatusChip } from "@/components/ui/strategy-status-chip"
import { useStrategies, startStrategy, stopStrategy } from "@/hooks/use-strategies"
import { formatDateTime } from "@/lib/utils"
import type { StrategyInstance } from "@/lib/types"

export default function StrategyPage() {
  const { strategies, refresh, error, isLoading } = useStrategies()

  async function handleToggle(id: string, status: string) {
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
    <AppShell>
      <main id="main-content" className="flex-1 space-y-4 overflow-y-auto p-3 md:p-5">
        <PageHeader title="策略管理" subtitle="启停策略实例并进入做市工作台" />

        {error ? (
          <p role="status" className="rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-sm text-warning">
            策略列表刷新失败，显示缓存数据
          </p>
        ) : null}

        {isLoading && strategies.length === 0 ? (
          <Panel>
            <EmptyState message="正在加载策略…" />
          </Panel>
        ) : strategies.length === 0 ? (
          <Panel>
            <EmptyState message="无策略实例" />
          </Panel>
        ) : (
          <div className="space-y-3">
            {strategies.map((strategy) => (
              <StrategyRow key={strategy.strategy_id} strategy={strategy} onToggle={handleToggle} />
            ))}
          </div>
        )}
      </main>
    </AppShell>
  )
}

function StrategyRow({
  strategy: s,
  onToggle,
}: {
  strategy: StrategyInstance
  onToggle: (id: string, status: string) => Promise<void>
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
            title={busy ? "生命周期切换中" : undefined}
            onClick={() => void onToggle(s.strategy_id, s.actual_state)}
          >
            {running ? "停止" : "启动"}
          </Button>
        </div>
      </div>
    </Panel>
  )
}
