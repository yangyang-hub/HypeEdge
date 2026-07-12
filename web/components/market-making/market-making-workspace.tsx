"use client"

import dynamic from "next/dynamic"
import Link from "next/link"
import Decimal from "decimal.js"
import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  CircleDollarSign,
  DatabaseZap,
  Gauge,
  RadioTower,
  RefreshCw,
  Settings2,
  ShieldAlert,
} from "lucide-react"
import { useState } from "react"
import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import {
  activateMarketMakerConfig,
  createMarketMakerConfig,
  rollbackMarketMakerConfig,
  runStrategyAction,
  useMarketMaking,
} from "@/hooks/use-market-making"
import { useMarketMakingRealtime, type MarketMakingDisplayOverlay } from "@/hooks/use-market-making-realtime"
import type {
  BudgetMode,
  DecimalString,
  ExternalReferenceQuality,
  ExternalReferenceSnapshot,
  MarketMakerConfig,
  MarketMakerConfigVersion,
  MarketMakingActionBudgetSnapshot,
  MarketMakingEvent,
  MarketMakingInventorySnapshot,
  MarketMakingPerformanceSnapshot,
  MarketMakingQuotesSnapshot,
  MarketMakingStateSnapshot,
  QuoteSlotSnapshot,
} from "@/lib/types"
import { cn, formatDateTime, formatPct, formatPrice, formatSize, formatUsd, pnlColor } from "@/lib/utils"

const PnlChart = dynamic(() => import("@/components/market-making/pnl-chart").then((module) => module.PnlChart), {
  loading: () => <div className="h-64 animate-pulse rounded-lg bg-zinc-800/60" aria-label="正在加载 PnL 图表" />,
  ssr: false,
})

const LIFECYCLE_ACTIONS = ["start", "pause", "resume", "drain", "stop"] as const

interface MarketMakingWorkspaceProps {
  strategyId: string
}

export function MarketMakingWorkspace({ strategyId }: MarketMakingWorkspaceProps) {
  const snapshot = useMarketMaking(strategyId)
  const realtime = useMarketMakingRealtime(
    strategyId,
    snapshot.state?.runtime_revision ?? 0,
    snapshot.quotes?.market_revision ?? 0,
    () => void snapshot.resync(),
  )
  const [pendingAction, setPendingAction] = useState<(typeof LIFECYCLE_ACTIONS)[number] | null>(null)
  const [confirmation, setConfirmation] = useState("")
  const [commandError, setCommandError] = useState<string | null>(null)
  const state = snapshot.state

  async function executeLifecycleAction(action: (typeof LIFECYCLE_ACTIONS)[number]) {
    const requiredConfirmation = state?.environment === "mainnet" ? `CONFIRM MAINNET ${action.toUpperCase()}` : ""
    if (requiredConfirmation && confirmation !== requiredConfirmation) {
      setPendingAction(action)
      return
    }
    setCommandError(null)
    try {
      await runStrategyAction(strategyId, action, state?.runtime_revision ?? 0, {
        confirmation: confirmation || undefined,
      })
      setPendingAction(null)
      setConfirmation("")
      await snapshot.resync()
    } catch (error) {
      setCommandError(error instanceof Error ? error.message : "生命周期命令失败")
    }
  }

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <main id="main-content" className="flex-1 space-y-5 overflow-y-auto p-3 md:p-6">
          <header className="space-y-3">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <Link href="/strategy" className="mb-2 inline-flex items-center gap-1 text-sm text-zinc-400 hover:text-white">
                  <ArrowLeft className="h-4 w-4" aria-hidden="true" /> 返回策略列表
                </Link>
                <h2 className="text-2xl font-bold tracking-tight">做市工作台 · {strategyId}</h2>
                <p className="mt-1 text-sm text-zinc-500">
                  REST 权威快照 · SSE 可靠事实 · WebSocket 高频数据仅用于显示
                </p>
              </div>
              <button
                type="button"
                onClick={() => void snapshot.resync()}
                className="inline-flex min-h-10 items-center gap-2 rounded-lg border border-zinc-700 px-3 text-sm hover:bg-zinc-800"
              >
                <RefreshCw className="h-4 w-4" aria-hidden="true" /> 全量同步
              </button>
            </div>
            <ConnectionStrip
              reliableConnected={snapshot.reliableConnected}
              realtimeState={realtime.connectionState}
              observedAt={state?.observed_at ?? null}
              stale={Boolean(state?.stale)}
            />
          </header>

          {state?.kill_switch_active ? (
            <div className="flex items-center gap-3 rounded-xl border border-critical bg-critical/15 p-4 text-critical" role="alert">
              <ShieldAlert className="h-6 w-6 shrink-0" aria-hidden="true" />
              <div>
                <p className="font-bold">Kill Switch 已触发：做市不得增加风险</p>
                <p className="text-sm">当前只允许权威撤单和恢复路径；请在风控页处理。</p>
              </div>
            </div>
          ) : null}

          {snapshot.error ? (
            <div className="rounded-xl border border-loss bg-loss/10 p-4 text-sm text-loss" role="alert">
              权威快照获取失败：{snapshot.error instanceof Error ? snapshot.error.message : "未知错误"}
            </div>
          ) : null}

          <nav className="flex gap-2 overflow-x-auto border-b border-zinc-800 pb-3 text-sm" aria-label="做市工作台分区">
            {[
              ["overview", "Overview"],
              ["quotes", "Quotes"],
              ["inventory", "Inventory"],
              ["accounting", "Accounting PnL"],
              ["quality", "Execution Quality"],
              ["budget", "Action Budget"],
              ["config", "Config"],
              ["events", "Events"],
            ].map(([href, label]) => (
              <a key={href} href={`#${href}`} className="whitespace-nowrap rounded-md px-2 py-1 text-zinc-400 hover:bg-zinc-800 hover:text-white">
                {label}
              </a>
            ))}
          </nav>

          <OverviewPanel
            state={state}
            externalReference={realtime.overlay?.external_reference ?? snapshot.quotes?.external_reference}
            isLoading={snapshot.isLoading}
          />
          <LifecycleControls
            state={state}
            pendingAction={pendingAction}
            confirmation={confirmation}
            commandError={commandError}
            onConfirmationChange={setConfirmation}
            onAction={(action) => void executeLifecycleAction(action)}
          />
          <QuotesPanel snapshot={snapshot.quotes} overlay={realtime.overlay} />
          <InventoryPanel snapshot={snapshot.inventory} overlay={realtime.overlay} />
          <AccountingPanel snapshot={snapshot.performance} />
          <ExecutionQualityPanel snapshot={snapshot.performance} />
          <ActionBudgetPanel snapshot={snapshot.budget} />
          <ConfigurationPanel
            strategyId={strategyId}
            versions={snapshot.configs}
            effectiveVersion={state?.config_version ?? null}
            runtimeRevision={state?.runtime_revision ?? 0}
            environment={state?.environment ?? "dev"}
            onChanged={snapshot.resync}
          />
          <EventsPanel events={snapshot.events} />
        </main>
        <StatusBar />
      </div>
    </div>
  )
}

function Panel({ id, title, icon, children }: { id: string; title: string; icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <section id={id} className="scroll-mt-4 rounded-xl border border-zinc-800 bg-zinc-900 p-4 md:p-5">
      <h3 className="mb-4 flex items-center gap-2 font-semibold">
        {icon} {title}
      </h3>
      {children}
    </section>
  )
}

function SnapshotTime({ observedAt, stale }: { observedAt: string | null; stale: boolean }) {
  return (
    <span className={cn("text-xs", stale ? "font-semibold text-warning" : "text-zinc-500")}>
      {stale ? "STALE · " : ""}更新 {formatDateTime(observedAt)}
    </span>
  )
}

function ConnectionStrip({
  reliableConnected,
  realtimeState,
  observedAt,
  stale,
}: {
  reliableConnected: boolean
  realtimeState: string
  observedAt: string | null
  stale: boolean
}) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-xs">
      <span className={cn("rounded-full px-2 py-1", reliableConnected ? "bg-profit/15 text-profit" : "bg-loss/15 text-loss")}>
        SSE {reliableConnected ? "可靠流已连接" : "已断开"}
      </span>
      <span className={cn("rounded-full px-2 py-1", realtimeState === "connected" ? "bg-profit/15 text-profit" : "bg-warning/15 text-warning")}>
        WS {realtimeState === "disabled" ? "未配置（不影响控制）" : realtimeState}
      </span>
      <SnapshotTime observedAt={observedAt} stale={stale || !reliableConnected} />
    </div>
  )
}

function OverviewPanel({
  state,
  externalReference,
  isLoading,
}: {
  state?: MarketMakingStateSnapshot
  externalReference?: ExternalReferenceSnapshot | null
  isLoading: boolean
}) {
  return (
    <Panel id="overview" title="Overview" icon={<Activity className="h-4 w-4" aria-hidden="true" />}>
      {isLoading && !state ? <div className="h-24 animate-pulse rounded-lg bg-zinc-800/60" /> : null}
      {state ? (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
            <Metric label="实际状态" value={state.actual_state.toUpperCase()} accent={state.actual_state === "faulted" ? "loss" : "default"} />
            <Metric label="会话模式" value={state.session_mode ?? "—"} />
            <Metric label="Quote uptime" value={state.quote_uptime_pct ? formatPct(state.quote_uptime_pct) : "—"} />
            <Metric label="Runtime revision" value={String(state.runtime_revision)} />
            <Metric label="配置版本" value={`v${state.config_version}`} />
          </div>
          <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
            {Object.entries(state.freshness).map(([name, freshness]) => (
              <div key={name} className="flex items-center justify-between rounded-lg bg-zinc-950/70 px-3 py-2 text-sm">
                <span className="text-zinc-400">{name}</span>
                <span className={cn(freshness.status === "fresh" ? "text-profit" : "text-warning")}>
                  {freshness.status} {freshness.age_ms === null ? "" : `${freshness.age_ms}ms`}
                </span>
              </div>
            ))}
          </div>
          <ExternalReferenceSummary reference={externalReference} />
          {state.alerts.length > 0 ? (
            <div className="space-y-2">
              {state.alerts.map((alert) => (
                <div key={alert.id} className={cn("flex gap-2 rounded-lg p-3 text-sm", alert.severity === "critical" ? "bg-loss/10 text-loss" : "bg-warning/10 text-warning")}>
                  <AlertTriangle className="h-4 w-4 shrink-0" aria-hidden="true" /> {alert.message}
                </div>
              ))}
            </div>
          ) : null}
          <SnapshotTime observedAt={state.observed_at} stale={state.stale} />
        </div>
      ) : null}
    </Panel>
  )
}

function Metric({ label, value, accent = "default" }: { label: string; value: string; accent?: "default" | "loss" }) {
  return (
    <div className="rounded-lg bg-zinc-950/70 p-3">
      <p className="text-xs text-zinc-500">{label}</p>
      <p className={cn("mt-1 font-mono text-lg font-semibold", accent === "loss" && "text-loss")}>{value}</p>
    </div>
  )
}

function LifecycleControls({
  state,
  pendingAction,
  confirmation,
  commandError,
  onConfirmationChange,
  onAction,
}: {
  state?: MarketMakingStateSnapshot
  pendingAction: (typeof LIFECYCLE_ACTIONS)[number] | null
  confirmation: string
  commandError: string | null
  onConfirmationChange: (value: string) => void
  onAction: (action: (typeof LIFECYCLE_ACTIONS)[number]) => void
}) {
  return (
    <section className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
      <div className="flex flex-wrap gap-2">
        {LIFECYCLE_ACTIONS.map((action) => (
          <button
            key={action}
            type="button"
            disabled={!state || state.kill_switch_active && action !== "stop"}
            onClick={() => onAction(action)}
            className={cn(
              "rounded-lg border border-zinc-700 px-3 py-2 text-sm uppercase hover:bg-zinc-800 disabled:cursor-not-allowed disabled:opacity-40",
              action === "stop" || action === "drain" ? "text-warning" : "text-zinc-200",
            )}
          >
            {action}
          </button>
        ))}
      </div>
      {pendingAction && state?.environment === "mainnet" ? (
        <div className="mt-3 rounded-lg border border-warning bg-warning/10 p-3">
          <label className="block text-sm text-warning" htmlFor="lifecycle-confirmation">
            MAINNET 二阶段确认：输入 CONFIRM MAINNET {pendingAction.toUpperCase()}
          </label>
          <div className="mt-2 flex flex-wrap gap-2">
            <input
              id="lifecycle-confirmation"
              value={confirmation}
              onChange={(event) => onConfirmationChange(event.target.value)}
              className="min-w-72 rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 font-mono text-sm"
            />
            <button type="button" onClick={() => onAction(pendingAction)} className="rounded-md bg-warning px-3 py-2 text-sm font-bold text-zinc-950">
              确认执行
            </button>
          </div>
        </div>
      ) : null}
      {commandError ? <p className="mt-2 text-sm text-loss">{commandError}</p> : null}
    </section>
  )
}

function QuotesPanel({ snapshot, overlay }: { snapshot?: MarketMakingQuotesSnapshot; overlay: MarketMakingDisplayOverlay | null }) {
  const slots = overlay?.slots ?? snapshot?.slots ?? []
  const fair = overlay?.fair_price ?? snapshot?.fair_price
  const reservation = overlay?.reservation_price ?? snapshot?.reservation_price
  const externalReference = overlay?.external_reference ?? snapshot?.external_reference
  return (
    <Panel id="quotes" title="Live Quotes" icon={<RadioTower className="h-4 w-4" aria-hidden="true" />}>
      {snapshot ? (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <Metric label="Best bid" value={formatNullablePrice(overlay?.best_bid ?? snapshot.best_bid)} />
            <Metric label="Fair" value={fair ? formatPrice(fair) : "—"} />
            <Metric label="Reservation" value={reservation ? formatPrice(reservation) : "—"} />
            <Metric label="Best ask" value={formatNullablePrice(overlay?.best_ask ?? snapshot.best_ask)} />
          </div>
          <ExternalReferenceDetails reference={externalReference} />
          <div className="overflow-x-auto">
            <table className="w-full min-w-[780px] text-left text-sm">
              <thead className="text-xs uppercase text-zinc-500">
                <tr><th className="p-2">Side</th><th>State</th><th>Desired</th><th>Live / Remaining</th><th>Edge</th><th>Revision</th><th>Age</th></tr>
              </thead>
              <tbody>
                {slots.map((slot) => <QuoteSlotRow key={`${slot.side}-${slot.level}`} slot={slot} />)}
              </tbody>
            </table>
          </div>
          <SnapshotTime observedAt={overlay?.observed_at ?? snapshot.observed_at} stale={snapshot.stale} />
        </div>
      ) : <EmptyState label="等待权威 quote snapshot" />}
    </Panel>
  )
}

function ExternalReferenceSummary({ reference }: { reference?: ExternalReferenceSnapshot | null }) {
  if (!reference) {
    return (
      <div className="rounded-lg border border-zinc-800 bg-zinc-950/70 px-3 py-2 text-sm text-zinc-500">
        External reference 未启用 · Hyperliquid 本地盘口为公平价主锚
      </div>
    )
  }
  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-zinc-800 bg-zinc-950/70 px-3 py-2 text-sm">
      <span className="font-medium">External · {reference.source ?? "unknown"}</span>
      <ExternalQualityBadge quality={reference.quality ?? "disabled"} />
      <span className="text-zinc-400">age {formatAge(reference.age_ms)}</span>
      <span className="text-zinc-400">weight {formatNullablePct(reference.effective_weight)}</span>
      <span className="text-xs text-zinc-500">Reference only · HL local anchor</span>
    </div>
  )
}

function ExternalReferenceDetails({ reference }: { reference?: ExternalReferenceSnapshot | null }) {
  if (!reference) return null
  return (
    <div className="space-y-3 rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="font-medium">
            External reference · {reference.source ?? "unknown"} / {reference.symbol ?? "—"}
          </span>
          <ExternalQualityBadge quality={reference.quality ?? "disabled"} />
        </div>
        <span className="text-xs text-zinc-500">Reference only · Hyperliquid local book remains the anchor</span>
      </div>
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4 xl:grid-cols-8">
        <Metric label="Raw price" value={formatNullablePrice(reference.raw_price ?? null)} />
        <Metric label="Basis-adjusted" value={formatNullablePrice(reference.adjusted_price ?? null)} />
        <Metric label="Basis" value={formatNullableBps(reference.basis_bps)} />
        <Metric label="HL divergence" value={formatNullableBps(reference.divergence_bps)} />
        <Metric label="Configured weight" value={formatNullablePct(reference.configured_weight)} />
        <Metric label="Effective weight" value={formatNullablePct(reference.effective_weight)} />
        <Metric label="Confidence" value={formatNullablePct(reference.confidence)} />
        <Metric label="Source age" value={formatAge(reference.age_ms)} />
      </div>
      <SnapshotTime observedAt={reference.observed_at ?? null} stale={reference.quality === "stale"} />
    </div>
  )
}

function ExternalQualityBadge({ quality }: { quality: ExternalReferenceQuality }) {
  return (
    <span
      className={cn(
        "inline-flex rounded-full px-2 py-1 text-xs font-bold uppercase",
        quality === "healthy"
          ? "bg-profit/15 text-profit"
          : quality === "disabled"
            ? "bg-zinc-800 text-zinc-400"
            : "bg-warning/15 text-warning",
      )}
    >
      {quality}
    </span>
  )
}

function formatAge(ageMs: number | null | undefined): string {
  return ageMs === null || ageMs === undefined ? "—" : `${ageMs}ms`
}

function formatNullableBps(value: DecimalString | null | undefined): string {
  return value === null || value === undefined ? "—" : `${formatPrice(value, 2)} bps`
}

function formatNullablePct(value: DecimalString | null | undefined): string {
  return value === null || value === undefined ? "—" : formatPct(value)
}

function QuoteSlotRow({ slot }: { slot: QuoteSlotSnapshot }) {
  const unknown = slot.state === "unknown" || slot.state === "orphaned_live" || slot.state === "recovery_required"
  return (
    <tr className="border-t border-zinc-800 font-mono">
      <td className={cn("p-2 font-semibold", slot.side === "buy" ? "text-profit" : "text-loss")}>{slot.side.toUpperCase()} L{slot.level}</td>
      <td className={cn(unknown && "text-warning")}>{slot.state.toUpperCase()}</td>
      <td>{formatQuote(slot.desired_price, slot.desired_size)}</td>
      <td>{formatQuote(slot.live_price, slot.live_remaining_size)}</td>
      <td>{slot.gross_edge_bps ? `${formatPrice(slot.gross_edge_bps, 2)} bps` : slot.no_quote_reason ?? "—"}</td>
      <td>{slot.quote_revision}</td>
      <td>{slot.quote_age_ms === null ? "—" : `${slot.quote_age_ms}ms`}</td>
    </tr>
  )
}

function InventoryPanel({ snapshot, overlay }: { snapshot?: MarketMakingInventorySnapshot; overlay: MarketMakingDisplayOverlay | null }) {
  if (!snapshot) return <Panel id="inventory" title="Inventory & Skew" icon={<Gauge className="h-4 w-4" />}><EmptyState label="等待库存快照" /></Panel>
  const utilization = overlay?.inventory_utilization ?? snapshot.inventory_utilization
  const notional = overlay?.inventory_notional ?? snapshot.inventory_notional
  return (
    <Panel id="inventory" title="Inventory & Skew" icon={<Gauge className="h-4 w-4" aria-hidden="true" />}>
      <div className="space-y-4">
        <div>
          <div className="mb-2 flex justify-between text-xs text-zinc-500"><span>库存带使用率</span><span>{formatPct(utilization)}</span></div>
          <div className="h-3 overflow-hidden rounded-full bg-zinc-800">
            <div className={cn("h-full", snapshot.reduction_mode === "emergency" ? "bg-loss" : snapshot.reduction_mode === "hard" ? "bg-warning" : "bg-profit")} style={{ width: `${clampedPercent(utilization)}%` }} />
          </div>
          <div className="mt-1 flex justify-between text-xs text-zinc-600"><span>Soft {formatUsd(snapshot.soft_limit_notional)}</span><span>Hard {formatUsd(snapshot.hard_limit_notional)}</span><span>Emergency {formatUsd(snapshot.emergency_limit_notional)}</span></div>
        </div>
        <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
          <Metric label="Position" value={formatSize(overlay?.position_size ?? snapshot.position_size)} />
          <Metric label="Inventory notional" value={formatUsd(notional)} />
          <Metric label="Skew" value={(overlay?.inventory_shift_bps ?? snapshot.inventory_shift_bps) ? `${formatPrice(overlay?.inventory_shift_bps ?? snapshot.inventory_shift_bps ?? "0", 2)} bps` : "—"} />
          <Metric label="Reduction mode" value={snapshot.reduction_mode} accent={snapshot.reduction_mode === "emergency" ? "loss" : "default"} />
          <Metric label="Available margin" value={snapshot.available_margin ? formatUsd(snapshot.available_margin) : "—"} />
          <Metric label="Margin used" value={snapshot.margin_used ? formatUsd(snapshot.margin_used) : "—"} />
          <Metric label="Liquidation distance" value={snapshot.liquidation_distance_pct ? formatPct(snapshot.liquidation_distance_pct) : "—"} />
          <Metric label="Funding carry" value={formatUsd(snapshot.funding_carry)} />
        </div>
        <SnapshotTime observedAt={overlay?.observed_at ?? snapshot.observed_at} stale={snapshot.stale} />
      </div>
    </Panel>
  )
}

function AccountingPanel({ snapshot }: { snapshot?: MarketMakingPerformanceSnapshot }) {
  const accounting = snapshot?.accounting
  return (
    <Panel id="accounting" title="Accounting PnL" icon={<CircleDollarSign className="h-4 w-4" aria-hidden="true" />}>
      {accounting ? (
        <div className="space-y-4">
          <p className="rounded-lg bg-zinc-950/70 p-3 text-xs text-zinc-400">
            权威口径来自 Postgres ledger。Markout 不计入 Accounting Net PnL，避免重复计算。
          </p>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-3">
            {[
              ["Realized trading", accounting.realized_trading_pnl],
              ["Unrealized inventory", accounting.unrealized_inventory_change],
              ["Fees / rebates", accounting.net_fees_and_rebates],
              ["Funding", accounting.funding_pnl],
              ["Paid actions", new Decimal(accounting.paid_action_cost).neg().toFixed() as DecimalString],
              ["Accounting net", accounting.accounting_net_pnl],
            ].map(([label, value]) => (
              <div key={label} className="rounded-lg bg-zinc-950/70 p-3">
                <p className="text-xs text-zinc-500">{label}</p>
                <p className={cn("mt-1 font-mono text-lg font-semibold", pnlColor(value))}>{formatUsd(value)}</p>
              </div>
            ))}
          </div>
          <div className={cn("text-sm", accounting.ledger_reconciled ? "text-profit" : "text-warning")}>
            {accounting.ledger_reconciled ? "Ledger 已对账" : "Ledger 尚未完成对账"}
          </div>
          <PnlChart points={snapshot.inventory_episodes} />
          <SnapshotTime observedAt={snapshot.as_of} stale={snapshot.stale} />
        </div>
      ) : <EmptyState label="等待会计 PnL" />}
    </Panel>
  )
}

function ExecutionQualityPanel({ snapshot }: { snapshot?: MarketMakingPerformanceSnapshot }) {
  const quality = snapshot?.execution_quality
  return (
    <Panel id="quality" title="Execution Quality / Markout" icon={<DatabaseZap className="h-4 w-4" aria-hidden="true" />}>
      {quality ? (
        <div className="space-y-4">
          <p className="text-xs text-zinc-500">ClickHouse 分析口径，仅用于执行质量诊断，不是会计账本。</p>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <Metric label="Quoted spread" value={`${formatPrice(quality.quoted_spread_bps, 2)} bps`} />
            <Metric label="Captured spread" value={`${formatPrice(quality.captured_spread_bps, 2)} bps`} />
            <Metric label="Maker ratio" value={formatPct(quality.maker_ratio)} />
            <Metric label="Actions / fill" value={formatPrice(quality.actions_per_fill, 2)} />
            <Metric label="Markout 1s" value={formatUsd(quality.markout_1s)} />
            <Metric label="Markout 5s" value={formatUsd(quality.markout_5s)} />
            <Metric label="Markout 30s" value={formatUsd(quality.markout_30s)} />
            <Metric label="Fill / Reject / Unknown" value={`${quality.fill_count} / ${quality.reject_count} / ${quality.unknown_count}`} />
          </div>
        </div>
      ) : <EmptyState label="分析库暂无执行质量数据" />}
    </Panel>
  )
}

function ActionBudgetPanel({ snapshot }: { snapshot?: MarketMakingActionBudgetSnapshot }) {
  return (
    <Panel id="budget" title="Action Budget" icon={<Gauge className="h-4 w-4" aria-hidden="true" />}>
      {snapshot ? (
        <div className="space-y-4">
          <BudgetModeBadge mode={snapshot.mode} />
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <Metric label="Remote remaining" value={formatPrice(snapshot.remote_remaining, 0)} />
            <Metric label="Shadow remaining" value={formatPrice(snapshot.shadow_remaining, 0)} />
            <Metric label="Emergency reserve" value={formatPrice(snapshot.emergency_reserve, 0)} />
            <Metric label="Cancel headroom" value={formatPrice(snapshot.cancel_headroom, 0)} />
            <Metric label="IP weight remaining" value={formatPrice(snapshot.ip_weight_remaining, 0)} />
            <Metric label="24h burn / earn" value={`${formatPrice(snapshot.burn_rate_24h, 1)} / ${formatPrice(snapshot.earned_rate_24h, 1)}`} />
            <Metric label="USDC / action" value={snapshot.usdc_per_action ? formatUsd(snapshot.usdc_per_action) : "—"} />
            <Metric label="Runway" value={snapshot.runway_hours ? `${formatPrice(snapshot.runway_hours, 1)}h` : "∞"} />
          </div>
          <SnapshotTime observedAt={snapshot.observed_at} stale={snapshot.stale} />
        </div>
      ) : <EmptyState label="等待动作额度快照" />}
    </Panel>
  )
}

function BudgetModeBadge({ mode }: { mode: BudgetMode }) {
  return <span className={cn("inline-flex rounded-full px-2 py-1 text-xs font-bold", mode === "normal" ? "bg-profit/15 text-profit" : mode === "conserve" ? "bg-warning/15 text-warning" : "bg-loss/15 text-loss")}>{mode.toUpperCase()}</span>
}

function ConfigurationPanel({
  strategyId,
  versions,
  effectiveVersion,
  runtimeRevision,
  environment,
  onChanged,
}: {
  strategyId: string
  versions: MarketMakerConfigVersion[]
  effectiveVersion: number | null
  runtimeRevision: number
  environment: "dev" | "testnet" | "mainnet"
  onChanged: () => Promise<unknown>
}) {
  const current = versions.find((version) => version.version === effectiveVersion) ?? versions[0]
  return (
    <Panel id="config" title="Configuration" icon={<Settings2 className="h-4 w-4" aria-hidden="true" />}>
      {current ? (
        <ConfigEditor
          key={current.id}
          strategyId={strategyId}
          current={current}
          versions={versions}
          runtimeRevision={runtimeRevision}
          environment={environment}
          onChanged={onChanged}
        />
      ) : <EmptyState label="暂无配置版本" />}
    </Panel>
  )
}

function ConfigEditor({
  strategyId,
  current,
  versions,
  runtimeRevision,
  environment,
  onChanged,
}: {
  strategyId: string
  current: MarketMakerConfigVersion
  versions: MarketMakerConfigVersion[]
  runtimeRevision: number
  environment: "dev" | "testnet" | "mainnet"
  onChanged: () => Promise<unknown>
}) {
  const [draft, setDraft] = useState<MarketMakerConfig>(current.config)
  const [selectedVersion, setSelectedVersion] = useState(current.version)
  const [confirmation, setConfirmation] = useState("")
  const [message, setMessage] = useState<string | null>(null)
  const selected = versions.find((version) => version.version === selectedVersion) ?? current

  function setDecimal(key: keyof MarketMakerConfig, value: string) {
    setDraft((previous) => ({ ...previous, [key]: value as DecimalString }))
  }

  function setInteger(key: keyof MarketMakerConfig, value: string) {
    setDraft((previous) => ({ ...previous, [key]: Number.parseInt(value, 10) || 0 }))
  }

  async function submit(operation: "create" | "activate" | "rollback") {
    const required = environment === "mainnet" ? "CONFIRM MAINNET CONFIG" : "CONFIRM"
    if (operation !== "create" && confirmation !== required) {
      setMessage(`请输入 ${required}`)
      return
    }
    try {
      if (operation === "create") await createMarketMakerConfig(strategyId, draft, runtimeRevision)
      if (operation === "activate") await activateMarketMakerConfig(strategyId, selected.version, runtimeRevision, confirmation)
      if (operation === "rollback") await rollbackMarketMakerConfig(strategyId, selected.version, runtimeRevision, confirmation)
      setMessage("操作已提交，等待运行时安全点确认")
      await onChanged()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "配置操作失败")
    }
  }

  const decimalFields: Array<[keyof MarketMakerConfig, string]> = [
    ["soft_inventory_notional", "Soft inventory (USDC)"],
    ["hard_inventory_notional", "Hard inventory (USDC)"],
    ["emergency_inventory_notional", "Emergency inventory (USDC)"],
    ["quote_size", "Quote size"],
    ["max_depth_participation", "Max depth participation"],
    ["inventory_skew_bps", "Inventory skew (bps)"],
    ["max_inventory_shift_bps", "Max inventory shift (bps)"],
    ["min_half_spread_bps", "Minimum half spread (bps)"],
    ["toxicity_spread_bps", "Toxicity spread (bps)"],
    ["min_expected_pnl_usdc", "Min expected PnL (USDC)"],
    ["external_reference_weight", "External reference weight"],
    ["external_max_age_seconds", "External max age (seconds)"],
    ["external_outlier_bps", "External outlier threshold (bps)"],
    ["max_external_shift_ticks", "Max external shift (ticks)"],
    ["max_total_fair_shift_ticks", "Max total fair shift (ticks)"],
    ["latency_risk_multiplier", "Latency risk multiplier"],
    ["conservative_latency_seconds", "Conservative latency (seconds)"],
    ["conservative_markout_bps", "Conservative markout (bps)"],
  ]
  const integerFields: Array<[keyof MarketMakerConfig, string]> = [
    ["min_quote_lifetime_ms", "Min quote lifetime (ms)"],
    ["refresh_cooldown_ms", "Refresh cooldown (ms)"],
    ["max_quote_age_ms", "Max quote age (ms)"],
    ["market_stale_after_ms", "Max market age (ms)"],
    ["account_stale_after_ms", "Max account age (ms)"],
    ["min_markout_samples", "Minimum mature markout samples"],
  ]

  return (
    <div className="space-y-5">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {decimalFields.map(([key, label]) => (
          <label key={key} className="text-xs text-zinc-400">{label}
            <input value={String(draft[key])} onChange={(event) => setDecimal(key, event.target.value)} inputMode="decimal" className="mt-1 w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 font-mono text-sm text-white" />
          </label>
        ))}
        {integerFields.map(([key, label]) => (
          <label key={key} className="text-xs text-zinc-400">{label}
            <input value={String(draft[key])} onChange={(event) => setInteger(key, event.target.value)} inputMode="numeric" className="mt-1 w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 font-mono text-sm text-white" />
          </label>
        ))}
      </div>
      <button type="button" onClick={() => void submit("create")} className="rounded-md border border-zinc-700 px-3 py-2 text-sm hover:bg-zinc-800">保存为不可变新版本</button>

      <div className="grid gap-4 border-t border-zinc-800 pt-4 lg:grid-cols-2">
        <div>
          <label className="text-xs text-zinc-400" htmlFor="config-version">比较 / 操作版本</label>
          <select id="config-version" value={selectedVersion} onChange={(event) => setSelectedVersion(Number(event.target.value))} className="mt-1 w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm">
            {versions.map((version) => <option key={version.id} value={version.version}>v{version.version} · {version.config_hash.slice(0, 10)} · {version.approved_by ? "已审批" : "未审批"}</option>)}
          </select>
          <div className="mt-3 max-h-64 overflow-auto rounded-lg bg-zinc-950/70 p-3 text-xs">
            {Object.keys(current.config).map((key) => {
              const typedKey = key as keyof MarketMakerConfig
              const before = String(current.config[typedKey])
              const after = String(selected.config[typedKey])
              return <div key={key} className={cn("grid grid-cols-3 gap-2 py-1", before !== after && "text-warning")}><span>{key}</span><span>{before}</span><span>{after}</span></div>
            })}
          </div>
        </div>
        <div className="space-y-3">
          <h4 className="text-sm font-medium">Shadow preview</h4>
          {selected.shadow_preview ? (
            <div className="grid grid-cols-2 gap-2 text-sm">
              <Metric label="Quote uptime" value={formatPct(selected.shadow_preview.expected_quote_uptime_pct)} />
              <Metric label="Actions / hour" value={formatPrice(selected.shadow_preview.expected_actions_per_hour, 1)} />
              <Metric label="Pessimistic edge" value={formatUsd(selected.shadow_preview.pessimistic_edge_usdc)} />
              <Metric label="Approval" value={selected.approved_by ?? "未审批"} />
              {selected.shadow_preview.warnings.map((warning) => <p key={warning} className="col-span-2 text-warning">⚠ {warning}</p>)}
            </div>
          ) : <p className="text-sm text-warning">尚无 shadow preview，不应激活增加风险的配置。</p>}
          <label className="block text-xs text-zinc-400">确认文字
            <input value={confirmation} onChange={(event) => setConfirmation(event.target.value)} placeholder={environment === "mainnet" ? "CONFIRM MAINNET CONFIG" : "CONFIRM"} className="mt-1 w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 font-mono text-sm text-white" />
          </label>
          <div className="flex gap-2">
            <button type="button" disabled={!selected.approved_by || !selected.shadow_preview} onClick={() => void submit("activate")} className="rounded-md bg-profit/15 px-3 py-2 text-sm text-profit disabled:opacity-40">激活</button>
            <button type="button" onClick={() => void submit("rollback")} className="rounded-md bg-warning/15 px-3 py-2 text-sm text-warning">回滚到此版本</button>
          </div>
          {message ? <p className="text-sm text-zinc-300">{message}</p> : null}
        </div>
      </div>
    </div>
  )
}

function EventsPanel({ events }: { events: MarketMakingEvent[] }) {
  return (
    <Panel id="events" title="Events" icon={<AlertTriangle className="h-4 w-4" aria-hidden="true" />}>
      <div className="max-h-[36rem] space-y-1 overflow-auto">
        {events.length === 0 ? <EmptyState label="暂无可靠事件" /> : events.map((event) => (
          <article key={event.id} className="[contain-intrinsic-size:0_64px] [content-visibility:auto] grid gap-1 border-b border-zinc-800 px-2 py-3 text-sm md:grid-cols-[10rem_8rem_1fr_auto]">
            <time className="font-mono text-xs text-zinc-500">{formatDateTime(event.created_at)}</time>
            <span className={cn("text-xs uppercase", event.severity === "critical" ? "text-loss" : event.severity === "warning" ? "text-warning" : "text-zinc-400")}>{event.category}</span>
            <span>{event.message}</span>
            <span className="text-xs text-zinc-600">{event.actor ?? "system"}</span>
          </article>
        ))}
      </div>
    </Panel>
  )
}

function EmptyState({ label }: { label: string }) {
  return <div className="grid min-h-24 place-items-center rounded-lg bg-zinc-950/50 text-sm text-zinc-500">{label}</div>
}

function formatNullablePrice(value: DecimalString | null | undefined) {
  return value === null || value === undefined ? "—" : formatPrice(value)
}

function formatQuote(price: DecimalString | null, size: DecimalString | null) {
  if (price === null || size === null) return "—"
  return `${formatPrice(price)} × ${formatSize(size)}`
}

function clampedPercent(value: DecimalString): number {
  // CSS width is intentionally the only lossy conversion; financial values stay as decimal strings.
  return new Decimal(value).mul(100).clamp(0, 100).toNumber()
}
