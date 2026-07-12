"use client"

import Link from "next/link"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { EmptyState, Metric, Panel, PnLText, ProgressBar, SideTag, StaleBanner } from "@/components/ui/data-display"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { useAccount } from "@/hooks/use-account"
import { usePositions } from "@/hooks/use-positions"
import { useRiskStatus } from "@/hooks/use-risk"
import { useInstrumentMeta } from "@/hooks/use-system-status"
import type { Position } from "@/lib/types"
import { decimalToNumber, formatPct, formatPrice, formatSize, formatUsd } from "@/lib/utils"

export default function DashboardPage() {
  const { account, error: accountError } = useAccount()
  const { positions, error: positionsError } = usePositions()
  const { risk } = useRiskStatus()

  return (
    <AppShell>
      <main id="main-content" className="flex-1 space-y-5 overflow-y-auto p-3 md:p-5">
        <PageHeader
          title="账户总览"
          subtitle={account?.last_update ? `最后更新 ${account.last_update}` : "等待账户数据"}
        />

        {accountError || positionsError ? (
          <StaleBanner message="部分数据刷新失败，正在显示最后一次成功结果" />
        ) : null}

        <Panel className="px-3 sm:px-0">
          <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5">
            <Metric
              label="总权益"
              value={account ? formatUsd(account.equity) : "—"}
              delta={account ? `${formatPct(account.drawdown_pct)} 回撤` : undefined}
            />
            <Metric label="可用余额" value={account ? formatUsd(account.available_balance) : "—"} />
            <Metric label="持仓保证金" value={account ? formatUsd(account.total_margin_used) : "—"} />
            <div className="border-b border-border-subtle py-3 sm:border-b-0 sm:border-r sm:px-4">
              <div className="text-2xs uppercase tracking-wider text-text-tertiary">未实现 PnL</div>
              <div className="mt-1 text-xl font-semibold">
                {account ? <PnLText value={account.total_unrealized_pnl} /> : "—"}
              </div>
            </div>
            <Metric
              label="杠杆"
              value={account ? `${formatPrice(account.leverage, 1)}x` : "—"}
              delta={account ? `${account.fill_count} 笔成交` : undefined}
            />
          </div>
        </Panel>

        <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_320px]">
          <Panel title={`活跃持仓 (${positions.length})`}>
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead>币种</TableHead>
                  <TableHead>方向</TableHead>
                  <TableHead className="text-right">数量</TableHead>
                  <TableHead className="text-right">入场价</TableHead>
                  <TableHead className="text-right">现价</TableHead>
                  <TableHead className="text-right">未实现 PnL</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {positions.length === 0 ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={6} className="p-0">
                      <EmptyState
                        message="暂无持仓。策略启动后将显示在这里。"
                        action={
                          <Link href="/strategy" className="text-xs text-accent hover:underline">
                            去策略
                          </Link>
                        }
                      />
                    </TableCell>
                  </TableRow>
                ) : (
                  positions.map((position) => <DashboardPositionRow key={position.symbol} position={position} />)
                )}
              </TableBody>
            </Table>
          </Panel>

          <div className="space-y-4">
            {risk ? (
              <Panel title="风控限额">
                <div className="space-y-3 p-3">
                  <div className="flex items-center justify-between text-xs">
                    <span className="text-text-secondary">Kill Switch</span>
                    <span className={risk.kill_switch_active ? "text-critical" : "text-profit"}>
                      {risk.kill_switch_active ? "已触发" : "正常"}
                    </span>
                  </div>
                  {risk.limits.map((limit) => {
                    const used = decimalToNumber(limit.pct_used)
                    return (
                      <div key={limit.name} className="space-y-1">
                        <div className="flex items-center justify-between text-xs">
                          <span className="text-text-secondary">{limit.name}</span>
                          <span className="font-mono text-text-tertiary">
                            {limit.unit === "%"
                              ? `${formatPct(limit.current)} / ${formatPct(limit.limit)}`
                              : `${formatPrice(limit.current, 1)} / ${formatPrice(limit.limit, 1)} ${limit.unit}`}
                          </span>
                        </div>
                        <ProgressBar value={used} />
                      </div>
                    )
                  })}
                  <Link href="/risk" className="inline-block text-xs text-accent hover:underline">
                    打开风控面板
                  </Link>
                </div>
              </Panel>
            ) : null}
          </div>
        </div>
      </main>
    </AppShell>
  )
}

function DashboardPositionRow({ position: p }: { position: Position }) {
  const { meta } = useInstrumentMeta(p.symbol)
  return (
    <TableRow>
      <TableCell className="font-medium">{p.symbol}</TableCell>
      <TableCell>
        <SideTag side={p.side} />
      </TableCell>
      <TableCell className="text-right font-mono">{formatSize(p.size, meta?.size_decimals ?? 6)}</TableCell>
      <TableCell className="text-right font-mono">
        {p.entry_price === null ? "—" : formatPrice(p.entry_price, meta?.price_decimals ?? 6)}
      </TableCell>
      <TableCell className="text-right font-mono">
        {p.mark_price === null ? "—" : formatPrice(p.mark_price, meta?.price_decimals ?? 6)}
      </TableCell>
      <TableCell className="text-right">
        <PnLText value={p.unrealized_pnl} />
      </TableCell>
    </TableRow>
  )
}
