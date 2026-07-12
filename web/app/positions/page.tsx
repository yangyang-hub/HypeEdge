"use client"

import { useState } from "react"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { AlertConfirmDialog } from "@/components/ui/alert-confirm-dialog"
import { Button } from "@/components/ui/button"
import { ConfirmPhraseDialog } from "@/components/ui/confirm-phrase-dialog"
import { EmptyState, Panel, PnLText, SideTag, StaleBanner } from "@/components/ui/data-display"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { closePosition, usePositions } from "@/hooks/use-positions"
import { useInstrumentMeta } from "@/hooks/use-system-status"
import type { Position } from "@/lib/types"
import { addDecimals, formatPrice, formatSize } from "@/lib/utils"

export default function PositionsPage() {
  const { positions, error, isLoading, refresh } = usePositions()
  const [pendingSymbol, setPendingSymbol] = useState<string | null>(null)
  const [closeTarget, setCloseTarget] = useState<string | null>(null)
  const [closeAllOpen, setCloseAllOpen] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const totalPnl = addDecimals(positions.map((position) => position.unrealized_pnl))

  async function handleClose(symbol: string) {
    setPendingSymbol(symbol)
    setActionError(null)
    try {
      await closePosition(symbol)
      await refresh()
      setCloseTarget(null)
    } catch (caught) {
      setActionError(caught instanceof Error ? caught.message : "平仓请求失败")
    } finally {
      setPendingSymbol(null)
    }
  }

  async function handleCloseAll() {
    setActionError(null)
    try {
      for (const position of positions) {
        setPendingSymbol(position.symbol)
        await closePosition(position.symbol)
      }
      await refresh()
      setCloseAllOpen(false)
    } catch (caught) {
      setActionError(caught instanceof Error ? caught.message : "全部平仓失败")
    } finally {
      setPendingSymbol(null)
    }
  }

  return (
    <AppShell>
      <main id="main-content" className="flex-1 space-y-4 overflow-y-auto p-3 md:p-5">
        <PageHeader
          title={`活跃持仓 (${positions.length})`}
          subtitle={
            <span className="inline-flex items-center gap-2">
              合计 uPnL <PnLText value={totalPnl} />
            </span>
          }
          actions={
            <Button
              type="button"
              variant="danger"
              size="sm"
              disabled={positions.length === 0}
              onClick={() => setCloseAllOpen(true)}
            >
              全部平仓
            </Button>
          }
        />

        {actionError ? (
          <p role="alert" className="rounded-md border border-loss/30 bg-loss/10 px-3 py-2 text-sm text-loss">
            {actionError}
          </p>
        ) : null}
        {error ? <StaleBanner message="数据刷新失败，正在显示最后一次成功结果" /> : null}

        <Panel>
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead>币种</TableHead>
                <TableHead>方向</TableHead>
                <TableHead className="text-right">数量</TableHead>
                <TableHead className="text-right">入场价</TableHead>
                <TableHead className="text-right">现价</TableHead>
                <TableHead className="text-right">杠杆</TableHead>
                <TableHead className="text-right">未实现 PnL</TableHead>
                <TableHead className="text-right">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading && positions.length === 0 ? (
                <TableRow className="hover:bg-transparent">
                  <TableCell colSpan={8} className="p-0">
                    <EmptyState message="正在加载持仓…" />
                  </TableCell>
                </TableRow>
              ) : positions.length === 0 ? (
                <TableRow className="hover:bg-transparent">
                  <TableCell colSpan={8} className="p-0">
                    <EmptyState message="暂无持仓。策略启动后将显示在这里。" />
                  </TableCell>
                </TableRow>
              ) : (
                positions.map((position) => (
                  <PositionRow
                    key={position.symbol}
                    position={position}
                    pending={pendingSymbol === position.symbol}
                    onClose={() => setCloseTarget(position.symbol)}
                  />
                ))
              )}
            </TableBody>
          </Table>
        </Panel>
      </main>

      <AlertConfirmDialog
        open={closeTarget !== null}
        onOpenChange={(open) => !open && setCloseTarget(null)}
        title={`平仓 ${closeTarget ?? ""}`}
        description={`确认以 reduce-only 市价单全部平掉 ${closeTarget ?? ""} 持仓？`}
        confirmLabel="确认平仓"
        danger
        loading={pendingSymbol === closeTarget && closeTarget !== null}
        onConfirm={async () => {
          if (closeTarget) await handleClose(closeTarget)
        }}
      />

      <ConfirmPhraseDialog
        open={closeAllOpen}
        onOpenChange={setCloseAllOpen}
        title="全部平仓"
        description="将以 reduce-only 市价单平掉所有活跃持仓。输入 CLOSE ALL 确认。"
        phrase="CLOSE ALL"
        confirmLabel="全部平仓"
        loading={pendingSymbol !== null && closeAllOpen}
        onConfirm={handleCloseAll}
      />
    </AppShell>
  )
}

function PositionRow({
  position: p,
  pending,
  onClose,
}: {
  position: Position
  pending: boolean
  onClose: () => void
}) {
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
      <TableCell className="text-right font-mono">{p.leverage}x</TableCell>
      <TableCell className="text-right">
        <PnLText value={p.unrealized_pnl} />
      </TableCell>
      <TableCell className="text-right">
        <Button type="button" variant="sell" size="sm" loading={pending} onClick={onClose}>
          平仓
        </Button>
      </TableCell>
    </TableRow>
  )
}
