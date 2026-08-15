"use client"

import { useState } from "react"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { AlertConfirmDialog } from "@/components/ui/alert-confirm-dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { EmptyState, Panel, SideTag, StaleBanner } from "@/components/ui/data-display"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { useOrders, cancelOrder } from "@/hooks/use-orders"
import { useInstrumentMeta } from "@/hooks/use-system-status"
import { ORDER_STATUS_LABELS } from "@/lib/constants"
import type { Order, OrderStatus } from "@/lib/types"
import { formatPrice, formatSize, formatTime } from "@/lib/utils"

function statusVariant(status: OrderStatus): "default" | "accent" | "profit" | "loss" | "warning" | "info" {
  switch (status) {
    case "filled":
      return "profit"
    case "rejected":
      return "loss"
    case "partial_fill":
    case "cancel_pending":
    case "cancel_unknown":
    case "submit_unknown":
      return "warning"
    case "acknowledged":
    case "submitted":
      return "info"
    default:
      return "default"
  }
}

export default function OrdersPage() {
  // M-FE5: 后端 GET /api/v1/orders 目前忽略 `status` 参数（恒返回活跃订单，
  // crates/api/src/routes/account.rs `orders`），历史 tab 会误展示活跃订单，
  // 因此先降级为仅活跃订单视图。契约实现保留在 use-orders.ts（status 参数照传），
  // 待后端子代理支持 status 过滤后再恢复双 tab。
  const { orders, refresh, error, isLoading } = useOrders("active")
  const [actionError, setActionError] = useState<string | null>(null)
  const [cancelTarget, setCancelTarget] = useState<string | null>(null)
  const [cancelling, setCancelling] = useState(false)

  async function handleCancel(cloid: string) {
    setCancelling(true)
    setActionError(null)
    try {
      await cancelOrder(cloid)
      refresh()
      setCancelTarget(null)
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "撤单失败")
    } finally {
      setCancelling(false)
    }
  }

  return (
    <AppShell>
      <main id="main-content" className="flex-1 space-y-4 overflow-y-auto p-3 md:p-5">
        <PageHeader title="订单" subtitle="活跃订单（历史终态订单待后端 status 过滤支持）" />

        <div className="mt-4 space-y-3">
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
                  <TableHead>时间</TableHead>
                  <TableHead>币种</TableHead>
                  <TableHead>方向</TableHead>
                  <TableHead className="text-right">数量</TableHead>
                  <TableHead className="text-right">价格</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead className="text-right">已成交</TableHead>
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading && orders.length === 0 ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={8} className="p-0">
                      <EmptyState message="正在加载订单…" />
                    </TableCell>
                  </TableRow>
                ) : orders.length === 0 ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={8} className="p-0">
                      <EmptyState message="无活跃订单" />
                    </TableCell>
                  </TableRow>
                ) : (
                  orders.map((order) => (
                    <OrderRow
                      key={order.cloid}
                      order={order}
                      showCancel
                      onCancel={() => setCancelTarget(order.cloid)}
                    />
                  ))
                )}
              </TableBody>
            </Table>
          </Panel>
        </div>
      </main>

      <AlertConfirmDialog
        open={cancelTarget !== null}
        onOpenChange={(open) => !open && setCancelTarget(null)}
        title="确认撤单"
        description="撤单请求将提交到执行引擎。若订单已成交，将返回失败原因。"
        confirmLabel="确认撤单"
        danger
        loading={cancelling}
        onConfirm={async () => {
          if (cancelTarget) await handleCancel(cancelTarget)
        }}
      />
    </AppShell>
  )
}

function OrderRow({
  order: o,
  showCancel,
  onCancel,
}: {
  order: Order
  showCancel: boolean
  onCancel: () => void
}) {
  const { meta } = useInstrumentMeta(o.symbol)
  const cancelInFlight = o.status === "cancel_pending" || o.status === "cancel_unknown"
  return (
    <TableRow>
      <TableCell className="font-mono text-xs text-text-secondary">{formatTime(o.created_at)}</TableCell>
      <TableCell className="font-medium">{o.symbol}</TableCell>
      <TableCell>
        <SideTag side={o.side} />
      </TableCell>
      <TableCell className="text-right font-mono">{formatSize(o.size, meta?.size_decimals ?? 6)}</TableCell>
      <TableCell className="text-right font-mono">
        {o.price === null ? "市价" : formatPrice(o.price, meta?.price_decimals ?? 6)}
      </TableCell>
      <TableCell>
        <Badge variant={statusVariant(o.status)}>{ORDER_STATUS_LABELS[o.status] ?? o.status}</Badge>
      </TableCell>
      <TableCell className="text-right font-mono">{formatSize(o.filled_size, meta?.size_decimals ?? 6)}</TableCell>
      <TableCell className="text-right">
        {showCancel ? (
          <Button type="button" variant="ghost" size="sm" disabled={cancelInFlight} onClick={onCancel}>
            {cancelInFlight ? "确认中…" : "撤单"}
          </Button>
        ) : null}
      </TableCell>
    </TableRow>
  )
}
