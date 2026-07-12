"use client"

import Decimal from "decimal.js"
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts"
import type { InventoryEpisodePoint } from "@/lib/types"
import { formatDateTime, formatUsd } from "@/lib/utils"

export interface PnlChartProps {
  points: InventoryEpisodePoint[]
}

export function PnlChart({ points }: PnlChartProps) {
  // Recharts requires JS numbers. Conversion is isolated to pixels only; labels and accounting stay Decimal strings.
  const data = points.map((point) => ({
    timestamp: point.observed_at,
    pnl: new Decimal(point.accounting_net_pnl).toNumber(),
    inventory: new Decimal(point.inventory_notional).toNumber(),
  }))

  if (data.length === 0) {
    return <div className="grid h-56 place-items-center text-sm text-zinc-500">暂无库存周期数据</div>
  }

  return (
    <div className="h-64 w-full" aria-label="会计 PnL 与库存周期图">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 12, right: 12, bottom: 8, left: 4 }}>
          <CartesianGrid stroke="var(--color-zinc-800)" strokeDasharray="3 3" />
          <XAxis
            dataKey="timestamp"
            tickFormatter={(value: string) => formatDateTime(value).slice(11, 16)}
            stroke="var(--color-zinc-500)"
            tick={{ fontSize: 11 }}
          />
          <YAxis stroke="var(--color-zinc-500)" tick={{ fontSize: 11 }} width={64} />
          <Tooltip
            labelFormatter={(value) => formatDateTime(String(value))}
            formatter={(value, name) => [formatUsd(String(value)), name === "pnl" ? "Accounting PnL" : "库存名义价值"]}
            contentStyle={{ background: "var(--color-zinc-900)", borderColor: "var(--color-zinc-700)" }}
          />
          <Line dataKey="pnl" name="pnl" stroke="var(--color-profit)" dot={false} strokeWidth={2} />
          <Line dataKey="inventory" name="inventory" stroke="var(--color-warning)" dot={false} strokeWidth={1.5} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
