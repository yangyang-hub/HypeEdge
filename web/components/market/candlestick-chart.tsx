"use client"

import type { CandleData } from "@/lib/types"
import { decimalToNumber } from "@/lib/utils"

export interface CandlestickChartProps {
  candles: CandleData[]
}

export function CandlestickChart({ candles }: CandlestickChartProps) {
  const points = candles.slice(-80).map((candle) => ({
    ...candle,
    openValue: decimalToNumber(candle.open),
    highValue: decimalToNumber(candle.high),
    lowValue: decimalToNumber(candle.low),
    closeValue: decimalToNumber(candle.close),
  }))
  if (points.length === 0) {
    return <div className="flex h-72 items-center justify-center text-sm text-zinc-500">K 线数据尚未就绪</div>
  }

  const width = 960
  const height = 320
  const padding = 24
  const minimum = Math.min(...points.map((candle) => candle.lowValue))
  const maximum = Math.max(...points.map((candle) => candle.highValue))
  const range = Math.max(maximum - minimum, Number.EPSILON)
  const step = (width - padding * 2) / points.length
  const y = (price: number) => padding + ((maximum - price) / range) * (height - padding * 2)

  return (
    <div className="h-72 w-full" role="img" aria-label={`最近 ${points.length} 根 K 线`}>
      <svg className="h-full w-full" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
        {points.map((candle, index) => {
          const x = padding + step * index + step / 2
          const rising = candle.closeValue >= candle.openValue
          const color = rising ? "var(--color-profit)" : "var(--color-loss)"
          const bodyTop = y(Math.max(candle.openValue, candle.closeValue))
          const bodyHeight = Math.max(1.5, Math.abs(y(candle.openValue) - y(candle.closeValue)))
          return (
            <g key={`${candle.timestamp}-${index}`}>
              <line x1={x} x2={x} y1={y(candle.highValue)} y2={y(candle.lowValue)} stroke={color} strokeWidth="1" />
              <rect
                x={x - Math.max(1, step * 0.3)}
                y={bodyTop}
                width={Math.max(2, step * 0.6)}
                height={bodyHeight}
                fill={color}
              />
            </g>
          )
        })}
      </svg>
    </div>
  )
}
