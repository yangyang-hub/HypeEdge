"use client"

import { useEffect, useRef } from "react"
import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts"
import type { CandleData } from "@/lib/types"
import { decimalToNumber } from "@/lib/utils"

export interface CandlestickChartProps {
  candles: CandleData[]
  priceDecimals?: number
}

const PROFIT = "#22c55e"
const LOSS = "#ef4444"
const GRID = "#27272a"
const TEXT = "#a1a1aa"
const CROSSHAIR = "#52525b"

interface ChartPoint {
  time: UTCTimestamp
  open: number
  high: number
  low: number
  close: number
  volume: number
  rising: boolean
}

function toChartData(candles: CandleData[]): ChartPoint[] {
  const byTime = new Map<number, CandleData>()
  for (const candle of candles) {
    byTime.set(candle.timestamp, candle)
  }
  return [...byTime.values()]
    .sort((left, right) => left.timestamp - right.timestamp)
    .map((candle) => {
      const open = decimalToNumber(candle.open)
      const close = decimalToNumber(candle.close)
      return {
        time: Math.floor(candle.timestamp / 1000) as UTCTimestamp,
        open,
        high: decimalToNumber(candle.high),
        low: decimalToNumber(candle.low),
        close,
        volume: decimalToNumber(candle.volume),
        rising: close >= open,
      }
    })
}

function applySeriesData(
  candleSeries: ISeriesApi<"Candlestick">,
  volumeSeries: ISeriesApi<"Histogram">,
  chart: IChartApi,
  candles: CandleData[],
  options: { fitContent: boolean },
): void {
  const points = toChartData(candles)
  if (points.length === 0) {
    candleSeries.setData([])
    volumeSeries.setData([])
    return
  }

  candleSeries.setData(
    points.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })),
  )
  volumeSeries.setData(
    points.map(({ time, volume, rising }) => ({
      time,
      value: volume,
      color: rising ? "rgba(34, 197, 94, 0.35)" : "rgba(239, 68, 68, 0.35)",
    })),
  )
  if (options.fitContent) {
    chart.timeScale().fitContent()
  }
}

export function CandlestickChart({ candles, priceDecimals = 2 }: CandlestickChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null)
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null)
  const candlesRef = useRef(candles)
  const rangeKeyRef = useRef("")
  candlesRef.current = candles

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: TEXT,
        fontSize: 11,
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
      },
      grid: {
        vertLines: { color: GRID },
        horzLines: { color: GRID },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: CROSSHAIR, labelBackgroundColor: CROSSHAIR },
        horzLine: { color: CROSSHAIR, labelBackgroundColor: CROSSHAIR },
      },
      rightPriceScale: {
        borderColor: GRID,
        scaleMargins: { top: 0.08, bottom: 0.22 },
      },
      timeScale: {
        borderColor: GRID,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 4,
        barSpacing: 8,
        minBarSpacing: 3,
      },
      localization: {
        locale: "zh-CN",
        priceFormatter: (price: number) =>
          price.toLocaleString("en-US", {
            minimumFractionDigits: priceDecimals,
            maximumFractionDigits: priceDecimals,
          }),
      },
    })

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: PROFIT,
      downColor: LOSS,
      borderUpColor: PROFIT,
      borderDownColor: LOSS,
      wickUpColor: PROFIT,
      wickDownColor: LOSS,
      priceFormat: {
        type: "price",
        precision: priceDecimals,
        minMove: 10 ** -priceDecimals,
      },
    })

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    })
    chart.priceScale("volume").applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
    })

    chartRef.current = chart
    candleSeriesRef.current = candleSeries
    volumeSeriesRef.current = volumeSeries
    rangeKeyRef.current = ""
    applySeriesData(candleSeries, volumeSeries, chart, candlesRef.current, { fitContent: true })

    return () => {
      chart.remove()
      chartRef.current = null
      candleSeriesRef.current = null
      volumeSeriesRef.current = null
    }
  }, [priceDecimals])

  useEffect(() => {
    const candleSeries = candleSeriesRef.current
    const volumeSeries = volumeSeriesRef.current
    const chart = chartRef.current
    if (!candleSeries || !volumeSeries || !chart) return

    const points = toChartData(candles)
    // Fit when the series identity changes (symbol/interval), not on every live bar.
    const rangeKey = points.length === 0 ? "" : String(points[0].time)
    const shouldFit = rangeKey !== rangeKeyRef.current
    rangeKeyRef.current = rangeKey
    applySeriesData(candleSeries, volumeSeries, chart, candles, { fitContent: shouldFit })
  }, [candles])

  return (
    <div className="relative h-80 w-full">
      {candles.length === 0 && (
        <div className="absolute inset-0 z-10 flex items-center justify-center text-sm text-zinc-500">
          K 线数据尚未就绪
        </div>
      )}
      <div
        ref={containerRef}
        className="h-full w-full"
        role="img"
        aria-label={candles.length > 0 ? `最近 ${candles.length} 根 K 线` : "K 线图表"}
      />
    </div>
  )
}
