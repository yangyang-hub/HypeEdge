// Utility functions for formatting prices, times, percentages

import { clsx, type ClassValue } from "clsx"
import Decimal from "decimal.js"
import { twMerge } from "tailwind-merge"
import type { DecimalString } from "@/lib/types"

/** Display timezone for all UI timestamps (storage remains UTC). */
export const DISPLAY_TIME_ZONE = "Asia/Shanghai"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

type DecimalValue = Decimal.Value

export function asDecimalString(value: DecimalValue): DecimalString {
  return new Decimal(value).toFixed() as DecimalString
}

function groupedFixed(value: DecimalValue, decimals: number, minimumDecimals: number = decimals): string {
  const fixed = new Decimal(value).toDecimalPlaces(decimals).toFixed(decimals)
  const [integer, fraction = ""] = fixed.split(".")
  const sign = integer.startsWith("-") ? "-" : ""
  const digits = sign ? integer.slice(1) : integer
  const grouped = digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",")
  const trimmed = fraction.replace(/0+$/, "")
  const kept = trimmed.padEnd(minimumDecimals, "0")
  return `${sign}${grouped}${kept ? `.${kept}` : ""}`
}

export function decimalToNumber(value: DecimalValue): number {
  return new Decimal(value).toNumber()
}

export function addDecimals(values: DecimalValue[]): DecimalString {
  return asDecimalString(values.reduce<Decimal>((sum, value) => sum.plus(value), new Decimal(0)))
}

export function formatUsd(value: DecimalValue): string {
  const decimal = new Decimal(value)
  const formatted = groupedFixed(decimal.abs(), 2)
  return decimal.isNegative() ? `-$${formatted}` : `$${formatted}`
}

export function formatPrice(value: DecimalValue, decimals: number = 2): string {
  return groupedFixed(value, decimals)
}

export function formatSize(value: DecimalValue, decimals: number = 6): string {
  return groupedFixed(value, decimals, 0)
}

export function formatPct(value: DecimalValue, decimals: number = 2): string {
  return `${new Decimal(value).mul(100).toFixed(decimals)}%`
}

/**
 * 后端 `pct_used` 已经是 0–100 的百分数值（crates/api/src/routes/risk.rs），
 * 这里直接按百分比展示，不再 ×100（避免 33.33 → "3333%" 的双重放大）。
 */
export function formatPctUsed(pctUsed: DecimalValue, decimals: number = 2): string {
  return `${formatPrice(pctUsed, decimals)}%`
}

/**
 * 后端 `pct_used`（0–100）转为 0–1 分数，供 ProgressBar 等比例组件使用。
 * 超过 100（超限）时饱和到 1。
 */
export function pctUsedFraction(pctUsed: DecimalValue): number {
  return new Decimal(pctUsed).div(100).clamp(0, 1).toNumber()
}

export function formatTime(isoString: string | null): string {
  if (!isoString) return "—"
  const d = new Date(isoString)
  return d.toLocaleString("en-US", {
    timeZone: DISPLAY_TIME_ZONE,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  })
}

export function formatDateTime(isoString: string | null): string {
  if (!isoString) return "—"
  const d = new Date(isoString)
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: DISPLAY_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(d)
  const get = (type: Intl.DateTimeFormatPartTypes) => parts.find((part) => part.type === type)?.value ?? ""
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`
}

export function pnlColor(value: DecimalValue): string {
  const decimal = new Decimal(value)
  if (decimal.isPositive()) return "text-profit"
  if (decimal.isNegative()) return "text-loss"
  return "text-text-tertiary"
}

export function pnlBg(value: DecimalValue): string {
  const decimal = new Decimal(value)
  if (decimal.isPositive()) return "bg-profit/10"
  if (decimal.isNegative()) return "bg-loss/10"
  return ""
}
