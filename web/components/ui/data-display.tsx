import Decimal from "decimal.js"
import type { ReactNode } from "react"
import { cn, formatUsd } from "@/lib/utils"

export interface MetricProps {
  label: string
  value: string
  delta?: string
  deltaTone?: "profit" | "loss" | "neutral"
  unavailable?: boolean
  className?: string
}

export function Metric({ label, value, delta, deltaTone = "neutral", unavailable = false, className }: MetricProps) {
  return (
    <div className={cn("border-b border-border-subtle py-3 sm:border-b-0 sm:border-r sm:px-4 sm:last:border-r-0", className)}>
      <div className="text-2xs uppercase tracking-wider text-text-tertiary">{label}</div>
      <div className="mt-1 font-mono text-xl font-semibold tabular-nums text-text-primary">{value}</div>
      {delta ? (
        <div
          className={cn(
            "mt-1 text-xs font-mono",
            deltaTone === "profit" && "text-profit",
            deltaTone === "loss" && "text-loss",
            deltaTone === "neutral" && "text-text-tertiary",
          )}
        >
          {delta}
        </div>
      ) : null}
      {unavailable ? <div className="mt-1 text-xs text-warning">数据未就绪</div> : null}
    </div>
  )
}

export interface PnLTextProps {
  value: Decimal.Value
  className?: string
}

export function PnLText({ value, className }: PnLTextProps) {
  const decimal = new Decimal(value)
  if (decimal.isZero()) {
    return <span className={cn("font-mono tabular-nums text-text-tertiary", className)}>{formatUsd(value)}</span>
  }
  const tone = decimal.isNegative() ? "text-loss" : "text-profit"
  const prefix = decimal.isNegative() ? "" : "+"
  return (
    <span className={cn("font-mono tabular-nums", tone, className)}>
      {prefix}
      {formatUsd(value)}
    </span>
  )
}

export interface SideTagProps {
  side: "long" | "short" | "buy" | "sell" | "flat"
}

export function SideTag({ side }: SideTagProps) {
  if (side === "flat") return <span className="text-text-tertiary">—</span>
  const isLong = side === "long" || side === "buy"
  return (
    <span
      className={cn(
        "inline-flex rounded-sm px-1.5 py-0.5 text-2xs font-medium",
        isLong ? "bg-profit/15 text-profit" : "bg-loss/15 text-loss",
      )}
    >
      {side === "long" ? "多" : side === "short" ? "空" : side === "buy" ? "买" : "卖"}
    </span>
  )
}

export interface ProgressBarProps {
  value: number
  className?: string
}

export function ProgressBar({ value, className }: ProgressBarProps) {
  const pct = Math.min(Math.max(value, 0), 1)
  const tone = pct > 0.8 ? "bg-loss" : pct > 0.6 ? "bg-warning" : "bg-profit"
  return (
    <div className={cn("h-1.5 overflow-hidden rounded-sm bg-bg-active", className)}>
      <div className={cn("h-full rounded-sm transition-all", tone)} style={{ width: `${pct * 100}%` }} />
    </div>
  )
}

export interface EmptyStateProps {
  message: string
  action?: ReactNode
  className?: string
}

export function EmptyState({ message, action, className }: EmptyStateProps) {
  return (
    <div className={cn("flex flex-col items-center justify-center gap-3 px-4 py-12 text-center", className)}>
      <p className="text-sm text-text-tertiary">{message}</p>
      {action}
    </div>
  )
}

export interface StaleBannerProps {
  message: string
  className?: string
}

export function StaleBanner({ message, className }: StaleBannerProps) {
  return (
    <div
      role="status"
      className={cn(
        "rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning",
        className,
      )}
    >
      {message}
    </div>
  )
}

export interface PanelProps {
  children: ReactNode
  className?: string
  title?: string
  action?: ReactNode
}

export function Panel({ children, className, title, action }: PanelProps) {
  return (
    <section className={cn("overflow-hidden rounded-md border border-border-default bg-bg-panel", className)}>
      {title ? (
        <div className="flex items-center justify-between border-b border-border-subtle px-3 py-2">
          <h3 className="text-sm font-medium text-text-primary">{title}</h3>
          {action}
        </div>
      ) : null}
      {children}
    </section>
  )
}
