import { cn } from "@/lib/utils"

export type LiveTone = "live" | "degraded" | "offline"

export interface LiveIndicatorProps {
  tone: LiveTone
  label?: string
  className?: string
  title?: string
}

const TONE_META: Record<LiveTone, { label: string; color: string }> = {
  live: { label: "LIVE", color: "text-accent" },
  degraded: { label: "DEGRADED", color: "text-warning" },
  offline: { label: "OFFLINE", color: "text-loss" },
}

export function LiveIndicator({ tone, label, className, title }: LiveIndicatorProps) {
  const meta = TONE_META[tone]
  return (
    <span
      title={title ?? meta.label}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border border-border-subtle bg-bg-panel px-2 py-0.5 text-2xs font-bold tracking-wider",
        meta.color,
        className,
      )}
    >
      <span className="relative flex h-1.5 w-1.5">
        <span
          className={cn(
            "absolute inset-0 rounded-full bg-current",
            tone === "live" && "live-pulse-ring opacity-60",
            tone === "offline" && "opacity-40",
          )}
        />
        <span className="relative h-1.5 w-1.5 rounded-full bg-current" />
      </span>
      {label ?? meta.label}
    </span>
  )
}
