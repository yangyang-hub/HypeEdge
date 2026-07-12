import { cn } from "@/lib/utils"
import type { StrategyLifecycleState } from "@/lib/types"

const STATUS_META: Record<
  StrategyLifecycleState,
  { label: string; title: string; className: string }
> = {
  stopped: { label: "已停止", title: "STOPPED", className: "bg-bg-active text-text-secondary" },
  warming: { label: "预热中", title: "WARMING", className: "bg-info/15 text-info" },
  shadow: { label: "Shadow", title: "SHADOW", className: "bg-info/15 text-info" },
  running: { label: "运行中", title: "RUNNING", className: "bg-profit/15 text-profit" },
  paused: { label: "已暂停", title: "PAUSED", className: "bg-warning/15 text-warning" },
  draining: { label: "排空中", title: "DRAINING", className: "bg-warning/15 text-warning" },
  faulted: { label: "故障", title: "FAULTED", className: "bg-critical/15 text-critical" },
}

export interface StrategyStatusChipProps {
  state?: StrategyLifecycleState | string | null
  className?: string
}

export function StrategyStatusChip({ state, className }: StrategyStatusChipProps) {
  const normalized = (state ?? "stopped").toString().toLowerCase() as StrategyLifecycleState
  const meta = STATUS_META[normalized] ?? {
    label: normalized || "unknown",
    title: (normalized || "unknown").toUpperCase(),
    className: "bg-bg-active text-text-secondary",
  }
  return (
    <span
      title={meta.title}
      className={cn(
        "inline-flex items-center rounded-sm px-1.5 py-0.5 text-2xs font-medium tracking-wide",
        meta.className,
        className,
      )}
    >
      {meta.label}
    </span>
  )
}
