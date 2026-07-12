"use client"

import { cn } from "@/lib/utils"

export interface SegmentedControlOption {
  value: string
  label: string
}

export interface SegmentedControlProps {
  options: SegmentedControlOption[]
  value: string
  onChange: (value: string) => void
  ariaLabel: string
  className?: string
  size?: "sm" | "md"
}

export function SegmentedControl({
  options,
  value,
  onChange,
  ariaLabel,
  className,
  size = "md",
}: SegmentedControlProps) {
  return (
    <div
      role="group"
      aria-label={ariaLabel}
      className={cn(
        "inline-flex items-center gap-0.5 rounded-md border border-border-default bg-bg-panel p-0.5",
        className,
      )}
    >
      {options.map((option) => {
        const active = option.value === value
        return (
          <button
            key={option.value}
            type="button"
            aria-pressed={active}
            onClick={() => onChange(option.value)}
            className={cn(
              "rounded-sm font-medium transition-colors",
              size === "sm" ? "h-6 min-h-6 px-2 text-2xs" : "h-7 min-h-7 px-2.5 text-xs",
              active
                ? "bg-bg-active text-text-primary"
                : "text-text-tertiary hover:text-text-secondary",
            )}
          >
            {option.label}
          </button>
        )
      })}
    </div>
  )
}
