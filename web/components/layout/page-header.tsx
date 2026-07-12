import type { ReactNode } from "react"
import { cn } from "@/lib/utils"

export interface PageHeaderProps {
  title: string
  subtitle?: ReactNode
  actions?: ReactNode
  className?: string
}

export function PageHeader({ title, subtitle, actions, className }: PageHeaderProps) {
  return (
    <header className={cn("flex flex-wrap items-start justify-between gap-3", className)}>
      <div className="min-w-0">
        <h1 className="text-xl font-semibold tracking-tight text-text-primary">{title}</h1>
        {subtitle ? <div className="mt-1 text-xs text-text-tertiary">{subtitle}</div> : null}
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
    </header>
  )
}
