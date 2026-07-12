import { cva, type VariantProps } from "class-variance-authority"
import type { HTMLAttributes } from "react"
import { cn } from "@/lib/utils"

const badgeVariants = cva(
  "inline-flex items-center rounded-sm px-1.5 py-0.5 text-2xs font-medium uppercase tracking-wider",
  {
    variants: {
      variant: {
        default: "bg-bg-active text-text-secondary",
        accent: "bg-accent-muted text-accent",
        profit: "bg-profit/15 text-profit",
        loss: "bg-loss/15 text-loss",
        warning: "bg-warning/15 text-warning",
        critical: "bg-critical/15 text-critical",
        info: "bg-info/15 text-info",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
)

export interface BadgeProps extends HTMLAttributes<HTMLSpanElement>, VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <span className={cn(badgeVariants({ variant }), className)} {...props} />
}
