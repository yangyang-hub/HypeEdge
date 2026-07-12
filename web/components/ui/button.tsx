"use client"

import { Slot } from "@radix-ui/react-slot"
import { cva, type VariantProps } from "class-variance-authority"
import { Loader2 } from "lucide-react"
import * as React from "react"
import { cn } from "@/lib/utils"

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-1.5 whitespace-nowrap rounded-md text-sm font-medium transition-colors duration-100 disabled:pointer-events-none disabled:opacity-40 disabled:cursor-not-allowed",
  {
    variants: {
      variant: {
        primary: "bg-accent text-accent-foreground hover:bg-accent-hover",
        secondary: "border border-border-strong bg-transparent text-text-primary hover:bg-bg-hover",
        ghost: "bg-transparent text-text-secondary hover:bg-bg-hover hover:text-text-primary",
        buy: "bg-profit/15 text-profit hover:bg-profit/25",
        sell: "bg-loss/15 text-loss hover:bg-loss/25",
        danger: "bg-critical text-white hover:bg-critical/90",
        "danger-soft": "bg-critical/15 text-critical hover:bg-critical/25",
      },
      size: {
        sm: "h-7 min-h-7 px-2.5 text-xs",
        md: "h-8 min-h-8 px-3",
        lg: "h-9 min-h-9 px-4",
      },
    },
    defaultVariants: {
      variant: "secondary",
      size: "md",
    },
  },
)

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean
  loading?: boolean
}

export function Button({
  className,
  variant,
  size,
  asChild = false,
  loading = false,
  disabled,
  children,
  ...props
}: ButtonProps) {
  const Comp = asChild ? Slot : "button"
  return (
    <Comp
      className={cn(buttonVariants({ variant, size }), className)}
      disabled={disabled || loading}
      {...props}
    >
      {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" /> : null}
      {children}
    </Comp>
  )
}

export { buttonVariants }
