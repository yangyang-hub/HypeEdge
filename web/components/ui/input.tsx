"use client"

import * as React from "react"
import { cn } from "@/lib/utils"

export type InputProps = React.InputHTMLAttributes<HTMLInputElement>

export const Input = React.forwardRef<HTMLInputElement, InputProps>(function Input(
  { className, type = "text", ...props },
  ref,
) {
  return (
    <input
      ref={ref}
      type={type}
      className={cn(
        "flex h-8 w-full rounded-md border border-border-default bg-bg-elevated px-3 text-sm text-text-primary placeholder:text-text-tertiary",
        "hover:border-border-strong focus-visible:border-accent focus-visible:outline-none",
        "disabled:cursor-not-allowed disabled:opacity-40",
        className,
      )}
      {...props}
    />
  )
})
