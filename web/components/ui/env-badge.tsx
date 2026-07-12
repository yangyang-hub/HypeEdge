import { cn } from "@/lib/utils"

export interface EnvBadgeProps {
  environment: "dev" | "testnet" | "mainnet" | string | null | undefined
  className?: string
}

export function EnvBadge({ environment, className }: EnvBadgeProps) {
  const env = (environment ?? "unknown").toLowerCase()
  const label = env.toUpperCase()
  const tone =
    env === "mainnet"
      ? "bg-env-mainnet text-white"
      : env === "testnet"
        ? "bg-env-testnet/20 text-env-testnet"
        : env === "dev"
          ? "bg-env-dev/20 text-env-dev"
          : "bg-bg-active text-text-tertiary"

  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2 py-0.5 text-2xs font-bold tracking-wider",
        tone,
        className,
      )}
    >
      {label}
    </span>
  )
}
