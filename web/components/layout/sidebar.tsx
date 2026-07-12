"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { useSystemStatus } from "@/hooks/use-system-status"

const NAV_ITEMS = [
  { href: "/", label: "总览", icon: "📊" },
  { href: "/market", label: "行情", icon: "📈" },
  { href: "/positions", label: "持仓", icon: "💼" },
  { href: "/orders", label: "订单", icon: "📋" },
  { href: "/strategy", label: "策略", icon: "🤖" },
  { href: "/risk", label: "风控", icon: "🛡️" },
  { href: "/settings", label: "设置", icon: "⚙️" },
]

export function Sidebar() {
  const pathname = usePathname()
  const { status } = useSystemStatus()

  return (
    <aside className="w-16 shrink-0 border-r border-zinc-800 bg-zinc-900/50 flex flex-col md:w-56">
      <div className="p-3 border-b border-zinc-800 md:p-4">
        <h1 className="text-lg font-bold tracking-tight">HypeEdge</h1>
        <span className="hidden text-xs px-2 py-0.5 rounded bg-zinc-800 text-zinc-400 md:inline-block">
          {status?.environment ?? "connecting"}
        </span>
      </div>
      <nav className="flex-1 p-2 space-y-1" aria-label="主导航">
        {NAV_ITEMS.map((item) => (
          <Link
            key={item.href}
            href={item.href}
            className={cn(
              "flex min-h-11 items-center justify-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors md:justify-start",
              pathname === item.href
                ? "bg-zinc-800 text-white"
                : "text-zinc-400 hover:text-white hover:bg-zinc-800/50"
            )}
          >
            <span aria-hidden="true">{item.icon}</span>
            <span className="sr-only md:not-sr-only">{item.label}</span>
          </Link>
        ))}
      </nav>
    </aside>
  )
}
