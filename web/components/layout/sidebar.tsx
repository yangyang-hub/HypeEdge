"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import {
  Bot,
  Briefcase,
  CandlestickChart,
  LayoutDashboard,
  ListOrdered,
  Settings,
  Shield,
} from "lucide-react"
import { cn } from "@/lib/utils"

const NAV_ITEMS = [
  { href: "/", label: "总览", icon: LayoutDashboard },
  { href: "/market", label: "行情", icon: CandlestickChart },
  { href: "/positions", label: "持仓", icon: Briefcase },
  { href: "/orders", label: "订单", icon: ListOrdered },
  { href: "/strategy", label: "策略", icon: Bot },
  { href: "/risk", label: "风控", icon: Shield },
  { href: "/settings", label: "设置", icon: Settings },
]

function isActive(pathname: string, href: string): boolean {
  if (href === "/") return pathname === "/"
  return pathname === href || pathname.startsWith(`${href}/`)
}

export function Sidebar() {
  const pathname = usePathname()

  return (
    <aside className="flex w-14 shrink-0 flex-col border-r border-border-default bg-bg-base md:w-[200px]">
      <nav className="flex flex-1 flex-col gap-0.5 p-2" aria-label="主导航">
        {NAV_ITEMS.map((item) => {
          const active = isActive(pathname, item.href)
          const Icon = item.icon
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "relative flex h-9 items-center justify-center gap-2.5 rounded-md px-2 text-sm transition-colors md:justify-start md:px-3",
                active
                  ? "bg-bg-active text-text-primary"
                  : "text-text-tertiary hover:bg-bg-hover hover:text-text-secondary",
              )}
            >
              {active ? (
                <span className="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-accent md:left-0" aria-hidden="true" />
              ) : null}
              <Icon className="h-4 w-4 shrink-0" aria-hidden="true" />
              <span className="sr-only md:not-sr-only">{item.label}</span>
            </Link>
          )
        })}
      </nav>
    </aside>
  )
}
