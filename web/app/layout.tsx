import type { Metadata } from "next"
import "@/styles/globals.css"
import { AppProviders } from "@/components/layout/app-providers"
import { GlobalAlerts } from "@/components/layout/global-alerts"

export const metadata: Metadata = {
  title: "HypeEdge — 量化交易仪表盘",
  description: "Hyperliquid 永续合约量化交易系统监控面板",
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN">
      <body className="min-h-screen bg-zinc-950 text-zinc-100">
        <a href="#main-content" className="skip-link">跳到主要内容</a>
        <AppProviders>
          <GlobalAlerts />
          {children}
        </AppProviders>
      </body>
    </html>
  )
}
