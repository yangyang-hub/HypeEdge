import type { Metadata } from "next"
import { IBM_Plex_Mono, IBM_Plex_Sans } from "next/font/google"
import "@/styles/globals.css"
import { AppProviders } from "@/components/layout/app-providers"
import { GlobalAlerts } from "@/components/layout/global-alerts"

const ibmPlexSans = IBM_Plex_Sans({
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  variable: "--font-ibm-plex-sans",
  display: "swap",
})

const ibmPlexMono = IBM_Plex_Mono({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  variable: "--font-ibm-plex-mono",
  display: "swap",
})

export const metadata: Metadata = {
  title: "HypeEdge — 量化交易仪表盘",
  description: "Hyperliquid 永续合约量化交易系统监控面板",
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN" className={`${ibmPlexSans.variable} ${ibmPlexMono.variable}`}>
      <body className="min-h-screen bg-bg-base font-sans text-text-primary antialiased">
        <a href="#main-content" className="skip-link">
          跳到主要内容
        </a>
        <AppProviders>
          <GlobalAlerts />
          {children}
        </AppProviders>
      </body>
    </html>
  )
}
