import type { ReactNode } from "react"
import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import { Topbar } from "@/components/layout/topbar"

export interface AppShellProps {
  children: ReactNode
}

export function AppShell({ children }: AppShellProps) {
  return (
    <div className="flex h-screen flex-col bg-bg-base text-text-primary">
      <Topbar />
      <div className="flex min-h-0 flex-1">
        <Sidebar />
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden bg-bg-elevated">
          {children}
          <StatusBar />
        </div>
      </div>
    </div>
  )
}
