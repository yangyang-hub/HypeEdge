"use client"

import type { ReactNode } from "react"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { EnvBadge } from "@/components/ui/env-badge"
import { Panel, StaleBanner } from "@/components/ui/data-display"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useAccount } from "@/hooks/use-account"
import { useSystemStatus } from "@/hooks/use-system-status"

export default function SettingsPage() {
  const { account } = useAccount()
  const { status, error, isLoading } = useSystemStatus()

  return (
    <AppShell>
      <main id="main-content" className="flex-1 space-y-5 overflow-y-auto p-3 md:p-5">
        <PageHeader title="系统设置" subtitle="连接、策略参数与告警（只读项不暴露密钥）" />

        {error ? <StaleBanner message="无法刷新系统配置，显示内容可能已过期" /> : null}

        <Tabs defaultValue="connection">
          <TabsList>
            <TabsTrigger value="connection">连接</TabsTrigger>
            <TabsTrigger value="risk">风控</TabsTrigger>
            <TabsTrigger value="strategy">策略参数</TabsTrigger>
            <TabsTrigger value="alerts">告警</TabsTrigger>
          </TabsList>

          <TabsContent value="connection">
            <Panel title="连接配置（只读）">
              <dl className="divide-y divide-border-subtle">
                <SettingsRow label="环境">
                  {isLoading ? "加载中…" : <EnvBadge environment={status?.environment} />}
                </SettingsRow>
                <SettingsRow label="API">
                  <span className="font-mono text-xs">同源 /api/v1</span>
                </SettingsRow>
                <SettingsRow label="交易状态">
                  <span className={account?.trading_enabled ? "text-profit" : "text-loss"}>
                    {account?.trading_enabled ? "已启用" : "未启用"}
                  </span>
                </SettingsRow>
                <SettingsRow label="Agent Wallet">
                  <span className="text-text-tertiary">仅后端环境变量可见</span>
                </SettingsRow>
              </dl>
            </Panel>
          </TabsContent>

          <TabsContent value="risk">
            <Panel title="风控">
              <div className="p-4 text-sm text-text-secondary">
                风控限额与 Kill Switch 请在「风控」页面操作。此处不提供绕过门禁的配置入口。
              </div>
            </Panel>
          </TabsContent>

          <TabsContent value="strategy">
            <Panel title="策略参数">
              <div className="space-y-2 p-4 text-sm text-text-secondary">
                <p>策略参数通过 configs/strategy_*.yaml 管理；修改后热更新并写入审计日志。</p>
                <p className="text-text-tertiary">前端参数编辑功能待实现。做市配置请使用做市工作台 Configuration 分区。</p>
              </div>
            </Panel>
          </TabsContent>

          <TabsContent value="alerts">
            <Panel title="告警配置">
              <dl className="divide-y divide-border-subtle">
                <SettingsRow label="Telegram">
                  <span className="text-text-tertiary">未配置</span>
                </SettingsRow>
                <SettingsRow label="钉钉">
                  <span className="text-text-tertiary">未配置</span>
                </SettingsRow>
              </dl>
            </Panel>
          </TabsContent>
        </Tabs>
      </main>
    </AppShell>
  )
}

function SettingsRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-4 px-4 py-3 text-sm">
      <dt className="text-text-tertiary">{label}</dt>
      <dd className="text-right text-text-primary">{children}</dd>
    </div>
  )
}
