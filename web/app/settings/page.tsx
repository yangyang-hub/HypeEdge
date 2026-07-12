"use client"

import { Sidebar } from "@/components/layout/sidebar"
import { StatusBar } from "@/components/layout/status-bar"
import { useAccount } from "@/hooks/use-account"
import { useSystemStatus } from "@/hooks/use-system-status"

export default function SettingsPage() {
  const { account } = useAccount()
  const { status, error, isLoading } = useSystemStatus()

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <main id="main-content" className="flex-1 overflow-y-auto p-3 space-y-6 md:p-6">
          <h2 className="text-2xl font-bold">系统设置</h2>

          {/* Connection Info */}
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
            <h3 className="font-medium mb-3">连接配置</h3>
            {error ? <p role="alert" className="mb-3 text-sm text-warning">无法刷新系统配置，显示内容可能已过期</p> : null}
            <div className="grid grid-cols-1 gap-3 text-sm md:grid-cols-2">
              <div className="flex justify-between bg-zinc-800/50 rounded px-3 py-2">
                <span className="text-zinc-500">环境</span>
                <span>{isLoading ? "加载中…" : status?.environment ?? "未知"}</span>
              </div>
              <div className="flex justify-between bg-zinc-800/50 rounded px-3 py-2">
                <span className="text-zinc-500">API</span>
                <span className="font-mono text-xs">同源 /api/v1</span>
              </div>
              <div className="flex justify-between bg-zinc-800/50 rounded px-3 py-2">
                <span className="text-zinc-500">交易状态</span>
                <span className={account?.trading_enabled ? "text-profit" : "text-loss"}>
                  {account?.trading_enabled ? "已启用" : "未启用"}
                </span>
              </div>
              <div className="flex justify-between bg-zinc-800/50 rounded px-3 py-2">
                <span className="text-zinc-500">Agent Wallet</span>
                <span className="text-zinc-400">仅后端环境变量可见</span>
              </div>
            </div>
          </div>

          {/* Strategy Params */}
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
            <h3 className="font-medium mb-3">策略参数</h3>
            <p className="text-xs text-zinc-500 mb-4">
              ⚠️ 修改后自动热更新，无需重启。每次变更记录审计日志。
            </p>
            <div className="text-sm text-zinc-400 bg-zinc-800/50 rounded p-4">
              策略参数通过 configs/strategy_trend.yaml 管理。<br />
              前端参数编辑功能待实现。
            </div>
          </div>

          {/* Alert Config */}
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
            <h3 className="font-medium mb-3">告警配置</h3>
            <div className="grid grid-cols-1 gap-3 text-sm md:grid-cols-2">
              <div className="flex justify-between bg-zinc-800/50 rounded px-3 py-2">
                <span className="text-zinc-500">Telegram</span>
                <span className="text-zinc-600">未配置</span>
              </div>
              <div className="flex justify-between bg-zinc-800/50 rounded px-3 py-2">
                <span className="text-zinc-500">钉钉</span>
                <span className="text-zinc-600">未配置</span>
              </div>
            </div>
          </div>
        </main>
        <StatusBar />
      </div>
    </div>
  )
}
