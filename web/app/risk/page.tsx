"use client"

import { useState } from "react"
import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { AlertConfirmDialog } from "@/components/ui/alert-confirm-dialog"
import { Button } from "@/components/ui/button"
import { ConfirmPhraseDialog } from "@/components/ui/confirm-phrase-dialog"
import { EmptyState, Metric, Panel, ProgressBar } from "@/components/ui/data-display"
import { useRiskStatus, triggerKillSwitch, resetKillSwitch } from "@/hooks/use-risk"
import { decimalToNumber, formatPct, formatPrice } from "@/lib/utils"

export default function RiskPage() {
  const { risk, refresh } = useRiskStatus()
  const [killOpen, setKillOpen] = useState(false)
  const [resetOpen, setResetOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  async function handleTrigger() {
    setLoading(true)
    setActionError(null)
    try {
      await triggerKillSwitch("manual_trigger_ui")
      await refresh()
      setKillOpen(false)
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "触发 Kill Switch 失败")
    } finally {
      setLoading(false)
    }
  }

  async function handleReset() {
    setLoading(true)
    setActionError(null)
    try {
      await resetKillSwitch()
      await refresh()
      setResetOpen(false)
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "重置 Kill Switch 失败")
    } finally {
      setLoading(false)
    }
  }

  return (
    <AppShell>
      <main id="main-content" className="flex-1 space-y-5 overflow-y-auto p-3 md:p-5">
        <PageHeader
          title="风控面板"
          subtitle="限额、动作额度与 Kill Switch"
          actions={
            <Button type="button" variant="danger-soft" size="sm" onClick={() => setResetOpen(true)}>
              重置 Kill Switch
            </Button>
          }
        />

        {actionError ? (
          <p role="alert" className="rounded-md border border-loss/30 bg-loss/10 px-3 py-2 text-sm text-loss">
            {actionError}
          </p>
        ) : null}

        <Panel
          className={risk?.kill_switch_active ? "border-critical" : undefined}
          title="Kill Switch"
          action={
            <span className={risk?.kill_switch_active ? "text-xs text-critical" : "text-xs text-profit"}>
              {risk?.kill_switch_active ? "已触发" : "正常"}
            </span>
          }
        >
          <div className="space-y-3 p-4">
            {risk?.kill_switch_reason ? (
              <p className="text-sm text-text-secondary">原因：{risk.kill_switch_reason}</p>
            ) : (
              <p className="text-sm text-text-tertiary">系统处于可交易状态。触发后将撤销挂单并拦截新单。</p>
            )}
            <div className="flex flex-wrap items-end gap-2">
              <Button
                type="button"
                variant="danger"
                size="sm"
                disabled={Boolean(risk?.kill_switch_active)}
                onClick={() => setKillOpen(true)}
              >
                触发 Kill Switch
              </Button>
              <span className="text-2xs text-text-tertiary">需输入 CONFIRM 二次确认</span>
            </div>
          </div>
        </Panel>

        {risk ? (
          <Panel title="限额使用">
            <div className="space-y-3 p-4">
              {risk.limits.length === 0 ? (
                <EmptyState message="暂无限额数据" />
              ) : (
                risk.limits.map((limit) => {
                  const used = decimalToNumber(limit.pct_used)
                  return (
                    <div key={limit.name} className="flex items-center gap-3">
                      <span className="w-28 shrink-0 text-sm text-text-secondary">{limit.name}</span>
                      <ProgressBar value={used} className="flex-1" />
                      <span className="w-28 shrink-0 text-right font-mono text-xs text-text-tertiary">
                        {limit.unit === "%"
                          ? `${formatPct(limit.current, 1)} / ${formatPct(limit.limit, 1)}`
                          : `${formatPrice(limit.current, 1)} / ${formatPrice(limit.limit, 1)} ${limit.unit}`}
                      </span>
                      <span
                        className={`w-12 shrink-0 text-right font-mono text-xs ${
                          used > 0.8 ? "text-loss" : used > 0.6 ? "text-warning" : "text-profit"
                        }`}
                      >
                        {formatPct(limit.pct_used, 0)}
                      </span>
                    </div>
                  )
                })
              )}
            </div>
          </Panel>
        ) : null}

        {risk?.check_stats ? (
          <Panel>
            <div className="grid grid-cols-1 sm:grid-cols-3">
              <Metric label="总检查" value={`${risk.check_stats.check_count ?? 0}`} />
              <Metric label="通过" value={`${risk.check_stats.pass_count ?? 0}`} />
              <Metric label="拒绝" value={`${risk.check_stats.reject_count ?? 0}`} />
            </div>
          </Panel>
        ) : null}
      </main>

      <ConfirmPhraseDialog
        open={killOpen}
        onOpenChange={setKillOpen}
        title="触发 Kill Switch"
        description="Kill Switch 已触发后，所有挂单将撤销。输入 CONFIRM 确认。"
        phrase="CONFIRM"
        confirmLabel="触发"
        loading={loading}
        onConfirm={handleTrigger}
      />

      <AlertConfirmDialog
        open={resetOpen}
        onOpenChange={setResetOpen}
        title="重置 Kill Switch"
        description="重置后策略不会自动恢复，需人工确认环境安全后再启动策略。"
        confirmLabel="确认重置"
        loading={loading}
        onConfirm={handleReset}
      />
    </AppShell>
  )
}
