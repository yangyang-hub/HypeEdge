import { AppShell } from "@/components/layout/app-shell"
import { PageHeader } from "@/components/layout/page-header"
import { Panel } from "@/components/ui/data-display"

export default async function FundingArbPage({
  params,
}: {
  params: Promise<{ id: string }>
}) {
  const { id } = await params
  return (
    <AppShell>
      <main id="main-content" className="flex-1 space-y-4 overflow-y-auto p-3 md:p-5">
        <PageHeader title="资金费套利工作台" subtitle={`策略 ${id} · 单所内对冲（HL 永续 + HL 现货）`} />
        <Panel>
          <div className="space-y-2 p-4 text-sm text-text-secondary">
            <p className="font-medium text-text-primary">开发中（骨架占位）</p>
            <p>
              资金费套利的控制面（创建、配置版本、启停）已就绪；真实的 delta-neutral 执行（现货腿行情 / 下单、对冲再平衡、funding 结算）将在后续阶段接入。
            </p>
            <p className="text-2xs text-text-tertiary">
              当前运行时为 stub：启动 / 停止只记录状态，不会下单。可在策略列表查看与修改该实例的配置版本与生命周期。
            </p>
          </div>
        </Panel>
      </main>
    </AppShell>
  )
}
