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
            <p className="font-medium text-text-primary">Testnet 真实两腿执行</p>
            <p>
              运行时按“先买现货、后空永续”入场，按“先 reduce-only 平永续、后卖现货”退出；状态只由认证成交与完整对账推进。
            </p>
            <p className="text-2xs text-text-tertiary">
              仅在 HYPE_ENV=testnet、完整 V2 链路和 funding_arb_execution_enabled 同时开启时下单，单 cycle 部署硬上限 25 USDC。dev 保持观察模式，mainnet 无解锁路径；现货 USDC 必须由操作员预先充值。
            </p>
          </div>
        </Panel>
      </main>
    </AppShell>
  )
}
