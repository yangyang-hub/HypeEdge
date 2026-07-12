import { MarketMakingWorkspace } from "@/components/market-making/market-making-workspace"

export default async function MarketMakingPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  return <MarketMakingWorkspace strategyId={id} />
}
