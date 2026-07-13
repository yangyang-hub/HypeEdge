import { MarketMakingWorkspace } from "@/components/market-making/market-making-workspace"

export default async function MarketMakingPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>
  searchParams: Promise<{ created?: string }>
}) {
  const { id } = await params
  const query = await searchParams
  return <MarketMakingWorkspace strategyId={id} justCreated={query.created === "1"} />
}
