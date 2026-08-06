import { cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { CreateStrategyDialog } from "@/components/strategy/create-strategy-dialog"
import type { StrategyInstance } from "@/lib/types"

vi.mock("next/navigation", () => ({ useRouter: () => ({ push: vi.fn() }) }))
vi.mock("@/hooks/use-account", () => ({ useAccount: () => ({ account: null }) }))
vi.mock("@/hooks/use-system-status", () => ({
  useInstrumentMeta: () => ({ meta: null, error: null }),
}))
const createStrategyMock = vi.hoisted(() => vi.fn())

vi.mock("@/hooks/use-strategies", () => ({ createStrategy: createStrategyMock }))

beforeEach(() => {
  createStrategyMock.mockResolvedValue({
    strategy_id: "fa-auto-1",
    strategy_type: "funding_arb",
    symbol: "AUTO",
    sub_account: "0xabc",
    desired_state: "stopped",
    actual_state: "stopped",
    desired_config_version_id: 1,
    effective_config_version_id: null,
    revision: 0,
    archived_at: null,
    created_at: new Date(0).toISOString(),
    updated_at: new Date(0).toISOString(),
  })
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

const stoppedStrategy: StrategyInstance = {
  strategy_id: "mm-btc-1",
  strategy_type: "market_maker",
  symbol: "BTC",
  sub_account: "0xabc",
  desired_state: "stopped",
  actual_state: "stopped",
  desired_config_version_id: 1,
  effective_config_version_id: 1,
  revision: 1,
  archived_at: null,
  created_at: new Date(0).toISOString(),
  updated_at: new Date(0).toISOString(),
  session_mode: null,
}

describe("CreateStrategyDialog", () => {
  it("omits manual market inputs for funding arbitrage", async () => {
    const user = userEvent.setup()
    render(<CreateStrategyDialog open onOpenChange={vi.fn()} existing={[stoppedStrategy]} />)

    await user.selectOptions(screen.getByLabelText(/策略类型/), "funding_arb")

    expect(screen.queryByLabelText(/交易品种/)).not.toBeInTheDocument()
    expect(screen.queryByLabelText(/路由账户地址/)).not.toBeInTheDocument()
    expect(screen.getByText(/后端安全配置自动绑定/)).toBeInTheDocument()
    expect(screen.getByPlaceholderText("fa-auto-1")).toBeInTheDocument()

    await user.type(screen.getByLabelText(/策略 ID/), "fa-auto-1")
    await user.click(screen.getByRole("button", { name: "下一步" }))

    expect(screen.getByText(/自动扫描 USDC 现货与同名永续/)).toBeInTheDocument()
    expect(screen.queryByText("现货标的")).not.toBeInTheDocument()

    await user.click(screen.getByRole("button", { name: "创建" }))

    await waitFor(() => expect(createStrategyMock).toHaveBeenCalledOnce())
    const request = createStrategyMock.mock.calls[0]?.[0]
    expect(request).not.toHaveProperty("sub_account")
  })

  it("warns when an active fixed market already occupies the AUTO account", async () => {
    const user = userEvent.setup()
    render(
      <CreateStrategyDialog
        open
        onOpenChange={vi.fn()}
        existing={[{ ...stoppedStrategy, actual_state: "running", desired_state: "running" }]}
      />,
    )

    await user.selectOptions(screen.getByLabelText(/策略类型/), "funding_arb")

    expect(screen.getByText(/后端路由账户已有活跃市场 allocation/)).toBeInTheDocument()
  })
})
