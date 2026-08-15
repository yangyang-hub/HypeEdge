import { cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { CreateStrategyDialog } from "@/components/strategy/create-strategy-dialog"
import type { DecimalString, InstrumentMeta, StrategyInstance } from "@/lib/types"

const d = (value: string) => value as DecimalString

vi.mock("next/navigation", () => ({ useRouter: () => ({ push: vi.fn() }) }))

// 可变 mock：模拟账户轮询（equity 变化）与 meta 轮询（新对象）场景。
const accountMock = vi.hoisted(() => ({ account: null as { equity: string } | null }))
const metaMock = vi.hoisted(() => ({
  meta: null as InstrumentMeta | null,
  error: null as string | null,
}))
vi.mock("@/hooks/use-account", () => ({ useAccount: () => accountMock }))
vi.mock("@/hooks/use-system-status", () => ({
  useInstrumentMeta: () => ({ meta: metaMock.meta, error: metaMock.error }),
}))
const createStrategyMock = vi.hoisted(() => vi.fn())

vi.mock("@/hooks/use-strategies", () => ({ createStrategy: createStrategyMock }))

function makeMeta(symbol: string, lotSize: string): InstrumentMeta {
  return {
    symbol,
    price_decimals: 2,
    size_decimals: 3,
    tick_size: d("0.1"),
    lot_size: d(lotSize),
    min_order_size: d("0.02"),
    max_leverage: 10,
  }
}

beforeEach(() => {
  accountMock.account = null
  metaMock.meta = null
  metaMock.error = null
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

  it("M-FE1: keeps typed values when account equity refreshes while open", async () => {
    const user = userEvent.setup()
    const { rerender } = render(<CreateStrategyDialog open onOpenChange={vi.fn()} existing={[]} />)

    await user.type(screen.getByLabelText(/策略 ID/), "mm-btc-1")
    expect(screen.getByLabelText(/策略 ID/)).toHaveValue("mm-btc-1")

    // 账户轮询更新 equity（旧实现依赖 account?.equity 会清空表单）。
    accountMock.account = { equity: "12345.67" }
    rerender(<CreateStrategyDialog open onOpenChange={vi.fn()} existing={[]} />)

    expect(screen.getByLabelText(/策略 ID/)).toHaveValue("mm-btc-1")
    expect(screen.getByLabelText(/策略 ID/)).not.toHaveValue("")
  })

  it("M-FE2: does not overwrite a user-edited quote_size when meta refreshes", async () => {
    const user = userEvent.setup()
    metaMock.meta = makeMeta("BTC", "0.05")
    const { rerender } = render(<CreateStrategyDialog open onOpenChange={vi.fn()} existing={[]} />)

    await user.type(screen.getByLabelText(/策略 ID/), "mm-btc-1")
    await user.click(screen.getByRole("button", { name: "下一步" }))

    // 首次加载：按 meta 建议 quote_size。
    const quoteSize = screen.getByLabelText(/单档报价数量/)
    expect(quoteSize).toHaveValue("0.05")

    // 用户编辑。
    await user.clear(quoteSize)
    await user.type(quoteSize, "0.08")
    expect(quoteSize).toHaveValue("0.08")

    // meta 轮询返回新对象（新 lot）：不得覆盖用户编辑值。
    metaMock.meta = makeMeta("BTC", "0.09")
    rerender(<CreateStrategyDialog open onOpenChange={vi.fn()} existing={[]} />)

    expect(screen.getByLabelText(/单档报价数量/)).toHaveValue("0.08")
  })
})
