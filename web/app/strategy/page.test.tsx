import type { ReactNode } from "react"
import { cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import StrategyPage from "@/app/strategy/page"
import type { StrategyInstance } from "@/lib/types"

const hookMocks = vi.hoisted(() => ({
  strategies: [] as StrategyInstance[],
  refresh: vi.fn(),
  archiveStrategy: vi.fn(),
  startStrategy: vi.fn(),
  stopStrategy: vi.fn(),
}))

vi.mock("@/hooks/use-strategies", () => ({
  useStrategies: () => ({
    strategies: hookMocks.strategies,
    refresh: hookMocks.refresh,
    error: null,
    isLoading: false,
  }),
  archiveStrategy: hookMocks.archiveStrategy,
  startStrategy: hookMocks.startStrategy,
  stopStrategy: hookMocks.stopStrategy,
}))

vi.mock("@/components/layout/app-shell", () => ({
  AppShell: ({ children }: { children: ReactNode }) => <>{children}</>,
}))

vi.mock("@/components/strategy/create-strategy-dialog", () => ({
  CreateStrategyDialog: () => null,
}))

const stoppedStrategy: StrategyInstance = {
  strategy_id: "funding-hype-testnet",
  strategy_type: "funding_arb",
  symbol: "AUTO",
  sub_account: "0xabc",
  desired_state: "stopped",
  actual_state: "stopped",
  desired_config_version_id: 1,
  effective_config_version_id: null,
  revision: 4,
  archived_at: null,
  created_at: "2026-08-01T00:00:00Z",
  updated_at: "2026-08-01T00:00:00Z",
}

beforeEach(() => {
  hookMocks.strategies = [stoppedStrategy]
  hookMocks.refresh.mockResolvedValue(undefined)
  hookMocks.archiveStrategy.mockResolvedValue({
    ...stoppedStrategy,
    revision: 5,
    archived_at: "2026-08-04T00:00:00Z",
  })
  hookMocks.startStrategy.mockResolvedValue(undefined)
  hookMocks.stopStrategy.mockResolvedValue(undefined)
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe("StrategyPage", () => {
  it("confirms and archives a stopped strategy while explaining history retention", async () => {
    const user = userEvent.setup()
    render(<StrategyPage />)

    await user.click(screen.getByRole("button", { name: "删除" }))

    expect(screen.getByRole("heading", { name: "删除策略" })).toBeInTheDocument()
    expect(screen.getByText(/订单、成交与审计历史仍会保留/)).toBeInTheDocument()

    await user.click(screen.getByRole("button", { name: "确认删除" }))

    await waitFor(() => expect(hookMocks.archiveStrategy).toHaveBeenCalledWith(stoppedStrategy))
    expect(hookMocks.refresh).toHaveBeenCalled()
  })

  it("offers stop instead of start for a faulted strategy before deletion", async () => {
    const user = userEvent.setup()
    const faultedStrategy: StrategyInstance = {
      ...stoppedStrategy,
      strategy_id: "t1",
      desired_state: "running",
      actual_state: "faulted",
      revision: 7,
    }
    hookMocks.strategies = [faultedStrategy]
    render(<StrategyPage />)

    expect(screen.queryByRole("button", { name: "启动" })).not.toBeInTheDocument()
    expect(screen.getByRole("button", { name: "删除" })).toBeDisabled()
    expect(screen.getByRole("button", { name: "删除" })).toHaveAttribute("title", "请先停止策略再删除")

    await user.click(screen.getByRole("button", { name: "停止" }))

    await waitFor(() => expect(hookMocks.stopStrategy).toHaveBeenCalledWith(faultedStrategy))
  })

  it("explains when a running strategy was paused by the system safety layer", () => {
    hookMocks.strategies = [
      {
        ...stoppedStrategy,
        desired_state: "running",
        actual_state: "paused",
        runtime_reason: "system_safety_pause:authenticated_stream_disconnected",
      },
    ]

    render(<StrategyPage />)

    expect(screen.getByText("系统安全暂停：认证数据流已断开")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "停止" })).toBeInTheDocument()
  })
})
