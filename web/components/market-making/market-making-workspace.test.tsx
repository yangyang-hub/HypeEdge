import { cleanup, render, screen } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { DecimalString } from "@/lib/types"

const useMarketMakingMock = vi.fn()
const useRealtimeMock = vi.fn()

vi.mock("next/dynamic", () => ({ default: () => () => <div data-testid="pnl-chart" /> }))
vi.mock("next/link", () => ({ default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a> }))
vi.mock("@/components/layout/app-shell", () => ({
  AppShell: ({ children }: { children: React.ReactNode }) => <div data-testid="app-shell">{children}</div>,
}))
vi.mock("@/hooks/use-market-making", () => ({
  useMarketMaking: (...args: unknown[]) => useMarketMakingMock(...args),
  runStrategyAction: vi.fn(),
  createMarketMakerConfig: vi.fn(),
  activateMarketMakerConfig: vi.fn(),
  rollbackMarketMakerConfig: vi.fn(),
}))
vi.mock("@/hooks/use-market-making-realtime", () => ({
  useMarketMakingRealtime: (...args: unknown[]) => useRealtimeMock(...args),
}))

import { MarketMakingWorkspace } from "@/components/market-making/market-making-workspace"

const d = (value: string) => value as DecimalString
const freshness = {
  status: "fresh" as const,
  observed_at: "2026-07-11T10:00:00Z",
  age_ms: 50,
  threshold_ms: 1000,
  reason: null,
}

describe("MarketMakingWorkspace", () => {
  afterEach(cleanup)

  beforeEach(() => {
    useRealtimeMock.mockReturnValue({ overlay: null, connectionState: "connected" })
    useMarketMakingMock.mockReturnValue({
      state: {
        strategy_id: "mm-btc",
        strategy_type: "market_maker",
        symbol: "BTC",
        sub_account: "0xsub",
        environment: "testnet",
        desired_state: "running",
        actual_state: "running",
        runtime_revision: 12,
        market_revision: 44,
        config_version: 3,
        session_id: "session-1",
        session_mode: "testnet",
        quote_uptime_pct: d("0.98"),
        kill_switch_active: false,
        safety_mode: "normal",
        freshness: {
          market: freshness,
          inventory: freshness,
          clearinghouse: freshness,
          user_stream: freshness,
          reconciliation: freshness,
          action_budget: freshness,
          postgres: freshness,
        },
        alerts: [],
        observed_at: "2026-07-11T10:00:00Z",
        stale: false,
      },
      quotes: {
        strategy_id: "mm-btc",
        symbol: "BTC",
        runtime_revision: 12,
        market_revision: 44,
        fair_price: d("60000.125"),
        reservation_price: d("59999.875"),
        best_bid: d("59999.5"),
        best_ask: d("60000.5"),
        external_reference: {
          source: "binance_perp",
          symbol: "BTCUSDT",
          raw_price: d("60010"),
          adjusted_price: d("60001"),
          basis_bps: d("-1.50"),
          divergence_bps: d("0.15"),
          configured_weight: d("0.25"),
          effective_weight: d("0.20"),
          confidence: d("0.80"),
          age_ms: 25,
          quality: "healthy",
          observed_at: "2026-07-11T10:00:00Z",
        },
        slots: [],
        observed_at: "2026-07-11T10:00:00Z",
        stale: false,
      },
      inventory: {
        strategy_id: "mm-btc",
        symbol: "BTC",
        runtime_revision: 12,
        market_revision: 44,
        position_size: d("0.001"),
        inventory_notional: d("60"),
        soft_limit_notional: d("500"),
        hard_limit_notional: d("750"),
        emergency_limit_notional: d("900"),
        inventory_utilization: d("0.12"),
        inventory_shift_bps: d("0.5"),
        margin_used: d("60"),
        available_margin: d("940"),
        liquidation_distance_pct: d("0.8"),
        funding_carry: d("-0.01"),
        reduction_mode: "none",
        observed_at: "2026-07-11T10:00:00Z",
        stale: false,
      },
      performance: {
        strategy_id: "mm-btc",
        accounting: {
          realized_trading_pnl: d("10"),
          unrealized_inventory_change: d("2"),
          net_fees_and_rebates: d("1"),
          funding_pnl: d("-0.5"),
          paid_action_cost: d("0.25"),
          accounting_net_pnl: d("12.25"),
          ledger_reconciled: true,
        },
        execution_quality: {
          quoted_spread_bps: d("2"),
          captured_spread_bps: d("1.5"),
          maker_ratio: d("0.95"),
          markout_1s: d("0.1"),
          markout_5s: d("0.2"),
          markout_30s: d("-0.1"),
          fill_count: 10,
          unknown_count: 0,
          reject_count: 0,
          actions_per_fill: d("1.2"),
        },
        inventory_episodes: [],
        source: "mixed",
        as_of: "2026-07-11T10:00:00Z",
        stale: false,
      },
      budget: {
        strategy_id: "mm-btc",
        mode: "normal",
        remote_cap: d("10000"),
        remote_used: d("100"),
        remote_remaining: d("9900"),
        shadow_remaining: d("9899"),
        emergency_reserve: d("10"),
        cancel_headroom: d("900"),
        ip_weight_remaining: d("1100"),
        burn_rate_1h: d("10"),
        burn_rate_6h: d("50"),
        burn_rate_24h: d("200"),
        earned_rate_24h: d("250"),
        usdc_per_action: d("1.5"),
        actions_per_fill: d("1.2"),
        runway_hours: d("72"),
        revision: 3,
        observed_at: "2026-07-11T10:00:00Z",
        stale: false,
      },
      configs: [],
      events: [],
      reliableConnected: true,
      isLoading: false,
      error: undefined,
      resync: vi.fn().mockResolvedValue(undefined),
    })
  })

  it("keeps accounting PnL and markout in separate sections", () => {
    render(<MarketMakingWorkspace strategyId="mm-btc" />)

    expect(screen.getByRole("heading", { name: "Accounting PnL" })).toBeInTheDocument()
    expect(screen.getByText("权威口径来自 Postgres ledger。Markout 不计入 Accounting Net PnL，避免重复计算。")).toBeInTheDocument()
    expect(screen.getByRole("heading", { name: "Execution Quality / Markout" })).toBeInTheDocument()
    expect(screen.getByText("Markout 5s")).toBeInTheDocument()
  })

  it("shows reliable and display-only connection semantics", () => {
    render(<MarketMakingWorkspace strategyId="mm-btc" />)

    expect(screen.getByText("SSE 可靠流已连接")).toBeInTheDocument()
    expect(screen.getByText("WS connected")).toBeInTheDocument()
    expect(screen.getByText(/WebSocket 高频数据仅用于显示/)).toBeInTheDocument()
  })

  it("labels external prices as reference-only and shows quality inputs", () => {
    render(<MarketMakingWorkspace strategyId="mm-btc" />)

    expect(screen.getAllByText(/Reference only/).length).toBeGreaterThan(0)
    expect(screen.getByText("Basis-adjusted")).toBeInTheDocument()
    expect(screen.getByText("HL divergence")).toBeInTheDocument()
    expect(screen.getAllByText("healthy").length).toBeGreaterThan(0)
    expect(screen.getAllByText("25ms").length).toBeGreaterThan(0)
  })

  it("remains compatible when the external reference field is absent", () => {
    const snapshot = useMarketMakingMock()
    delete snapshot.quotes.external_reference
    useMarketMakingMock.mockReturnValue(snapshot)

    render(<MarketMakingWorkspace strategyId="mm-btc" />)

    expect(screen.getByText(/External reference 未启用/)).toBeInTheDocument()
  })
})
