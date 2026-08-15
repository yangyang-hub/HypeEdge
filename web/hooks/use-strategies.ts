"use client"

import useSWR from "swr"
import { fetcher, poster, type CommandOptions } from "@/lib/api"
import type {
  DecimalString,
  StrategyCreateRequest,
  StrategyDesiredState,
  StrategyInstance,
  StrategyLifecycleState,
  TrendFollowStrategyInstance,
} from "@/lib/types"
import { SWR_REFRESH_INTERVAL } from "@/lib/constants"

type StrategyListItem = Partial<StrategyInstance> & {
  strategy_id: string
  status?: string
  metadata?: Record<string, string>
  desired_config_version?: number | null
  parameters?: TrendFollowStrategyInstance["parameters"]
  legacy_kind?: string
}

const DEFAULT_TREND_PARAMETERS: TrendFollowStrategyInstance["parameters"] = {
  fast_ema_period: 12,
  slow_ema_period: 26,
  atr_period: 14,
  atr_stop_multiplier: "2" as DecimalString,
  max_position_pct: "0.15" as DecimalString,
}

function normalizeLifecycle(value: unknown, fallback: StrategyLifecycleState = "stopped"): StrategyLifecycleState {
  if (typeof value !== "string" || !value) return fallback
  return value.toLowerCase() as StrategyLifecycleState
}

export function normalizeStrategy(raw: StrategyListItem): StrategyInstance {
  const actual_state = normalizeLifecycle(raw.actual_state ?? raw.status)
  const desired_state = normalizeLifecycle(
    raw.desired_state,
    actual_state === "running" || actual_state === "shadow" || actual_state === "paused"
      ? (actual_state as StrategyDesiredState)
      : "stopped",
  ) as StrategyDesiredState

  const desiredConfig =
    raw.desired_config_version_id ??
    (typeof raw.desired_config_version === "number" ? raw.desired_config_version : null)

  const base = {
    strategy_id: raw.strategy_id,
    symbol: raw.symbol ?? "—",
    sub_account: raw.sub_account ?? null,
    desired_state,
    actual_state,
    desired_config_version_id: desiredConfig,
    effective_config_version_id: raw.effective_config_version_id ?? null,
    revision: raw.revision ?? 0,
    archived_at: raw.archived_at ?? null,
    // L-FE5: 后端可能不返回 created_at/updated_at；不再 fallback 到
    // new Date(0)（显示 1970），保留 null 由 formatDateTime 渲染为 "—"。
    created_at: raw.created_at ?? null,
    updated_at: raw.updated_at ?? null,
    metadata: raw.metadata,
  }

  if (raw.strategy_type === "market_maker") {
    return {
      ...base,
      strategy_type: "market_maker",
      session_mode: null,
    }
  }
  if (raw.strategy_type === "trend_follow") {
    return {
      ...base,
      strategy_type: "trend_follow",
      parameters: raw.parameters ?? DEFAULT_TREND_PARAMETERS,
    }
  }
  if (raw.strategy_type === "funding_arb") {
    return {
      ...base,
      strategy_type: "funding_arb",
    }
  }
  return {
    ...base,
    strategy_type: "legacy",
    legacy_kind: raw.legacy_kind ?? "unknown",
  }
}

async function strategiesFetcher(url: string): Promise<StrategyInstance[]> {
  const data = await fetcher<StrategyListItem[]>(url)
  return (data ?? []).map(normalizeStrategy)
}

export function useStrategies() {
  const { data, error, isLoading, mutate } = useSWR<StrategyInstance[]>(
    "/api/v1/strategies",
    strategiesFetcher,
    { refreshInterval: SWR_REFRESH_INTERVAL },
  )
  return { strategies: data ?? [], error, isLoading, refresh: mutate }
}

export async function createStrategy(
  body: StrategyCreateRequest,
  options: CommandOptions = {},
): Promise<StrategyInstance> {
  const data = await poster<StrategyListItem>("/api/v1/strategies", body, options)
  return normalizeStrategy(data)
}

/** Start strategy via control-plane actions; legacy singleton uses /start. */
export async function startStrategy(
  strategy: Pick<StrategyInstance, "strategy_id" | "strategy_type" | "revision">,
  target: StrategyDesiredState = "shadow",
) {
  if (strategy.strategy_type === "legacy") {
    return poster(`/api/v1/strategies/${encodeURIComponent(strategy.strategy_id)}/start`, {})
  }
  return poster(
    `/api/v1/strategies/${encodeURIComponent(strategy.strategy_id)}/actions/start`,
    { target },
    { ifMatch: strategy.revision },
  )
}

export async function stopStrategy(
  strategy: Pick<StrategyInstance, "strategy_id" | "strategy_type" | "revision">,
) {
  if (strategy.strategy_type === "legacy") {
    return poster(`/api/v1/strategies/${encodeURIComponent(strategy.strategy_id)}/stop`, {})
  }
  return poster(
    `/api/v1/strategies/${encodeURIComponent(strategy.strategy_id)}/actions/stop`,
    {},
    { ifMatch: strategy.revision },
  )
}

/** Archive a fully stopped managed strategy while retaining its trading and audit history. */
export async function archiveStrategy(
  strategy: Pick<StrategyInstance, "strategy_id" | "strategy_type" | "revision">,
): Promise<StrategyInstance> {
  if (strategy.strategy_type === "legacy") {
    throw new Error("兼容策略不能从统一控制面删除")
  }
  const data = await poster<StrategyListItem>(
    `/api/v1/strategies/${encodeURIComponent(strategy.strategy_id)}/archive`,
    {},
    { ifMatch: strategy.revision },
  )
  return normalizeStrategy(data)
}
