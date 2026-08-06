import { afterEach, describe, expect, it, vi } from "vitest"
import { archiveStrategy } from "@/hooks/use-strategies"
import type { StrategyInstance } from "@/lib/types"

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

afterEach(() => {
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

describe("archiveStrategy", () => {
  it("posts a revision-fenced archive command", async () => {
    const archived = {
      ...stoppedStrategy,
      revision: 5,
      archived_at: "2026-08-04T00:00:00Z",
    }
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ok: true, data: archived }), {
        headers: { "Content-Type": "application/json" },
      }),
    )
    vi.stubGlobal("fetch", fetchMock)

    await expect(archiveStrategy(stoppedStrategy)).resolves.toMatchObject({
      strategy_id: stoppedStrategy.strategy_id,
      archived_at: archived.archived_at,
    })

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    const headers = new Headers(init.headers)
    expect(url).toBe("/api/v1/strategies/funding-hype-testnet/archive")
    expect(init.method).toBe("POST")
    expect(headers.get("If-Match")).toBe('"4"')
    expect(headers.get("Idempotency-Key")).toBeTruthy()
  })
})
