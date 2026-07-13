import { afterEach, describe, expect, it, vi } from "vitest"
import { createIdempotencyKey, fetcher, poster } from "@/lib/api"

const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

describe("API client", () => {
  afterEach(() => vi.restoreAllMocks())

  it("parses successful wrapped responses", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true, data: { value: 1 } }))))
    await expect(fetcher<{ value: number }>("/api/v1/test")).resolves.toEqual({ value: 1 })
  })

  it("surfaces stable problem codes", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(
      JSON.stringify({ detail: "Denied", code: "RISK_DENIED", retryable: false }),
      { status: 409 },
    )))
    await expect(fetcher("/api/v1/test")).rejects.toMatchObject({ code: "RISK_DENIED", status: 409 })
  })

  it("adds an idempotency key to commands", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true, data: {} })))
    vi.stubGlobal("fetch", fetchMock)
    await poster("/api/v1/orders/test/cancel", {})
    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(new Headers(init.headers).get("Idempotency-Key")).toBeTruthy()
  })

  it("reuses a caller-provided idempotency key across retries", async () => {
    const fetchMock = vi.fn().mockImplementation(
      async () => new Response(JSON.stringify({ ok: true, data: {} })),
    )
    vi.stubGlobal("fetch", fetchMock)

    await poster("/api/v1/orders", { symbol: "BTC" }, { idempotencyKey: "place-btc-lifecycle-1" })
    await poster("/api/v1/orders", { symbol: "BTC" }, { idempotencyKey: "place-btc-lifecycle-1" })

    for (const call of fetchMock.mock.calls) {
      const init = call[1] as RequestInit
      expect(new Headers(init.headers).get("Idempotency-Key")).toBe("place-btc-lifecycle-1")
    }
  })

  it("fences configuration activation with If-Match", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true, data: {} })))
    vi.stubGlobal("fetch", fetchMock)

    await poster("/api/v1/strategies/mm/config-versions/3/activate", {}, { ifMatch: 17 })

    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(new Headers(init.headers).get("If-Match")).toBe('"17"')
    expect(new Headers(init.headers).get("Idempotency-Key")).toBeTruthy()
  })

  it("creates a UUID even when crypto.randomUUID is unavailable", () => {
    const original = globalThis.crypto
    vi.stubGlobal("crypto", {
      getRandomValues: (arr: Uint8Array) => {
        for (let i = 0; i < arr.length; i++) arr[i] = i
        return arr
      },
    })
    try {
      expect(createIdempotencyKey()).toMatch(UUID_V4)
    } finally {
      vi.stubGlobal("crypto", original)
    }
  })
})
