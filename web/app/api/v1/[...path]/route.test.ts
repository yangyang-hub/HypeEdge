import { NextRequest } from "next/server"
import { afterEach, describe, expect, it, vi } from "vitest"
import { GET, POST } from "@/app/api/v1/[...path]/route"

describe("HypeEdge backend proxy", () => {
  afterEach(() => {
    vi.unstubAllEnvs()
    vi.restoreAllMocks()
  })

  it("injects the server-side bearer token without trusting browser authorization", async () => {
    vi.stubEnv("HYPEEDGE_BACKEND_URL", "http://backend.internal:8080")
    vi.stubEnv("HYPEEDGE_API_TOKEN", "server-secret".repeat(3))
    vi.stubEnv("HYPEEDGE_DASHBOARD_USERNAME", "operator")
    vi.stubEnv("HYPEEDGE_DASHBOARD_PASSWORD", "dashboard-secret")
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ok: true, data: {} }), {
        headers: { "Content-Type": "application/json" },
      }),
    )
    vi.stubGlobal("fetch", fetchMock)

    await GET(
      new NextRequest("http://dashboard.local/api/v1/system/status", {
        headers: { Authorization: `Basic ${Buffer.from("operator:dashboard-secret").toString("base64")}` },
      }),
      { params: Promise.resolve({ path: ["system", "status"] }) },
    )

    const [url, init] = fetchMock.mock.calls[0] as [URL, RequestInit]
    expect(url.toString()).toBe("http://backend.internal:8080/api/v1/system/status")
    expect(new Headers(init.headers).get("Authorization")).toBe(`Bearer ${"server-secret".repeat(3)}`)
  })

  it("does not expose a configured backend token through an unauthenticated proxy", async () => {
    vi.stubEnv("HYPEEDGE_API_TOKEN", "server-secret".repeat(3))
    const fetchMock = vi.fn()
    vi.stubGlobal("fetch", fetchMock)

    const response = await GET(
      new NextRequest("http://dashboard.local/api/v1/account"),
      { params: Promise.resolve({ path: ["account"] }) },
    )

    expect(response.status).toBe(503)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it("rejects cross-site commands before forwarding them", async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal("fetch", fetchMock)
    const request = new NextRequest("http://dashboard.local/api/v1/orders", {
      method: "POST",
      headers: { Origin: "https://attacker.example", "Content-Type": "application/json" },
      body: "{}",
    })

    const response = await POST(request, { params: Promise.resolve({ path: ["orders"] }) })

    expect(response.status).toBe(403)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it("rejects malformed origins without contacting the backend", async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal("fetch", fetchMock)
    const request = new NextRequest("http://dashboard.local/api/v1/orders", {
      method: "POST",
      headers: { Origin: "not a valid origin", "Content-Type": "application/json" },
      body: "{}",
    })

    const response = await POST(request, { params: Promise.resolve({ path: ["orders"] }) })

    expect(response.status).toBe(403)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it("forwards command bodies and idempotency keys", async () => {
    vi.stubEnv("HYPEEDGE_BACKEND_URL", "http://127.0.0.1:8080")
    vi.stubEnv("HYPEEDGE_DASHBOARD_OPERATOR_USERNAME", "operator")
    vi.stubEnv("HYPEEDGE_DASHBOARD_OPERATOR_PASSWORD", "operator-password")
    vi.stubEnv("HYPEEDGE_OPERATOR_API_TOKEN", "o".repeat(32))
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 202 }))
    vi.stubGlobal("fetch", fetchMock)
    const request = new NextRequest("http://dashboard.local/api/v1/orders", {
      method: "POST",
      headers: {
        Authorization: `Basic ${Buffer.from("operator:operator-password").toString("base64")}`,
        "Content-Type": "application/json",
        "Idempotency-Key": "command-1",
      },
      body: JSON.stringify({ symbol: "BTC" }),
    })

    const response = await POST(request, { params: Promise.resolve({ path: ["orders"] }) })
    const [, init] = fetchMock.mock.calls[0] as [URL, RequestInit]
    expect(response.status).toBe(202)
    expect(new Headers(init.headers).get("Idempotency-Key")).toBe("command-1")
    expect(init.body).toBeTruthy()
  })

  it("keeps legacy dashboard credentials read-only even when a backend token exists", async () => {
    vi.stubEnv("HYPEEDGE_DASHBOARD_USERNAME", "legacy")
    vi.stubEnv("HYPEEDGE_DASHBOARD_PASSWORD", "legacy-password")
    vi.stubEnv("HYPEEDGE_API_TOKEN", "a".repeat(32))
    const fetchMock = vi.fn()
    vi.stubGlobal("fetch", fetchMock)

    const response = await POST(
      new NextRequest("http://dashboard.local/api/v1/kill-switch", {
        method: "POST",
        headers: {
          Authorization: `Basic ${Buffer.from("legacy:legacy-password").toString("base64")}`,
          Origin: "http://dashboard.local",
          "Idempotency-Key": "legacy-kill",
        },
        body: JSON.stringify({ action: "trigger" }),
      }),
      { params: Promise.resolve({ path: ["kill-switch"] }) },
    )

    expect(response.status).toBe(403)
    expect(fetchMock).not.toHaveBeenCalled()
  })
})
