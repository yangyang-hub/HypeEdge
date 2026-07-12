import { NextRequest } from "next/server"
import { afterEach, describe, expect, it, vi } from "vitest"
import { middleware } from "@/middleware"

describe("dashboard authentication middleware", () => {
  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it("fails closed on an incomplete role credential", () => {
    vi.stubEnv("HYPEEDGE_DASHBOARD_OPERATOR_USERNAME", "operator")
    const response = middleware(new NextRequest("http://dashboard.local/"))
    expect(response.status).toBe(503)
  })

  it("rate limits repeated Basic authentication failures", () => {
    vi.stubEnv("HYPEEDGE_DASHBOARD_VIEWER_USERNAME", "viewer")
    vi.stubEnv("HYPEEDGE_DASHBOARD_VIEWER_PASSWORD", "viewer-password")
    vi.stubEnv("HYPEEDGE_VIEWER_API_TOKEN", "v".repeat(32))
    vi.stubEnv("HYPEEDGE_BASIC_AUTH_FAILURES_PER_MINUTE", "1")
    const request = () => new NextRequest("http://dashboard.local/", { headers: { "X-Real-IP": "192.0.2.44" } })

    expect(middleware(request()).status).toBe(401)
    expect(middleware(request()).status).toBe(429)
  })
})
