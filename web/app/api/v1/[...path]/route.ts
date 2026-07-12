import type { NextRequest } from "next/server"
import { timingSafeEqual } from "node:crypto"

export const dynamic = "force-dynamic"
export const runtime = "nodejs"

interface RouteContext {
  params: Promise<{ path: string[] }>
}

type DashboardRole = "viewer" | "operator" | "admin"

interface DashboardPrincipal {
  role: DashboardRole
  backendToken: string
}

interface DashboardCredential extends DashboardPrincipal {
  username: string
  password: string
}

const ROLE_RANK: Record<DashboardRole, number> = { viewer: 10, operator: 20, admin: 30 }

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "content-length",
  "host",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
])

function constantTimeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left)
  const rightBuffer = Buffer.from(right)
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer)
}

function configuredCredentials(): DashboardCredential[] | Response {
  const credentials: DashboardCredential[] = []
  const definitions: Array<[DashboardRole, string | undefined, string | undefined, string | undefined]> = [
    ["viewer", process.env.HYPEEDGE_DASHBOARD_VIEWER_USERNAME, process.env.HYPEEDGE_DASHBOARD_VIEWER_PASSWORD, process.env.HYPEEDGE_VIEWER_API_TOKEN],
    ["operator", process.env.HYPEEDGE_DASHBOARD_OPERATOR_USERNAME, process.env.HYPEEDGE_DASHBOARD_OPERATOR_PASSWORD, process.env.HYPEEDGE_OPERATOR_API_TOKEN],
    ["admin", process.env.HYPEEDGE_DASHBOARD_ADMIN_USERNAME, process.env.HYPEEDGE_DASHBOARD_ADMIN_PASSWORD, process.env.HYPEEDGE_ADMIN_API_TOKEN],
    // Backwards compatibility is intentionally read-only: legacy credentials
    // can never silently inherit an admin backend token.
    ["viewer", process.env.HYPEEDGE_DASHBOARD_USERNAME, process.env.HYPEEDGE_DASHBOARD_PASSWORD, process.env.HYPEEDGE_API_TOKEN],
  ]
  for (const [role, username, password, backendToken] of definitions) {
    const configured = [username, password, backendToken].filter(Boolean).length
    if (configured !== 0 && configured !== 3) {
      return Response.json({ code: "DASHBOARD_AUTH_MISCONFIGURED", detail: `${role} dashboard credentials are incomplete` }, { status: 503 })
    }
    if (username && password && backendToken) {
      if (backendToken.length < 32) {
        return Response.json({ code: "DASHBOARD_AUTH_MISCONFIGURED", detail: `${role} backend token is too short` }, { status: 503 })
      }
      credentials.push({ role, username, password, backendToken })
    }
  }
  const usernames = credentials.map((credential) => credential.username)
  if (new Set(usernames).size !== usernames.length) {
    return Response.json({ code: "DASHBOARD_AUTH_MISCONFIGURED", detail: "Dashboard usernames must be unique" }, { status: 503 })
  }
  return credentials
}

function dashboardPrincipal(request: NextRequest): DashboardPrincipal | Response {
  const configured = configuredCredentials()
  if (configured instanceof Response) return configured
  if (configured.length === 0) {
    return { role: "viewer", backendToken: "" }
  }
  const authorization = request.headers.get("authorization") ?? ""
  const [scheme, encoded = ""] = authorization.split(" ", 2)
  let supplied = ""
  try {
    supplied = Buffer.from(encoded, "base64").toString("utf8")
  } catch {
    supplied = ""
  }
  let principal: DashboardPrincipal | null = null
  for (const credential of configured) {
    const matched = scheme.toLowerCase() === "basic" && constantTimeEqual(supplied, `${credential.username}:${credential.password}`)
    if (matched && (principal === null || ROLE_RANK[credential.role] > ROLE_RANK[principal.role])) {
      principal = { role: credential.role, backendToken: credential.backendToken }
    }
  }
  if (principal === null) {
    return Response.json(
      { code: "AUTHENTICATION_REQUIRED", detail: "Valid dashboard credentials are required" },
      { status: 401, headers: { "WWW-Authenticate": 'Basic realm="HypeEdge", charset="UTF-8"' } },
    )
  }
  return principal
}

function csrfFailure(request: NextRequest): Response | null {
  if (request.method === "GET" || request.method === "HEAD" || request.method === "OPTIONS") return null
  if (request.headers.get("sec-fetch-site") === "cross-site") {
    return Response.json({ code: "CROSS_SITE_REQUEST_REJECTED", detail: "Cross-site commands are not allowed" }, { status: 403 })
  }
  const origin = request.headers.get("origin")
  if (origin) {
    try {
      if (new URL(origin).host !== request.nextUrl.host) {
        return Response.json({ code: "CROSS_SITE_REQUEST_REJECTED", detail: "Cross-site commands are not allowed" }, { status: 403 })
      }
    } catch {
      return Response.json({ code: "CROSS_SITE_REQUEST_REJECTED", detail: "Cross-site commands are not allowed" }, { status: 403 })
    }
  }
  return null
}

function backendBaseUrl(): URL {
  const configured = process.env.HYPEEDGE_BACKEND_URL ?? "http://127.0.0.1:37001"
  const url = new URL(configured)
  if (!url.protocol.startsWith("http")) throw new Error("HYPEEDGE_BACKEND_URL must use HTTP or HTTPS")
  return url
}

function upstreamHeaders(request: NextRequest, backendToken: string): Headers {
  const headers = new Headers()
  for (const [name, value] of request.headers) {
    const lowerName = name.toLowerCase()
    if (!HOP_BY_HOP_HEADERS.has(lowerName) && lowerName !== "authorization" && lowerName !== "cookie") {
      headers.set(name, value)
    }
  }
  if (backendToken) headers.set("Authorization", `Bearer ${backendToken}`)
  headers.set("X-Forwarded-Host", request.nextUrl.host)
  headers.set("X-Forwarded-Proto", request.nextUrl.protocol.replace(":", ""))
  // So backend rate limits are per browser, not collapsed onto 127.0.0.1.
  const forwardedFor = request.headers.get("x-forwarded-for")
  const clientIp = request.headers.get("x-real-ip") ?? request.ip
  if (forwardedFor) {
    headers.set("X-Forwarded-For", forwardedFor)
  } else if (clientIp) {
    headers.set("X-Forwarded-For", clientIp)
  }
  return headers
}

async function proxy(request: NextRequest, context: RouteContext): Promise<Response> {
  const principal = dashboardPrincipal(request)
  if (principal instanceof Response) return principal
  const crossSiteFailure = csrfFailure(request)
  if (crossSiteFailure) return crossSiteFailure

  const { path } = await context.params
  const requiredRole: DashboardRole = request.method === "GET" || request.method === "HEAD"
    ? "viewer"
    : path.join("/") === "kill-switch" ? "admin" : "operator"
  if (ROLE_RANK[principal.role] < ROLE_RANK[requiredRole]) {
    return Response.json(
      { code: "INSUFFICIENT_ROLE", detail: `The ${requiredRole} dashboard role is required` },
      { status: 403 },
    )
  }
  const upstream = new URL(`/api/v1/${path.map(encodeURIComponent).join("/")}`, backendBaseUrl())
  upstream.search = request.nextUrl.search

  const hasBody = request.method !== "GET" && request.method !== "HEAD"
  const isEventStream = path.join("/") === "events"
  try {
    const response = await fetch(upstream, {
      method: request.method,
      headers: upstreamHeaders(request, principal.backendToken),
      body: hasBody ? request.body : undefined,
      cache: "no-store",
      redirect: "manual",
      duplex: hasBody ? "half" : undefined,
    } as RequestInit & { duplex?: "half" })

    const headers = new Headers()
    for (const [name, value] of response.headers) {
      if (!HOP_BY_HOP_HEADERS.has(name.toLowerCase())) headers.set(name, value)
    }
    headers.set("Cache-Control", response.headers.get("Cache-Control") ?? "no-store")
    if (isEventStream) {
      // Backend yields an immediate ": connected" comment so Next does not buffer
      // the SSE response until the first durable event / heartbeat.
      headers.set("Cache-Control", "no-cache, no-transform")
      headers.set("X-Accel-Buffering", "no")
    }
    return new Response(response.body, { status: response.status, headers })
  } catch {
    return Response.json(
      {
        type: "https://hypeedge.local/problems/backend-unavailable",
        title: "BACKEND_UNAVAILABLE",
        status: 502,
        code: "BACKEND_UNAVAILABLE",
        detail: "HypeEdge backend is unavailable",
        retryable: true,
        context: {},
      },
      { status: 502, headers: { "Content-Type": "application/problem+json" } },
    )
  }
}

export const GET = proxy
export const POST = proxy
export const PUT = proxy
export const PATCH = proxy
export const DELETE = proxy
