import { NextResponse } from "next/server"
import type { NextRequest } from "next/server"

const failedAttempts = new Map<string, number[]>()
const AUTH_WINDOW_MS = 60_000

function clientKey(request: NextRequest): string {
  return request.headers.get("x-real-ip") ?? request.headers.get("x-forwarded-for")?.split(",", 1)[0]?.trim() ?? "unknown"
}

function authenticationAllowed(request: NextRequest): boolean {
  const now = Date.now()
  const key = clientKey(request)
  if (!failedAttempts.has(key) && failedAttempts.size >= 10_000) {
    failedAttempts.delete(failedAttempts.keys().next().value ?? "")
  }
  const attempts = (failedAttempts.get(key) ?? []).filter((timestamp) => timestamp > now - AUTH_WINDOW_MS)
  const limit = Number.parseInt(process.env.HYPEEDGE_BASIC_AUTH_FAILURES_PER_MINUTE ?? "10", 10)
  if (attempts.length >= (Number.isFinite(limit) && limit > 0 ? limit : 10)) {
    failedAttempts.set(key, attempts)
    return false
  }
  attempts.push(now)
  failedAttempts.set(key, attempts)
  return true
}

function equalCredential(left: string, right: string): boolean {
  let mismatch = left.length ^ right.length
  const length = Math.max(left.length, right.length)
  for (let index = 0; index < length; index += 1) {
    mismatch |= (left.charCodeAt(index) || 0) ^ (right.charCodeAt(index) || 0)
  }
  return mismatch === 0
}

export function middleware(request: NextRequest) {
  // Default open for intranet personal use. Opt in with HYPEEDGE_DASHBOARD_AUTH=on.
  const authFlag = (process.env.HYPEEDGE_DASHBOARD_AUTH ?? "").trim().toLowerCase()
  if (authFlag !== "1" && authFlag !== "true" && authFlag !== "on") {
    return NextResponse.next()
  }

  const definitions = [
    [process.env.HYPEEDGE_DASHBOARD_VIEWER_USERNAME, process.env.HYPEEDGE_DASHBOARD_VIEWER_PASSWORD, process.env.HYPEEDGE_VIEWER_API_TOKEN],
    [process.env.HYPEEDGE_DASHBOARD_OPERATOR_USERNAME, process.env.HYPEEDGE_DASHBOARD_OPERATOR_PASSWORD, process.env.HYPEEDGE_OPERATOR_API_TOKEN],
    [process.env.HYPEEDGE_DASHBOARD_ADMIN_USERNAME, process.env.HYPEEDGE_DASHBOARD_ADMIN_PASSWORD, process.env.HYPEEDGE_ADMIN_API_TOKEN],
    [process.env.HYPEEDGE_DASHBOARD_USERNAME, process.env.HYPEEDGE_DASHBOARD_PASSWORD, process.env.HYPEEDGE_API_TOKEN],
  ] as const
  for (const definition of definitions) {
    const count = definition.filter(Boolean).length
    if (count !== 0 && count !== 3) {
      return new NextResponse("Dashboard authentication is incomplete", { status: 503 })
    }
  }
  const credentials = definitions.filter(
    (definition): definition is readonly [string, string, string] => definition.every(Boolean),
  )
  if (credentials.length === 0) {
    return NextResponse.next()
  }

  const authorization = request.headers.get("authorization") ?? ""
  const [scheme, encoded = ""] = authorization.split(" ", 2)
  let supplied = ""
  try {
    supplied = atob(encoded)
  } catch {
    supplied = ""
  }
  if (scheme.toLowerCase() === "basic" && credentials.some(([username, password]) => equalCredential(supplied, `${username}:${password}`))) {
    return NextResponse.next()
  }
  if (!authenticationAllowed(request)) {
    return new NextResponse("Too many failed authentication attempts", { status: 429, headers: { "Retry-After": "60" } })
  }
  return new NextResponse("Authentication required", {
    status: 401,
    headers: { "WWW-Authenticate": 'Basic realm="HypeEdge", charset="UTF-8"' },
  })
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
}
