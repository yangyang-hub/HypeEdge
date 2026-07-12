import type { NextConfig } from "next"

function websocketOrigin(value: string | undefined): string | null {
  if (!value) return null
  try {
    const origin = new URL(value).origin
    return origin.startsWith("ws://") || origin.startsWith("wss://") ? origin : null
  } catch {
    return null
  }
}

const websocketOrigins = [
  websocketOrigin(process.env.NEXT_PUBLIC_HYPEEDGE_MARKET_WS_URL),
  websocketOrigin(process.env.NEXT_PUBLIC_HYPEEDGE_MM_WS_URL),
].filter((value): value is string => value !== null)

const contentSecurityPolicy = [
  "default-src 'self'",
  "base-uri 'self'",
  `connect-src 'self' ${[...new Set(websocketOrigins)].join(" ")}`.trim(),
  "font-src 'self'",
  "form-action 'self'",
  "frame-ancestors 'none'",
  "img-src 'self' data:",
  "object-src 'none'",
  "script-src 'self' 'unsafe-inline'",
  "style-src 'self' 'unsafe-inline'",
].join("; ")

const nextConfig: NextConfig = {
  poweredByHeader: false,
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [
          { key: "Content-Security-Policy", value: contentSecurityPolicy },
          { key: "Permissions-Policy", value: "camera=(), microphone=(), geolocation=()" },
          { key: "Referrer-Policy", value: "no-referrer" },
          { key: "X-Content-Type-Options", value: "nosniff" },
          { key: "X-Frame-Options", value: "DENY" },
        ],
      },
    ]
  },
}

export default nextConfig
