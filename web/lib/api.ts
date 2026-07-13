// API client — unified fetch wrapper with error handling

import type { ApiProblem, ApiResponse } from "./types"

const BASE_URL = ""

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code: string,
    readonly retryable: boolean,
  ) {
    super(message)
    this.name = "ApiError"
  }
}

async function readResponse<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const problem = (await res.json().catch(() => null)) as ApiProblem | null
    throw new ApiError(
      problem?.detail ?? `API request failed (${res.status})`,
      res.status,
      problem?.code ?? "HTTP_ERROR",
      problem?.retryable ?? false,
    )
  }
  const json = (await res.json()) as ApiResponse<T>
  if (!json.ok) throw new ApiError(json.error ?? "Unknown API error", 400, "LEGACY_API_ERROR", false)
  return json.data
}

export async function fetcher<T>(url: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${url}`)
  return readResponse<T>(res)
}

export interface CommandOptions {
  idempotencyKey?: string
  ifMatch?: number
}

/** UUID v4; falls back when `crypto.randomUUID` is missing (non-secure HTTP / LAN). */
export function createIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID()
  }
  const bytes = new Uint8Array(16)
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    crypto.getRandomValues(bytes)
  } else {
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = Math.floor(Math.random() * 256)
    }
  }
  // RFC 4122 version 4 / variant 1
  bytes[6] = (bytes[6]! & 0x0f) | 0x40
  bytes[8] = (bytes[8]! & 0x3f) | 0x80
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("")
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

export async function poster<T>(url: string, body: unknown, options: CommandOptions = {}): Promise<T> {
  const idempotencyKey = options.idempotencyKey ?? createIdempotencyKey()
  const res = await fetch(`${BASE_URL}${url}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
      ...(options.ifMatch === undefined ? {} : { "If-Match": `\"${options.ifMatch}\"` }),
    },
    body: JSON.stringify(body),
  })
  return readResponse<T>(res)
}

export async function patcher<T>(url: string, body: unknown, options: CommandOptions = {}): Promise<T> {
  const idempotencyKey = options.idempotencyKey ?? createIdempotencyKey()
  const res = await fetch(`${BASE_URL}${url}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
      ...(options.ifMatch === undefined ? {} : { "If-Match": `\"${options.ifMatch}\"` }),
    },
    body: JSON.stringify(body),
  })
  return readResponse<T>(res)
}

export async function deleter<T>(url: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${url}`, { method: "DELETE" })
  return readResponse<T>(res)
}
