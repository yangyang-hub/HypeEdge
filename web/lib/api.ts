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

export function createIdempotencyKey(): string {
  return crypto.randomUUID()
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
